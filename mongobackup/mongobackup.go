// Package mongobackup creates BSON data from the contents of a MongoDB instance.
package mongobackup

import (
	"compress/gzip"
	"fmt"
	"io"
	"mongoIncbackup-1.1/common/archive"
	"mongoIncbackup-1.1/common/auth"
	"mongoIncbackup-1.1/common/bsonutil"
	"mongoIncbackup-1.1/common/db"
	"mongoIncbackup-1.1/common/intents"
	"mongoIncbackup-1.1/common/json"
	"mongoIncbackup-1.1/common/log"
	"mongoIncbackup-1.1/common/options"
	"mongoIncbackup-1.1/common/progress"
	"mongoIncbackup-1.1/common/util"
	"mongoIncbackup-1.1/mgo.v2"
	"mongoIncbackup-1.1/mgo.v2/bson"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	progressBarLength   = 24
	progressBarWaitTime = time.Second * 3
	defaultPermissions  = 0755
)

// MongoBackup is a container for the user-specified options and
// internal state used for running mongobackup.
type MongoBackup struct {
	// basic mongo tool options
	ToolOptions   *options.ToolOptions
	InputOptions  *InputOptions
	OutputOptions *OutputOptions

	// useful internals that we don't directly expose as options
	sessionProvider *db.SessionProvider
	manager         *intents.Manager
	query           bson.M
	oplogCollection string
	oplogStart      bson.MongoTimestamp
	isMongos        bool
	authVersion     int
	archive         *archive.Writer
	progressManager *progress.Manager
	// get counter of bsonfile
	seqCountOfFile int
	// shutdownIntentsNotifier is provided to the multiplexer
	// as well as the signal handler, and allows them to notify
	// the intent backupers that they should shutdown
	shutdownIntentsNotifier *notifier
	// the value of stdout gets initizlied to os.Stdout if it's unset
	stdout       io.Writer
	readPrefMode mgo.Mode
	readPrefTags []bson.D
}

type notifier struct {
	notified chan struct{}
	once     sync.Once
}

func (n *notifier) Notify() { n.once.Do(func() { close(n.notified) }) }

func newNotifier() *notifier { return &notifier{notified: make(chan struct{})} }

// ValidateOptions checks for any incompatible sets of options.
func (backup *MongoBackup) ValidateOptions() error {
	switch {
	case backup.OutputOptions.Out == "-" && backup.ToolOptions.Namespace.Collection == "":
		return fmt.Errorf("can only backup a single collection to stdout")
	case backup.ToolOptions.Namespace.DB == "" && backup.ToolOptions.Namespace.Collection != "":
		return fmt.Errorf("cannot backup a collection without a specified database")
	case backup.InputOptions.Query != "" && backup.ToolOptions.Namespace.Collection == "":
		return fmt.Errorf("cannot backup using a query without a specified collection")
	case backup.InputOptions.QueryFile != "" && backup.ToolOptions.Namespace.Collection == "":
		return fmt.Errorf("cannot backup using a queryFile without a specified collection")
	case backup.InputOptions.Query != "" && backup.InputOptions.QueryFile != "":
		return fmt.Errorf("either query or queryFile can be specified as a query option, not both")
	case backup.InputOptions.Query != "" && backup.InputOptions.TableScan:
		return fmt.Errorf("cannot use --forceTableScan when specifying --query")
	case backup.OutputOptions.BackupDBUsersAndRoles && backup.ToolOptions.Namespace.DB == "":
		return fmt.Errorf("must specify a database when running with backupDbUsersAndRoles")
	case backup.OutputOptions.BackupDBUsersAndRoles && backup.ToolOptions.Namespace.Collection != "":
		return fmt.Errorf("cannot specify a collection when running with backupDbUsersAndRoles")
	case backup.OutputOptions.Oplog && backup.ToolOptions.Namespace.DB != "":
		return fmt.Errorf("--oplog mode only supported on full backups")
	case len(backup.OutputOptions.ExcludedCollections) > 0 && backup.ToolOptions.Namespace.Collection != "":
		return fmt.Errorf("--collection is not allowed when --excludeCollection is specified")
	case len(backup.OutputOptions.ExcludedCollectionPrefixes) > 0 && backup.ToolOptions.Namespace.Collection != "":
		return fmt.Errorf("--collection is not allowed when --excludeCollectionsWithPrefix is specified")
	case len(backup.OutputOptions.ExcludedCollections) > 0 && backup.ToolOptions.Namespace.DB == "":
		return fmt.Errorf("--db is required when --excludeCollection is specified")
	case len(backup.OutputOptions.ExcludedCollectionPrefixes) > 0 && backup.ToolOptions.Namespace.DB == "":
		return fmt.Errorf("--db is required when --excludeCollectionsWithPrefix is specified")
	case backup.OutputOptions.Repair && backup.InputOptions.Query != "":
		return fmt.Errorf("cannot run a query with --repair enabled")
	case backup.OutputOptions.Repair && backup.InputOptions.QueryFile != "":
		return fmt.Errorf("cannot run a queryFile with --repair enabled")
	case backup.OutputOptions.Out != "" && backup.OutputOptions.Archive != "":
		return fmt.Errorf("--out not allowed when --archive is specified")
	case backup.OutputOptions.Out == "-" && backup.OutputOptions.Gzip:
		return fmt.Errorf("compression can't be used when backuping a single collection to standard output")
	case backup.OutputOptions.NumParallelCollections <= 0:
		return fmt.Errorf("numParallelCollections must be positive")
	}
	return nil
}

// Init performs preliminary setup operations for MongoBackup.
func (backup *MongoBackup) Init() error {
	err := backup.ValidateOptions()
	if err != nil {
		return fmt.Errorf("bad option: %v", err)
	}
	if backup.stdout == nil {
		backup.stdout = os.Stdout
	}
	backup.sessionProvider, err = db.NewSessionProvider(*backup.ToolOptions)
	if err != nil {
		return fmt.Errorf("can't create session: %v", err)
	}

	// temporarily allow secondary reads for the isMongos check
	backup.sessionProvider.SetReadPreference(mgo.Nearest)
	backup.isMongos, err = backup.sessionProvider.IsMongos()
	if err != nil {
		return err
	}

	if backup.isMongos && backup.OutputOptions.Oplog {
		return fmt.Errorf("can't use --oplog option when backuping from a mongos")
	}

	var mode mgo.Mode
	if backup.ToolOptions.ReplicaSetName != "" || backup.isMongos {
		mode = mgo.Primary
	} else {
		mode = mgo.Nearest
	}
	var tags bson.D

	if backup.InputOptions.ReadPreference != "" {
		mode, tags, err = db.ParseReadPreference(backup.InputOptions.ReadPreference)
		if err != nil {
			return fmt.Errorf("error parsing --readPreference : %v", err)
		}
		if len(tags) > 0 {
			backup.sessionProvider.SetTags(tags)
		}
	}

	// warn if we are trying to backup from a secondary in a sharded cluster
	if backup.isMongos && mode != mgo.Primary {
		log.Logf(log.Always, db.WarningNonPrimaryMongosConnection)
	}

	backup.sessionProvider.SetReadPreference(mode)
	backup.sessionProvider.SetTags(tags)
	backup.sessionProvider.SetFlags(db.DisableSocketTimeout)

	// return a helpful error message for mongos --repair
	if backup.OutputOptions.Repair && backup.isMongos {
		return fmt.Errorf("--repair flag cannot be used on a mongos")
	}

	backup.manager = intents.NewIntentManager()
	backup.progressManager = progress.NewProgressBarManager(log.Writer(0), progressBarWaitTime)
	return nil
}

// Backup handles some final options checking and executes MongoBackup.
func (backup *MongoBackup) Backup() (err error) {

	backup.shutdownIntentsNotifier = newNotifier()

	if backup.InputOptions.HasQuery() {
		// parse JSON then convert extended JSON values
		var asJSON interface{}
		content, err := backup.InputOptions.GetQuery()
		if err != nil {
			return err
		}
		err = json.Unmarshal(content, &asJSON)
		if err != nil {
			return fmt.Errorf("error parsing query as json: %v", err)
		}
		convertedJSON, err := bsonutil.ConvertJSONValueToBSON(asJSON)
		if err != nil {
			return fmt.Errorf("error converting query to bson: %v", err)
		}
		asMap, ok := convertedJSON.(map[string]interface{})
		if !ok {
			// unlikely to be reached
			return fmt.Errorf("query is not in proper format")
		}
		backup.query = bson.M(asMap)
	}

	if backup.OutputOptions.BackupDBUsersAndRoles {
		// first make sure this is possible with the connected database
		backup.authVersion, err = auth.GetAuthVersion(backup.sessionProvider)
		if err == nil {
			err = auth.VerifySystemAuthVersion(backup.sessionProvider)
		}
		if err != nil {
			return fmt.Errorf("error getting auth schema version for backupDbUsersAndRoles: %v", err)
		}
		log.Logf(log.DebugLow, "using auth schema version %v", backup.authVersion)
		if backup.authVersion < 3 {
			return fmt.Errorf("backing up users and roles is only supported for "+
				"deployments with auth schema versions >= 3, found: %v", backup.authVersion)
		}
	}

	if backup.OutputOptions.Archive != "" {
		//getArchiveOut gives us a WriteCloser to which we should write the archive
		var archiveOut io.WriteCloser
		archiveOut, err = backup.getArchiveOut()
		if err != nil {
			return err
		}
		backup.archive = &archive.Writer{
			// The archive.Writer needs its own copy of archiveOut because things
			// like the prelude are not written by the multiplexer.
			Out: archiveOut,
			Mux: archive.NewMultiplexer(archiveOut, backup.shutdownIntentsNotifier),
		}
		go backup.archive.Mux.Run()
		defer func() {
			// The Mux runs until its Control is closed
			close(backup.archive.Mux.Control)
			muxErr := <-backup.archive.Mux.Completed
			archiveOut.Close()
			if muxErr != nil {
				if err != nil {
					err = fmt.Errorf("archive writer: %v / %v", err, muxErr)
				} else {
					err = fmt.Errorf("archive writer: %v", muxErr)
				}
				log.Logf(log.DebugLow, "%v", err)
			} else {
				log.Logf(log.DebugLow, "mux completed successfully")
			}
		}()
	}

	// switch on what kind of execution to do
	switch {
	case backup.ToolOptions.DB == "" && backup.ToolOptions.Collection == "":
		err = backup.CreateAllIntents()
	case backup.ToolOptions.DB != "" && backup.ToolOptions.Collection == "":
		err = backup.CreateIntentsForDatabase(backup.ToolOptions.DB)
	case backup.ToolOptions.DB != "" && backup.ToolOptions.Collection != "":
		err = backup.CreateCollectionIntent(backup.ToolOptions.DB, backup.ToolOptions.Collection)
	}
	if err != nil {
		return err
	}

	if backup.OutputOptions.Oplog {
		err = backup.CreateOplogIntents()
		if err != nil {
			return err
		}
	}

	if backup.OutputOptions.BackupDBUsersAndRoles && backup.ToolOptions.DB != "admin" {
		err = backup.CreateUsersRolesVersionIntentsForDB(backup.ToolOptions.DB)
		if err != nil {
			return err
		}
	}

	// verify we can use repair cursors
	if backup.OutputOptions.Repair {
		log.Log(log.DebugLow, "verifying that the connected server supports repairCursor")
		if backup.isMongos {
			return fmt.Errorf("cannot use --repair on mongos")
		}
		exampleIntent := backup.manager.Peek()
		if exampleIntent != nil {
			supported, err := backup.sessionProvider.SupportsRepairCursor(
				exampleIntent.DB, exampleIntent.C)
			if !supported {
				return err // no extra context needed
			}
		}
	}

	// IO Phase I
	// metadata, users, roles, and versions

	// TODO, either remove this debug or improve the language
	log.Logf(log.DebugHigh, "backup phase I: metadata, indexes, users, roles, version")

	err = backup.BackupMetadata()
	if err != nil {
		return fmt.Errorf("error backuping metadata: %v", err)
	}

	if backup.OutputOptions.Archive != "" {
		session, err := backup.sessionProvider.GetSession()
		if err != nil {
			return err
		}
		buildInfo, err := session.BuildInfo()
		var serverVersion string
		if err != nil {
			log.Logf(log.Always, "warning, couldn't get version information from server: %v", err)
			serverVersion = "unknown"
		} else {
			serverVersion = buildInfo.Version
		}
		backup.archive.Prelude, err = archive.NewPrelude(backup.manager, backup.OutputOptions.NumParallelCollections, serverVersion)
		if err != nil {
			return fmt.Errorf("creating archive prelude: %v", err)
		}
		err = backup.archive.Prelude.Write(backup.archive.Out)
		if err != nil {
			return fmt.Errorf("error writing metadata into archive: %v", err)
		}
	}

	err = backup.BackupSystemIndexes()
	if err != nil {
		return fmt.Errorf("error backuping system indexes: %v", err)
	}

	if backup.ToolOptions.DB == "admin" || backup.ToolOptions.DB == "" {
		err = backup.BackupUsersAndRoles()
		if err != nil {
			return fmt.Errorf("error backuping users and roles: %v", err)
		}
	}
	if backup.OutputOptions.BackupDBUsersAndRoles {
		log.Logf(log.Always, "backuping users and roles for %v", backup.ToolOptions.DB)
		if backup.ToolOptions.DB == "admin" {
			log.Logf(log.Always, "skipping users/roles backup, already backuped admin database")
		} else {
			err = backup.BackupUsersAndRolesForDB(backup.ToolOptions.DB)
			if err != nil {
				return fmt.Errorf("error backuping users and roles for db: %v", err)
			}
		}
	}

	// If oplog capturing is enabled, we first check the most recent
	// oplog entry and save its timestamp, this will let us later
	// copy all oplog entries that occurred while backuping, creating
	// what is effectively a point-in-time snapshot.
	if backup.OutputOptions.Oplog {
		err := backup.determineOplogCollectionName()
		if err != nil {
			return fmt.Errorf("error finding oplog: %v", err)
		}
		log.Logf(log.Info, "getting most recent oplog timestamp")
		backup.oplogStart, err = backup.getOplogStartTime()
		if err != nil {
			return fmt.Errorf("error getting oplog start: %v", err)
		}
	}

	// IO Phase II
	// regular collections

	// TODO, either remove this debug or improve the language
	log.Logf(log.DebugHigh, "backup phase II: regular collections")

	// kick off the progress bar manager and begin backuping intents
	backup.progressManager.Start()
	defer backup.progressManager.Stop()

	go backup.handleSignals()

	if err := backup.BackupIntents(); err != nil {
		return err
	}

	// IO Phase III
	// oplog

	// TODO, either remove this debug or improve the language
	log.Logf(log.DebugLow, "backup phase III: the oplog")

	// If we are capturing the oplog, we backup all oplog entries that occurred
	// while backuping the database. Before and after backuping the oplog,
	// we check to see if the oplog has rolled over (i.e. the most recent entry when
	// we started still exist, so we know we haven't lost data)
	if backup.OutputOptions.Oplog {
		log.Logf(log.DebugLow, "checking if oplog entry %v still exists", backup.oplogStart)
		exists, err := backup.checkOplogTimestampExists(backup.oplogStart)
		if !exists {
			return fmt.Errorf(
				"oplog overflow: mongobackup was unable to capture all new oplog entries during execution")
		}
		if err != nil {
			return fmt.Errorf("unable to check oplog for overflow: %v", err)
		}
		log.Logf(log.DebugHigh, "oplog entry %v still exists", backup.oplogStart)

		log.Logf(log.Always, "writing captured oplog to %v", backup.manager.Oplog().Location)
		err = backup.BackupOplogAfterTimestamp(backup.oplogStart)
		if err != nil {
			return fmt.Errorf("error backuping oplog: %v", err)
		}

		// check the oplog for a rollover one last time, to avoid a race condition
		// wherein the oplog rolls over in the time after our first check, but before
		// we copy it.
		log.Logf(log.DebugLow, "checking again if oplog entry %v still exists", backup.oplogStart)
		exists, err = backup.checkOplogTimestampExists(backup.oplogStart)
		if !exists {
			return fmt.Errorf(
				"oplog overflow: mongobackup was unable to capture all new oplog entries during execution")
		}
		if err != nil {
			return fmt.Errorf("unable to check oplog for overflow: %v", err)
		}
		log.Logf(log.DebugHigh, "oplog entry %v still exists", backup.oplogStart)
	}

	log.Logf(log.DebugLow, "finishing backup")

	return err
}

// BackupIntents iterates through the previously-created intents and
// backups all of the found collections.
func (backup *MongoBackup) BackupIntents() error {
	resultChan := make(chan error)

	jobs := backup.OutputOptions.NumParallelCollections
	if numIntents := len(backup.manager.Intents()); jobs > numIntents {
		jobs = numIntents
	}

	if jobs > 1 {
		backup.manager.Finalize(intents.LongestTaskFirst)
	} else {
		backup.manager.Finalize(intents.Legacy)
	}

	log.Logf(log.Info, "backuping up to %v collections in parallel", jobs)

	// start a goroutine for each job thread
	for i := 0; i < jobs; i++ {
		go func(id int) {
			log.Logf(log.DebugHigh, "starting backup routine with id=%v", id)
			for {
				intent := backup.manager.Pop()
				if intent == nil {
					log.Logf(log.DebugHigh, "ending backup routine with id=%v, no more work to do", id)
					resultChan <- nil
					return
				}
				err := backup.BackupIntent(intent)
				if err != nil {
					resultChan <- err
					return
				}
				backup.manager.Finish(intent)
			}
		}(i)
	}

	// wait until all goroutines are done or one of them errors out
	for i := 0; i < jobs; i++ {
		if err := <-resultChan; err != nil {
			return err
		}
	}

	return nil
}

// BackupIntent backups the specified database's collection.
func (backup *MongoBackup) BackupIntent(intent *intents.Intent) error {
	session, err := backup.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()
	// in mgo, setting prefetch = 1.0 causes the driver to make requests for
	// more results as soon as results are returned. This effectively
	// duplicates the behavior of an exhaust cursor.
	session.SetPrefetch(1.0)

	err = intent.BSONFile.Open()
	if err != nil {
		return err
	}
	defer intent.BSONFile.Close()

	var findQuery *mgo.Query
	switch {
	case len(backup.query) > 0:
		findQuery = session.DB(intent.DB).C(intent.C).Find(backup.query)
	case backup.InputOptions.TableScan:
		// ---forceTablesScan runs the query without snapshot enabled
		findQuery = session.DB(intent.DB).C(intent.C).Find(nil)
	default:
		findQuery = session.DB(intent.DB).C(intent.C).Find(nil).Snapshot()

	}

	var backupCount int64

	if backup.OutputOptions.Out == "-" {
		log.Logf(log.Always, "writing %v to stdout", intent.Namespace())
		backupCount, err = backup.backupQueryToWriter(findQuery, intent)
		if err == nil {
			// on success, print the document count
			log.Logf(log.Always, "backuped %v %v", backupCount, docPlural(backupCount))
		}
		return err
	}

	// set where the intent will be written to
	if backup.OutputOptions.Archive != "" {
		if backup.OutputOptions.Archive == "-" {
			intent.Location = "archive on stdout"
		} else {
			intent.Location = fmt.Sprintf("archive '%v'", backup.OutputOptions.Archive)
		}
	}

	if !backup.OutputOptions.Repair {
		log.Logf(log.Always, "writing %v to %v", intent.Namespace(), intent.Location)
		if backupCount, err = backup.backupQueryToWriter(findQuery, intent); err != nil {
			return err
		}
	} else {
		// handle repairs as a special case, since we cannot count them
		log.Logf(log.Always, "writing repair of %v to %v", intent.Namespace(), intent.Location)
		repairIter := session.DB(intent.DB).C(intent.C).Repair()
		repairCounter := progress.NewCounter(1) // this counter is ignored
		if err := backup.backupIterToWriter(repairIter, intent.BSONFile, repairCounter); err != nil {
			return fmt.Errorf("repair error: %v", err)
		}
		_, repairCount := repairCounter.Progress()
		log.Logf(log.Always, "\trepair cursor found %v %v in %v",
			repairCount, docPlural(repairCount), intent.Namespace())
	}

	log.Logf(log.Always, "done backuping %v (%v %v)", intent.Namespace(), backupCount, docPlural(backupCount))
	return nil
}

/*

1....

*/
func (backup *MongoBackup) backupQueryToWriterM(
	query *mgo.Query, intent *intents.Intent) (int64, error) {
	var total int
	var err error

	if len(backup.query) == 0 {
		total, err = query.Count()
		if err != nil {
			return int64(0), fmt.Errorf("error reading from db: %v", err)
		}
		log.Logf(log.DebugLow, "counted %v %v in %v", total, docPlural(int64(total)), intent.Namespace())
	} else {
		log.Logf(log.DebugLow, "not counting query on %v", intent.Namespace())
	}

	backupProgressor := progress.NewCounter(int64(total))
	bar := &progress.Bar{
		Name:      intent.Namespace(),
		Watching:  backupProgressor,
		BarLength: progressBarLength,
	}
	backup.progressManager.Attach(bar)
	defer backup.progressManager.Detach(bar)

	// modify here queryT
	// queryTial := query.Sort("$natural").Tail(-1)
	// queryTial := query
	// err = backup.backupIterToWriter(query.Iter(), intent.BSONFile, backupProgressor)
	err = backup.backupIterToWriterM(query, intent.BSONFile, backupProgressor)
	// modify start

	// modify end
	_, backupCount := backupProgressor.Progress()

	return backupCount, err
}

// modify
func (backup *MongoBackup) backupIterToWriterM(
	query *mgo.Query, writer io.Writer, progressCount progress.Updateable) error {
	// iter *mgo.Iter, writer io.Writer, progressCount progress.Updateable) error {
	var termErr error
	var iter *mgo.Iter
	// .Sort("$natural").Tail(-1)
	// We run the result iteration in its own goroutine,
	// this allows disk i/o to not block reads from the db,
	// which gives a slight speedup on benchmarks
	// iter = query.Sort("$natural").Tail(-1)
	iter = query.Sort("$natural").Tail(-1)
	buffChan := make(chan []byte)
	go func() {
		for {
			select {
			case <-backup.shutdownIntentsNotifier.notified:
				log.Logf(log.DebugHigh, "terminating writes")
				termErr = util.ErrTerminated
				close(buffChan)
				return
			default:
				raw := &bson.Raw{}
				next := iter.Next(raw)
				if !next {
					// we check the iterator for errors below
					close(buffChan)
					return
				}
				nextCopy := make([]byte, len(raw.Data))
				copy(nextCopy, raw.Data)
				buffChan <- nextCopy
			}
		}
	}()

	// while there are still results in the database,
	// grab results from the goroutine and write them to filesystem
	for {
		buff, alive := <-buffChan
		if !alive {
			if iter.Err() != nil {
				fmt.Printf("data.....: %v", buff)
				return fmt.Errorf("error reading collection: %v", iter.Err())
			}
			break
		}
		_, err := writer.Write(buff)
		if err != nil {
			return fmt.Errorf("error writing to file: %v", err)
		}
		progressCount.Inc(1)
	}
	return termErr
}

/*

2....

*/
// backupQueryToWriter takes an mgo Query, its intent, and a writer, performs the query,
// and writes the raw bson results to the writer. Returns a final count of documents
// backuped, and any errors that occured.
func (backup *MongoBackup) backupQueryToWriter(
	query *mgo.Query, intent *intents.Intent) (int64, error) {
	var total int
	var err error

	if len(backup.query) == 0 {
		total, err = query.Count()
		if err != nil {
			return int64(0), fmt.Errorf("error reading from db: %v", err)
		}
		log.Logf(log.DebugLow, "counted %v %v in %v", total, docPlural(int64(total)), intent.Namespace())
	} else {
		log.Logf(log.DebugLow, "not counting query on %v", intent.Namespace())
	}

	backupProgressor := progress.NewCounter(int64(total))
	bar := &progress.Bar{
		Name:      intent.Namespace(),
		Watching:  backupProgressor,
		BarLength: progressBarLength,
	}
	backup.progressManager.Attach(bar)
	defer backup.progressManager.Detach(bar)

	// modify here queryT
	// queryTial := query.Sort("$natural").Tail(-1)
	// err = backup.backupIterToWriter(query.Iter(), intent.BSONFile, backupProgressor)
	err = backup.backupIterToWriter(query.Iter(), intent.BSONFile, backupProgressor)
	// modify start

	// modify end
	_, backupCount := backupProgressor.Progress()

	return backupCount, err
}

// backupIterToWriter takes an mgo iterator, a writer, and a pointer to
// a counter, and backups the iterator's contents to the writer.
func (backup *MongoBackup) backupIterToWriter(
	iter *mgo.Iter, writer io.Writer, progressCount progress.Updateable) error {
	var termErr error

	// We run the result iteration in its own goroutine,
	// this allows disk i/o to not block reads from the db,
	// which gives a slight speedup on benchmarks
	buffChan := make(chan []byte)
	go func() {
		for {
			select {
			case <-backup.shutdownIntentsNotifier.notified:
				log.Logf(log.DebugHigh, "terminating writes")
				termErr = util.ErrTerminated
				close(buffChan)
				return
			default:
				raw := &bson.Raw{}
				next := iter.Next(raw)
				if !next {
					// we check the iterator for errors below
					close(buffChan)
					return
				}
				nextCopy := make([]byte, len(raw.Data))
				copy(nextCopy, raw.Data)
				buffChan <- nextCopy
			}
		}
	}()

	// while there are still results in the database,
	// grab results from the goroutine and write them to filesystem
	for {
		buff, alive := <-buffChan
		if !alive {
			if iter.Err() != nil {
				return fmt.Errorf("error reading collection: %v", iter.Err())
			}
			break
		}
		_, err := writer.Write(buff)
		if err != nil {
			return fmt.Errorf("error writing to file: %v", err)
		}
		progressCount.Inc(1)
	}
	return termErr
}

// BackupUsersAndRolesForDB queries and backups the users and roles tied to the given
// database. Only works with an authentication schema version >= 3.
func (backup *MongoBackup) BackupUsersAndRolesForDB(db string) error {
	session, err := backup.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()

	dbQuery := bson.M{"db": db}
	usersQuery := session.DB("admin").C("system.users").Find(dbQuery)
	intent := backup.manager.Users()
	err = intent.BSONFile.Open()
	if err != nil {
		return fmt.Errorf("error opening output stream for backuping Users: %v", err)
	}
	defer intent.BSONFile.Close()
	_, err = backup.backupQueryToWriter(usersQuery, intent)
	if err != nil {
		return fmt.Errorf("error backuping db users: %v", err)
	}

	rolesQuery := session.DB("admin").C("system.roles").Find(dbQuery)
	intent = backup.manager.Roles()
	err = intent.BSONFile.Open()
	if err != nil {
		return fmt.Errorf("error opening output stream for backuping Roles: %v", err)
	}
	defer intent.BSONFile.Close()
	_, err = backup.backupQueryToWriter(rolesQuery, intent)
	if err != nil {
		return fmt.Errorf("error backuping db roles: %v", err)
	}

	versionQuery := session.DB("admin").C("system.version").Find(nil)
	intent = backup.manager.AuthVersion()
	err = intent.BSONFile.Open()
	if err != nil {
		return fmt.Errorf("error opening output stream for backuping AuthVersion: %v", err)
	}
	defer intent.BSONFile.Close()
	_, err = backup.backupQueryToWriter(versionQuery, intent)
	if err != nil {
		return fmt.Errorf("error backuping db auth version: %v", err)
	}

	return nil
}

// BackupUsersAndRoles backups all of the users and roles and versions
// TODO: This and BackupUsersAndRolesForDB should be merged, correctly
func (backup *MongoBackup) BackupUsersAndRoles() error {
	var err error
	if backup.manager.Users() != nil {
		err = backup.BackupIntent(backup.manager.Users())
		if err != nil {
			return err
		}
	}
	if backup.manager.Roles() != nil {
		err = backup.BackupIntent(backup.manager.Roles())
		if err != nil {
			return err
		}
	}
	if backup.manager.AuthVersion() != nil {
		err = backup.BackupIntent(backup.manager.AuthVersion())
		if err != nil {
			return err
		}
	}

	return nil
}

// BackupSystemIndexes backups all of the system.indexes
func (backup *MongoBackup) BackupSystemIndexes() error {
	for _, dbName := range backup.manager.SystemIndexDBs() {
		err := backup.BackupIntent(backup.manager.SystemIndexes(dbName))
		if err != nil {
			return err
		}
	}
	return nil
}

// BackupMetadata backups the metadata for each intent in the manager
// that has metadata
func (backup *MongoBackup) BackupMetadata() error {
	allIntents := backup.manager.Intents()
	for _, intent := range allIntents {
		if intent.MetadataFile != nil {
			err := backup.backupMetadata(intent)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// nopCloseWriter implements io.WriteCloser. It wraps up a io.Writer, and adds a no-op Close
type nopCloseWriter struct {
	io.Writer
}

// Close does nothing on nopCloseWriters
func (*nopCloseWriter) Close() error {
	return nil
}

// wrappedWriteCloser implements io.WriteCloser. It wraps up two WriteClosers. The Write method
// of the io.WriteCloser is implemented by the embedded io.WriteCloser
type wrappedWriteCloser struct {
	io.WriteCloser
	inner io.WriteCloser
}

// Close is part of the io.WriteCloser interface. Close closes both the embedded io.WriteCloser as
// well as the inner io.WriteCloser
func (wwc *wrappedWriteCloser) Close() error {
	err := wwc.WriteCloser.Close()
	if err != nil {
		return err
	}
	return wwc.inner.Close()
}

func (backup *MongoBackup) getArchiveOut() (out io.WriteCloser, err error) {
	if backup.OutputOptions.Archive == "-" {
		out = &nopCloseWriter{backup.stdout}
	} else {
		targetStat, err := os.Stat(backup.OutputOptions.Archive)
		if err == nil && targetStat.IsDir() {
			defaultArchiveFilePath :=
				filepath.Join(backup.OutputOptions.Archive, "archive")
			if backup.OutputOptions.Gzip {
				defaultArchiveFilePath = defaultArchiveFilePath + ".gz"
			}
			out, err = os.Create(defaultArchiveFilePath)
			if err != nil {
				return nil, err
			}
		} else {
			out, err = os.Create(backup.OutputOptions.Archive)
			if err != nil {
				return nil, err
			}
		}
	}
	if backup.OutputOptions.Gzip {
		return &wrappedWriteCloser{
			WriteCloser: gzip.NewWriter(out),
			inner:       out,
		}, nil
	}
	return out, nil
}

// handleSignals listens for either SIGTERM, SIGINT or the
// SIGHUP signal. It ends restore reads for all goroutines
// as soon as any of those signals is received.
func (backup *MongoBackup) handleSignals() {
	log.Log(log.DebugLow, "will listen for SIGTERM, SIGINT and SIGHUP")
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGPIPE)
	// first signal cleanly terminates backup writes
	<-sigChan
	log.Log(log.Always, "signal received; shutting down mongobackup")
	//
	backup.shutdownIntentsNotifier.Notify()
	// second signal exits immediately
	<-sigChan
	log.Log(log.Always, "second signal received; forcefully terminating mongobackup")
	os.Exit(util.ExitKill)
}

// docPlural returns "document" or "documents" depending on the
// count of documents passed in.
func docPlural(count int64) string {
	return util.Pluralize(int(count), "document", "documents")
}
