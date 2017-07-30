/*
===--- mongorsync.go ---------------------------------------------------===//
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See https://github.com/YuanDdQiao/dbtool/blob/master/README.md for more details.
===----------------------------------------------------------------------===
*/
// Package mongorsync writes BSON data to a MongoDB instance.
package mongorsync

import (
	// "compress/gzip"
	"fmt"
	"io"
	// "io/ioutil"
	// "mongorsync-1.1/common/archive"
	"mongorsync-1.1/common/auth"
	"mongorsync-1.1/common/db"
	"mongorsync-1.1/common/intents"
	"mongorsync-1.1/common/log"
	"mongorsync-1.1/common/options"
	"mongorsync-1.1/common/progress"
	"mongorsync-1.1/common/util"
	"mongorsync-1.1/mgo.v2"
	"mongorsync-1.1/mgo.v2/bson"
	"os"
	"os/signal"
	// "path/filepath"
	"sync"
	"syscall"

	"mongorsync-1.1/common/bsonutil"
	"mongorsync-1.1/common/json"
)

// MongoRsync is a container for the user-specified options and
// internal state used for running mongorsync.
type MongoRsync struct {
	ToolOptions   *options.ToolOptions
	InputOptions  *InputOptions
	OutputOptions *OutputOptions

	FSessionProvider *db.SessionProvider
	SessionProvider  *db.SessionProvider

	//source connect_url
	// TargetDirectory string

	tempUsersCol string
	tempRolesCol string

	// other internal state
	manager              *intents.Manager
	managerOplog         *intents.Manager
	managerUserRoleOther *intents.Manager
	safety               *mgo.Safe

	// param timestamp

	oplogStime bson.MongoTimestamp
	oplogEtime bson.MongoTimestamp

	progressManager *progress.Manager

	objCheck           bool
	oplogLastOptimeFor bson.MongoTimestamp
	oplogStart         bson.MongoTimestamp

	isMongos         bool
	fisMongos        bool
	useWriteCommands bool
	// authVersions     authVersionPair
	authVersions  int
	fauthVersions int

	// a map of database names to a list of collection names
	knownCollections      map[string][]string
	knownCollectionsMutex sync.Mutex

	// indexes belonging to dbs and collections
	dbCollectionIndexes     map[string]collectionIndexes
	shutdownIntentsNotifier *notifier
	oplogCollection         string

	// setp
	query bson.M
	// archive *archive.Reader

	// channel on which to notify if/when a termination signal is received
	termChan chan struct{}

	// for testing. If set, this value will be used instead of os.Stdin
	stdin io.Reader
}

type notifier struct {
	notified chan struct{}
	once     sync.Once
}

func (n *notifier) Notify() { n.once.Do(func() { close(n.notified) }) }

func newNotifier() *notifier { return &notifier{notified: make(chan struct{})} }

type collectionIndexes map[string][]IndexDocument

// ParseAndValidateOptions returns a non-nil error if user-supplied options are invalid.
func (rsync *MongoRsync) ParseAndValidateOptions() error {
	// Can't use option pkg defaults for --objcheck because it's two separate flags,
	// and we need to be able to see if they're both being used. We default to
	// true here and then see if noobjcheck is enabled.
	log.Log(log.DebugHigh, "checking options")
	switch {
	case rsync.ToolOptions.FNamespace.FDB == "" && rsync.ToolOptions.FNamespace.FCollection != "":
		return fmt.Errorf("cannot synchronous a collection without a specified database")
	case rsync.ToolOptions.Connection.Host == "" && rsync.ToolOptions.Connection.FHost != "":
		return fmt.Errorf("cannot synchronous a collection without a specified current target host")
	case rsync.ToolOptions.Connection.FHost == "" && rsync.ToolOptions.Connection.Host != "":
		return fmt.Errorf("cannot synchronous a collection without a specified current source host")
	case rsync.InputOptions.Query != "" && rsync.ToolOptions.FNamespace.FCollection == "":
		return fmt.Errorf("cannot synchronous data using a query without a specified collection")
	case rsync.InputOptions.QueryFile != "" && rsync.ToolOptions.FNamespace.FCollection == "":
		return fmt.Errorf("cannot synchronous data using a queryFile without a specified collection")
	case rsync.InputOptions.Query != "" && rsync.InputOptions.QueryFile != "":
		return fmt.Errorf("either query or queryFile can be specified as a query option, not both")
	case rsync.InputOptions.Oplog && len(rsync.query) > 0:
		return fmt.Errorf("either query or queryFile can be specified as a Oplog option, not both")
	case rsync.InputOptions.Query != "" && rsync.InputOptions.TableScan:
		return fmt.Errorf("cannot use --forceTableScan when specifying --query")
	// case rsync.InputOptions.Oplog != true && rsync.InputOptions.Oplog != false:
	// 	return fmt.Errorf("--oplog mode not is specified value [true or false]")
	case len(rsync.InputOptions.ExcludedCollections) > 0 && rsync.ToolOptions.FNamespace.FCollection != "":
		return fmt.Errorf("--collection is not allowed when --excludeCollection is specified")
	case len(rsync.InputOptions.ExcludedCollectionPrefixes) > 0 && rsync.ToolOptions.FNamespace.FCollection != "":
		return fmt.Errorf("--collection is not allowed when --excludeCollectionsWithPrefix is specified")
	case len(rsync.InputOptions.ExcludedCollections) > 0 && rsync.ToolOptions.FNamespace.FDB == "":
		return fmt.Errorf("--db is required when --excludeCollection is specified")
	case len(rsync.InputOptions.ExcludedCollectionPrefixes) > 0 && rsync.ToolOptions.FNamespace.FDB == "":
		return fmt.Errorf("--db is required when --excludeCollectionsWithPrefix is specified")
	case rsync.OutputOptions.NumParallelCollections <= 0:
		return fmt.Errorf("numParallelCollections must be positive")
	}

	if _, err := ParseTimestampFlag(rsync.InputOptions.OEt); err != nil && len(rsync.InputOptions.OEt) != 0 {
		return err
	}

	if _, err := ParseTimestampFlag(rsync.InputOptions.OSt); err != nil && len(rsync.InputOptions.OSt) != 0 {
		return err
	}

	if rsync.ToolOptions.FDB == "" && rsync.ToolOptions.FCollection != "" {
		return fmt.Errorf("cannot synchronous a collection without a specified database")
	}

	if rsync.ToolOptions.FDB != "" {
		if err := util.ValidateDBName(rsync.ToolOptions.FDB); err != nil {
			return fmt.Errorf("invalid db name: %v", err)
		}
	}
	if rsync.ToolOptions.FCollection != "" {
		if err := util.ValidateCollectionGrammar(rsync.ToolOptions.FCollection); err != nil {
			return fmt.Errorf("invalid collection name: %v", err)
		}
	}
	// pass RsyncDBUsersAndRoles
	if rsync.InputOptions.RsyncDBUsersAndRoles && rsync.ToolOptions.FDB == "" {
		return fmt.Errorf("cannot use --rsyncDbUsersAndRoles without a specified database")
	}
	if rsync.InputOptions.RsyncDBUsersAndRoles && rsync.ToolOptions.FDB == "admin" {
		return fmt.Errorf("cannot use --rsyncDbUsersAndRoles with the admin database")
	}

	var err error
	rsync.isMongos, err = rsync.SessionProvider.IsMongos()
	if err != nil {
		return err
	}
	if rsync.isMongos {
		log.Log(log.DebugLow, "rsyncing to a sharded system")
	}

	/*	if rsync.InputOptions.OplogLimit != "" {
					if !rsync.InputOptions.OplogReplay {
						return fmt.Errorf("cannot use --oplogLimit without --oplogReplay enabled")
					}

			rsync.oplogLimit, err = ParseTimestampFlag(rsync.InputOptions.OplogLimit)
			if err != nil {
				return fmt.Errorf("error parsing timestamp argument to --oplogLimit: %v", err)
			}
		}
	*/
	// check if we are using a replica set and fall back to w=1 if we aren't (for <= 2.4)
	nodeType, err := rsync.SessionProvider.GetNodeType()
	if err != nil {
		return fmt.Errorf("error determining type of connected node: %v", err)
	}

	log.Logf(log.DebugLow, "connected to node type: %v", nodeType)
	rsync.safety, err = db.BuildWriteConcern(rsync.OutputOptions.WriteConcern, nodeType)
	if err != nil {
		return fmt.Errorf("error parsing write concern: %v", err)
	}

	// handle the hidden auth collection flags
	if rsync.ToolOptions.HiddenOptions.TempUsersColl == nil {
		rsync.tempUsersCol = "tempusers"
	} else {
		rsync.tempUsersCol = *rsync.ToolOptions.HiddenOptions.TempUsersColl
	}
	if rsync.ToolOptions.HiddenOptions.TempRolesColl == nil {
		rsync.tempRolesCol = "temproles"
	} else {
		rsync.tempRolesCol = *rsync.ToolOptions.HiddenOptions.TempRolesColl
	}

	if rsync.OutputOptions.NumInsertionWorkers < 0 {
		return fmt.Errorf(
			"cannot specify a negative number of insertion workers per collection")
	}

	// a single dash signals reading from source from
	/*if rsync.TargetDirectory == "-" {
		if rsync.InputOptions.Archive != "" {
			return fmt.Errorf(
				"cannot rsync from \"-\" when --archive is specified")
		}
		if rsync.ToolOptions.Collection == "" {
			return fmt.Errorf("cannot rsync from stdin without a specified collection")
		}
	}
	if rsync.stdin == nil {
		rsync.stdin = os.Stdin
	}
	*/
	rsync.FSessionProvider, err = db.FNewSessionProvider(*rsync.ToolOptions)
	if err != nil {
		return fmt.Errorf("can't create source session: %v", err)
	}

	// temporarily allow secondary reads for the isMongos check
	rsync.FSessionProvider.SetReadPreference(mgo.Nearest)
	rsync.fisMongos, err = rsync.FSessionProvider.IsMongos()
	if err != nil {
		return err
	}

	if rsync.fisMongos && rsync.InputOptions.Oplog {
		return fmt.Errorf("can't use --oplog option when rsyncing from a mongos")
	}

	var mode mgo.Mode
	if rsync.ToolOptions.FReplicaSetName != "" || rsync.fisMongos {
		mode = mgo.Primary
	} else {
		mode = mgo.Nearest
	}
	var tags bson.D

	if rsync.InputOptions.ReadPreference != "" {
		mode, tags, err = db.ParseReadPreference(rsync.InputOptions.ReadPreference)
		if err != nil {
			return fmt.Errorf("error parsing --readPreference : %v", err)
		}
		if len(tags) > 0 {
			rsync.FSessionProvider.SetTags(tags)
		}
	}

	// warn if we are trying to rsync from a secondary in a sharded cluster
	if rsync.isMongos && mode != mgo.Primary {
		log.Logf(log.Always, db.WarningNonPrimaryMongosConnection)
	}

	rsync.FSessionProvider.SetReadPreference(mode)
	rsync.FSessionProvider.SetTags(tags)
	rsync.FSessionProvider.SetFlags(db.DisableSocketTimeout)

	return nil
}

// Rsync runs the mongorsync program.
func (rsync *MongoRsync) Rsync() error {
	rsync.shutdownIntentsNotifier = newNotifier()
	// from restore file replaced by db session
	// var target archive.DirLike
	err := rsync.ParseAndValidateOptions()
	if err != nil {
		log.Logf(log.DebugLow, "got error from options parsing: %v", err)
		return err
	}

	if rsync.InputOptions.HasQuery() {
		// parse JSON then convert extended JSON values
		var asJSON interface{}
		content, err := rsync.InputOptions.GetQuery()
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
		rsync.query = bson.M(asMap)
	}

	if rsync.InputOptions.RsyncDBUsersAndRoles {
		// first make sure this is possible with the connected database
		rsync.fauthVersions, err = auth.GetAuthVersion(rsync.FSessionProvider)
		if err == nil {
			err = auth.VerifySystemAuthVersion(rsync.FSessionProvider)
		}
		if err != nil {
			return fmt.Errorf("error getting auth schema version for rsyncDbUsersAndRoles: %v", err)
		}
		log.Logf(log.DebugLow, "using source server auth schema version %v", rsync.fauthVersions)
		if rsync.fauthVersions < 3 {
			return fmt.Errorf("backing up users and roles is only supported for "+
				"deployments with auth schema versions >= 3, found: %v", rsync.fauthVersions)
		}
		// target
		// If rsyncing users and roles, make sure we validate auth versions
		rsync.authVersions, err = auth.GetAuthVersion(rsync.SessionProvider)
		if err == nil {
			err = auth.VerifySystemAuthVersion(rsync.SessionProvider)
		}
		if err != nil {
			return fmt.Errorf("error getting auth schema version for rsyncDbUsersAndRoles: %v", err)
		}
		log.Logf(log.DebugLow, "using target server auth schema version %v", rsync.authVersions)
		if rsync.authVersions < 3 {
			return fmt.Errorf("backing up users and roles is only supported for "+
				"deployments with auth schema versions >= 3, found: %v", rsync.authVersions)
		}
	}

	// Build up all intents to be rsyncd
	rsync.manager = intents.NewIntentManager()
	rsync.managerOplog = intents.NewIntentManager()
	rsync.managerUserRoleOther = intents.NewIntentManager()

	// start prepare
	/*get the timestamp from the source server session*/
	// RIGHT HERE ,WE ARE SHOULD BE HERE ,AND GET OPLOGSTART TIMESTAMP
	// rsync.oplogStart=
	switch {
	case rsync.ToolOptions.FDB == "" && rsync.ToolOptions.FCollection == "":
		err = rsync.CreateAllIntents()
	case rsync.ToolOptions.FDB != "" && rsync.ToolOptions.FCollection == "":
		err = rsync.CreateIntentsForDB(rsync.ToolOptions.FDB)
	case rsync.ToolOptions.FDB != "" && rsync.ToolOptions.FCollection != "":
		err = rsync.CreateCollectionIntent(rsync.ToolOptions.FDB, rsync.ToolOptions.FCollection)
	}
	if err != nil {
		return err
	}

	if rsync.InputOptions.Oplog {
		err = rsync.CreateOplogIntents()
		if err != nil {
			return err
		}
	}

	if rsync.InputOptions.RsyncDBUsersAndRoles && rsync.ToolOptions.FDB != "admin" {
		err = rsync.CreateUsersRolesVersionIntentsForDB(rsync.ToolOptions.FDB)
		if err != nil {
			return err
		}
	}

	// end prepare

	/*
		indexes get

	*/
	// rsync.managerIndexes = rsync.manager
	/*	err = rsync.RsyncMetadata()
		if err != nil {
			return fmt.Errorf("error rsyncing metadata: %v", err)
		}
	*/
	/*
		indexes end

	*/
	// Rsync the regular collections
	if rsync.OutputOptions.NumParallelCollections > 1 {
		rsync.manager.Finalize(intents.MultiDatabaseLTF)
	} else {
		// use legacy restoration order if we are single-threaded
		rsync.manager.Finalize(intents.Legacy)
	}
	// 上面是准备表和库，下面是操作数据库
	rsync.termChan = make(chan struct{})
	// session.SetSafe(rsync.safety)
	// defer session.Close()
	// take the copy intents
	// rsync.managerOplog = rsync.manager.Oplog()
	go rsync.handleSignals()

	if err := rsync.RsyncIntents(); err != nil {
		return err
	}

	// Rsync users/roles
	if rsync.ShouldRsyncUsersAndRoles() {
		err = rsync.RsyncUsersOrRoles(rsync.managerUserRoleOther.Users(), rsync.managerUserRoleOther.Roles())
		if err != nil {
			return fmt.Errorf("rsync error: %v", err)
		}
	}
	// rsync indexes
	// pass in RsyncIntents()
	/*	err = rsync.LoadIndexesFromBSON()
		if err != nil {
			return fmt.Errorf("rsync error: %v", err)
		}
	*/
	// Rsync oplog
	if rsync.InputOptions.Oplog && len(rsync.query) == 0 {
		err = rsync.RsyncOplog()
		if err != nil {
			return fmt.Errorf("rsync error: %v", err)
		}
	}

	log.Log(log.Always, "done")
	return nil
}

type wrappedReadCloser struct {
	io.ReadCloser
	inner io.ReadCloser
}

func (wrc *wrappedReadCloser) Close() error {
	err := wrc.ReadCloser.Close()
	if err != nil {
		return err
	}
	return wrc.inner.Close()
}

/*
func (rsync *MongoRsync) getArchiveReader() (rc io.ReadCloser, err error) {
	if rsync.InputOptions.Archive == "-" {
		rc = ioutil.NopCloser(rsync.stdin)
	} else {
		targetStat, err := os.Stat(rsync.InputOptions.Archive)
		if err != nil {
			return nil, err
		}
		if targetStat.IsDir() {
			defaultArchiveFilePath := filepath.Join(rsync.InputOptions.Archive, "archive")
			if rsync.InputOptions.Gzip {
				defaultArchiveFilePath = defaultArchiveFilePath + ".gz"
			}
			rc, err = os.Open(defaultArchiveFilePath)
			if err != nil {
				return nil, err
			}
		} else {
			rc, err = os.Open(rsync.InputOptions.Archive)
			if err != nil {
				return nil, err
			}
		}
	}
	if rsync.InputOptions.Gzip {
		gzrc, err := gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
		return &wrappedReadCloser{gzrc, rc}, nil
	}
	return rc, nil
}
*/
// handleSignals listens for either SIGTERM, SIGINT or the
// SIGHUP signal. It ends rsync reads for all goroutines
// as soon as any of those signals is received.
func (rsync *MongoRsync) handleSignals() {
	log.Log(log.DebugLow, "will listen for SIGTERM and SIGINT")
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	// first signal cleanly terminates rsync reads
	<-sigChan
	log.Log(log.Always, "ending sync reads...")
	close(rsync.termChan)
	// second signal exits immediately
	<-sigChan
	log.Log(log.Always, "forcefully terminating mongorsync")
	os.Exit(util.ExitKill)
}
func (rsync *MongoRsync) RsyncMetadata() error {
	allIntents := rsync.manager.Intents()
	for _, intent := range allIntents {
		err := rsync.rsyncMetadata(intent)
		if err != nil {
			return err
		}
	}
	return nil
}
func (rsync *MongoRsync) CreateOplogIntents() error {
	err := rsync.determineOplogCollectionName()
	if err != nil {
		return err
	}
	oplogIntent := &intents.Intent{
		DB: "",
		C:  "oplog",
	}
	rsync.managerOplog.Put(oplogIntent)
	return nil
}
func docPlural(count int64) string {
	return util.Pluralize(int(count), "document", "documents")
}
