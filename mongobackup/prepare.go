package mongobackup

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"mongoIncbackup-1.1/common/archive"
	"mongoIncbackup-1.1/common/bsonutil"
	"mongoIncbackup-1.1/common/db"
	"mongoIncbackup-1.1/common/intents"
	"mongoIncbackup-1.1/common/log"
	"mongoIncbackup-1.1/mgo.v2/bson"
	"os"
	"path/filepath"
	"strings"
)

type NilPos struct{}

func (NilPos) Pos() int64 {
	return -1
}

type collectionInfo struct {
	Name    string  `bson:"name"`
	Options *bson.D `bson:"options"`
}

// writeFlusher wraps an io.Writer and adds a Flush function.
type writeFlusher interface {
	Flush() error
	io.Writer
}

// writeFlushCloser is a writeFlusher implementation which exposes
// a Close function which is implemented by calling Flush.
type writeFlushCloser struct {
	writeFlusher
}

// availableWriteFlusher wraps a writeFlusher and adds an Available function.
type availableWriteFlusher interface {
	Available() int
	writeFlusher
}

// atomicFlusher is a availableWriteFlusher implementation
// which guarantees atomic writes.
type atomicFlusher struct {
	availableWriteFlusher
}

// errorReader implements io.Reader.
type errorReader struct{}

// Read on an errorReader already returns an error.
func (errorReader) Read([]byte) (int, error) {
	return 0, os.ErrInvalid
}

// Close calls Flush.
func (bwc writeFlushCloser) Close() error {
	return bwc.Flush()
}

// realBSONFile implements the intents.file interface. It lets intents write to real BSON files
// ok disk via an embedded bufio.Writer
type realBSONFile struct {
	io.WriteCloser
	path string
	// errorWrite adds a Read() method to this object allowing it to be an
	// intent.file ( a ReadWriteOpenCloser )
	errorReader
	intent *intents.Intent
	gzip   bool
	NilPos
}

// Open is part of the intents.file interface. realBSONFiles need to have Open called before
// Read can be called
func (f *realBSONFile) Open() (err error) {
	if f.path == "" {
		// This should not occur normally. All realBSONFile's should have a path
		return fmt.Errorf("error creating BSON file without a path, namespace: %v",
			f.intent.Namespace())
	}
	err = os.MkdirAll(filepath.Dir(f.path), os.ModeDir|os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating directory for BSON file %v: %v",
			filepath.Dir(f.path), err)
	}

	fileName := f.path
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("error creating BSON file %v: %v", fileName, err)
	}
	var writeCloser io.WriteCloser
	if f.gzip {
		writeCloser = gzip.NewWriter(file)
	} else {
		// wrap writer in buffer to reduce load on disk
		writeCloser = writeFlushCloser{
			atomicFlusher{
				bufio.NewWriterSize(file, 32*1024),
			},
		}
	}
	f.WriteCloser = &wrappedWriteCloser{
		WriteCloser: writeCloser,
		inner:       file,
	}

	return nil
}

// Write guarantees that when it returns, either the entire
// contents of buf or none of it, has been flushed by the writer.
// This is useful in the unlikely case that mongobackup crashes.
func (f atomicFlusher) Write(buf []byte) (int, error) {
	if len(buf) > f.availableWriteFlusher.Available() {
		f.availableWriteFlusher.Flush()
	}
	if len(buf) > f.availableWriteFlusher.Available() {
		l, e := f.availableWriteFlusher.Write(buf)
		f.availableWriteFlusher.Flush()
		return l, e
	}
	return f.availableWriteFlusher.Write(buf)
}

// realMetadataFile implements intent.file, and corresponds to a Metadata file on disk
type realMetadataFile struct {
	io.WriteCloser
	path string
	errorReader
	// errorWrite adds a Read() method to this object allowing it to be an
	// intent.file ( a ReadWriteOpenCloser )
	intent *intents.Intent
	gzip   bool
	NilPos
}

// Open opens the file on disk that the intent indicates. Any directories needed are created.
// If compression is needed, the File gets wrapped in a gzip.Writer
func (f *realMetadataFile) Open() (err error) {
	if f.path == "" {
		return fmt.Errorf("No metadata path for %v.%v", f.intent.DB, f.intent.C)
	}
	err = os.MkdirAll(filepath.Dir(f.path), os.ModeDir|os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating directory for metadata file %v: %v",
			filepath.Dir(f.path), err)
	}

	fileName := f.path
	f.WriteCloser, err = os.Create(fileName)
	if err != nil {
		return fmt.Errorf("error creating metadata file %v: %v", fileName, err)
	}
	if f.gzip {
		f.WriteCloser = &wrappedWriteCloser{
			WriteCloser: gzip.NewWriter(f.WriteCloser),
			inner:       f.WriteCloser,
		}
	}
	return nil
}

// stdoutFile implements the intents.file interface. stdoutFiles are used when single collections
// are written directly (non-archive-mode) to standard out, via "--dir -"
type stdoutFile struct {
	io.Writer
	errorReader
	NilPos
}

// Open is part of the intents.file interface.
func (f *stdoutFile) Open() error {
	return nil
}

// Close is part of the intents.file interface. While we could actually close os.Stdout here,
// that's actually a bad idea. Unsetting f.File here will cause future Writes to fail, which
// is all we want.
func (f *stdoutFile) Close() error {
	f.Writer = nil
	return nil
}

// shouldSkipCollection returns true when a collection name is excluded
// by the mongobackup options.
func (backup *MongoBackup) shouldSkipCollection(colName string) bool {
	for _, excludedCollection := range backup.OutputOptions.ExcludedCollections {
		if colName == excludedCollection {
			return true
		}
	}
	for _, excludedCollectionPrefix := range backup.OutputOptions.ExcludedCollectionPrefixes {
		if strings.HasPrefix(colName, excludedCollectionPrefix) {
			return true
		}
	}
	return false
}

// outputPath creates a path for the collection to be written to (sans file extension).
func (backup *MongoBackup) outputPath(dbName, colName string) string {
	var root string
	if backup.OutputOptions.Out == "" {
		root = "backup"
	} else {
		root = backup.OutputOptions.Out
	}
	if dbName == "" {
		return filepath.Join(root, colName)
	}
	return filepath.Join(root, dbName, colName)
}

func checkStringForPathSeparator(s string, c *rune) bool {
	for _, *c = range s {
		if os.IsPathSeparator(uint8(*c)) {
			return true
		}
	}
	return false
}

// NewIntent creates a bare intent without populating the options.
func (backup *MongoBackup) NewIntent(dbName, colName string) (*intents.Intent, error) {
	intent := &intents.Intent{
		DB: dbName,
		C:  colName,
	}
	if backup.OutputOptions.Out == "-" {
		intent.BSONFile = &stdoutFile{Writer: backup.stdout}
	} else {
		if backup.OutputOptions.Archive != "" {
			intent.BSONFile = &archive.MuxIn{Intent: intent, Mux: backup.archive.Mux}
		} else {
			var c rune
			if checkStringForPathSeparator(colName, &c) || checkStringForPathSeparator(dbName, &c) {
				return nil, fmt.Errorf(`"%v.%v" contains a path separator '%c' `+
					`and can't be backuped to the filesystem`, dbName, colName, c)
			}
			path := nameGz(backup.OutputOptions.Gzip, backup.outputPath(dbName, colName)+".bson")
			intent.BSONFile = &realBSONFile{path: path, intent: intent, gzip: backup.OutputOptions.Gzip}
		}
		if !intent.IsSystemIndexes() {
			if backup.OutputOptions.Archive != "" {
				intent.MetadataFile = &archive.MetadataFile{
					Intent: intent,
					Buffer: &bytes.Buffer{},
				}
			} else {
				path := nameGz(backup.OutputOptions.Gzip, backup.outputPath(dbName, colName+".metadata.json"))
				intent.MetadataFile = &realMetadataFile{path: path, intent: intent, gzip: backup.OutputOptions.Gzip}
			}
		}
	}

	// get a document count for scheduling purposes
	session, err := backup.sessionProvider.GetSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	count, err := session.DB(dbName).C(colName).Count()
	if err != nil {
		return nil, fmt.Errorf("error counting %v: %v", intent.Namespace(), err)
	}
	intent.Size = int64(count)

	return intent, nil
}

// CreateOplogIntents creates an intents.Intent for the oplog and adds it to the manager
func (backup *MongoBackup) CreateOplogIntents() error {
	err := backup.determineOplogCollectionName()
	if err != nil {
		return err
	}
	oplogIntent := &intents.Intent{
		DB: "",
		C:  "oplog",
	}
	if backup.OutputOptions.Archive != "" {
		oplogIntent.BSONFile = &archive.MuxIn{Mux: backup.archive.Mux, Intent: oplogIntent}
	} else {
		oplogIntent.BSONFile = &realBSONFile{path: backup.outputPath("oplog.bson", ""), intent: oplogIntent, gzip: backup.OutputOptions.Gzip}
	}
	backup.manager.Put(oplogIntent)
	return nil
}

// CreateUsersRolesVersionIntentsForDB create intents to be written in to the specific
// database folder, for the users, roles and version admin database collections
// And then it adds the intents in to the manager
func (backup *MongoBackup) CreateUsersRolesVersionIntentsForDB(db string) error {

	outDir := backup.outputPath(db, "")

	usersIntent := &intents.Intent{
		DB: db,
		C:  "$admin.system.users",
	}
	rolesIntent := &intents.Intent{
		DB: db,
		C:  "$admin.system.roles",
	}
	versionIntent := &intents.Intent{
		DB: db,
		C:  "$admin.system.version",
	}
	if backup.OutputOptions.Archive != "" {
		usersIntent.BSONFile = &archive.MuxIn{Intent: usersIntent, Mux: backup.archive.Mux}
		rolesIntent.BSONFile = &archive.MuxIn{Intent: rolesIntent, Mux: backup.archive.Mux}
		versionIntent.BSONFile = &archive.MuxIn{Intent: versionIntent, Mux: backup.archive.Mux}
	} else {
		usersIntent.BSONFile = &realBSONFile{path: filepath.Join(outDir, nameGz(backup.OutputOptions.Gzip, "$admin.system.users.bson")), intent: usersIntent, gzip: backup.OutputOptions.Gzip}
		rolesIntent.BSONFile = &realBSONFile{path: filepath.Join(outDir, nameGz(backup.OutputOptions.Gzip, "$admin.system.roles.bson")), intent: rolesIntent, gzip: backup.OutputOptions.Gzip}
		versionIntent.BSONFile = &realBSONFile{path: filepath.Join(outDir, nameGz(backup.OutputOptions.Gzip, "$admin.system.version.bson")), intent: versionIntent, gzip: backup.OutputOptions.Gzip}
	}
	backup.manager.Put(usersIntent)
	backup.manager.Put(rolesIntent)
	backup.manager.Put(versionIntent)

	return nil
}

// CreateCollectionIntent builds an intent for a given collection and
// puts it into the intent manager.
func (backup *MongoBackup) CreateCollectionIntent(dbName, colName string) error {
	if backup.shouldSkipCollection(colName) {
		log.Logf(log.DebugLow, "skipping backup of %v.%v, it is excluded", dbName, colName)
		return nil
	}

	intent, err := backup.NewIntent(dbName, colName)
	if err != nil {
		return err
	}

	session, err := backup.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()

	opts, err := db.GetCollectionOptions(session.DB(dbName).C(colName))
	if err != nil {
		return fmt.Errorf("error getting collection options: %v", err)
	}

	intent.Options = nil
	if opts != nil {
		optsInterface, _ := bsonutil.FindValueByKey("options", opts)
		if optsInterface != nil {
			if optsD, ok := optsInterface.(bson.D); ok {
				intent.Options = &optsD
			} else {
				return fmt.Errorf("Failed to parse collection options as bson.D")
			}
		}
	}

	backup.manager.Put(intent)

	log.Logf(log.DebugLow, "enqueued collection '%v'", intent.Namespace())
	return nil
}

func (backup *MongoBackup) createIntentFromOptions(dbName string, ci *collectionInfo) error {
	if backup.shouldSkipCollection(ci.Name) {
		log.Logf(log.DebugLow, "skipping backup of %v.%v, it is excluded", dbName, ci.Name)
		return nil
	}
	intent, err := backup.NewIntent(dbName, ci.Name)
	if err != nil {
		return err
	}
	intent.Options = ci.Options
	backup.manager.Put(intent)
	log.Logf(log.DebugLow, "enqueued collection '%v'", intent.Namespace())
	return nil
}

// CreateIntentsForDatabase iterates through collections in a db
// and builds backup intents for each collection.
func (backup *MongoBackup) CreateIntentsForDatabase(dbName string) error {
	// we must ensure folders for empty databases are still created, for legacy purposes

	session, err := backup.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()

	colsIter, fullName, err := db.GetCollections(session.DB(dbName), "")
	if err != nil {
		return fmt.Errorf("error getting collections for database `%v`: %v", dbName, err)
	}

	collInfo := &collectionInfo{}
	for colsIter.Next(collInfo) {
		// Skip over indexes since they are also listed in system.namespaces in 2.6 or earlier
		if strings.Contains(collInfo.Name, "$") && !strings.Contains(collInfo.Name, ".oplog.$") {
			continue
		}
		if fullName {
			namespacePrefix := dbName + "."
			// if the collection info came from querying system.indexes (2.6 or earlier) then the
			// "name" we get includes the db name as well, so we must remove it
			if strings.HasPrefix(collInfo.Name, namespacePrefix) {
				collInfo.Name = collInfo.Name[len(namespacePrefix):]
			} else {
				return fmt.Errorf("namespace '%v' format is invalid - expected to start with '%v'", collInfo.Name, namespacePrefix)
			}
		}
		err := backup.createIntentFromOptions(dbName, collInfo)
		if err != nil {
			return err
		}
	}
	return colsIter.Err()
}

// CreateAllIntents iterates through all dbs and collections and builds
// backup intents for each collection.
func (backup *MongoBackup) CreateAllIntents() error {
	dbs, err := backup.sessionProvider.DatabaseNames()
	if err != nil {
		return fmt.Errorf("error getting database names: %v", err)
	}
	log.Logf(log.DebugHigh, "found databases: %v", strings.Join(dbs, ", "))
	for _, dbName := range dbs {
		if dbName == "local" {
			// local can only be explicitly backuped
			continue
		}
		if err := backup.CreateIntentsForDatabase(dbName); err != nil {
			return err
		}
	}
	return nil
}

func nameGz(gz bool, name string) string {
	if gz {
		return name + ".gz"
	}
	return name
}
