package mongorsync

import (
	"compress/gzip"
	"fmt"
	"io"
	// "io/ioutil"
	// "mongorsync-1.1/common/archive"
	"mongorsync-1.1/common/intents"
	"mongorsync-1.1/common/log"
	// "mongorsync-1.1/common/util"
	"os"
	// "path/filepath"
	"strings"
	"sync/atomic"

	// add
	"mongorsync-1.1/common/bsonutil"
	"mongorsync-1.1/common/db"
	"mongorsync-1.1/mgo.v2/bson"
)

// FileType describes the various types of rsync documents.
type FileType uint

// File types constants used by mongorsync.
const (
	UnknownFileType FileType = iota
	BSONFileType
	MetadataFileType
)

type collectionInfo struct {
	Name    string  `bson:"name"`
	Options *bson.D `bson:"options"`
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, os.ErrInvalid
}

type PosReader interface {
	io.ReadCloser
	Pos() int64
}

// posTrackingReader is a type for reading from a file and being able to determine
// what position the file is at.
type posTrackingReader struct {
	io.ReadCloser
	pos int64
}

func (f *posTrackingReader) Read(p []byte) (int, error) {
	n, err := f.ReadCloser.Read(p)
	atomic.AddInt64(&f.pos, int64(n))
	return n, err
}

func (f *posTrackingReader) Pos() int64 {
	return atomic.LoadInt64(&f.pos)
}

// mixedPosTrackingReader is a type for reading from one file but getting the position of a
// different file. This is useful for compressed files where the appropriate position for progress
// bars is that of the compressed file, but file should be read from the uncompressed file.
type mixedPosTrackingReader struct {
	readHolder PosReader
	posHolder  PosReader
}

func (f *mixedPosTrackingReader) Read(p []byte) (int, error) {
	return f.readHolder.Read(p)
}

func (f *mixedPosTrackingReader) Pos() int64 {
	return f.posHolder.Pos()
}

func (f *mixedPosTrackingReader) Close() error {
	err := f.readHolder.Close()
	if err != nil {
		return err
	}
	return f.posHolder.Close()
}

// realBSONFile implements the intents.file interface. It lets intents read from real BSON files
// ok disk via an embedded os.File
// The Read, Write and Close methods of the intents.file interface is implemented here by the
// embedded os.File, the Write will return an error and not succeed
type realBSONFile struct {
	path string
	PosReader
	// errorWrite adds a Write() method to this object allowing it to be an
	// intent.file ( a ReadWriteOpenCloser )
	errorWriter
	intent *intents.Intent
	gzip   bool
}

// Open is part of the intents.file interface. realBSONFiles need to be Opened before Read
// can be called on them.
func (f *realBSONFile) Open() (err error) {
	if f.path == "" {
		// this error shouldn't happen normally
		return fmt.Errorf("error reading BSON file for %v", f.intent.Namespace())
	}
	file, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("error reading BSON file %v: %v", f.path, err)
	}
	posFile := &posTrackingReader{file, 0}
	if f.gzip {
		gzFile, err := gzip.NewReader(posFile)
		posUncompressedFile := &posTrackingReader{gzFile, 0}
		if err != nil {
			return fmt.Errorf("error decompressing compresed BSON file %v: %v", f.path, err)
		}
		f.PosReader = &mixedPosTrackingReader{
			readHolder: posUncompressedFile,
			posHolder:  posFile}
	} else {
		f.PosReader = posFile
	}
	return nil
}

// realMetadataFile implements the intents.file interface. It lets intents read from real
// metadata.json files ok disk via an embedded os.File
// The Read, Write and Close methods of the intents.file interface is implemented here by the
// embedded os.File, the Write will return an error and not succeed
type realMetadataFile struct {
	io.ReadCloser
	path string
	// errorWrite adds a Write() method to this object allowing it to be an
	// intent.file ( a ReadWriteOpenCloser )
	errorWriter
	intent *intents.Intent
	gzip   bool
	pos    int64
}

// Open is part of the intents.file interface. realMetadataFiles need to be Opened before Read
// can be called on them.
func (f *realMetadataFile) Open() (err error) {
	if f.path == "" {
		return fmt.Errorf("error reading metadata for %v", f.intent.Namespace())
	}
	file, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("error reading metadata %v: %v", f.path, err)
	}
	if f.gzip {
		gzFile, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("error reading compressed metadata %v: %v", f.path, err)
		}
		f.ReadCloser = &wrappedReadCloser{gzFile, file}
	} else {
		f.ReadCloser = file
	}
	return nil
}

func (f *realMetadataFile) Read(p []byte) (int, error) {
	n, err := f.ReadCloser.Read(p)
	atomic.AddInt64(&f.pos, int64(n))
	return n, err
}

func (f *realMetadataFile) Pos() int64 {
	return atomic.LoadInt64(&f.pos)
}

// stdinFile implements the intents.file interface. They allow intents to read single collections
// from standard input
type stdinFile struct {
	io.Reader
	errorWriter
	pos int64
}

// Open is part of the intents.file interface. stdinFile needs to have Open called on it before
// Read can be called on it.
func (f *stdinFile) Open() error {
	return nil
}

func (f *stdinFile) Read(p []byte) (int, error) {
	n, err := f.Reader.Read(p)
	atomic.AddInt64(&f.pos, int64(n))
	return n, err
}

func (f *stdinFile) Pos() int64 {
	return atomic.LoadInt64(&f.pos)
}

// Close is part of the intents.file interface. After Close is called, Read will fail.
func (f *stdinFile) Close() error {
	f.Reader = nil
	return nil
}
func (rsync *MongoRsync) CreateUsersRolesVersionIntentsForDB(db string) error {

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
	rsync.managerUserRoleOther.Put(usersIntent)
	rsync.managerUserRoleOther.Put(rolesIntent)
	rsync.managerUserRoleOther.Put(versionIntent)

	return nil
}

// CreateAllIntents drills down into a dump folder, creating intents for all of
// the databases and collections it finds.
func (rsync *MongoRsync) CreateAllIntents() error {
	log.Logf(log.DebugHigh, "using %v as rsync root directory", "source session") //dir.Path())
	//start
	dbs, err := rsync.FSessionProvider.DatabaseNames()
	if err != nil {
		return fmt.Errorf("error getting database names: %v", err)
	}
	log.Logf(log.DebugHigh, "found databases: %v", strings.Join(dbs, ", "))
	var foundOplog bool
	foundOplog = false
	for _, dbName := range dbs {
		if dbName == "local" {
			// local can only be explicitly dumped
			/*			oplogIntent := &intents.Intent{
							DB: dbName,
							C:  "oplog.rs",
						}
						rsync.manager.Put(oplogIntent)
			*/
			foundOplog = true
			continue
		}
		if dbName == "config" && rsync.fisMongos {
			log.Logf(log.DebugHigh, "skiping %v as rsync", dbName)
			continue
		}
		if err := rsync.CreateIntentsForDB(dbName); err != nil {
			return err
		}
	}
	if rsync.InputOptions.Oplog && !foundOplog {
		return fmt.Errorf("no %v to point-in-time; make sure you run mongorsync with --oplog on oplog.rs", "local db")
	}
	// end
	return nil
}

// CreateIntentsForDB drills down into the dir folder, creating intents
// for all of te collection dump files it finds for the db database.h
func (rsync *MongoRsync) CreateIntentsForDB(dbName string) (err error) {
	// var entries []archive.DirLike
	log.Logf(log.DebugHigh, "reading collections for database %v", dbName)
	// entries, err = dir.ReadDir()
	fsession, err := rsync.FSessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error getting database names: %v: %v", dbName, err)
	}
	defer fsession.Close()

	// oldest timestamp get start
	rsync.oplogStart, err = rsync.getOplogStartTime()
	if err != nil {
		return err
	}

	/*	entryAsOplogOflastOp := db.Oplog{}
		lastOpDoc := &bson.Raw{}
		err = fsession.DB("local").C("oplog.rs").Find(nil).Sort("-$natural").One(&lastOpDoc)
		if err != nil {
			return err
		}
		err = bson.Unmarshal(lastOpDoc.Data, &entryAsOplogOflastOp)
		if err != nil {
			return fmt.Errorf("error reading oplog: %v", err)
		}

		rsync.oplogStart = entryAsOplogOflastOp.Timestamp
		if len(rsync.oplogStart) == 0 || len(rsync.oplogStart) != 10 {
			return fmt.Errorf("error reading oplog timestamp: %v", rsync.oplogStart)
		}
	*/
	// oldest timestamp get end

	colsIter, fullName, err := db.GetCollections(fsession.DB(dbName), "")
	if err != nil {
		return fmt.Errorf("error getting collections for database `%v`: %v", dbName, err)
	}

	// usesMetadataFiles := hasMetadataFiles(entries)
	collInfo := &collectionInfo{}
	for colsIter.Next(collInfo) {
		// Skip over indexes since they are also listed in system.namespaces in 2.6 or earlier
		if strings.Contains(collInfo.Name, "$") && !strings.Contains(collInfo.Name, ".oplog.$") {
			continue
		}
		if fullName {
			namespacePrefix := dbName + "."
			// if the collection info came from querying system.indexes (2.6 or earlier) then the
			// "name" we get includes the dbName name as well, so we must remove it
			if strings.HasPrefix(collInfo.Name, namespacePrefix) {
				collInfo.Name = collInfo.Name[len(namespacePrefix):]
			} else {
				return fmt.Errorf("namespace '%v' format is invalid - expected to start with '%v'", collInfo.Name, namespacePrefix)
			}
		}
		/*err := rsync.createIntentFromOptions(dbName, collInfo)
		if err != nil {
			return err
		}*/

		if rsync.shouldSkipCollection(collInfo.Name) {
			log.Logf(log.DebugLow, "skipping rsync of %v.%v, it is excluded", dbName, collInfo.Name)
			continue
		}

		intent := &intents.Intent{
			DB: dbName,
			C:  collInfo.Name,
		}
		intent.Options = collInfo.Options
		rsync.manager.Put(intent)
		log.Logf(log.DebugLow, "enqueued collection '%v'", intent.Namespace())
	}
	return colsIter.Err()
	// return nil
}
func (rsync *MongoRsync) CreateCollectionIntent(dbName, colName string) error {
	if rsync.shouldSkipCollection(colName) {
		log.Logf(log.DebugLow, "skipping rsync of %v.%v, it is excluded", dbName, colName)
		return nil
	}
	intent, err := rsync.NewIntent(dbName, colName)
	if err != nil {
		return err
	}

	session, err := rsync.FSessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()
	// get first timestamp from oplog
	rsync.oplogStart, err = rsync.getOplogStartTime()
	if err != nil {
		return fmt.Errorf("error getting timestamp from local oplog: %v", err)
	}

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

	rsync.manager.Put(intent)
	log.Logf(log.DebugLow, "enqueued collection '%v'", intent.Namespace())
	/*replaced*/
	return nil
}

func (rsync *MongoRsync) NewIntent(dbName, colName string) (*intents.Intent, error) {
	intent := &intents.Intent{
		DB: dbName,
		C:  colName,
	}
	/*	if rsync.OutputOptions.Out == "-" {
			intent.BSONFile = &stdoutFile{Writer: rsync.stdout}
		} else {
			if dump.OutputOptions.Archive != "" {
				intent.BSONFile = &archive.MuxIn{Intent: intent, Mux: dump.archive.Mux}
			} else {
				var c rune
				if checkStringForPathSeparator(colName, &c) || checkStringForPathSeparator(dbName, &c) {
					return nil, fmt.Errorf(`"%v.%v" contains a path separator '%c' `+
						`and can't be dumped to the filesystem`, dbName, colName, c)
				}
				path := nameGz(dump.OutputOptions.Gzip, dump.outputPath(dbName, colName)+".bson")
				intent.BSONFile = &realBSONFile{path: path, intent: intent, gzip: dump.OutputOptions.Gzip}
			}
			if !intent.IsSystemIndexes() {
				if dump.OutputOptions.Archive != "" {
					intent.MetadataFile = &archive.MetadataFile{
						Intent: intent,
						Buffer: &bytes.Buffer{},
					}
				} else {
					path := nameGz(dump.OutputOptions.Gzip, dump.outputPath(dbName, colName+".metadata.json"))
					intent.MetadataFile = &realMetadataFile{path: path, intent: intent, gzip: dump.OutputOptions.Gzip}
				}
			}
		}
	*/
	// get a document count for scheduling purposes
	fsession, err := rsync.FSessionProvider.GetSession()
	if err != nil {
		return nil, err
	}
	defer fsession.Close()

	count, err := fsession.DB(dbName).C(colName).Count()
	if err != nil {
		return nil, fmt.Errorf("error counting %v: %v", intent.Namespace(), err)
	}
	intent.Size = int64(count)

	return intent, nil
}

// CreateStdinIntentForCollection builds an intent for the given database and collection name
// that is to be read from standard input
/*func (rsync *MongoRsync) CreateStdinIntentForCollection(db string, collection string) error {
	log.Logf(log.DebugLow, "reading collection %v for database %v from standard input",
		collection, db)
	intent := &intents.Intent{
		DB:       db,
		C:        collection,
		Location: "-",
	}
	intent.BSONFile = &stdinFile{Reader: rsync.stdin}
	rsync.manager.Put(intent)
	return nil
}
*/
// CreateIntentForCollection builds an intent for the given database and collection name
// along with a path to a .bson collection file. It searches the file's parent directory
// for a matching metadata file.
//
// This method is not called by CreateIntentsForDB,
// it is only used in the case where --db and --collection flags are set.
/*func (rsync *MongoRsync) CreateIntentForCollection(db string, collection string, dir archive.DirLike) error {
	log.Logf(log.DebugLow, "reading collection %v for database %v from %v",
		collection, db, dir.Path())
	// first make sure the bson file exists and is valid
	_, err := dir.Stat()
	if err != nil {
		return err
	}
	if dir.IsDir() {
		return fmt.Errorf("file %v is a directory, not a bson file", dir.Path())
	}

	baseName, fileType := rsync.getInfoFromFilename(dir.Name())
	if fileType != BSONFileType {
		return fmt.Errorf("file %v does not have .bson extension", dir.Path())
	}

	// then create its intent
	intent := &intents.Intent{
		DB:       db,
		C:        collection,
		Size:     dir.Size(),
		Location: dir.Path(),
	}
	intent.BSONFile = &realBSONFile{path: dir.Path(), intent: intent, gzip: rsync.InputOptions.Gzip}

	// finally, check if it has a .metadata.json file in its folder
	log.Logf(log.DebugLow, "scanning directory %v for metadata", dir.Name())
	entries, err := dir.Parent().ReadDir()
	if err != nil {
		// try and carry on if we can
		log.Logf(log.Info, "error attempting to locate metadata for file: %v", err)
		log.Log(log.Info, "rsyncing collection without metadata")
		rsync.manager.Put(intent)
		return nil
	}
	metadataName := baseName + ".metadata.json"
	if rsync.InputOptions.Gzip {
		metadataName += ".gz"
	}
	for _, entry := range entries {
		if entry.Name() == metadataName {
			metadataPath := entry.Path()
			log.Logf(log.Info, "found metadata for collection at %v", metadataPath)
			intent.MetadataLocation = metadataPath
			intent.MetadataFile = &realMetadataFile{path: metadataPath, intent: intent, gzip: rsync.InputOptions.Gzip}
			break
		}
	}

	if intent.MetadataFile == nil {
		log.Log(log.Info, "rsyncing collection without metadata")
	}

	rsync.manager.Put(intent)

	return nil
}

// helper for searching a list of FileInfo for metadata files
func hasMetadataFiles(files []archive.DirLike) bool {
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".metadata.json") {
			return true
		}
	}
	return false
}*/

// handleBSONInsteadOfDirectory updates -d and -c settings based on
// the path to the BSON file passed to mongorsync. This is only
// applicable if the target path points to a .bson file.
//
// As an example, when the user passes 'dump/mydb/col.bson', this method
// will infer that 'mydb' is the database and 'col' is the collection name.
/*func (rsync *MongoRsync) handleBSONInsteadOfDirectory(path string) error {
	// we know we have been given a non-directory, so we should handle it
	// like a bson file and infer as much as we can
	if rsync.ToolOptions.Collection == "" {
		// if the user did not set -c, use the file name for the collection
		newCollectionName, fileType := rsync.getInfoFromFilename(path)
		if fileType != BSONFileType {
			return fmt.Errorf("file %v does not have .bson extension", path)
		}
		rsync.ToolOptions.Collection = newCollectionName
		log.Logf(log.DebugLow, "inferred collection '%v' from file", rsync.ToolOptions.Collection)
	}
	if rsync.ToolOptions.DB == "" {
		// if the user did not set -d, use the directory containing the target
		// file as the db name (as it would be in a dump directory). If
		// we cannot determine the directory name, use "test"
		dirForFile := filepath.Base(filepath.Dir(path))
		if dirForFile == "." || dirForFile == ".." {
			dirForFile = "test"
		}
		rsync.ToolOptions.DB = dirForFile
		log.Logf(log.DebugLow, "inferred db '%v' from the file's directory", rsync.ToolOptions.DB)
	}
	return nil
}

type actualPath struct {
	os.FileInfo
	path   string
	parent *actualPath
}

func newActualPath(dir string) (*actualPath, error) {
	stat, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	path := filepath.Dir(filepath.Clean(dir))
	parent := &actualPath{}
	parentStat, err := os.Stat(path)
	if err == nil {
		parent.FileInfo = parentStat
		parent.path = filepath.Dir(path)
	}
	ap := &actualPath{
		FileInfo: stat,
		path:     path,
		parent:   parent,
	}
	return ap, nil
}

func (ap actualPath) Path() string {
	return filepath.Join(ap.path, ap.Name())
}

func (ap actualPath) Parent() archive.DirLike {
	// returns nil if there is no parent
	return ap.parent
}

func (ap actualPath) ReadDir() ([]archive.DirLike, error) {
	entries, err := ioutil.ReadDir(ap.Path())
	if err != nil {
		return nil, err
	}
	var returnFileInfo = make([]archive.DirLike, 0, len(entries))
	for _, entry := range entries {
		returnFileInfo = append(returnFileInfo,
			actualPath{
				FileInfo: entry,
				path:     ap.Path(),
				parent:   &ap,
			})
	}
	return returnFileInfo, nil
}

func (ap actualPath) Stat() (archive.DirLike, error) {
	stat, err := os.Stat(ap.Path())
	if err != nil {
		return nil, err
	}
	return &actualPath{FileInfo: stat, path: ap.Path()}, nil
}

func (ap actualPath) IsDir() bool {
	stat, err := os.Stat(ap.Path())
	if err != nil {
		return false
	}
	return stat.IsDir()
}
*/
