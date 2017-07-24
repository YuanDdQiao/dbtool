package mongobackup

import (
	"bytes"
	"fmt"
	"mongoIncbackup-1.1/common/bsonutil"
	"mongoIncbackup-1.1/common/db"
	"mongoIncbackup-1.1/common/json"
	"mongoIncbackup-1.1/common/log"
	"mongoIncbackup-1.1/common/options"
	"mongoIncbackup-1.1/common/testutil"
	"mongoIncbackup-1.1/common/util"
	. "mongoIncbackup-1.1/smartystreets/goconvey/convey"
	"mongoIncbackup-1.1/mgo.v2"
	"mongoIncbackup-1.1/mgo.v2/bson"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	// database with test data
	testDB = "mongobackup_test_db"
	// temp database used for restoring a DB
	testRestoreDB       = "temp_mongobackup_restore_test_db"
	testCollectionNames = []string{"coll1", "coll2", "coll3"}
	testServer          = "localhost"
	testPort            = db.DefaultTestPort
)

const (
	KerberosBackupDirectory = "backup-kerberos"
)

func simpleMongoBackupInstance() *MongoBackup {
	ssl := testutil.GetSSLOptions()
	auth := testutil.GetAuthOptions()
	namespace := &options.Namespace{
		DB: testDB,
	}
	connection := &options.Connection{
		Host: testServer,
		Port: testPort,
	}
	toolOptions := &options.ToolOptions{
		SSL:           &ssl,
		Namespace:     namespace,
		Connection:    connection,
		Auth:          &auth,
		HiddenOptions: &options.HiddenOptions{},
		Verbosity:     &options.Verbosity{},
	}
	outputOptions := &OutputOptions{
		NumParallelCollections: 1,
	}
	inputOptions := &InputOptions{}

	log.SetVerbosity(toolOptions.Verbosity)

	return &MongoBackup{
		ToolOptions:   toolOptions,
		InputOptions:  inputOptions,
		OutputOptions: outputOptions,
	}
}

func getBareSession() (*mgo.Session, error) {
	ssl := testutil.GetSSLOptions()
	auth := testutil.GetAuthOptions()
	sessionProvider, err := db.NewSessionProvider(options.ToolOptions{
		Connection: &options.Connection{
			Host: testServer,
			Port: testPort,
		},
		Auth: &auth,
		SSL:  &ssl,
	})
	if err != nil {
		return nil, err
	}
	session, err := sessionProvider.GetSession()
	if err != nil {
		return nil, err
	}
	return session, nil
}

// returns the number of .bson files in a directory
// excluding system.indexes.bson
func countNonIndexBSONFiles(dir string) (int, error) {
	matchingFiles, err := getMatchingFiles(dir, ".*\\.bson")
	if err != nil {
		return 0, err
	}
	count := 0
	for _, fileName := range matchingFiles {
		if fileName != "system.indexes.bson" {
			count++
		}
	}
	return count, nil
}

// returns count of metadata files
func countMetaDataFiles(dir string) (int, error) {
	matchingFiles, err := getMatchingFiles(dir, ".*\\.metadata\\.json")
	if err != nil {
		return 0, err
	}
	return len(matchingFiles), nil
}

// returns filenames that match the given pattern
func getMatchingFiles(dir, pattern string) ([]string, error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	matchingFiles := []string{}
	var matched bool
	for _, fileInfo := range fileInfos {
		fileName := fileInfo.Name()
		if matched, err = regexp.MatchString(pattern, fileName); matched {
			matchingFiles = append(matchingFiles, fileName)
		}
		if err != nil {
			return nil, err
		}
	}
	return matchingFiles, nil
}

// read all the database bson documents from dir and put it into another DB
// ignore the inddexes for now
func readBSONIntoDatabase(dir, restoreDBName string) error {
	if ok := fileDirExists(dir); !ok {
		return fmt.Errorf("error finding '%v' on local FS", dir)
	}

	session, err := getBareSession()
	if err != nil {
		return err
	}
	defer session.Close()

	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, fileInfo := range fileInfos {
		fileName := fileInfo.Name()
		if !strings.HasSuffix(fileName, ".bson") || fileName == "system.indexes.bson" {
			continue
		}

		collectionName := fileName[:strings.LastIndex(fileName, ".bson")]
		collection := session.DB(restoreDBName).C(collectionName)

		file, err := os.Open(fmt.Sprintf("%s/%s", dir, fileName))
		if err != nil {
			return err
		}
		defer file.Close()

		bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(file))
		defer bsonSource.Close()

		var result bson.M
		for bsonSource.Next(&result) {
			err = collection.Insert(result)
			if err != nil {
				return err
			}
		}
		if err = bsonSource.Err(); err != nil {
			return err
		}
	}

	return nil
}

func setUpMongoBackupTestData() error {
	session, err := getBareSession()
	if err != nil {
		return err
	}
	defer session.Close()

	for i, collectionName := range testCollectionNames {
		coll := session.DB(testDB).C(collectionName)

		for j := 0; j < 10*(i+1); j++ {
			err = coll.Insert(bson.M{"collectionName": collectionName, "age": j})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func tearDownMongoBackupTestData() error {
	session, err := getBareSession()
	if err != nil {
		return err
	}
	defer session.Close()

	err = session.DB(testDB).DropDatabase()
	if err != nil {
		return err
	}
	return nil
}

func fileDirExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func testQuery(md *MongoBackup, session *mgo.Session) string {
	origDB := session.DB(testDB)
	restoredDB := session.DB(testRestoreDB)

	// query to test --query* flags
	bsonQuery := bson.M{"age": bson.M{"$lt": 10}}

	// we can only backup using query per collection
	for _, testCollName := range testCollectionNames {
		md.ToolOptions.Namespace.Collection = testCollName

		err := md.Init()
		So(err, ShouldBeNil)

		err = md.Backup()
		So(err, ShouldBeNil)
	}

	path, err := os.Getwd()
	So(err, ShouldBeNil)

	backupDir := util.ToUniversalPath(filepath.Join(path, "backup"))
	backupDBDir := util.ToUniversalPath(filepath.Join(backupDir, testDB))
	So(fileDirExists(backupDir), ShouldBeTrue)
	So(fileDirExists(backupDBDir), ShouldBeTrue)

	err = readBSONIntoDatabase(backupDBDir, testRestoreDB)
	So(err, ShouldBeNil)

	for _, testCollName := range testCollectionNames {
		// count filtered docs
		numDocs1, err := origDB.C(testCollName).Find(bsonQuery).Count()
		So(err, ShouldBeNil)

		// count number of all restored documents
		numDocs2, err := restoredDB.C(testCollName).Find(nil).Count()
		So(err, ShouldBeNil)

		So(numDocs1, ShouldEqual, numDocs2)
	}
	return backupDir
}

func TestMongoBackupValidateOptions(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	Convey("With a MongoBackup instance", t, func() {
		md := simpleMongoBackupInstance()

		Convey("we cannot backup a collection when a database specified", func() {
			md.ToolOptions.Namespace.Collection = "some_collection"
			md.ToolOptions.Namespace.DB = ""

			err := md.Init()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "cannot backup a collection without a specified database")
		})

		Convey("we have to specify a collection name if using a query", func() {
			md.ToolOptions.Namespace.Collection = ""
			md.OutputOptions.Out = ""
			md.InputOptions.Query = "{_id:\"\"}"

			err := md.Init()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "cannot backup using a query without a specified collection")
		})

	})
}

func TestMongoBackupKerberos(t *testing.T) {
	testutil.VerifyTestType(t, testutil.KerberosTestType)

	Convey("Should be able to run mongobackup with Kerberos auth", t, func() {
		opts, err := testutil.GetKerberosOptions()

		So(err, ShouldBeNil)

		mongoBackup := MongoBackup{
			ToolOptions:  opts,
			InputOptions: &InputOptions{},
			OutputOptions: &OutputOptions{
				NumParallelCollections: 1,
			},
		}

		mongoBackup.OutputOptions.Out = KerberosBackupDirectory

		err = mongoBackup.Init()
		So(err, ShouldBeNil)
		err = mongoBackup.Backup()
		So(err, ShouldBeNil)
		path, err := os.Getwd()
		So(err, ShouldBeNil)

		backupDir := util.ToUniversalPath(filepath.Join(path, KerberosBackupDirectory))
		backupDBDir := util.ToUniversalPath(filepath.Join(backupDir, opts.Namespace.DB))
		So(fileDirExists(backupDir), ShouldBeTrue)
		So(fileDirExists(backupDBDir), ShouldBeTrue)

		backupCollectionFile := util.ToUniversalPath(filepath.Join(backupDBDir, opts.Namespace.Collection+".bson"))
		So(fileDirExists(backupCollectionFile), ShouldBeTrue)

		countColls, err := countNonIndexBSONFiles(backupDBDir)
		So(err, ShouldBeNil)
		So(countColls, ShouldEqual, 1)
	})
}

func TestMongoBackupBSON(t *testing.T) {
	testutil.VerifyTestType(t, testutil.IntegrationTestType)
	log.SetWriter(ioutil.Discard)

	Convey("With a MongoBackup instance", t, func() {
		err := setUpMongoBackupTestData()
		So(err, ShouldBeNil)

		Convey("testing that using MongoBackup WITHOUT giving a query backups everything in the database and/or collection", func() {
			md := simpleMongoBackupInstance()
			md.InputOptions.Query = ""

			Convey("and that for a particular collection", func() {
				md.ToolOptions.Namespace.Collection = testCollectionNames[0]
				err = md.Init()
				So(err, ShouldBeNil)

				Convey("it backups to the default output directory", func() {
					// we don't have to set this manually if parsing options via command line
					md.OutputOptions.Out = "backup"
					err = md.Backup()
					So(err, ShouldBeNil)
					path, err := os.Getwd()
					So(err, ShouldBeNil)

					backupDir := util.ToUniversalPath(filepath.Join(path, "backup"))
					backupDBDir := util.ToUniversalPath(filepath.Join(backupDir, testDB))
					So(fileDirExists(backupDir), ShouldBeTrue)
					So(fileDirExists(backupDBDir), ShouldBeTrue)

					err = readBSONIntoDatabase(backupDBDir, testRestoreDB)
					So(err, ShouldBeNil)

					session, err := getBareSession()
					So(err, ShouldBeNil)

					countColls, err := countNonIndexBSONFiles(backupDBDir)
					So(err, ShouldBeNil)
					So(countColls, ShouldEqual, 1)

					collOriginal := session.DB(testDB).C(testCollectionNames[0])
					collRestore := session.DB(testRestoreDB).C(testCollectionNames[0])

					Convey("with the correct number of documents", func() {
						numDocsOrig, err := collOriginal.Count()
						So(err, ShouldBeNil)

						numDocsRestore, err := collRestore.Count()
						So(err, ShouldBeNil)

						So(numDocsOrig, ShouldEqual, numDocsRestore)
					})

					Convey("that are the same as the documents in the test database", func() {
						iter := collOriginal.Find(nil).Iter()

						var result bson.M
						for iter.Next(&result) {
							restoredCount, err := collRestore.Find(result).Count()
							So(err, ShouldBeNil)
							So(restoredCount, ShouldNotEqual, 0)
						}
						So(iter.Close(), ShouldBeNil)
					})

					Reset(func() {
						So(session.DB(testRestoreDB).DropDatabase(), ShouldBeNil)
						So(os.RemoveAll(backupDir), ShouldBeNil)
					})
				})

				Convey("it backups to a user-specified output directory", func() {
					md.OutputOptions.Out = "backup_user"
					err = md.Backup()
					So(err, ShouldBeNil)
					path, err := os.Getwd()
					So(err, ShouldBeNil)

					backupDir := util.ToUniversalPath(filepath.Join(path, "backup_user"))
					backupDBDir := util.ToUniversalPath(filepath.Join(backupDir, testDB))
					So(fileDirExists(backupDir), ShouldBeTrue)
					So(fileDirExists(backupDBDir), ShouldBeTrue)

					countColls, err := countNonIndexBSONFiles(backupDBDir)
					So(err, ShouldBeNil)
					So(countColls, ShouldEqual, 1)

					Reset(func() {
						So(os.RemoveAll(backupDir), ShouldBeNil)
					})

				})

				Convey("it backups to standard output", func() {
					md.OutputOptions.Out = "-"
					stdoutBuf := &bytes.Buffer{}
					md.stdout = stdoutBuf
					err = md.Backup()
					So(err, ShouldBeNil)
					var count int
					bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(ioutil.NopCloser(stdoutBuf)))
					defer bsonSource.Close()

					var result bson.Raw
					for bsonSource.Next(&result) {
						count++
					}
					So(bsonSource.Err(), ShouldBeNil)
					So(count, ShouldEqual, 10) //The 0th collection has 10 documents

					Reset(func() {
					})

				})

			})

			Convey("for an entire database", func() {
				md.ToolOptions.Namespace.Collection = ""
				err = md.Init()
				So(err, ShouldBeNil)

				Convey("that exists. The backuped directory should contain the necessary bson files", func() {
					md.OutputOptions.Out = "backup"
					err = md.Backup()
					So(err, ShouldBeNil)
					path, err := os.Getwd()
					So(err, ShouldBeNil)

					backupDir := util.ToUniversalPath(filepath.Join(path, "backup"))
					backupDBDir := util.ToUniversalPath(filepath.Join(backupDir, testDB))
					So(fileDirExists(backupDir), ShouldBeTrue)
					So(fileDirExists(backupDBDir), ShouldBeTrue)

					countColls, err := countNonIndexBSONFiles(backupDBDir)
					So(err, ShouldBeNil)
					So(countColls, ShouldEqual, len(testCollectionNames))

					Reset(func() {
						So(os.RemoveAll(backupDir), ShouldBeNil)
					})

				})

				Convey("that does not exist. The backuped directory shouldn't be created", func() {
					md.OutputOptions.Out = "backup"
					md.ToolOptions.Namespace.DB = "nottestdb"
					err = md.Backup()
					So(err, ShouldBeNil)

					path, err := os.Getwd()
					So(err, ShouldBeNil)

					backupDir := util.ToUniversalPath(filepath.Join(path, "backup"))
					backupDBDir := util.ToUniversalPath(filepath.Join(backupDir, "nottestdb"))

					So(fileDirExists(backupDir), ShouldBeFalse)
					So(fileDirExists(backupDBDir), ShouldBeFalse)
				})

			})
		})

		Convey("testing that using MongoBackup WITH a query backups a subset of documents in a database and/or collection", func() {
			session, err := getBareSession()
			So(err, ShouldBeNil)
			md := simpleMongoBackupInstance()

			// expect 10 documents per collection
			bsonQuery := bson.M{"age": bson.M{"$lt": 10}}
			jsonQuery, err := bsonutil.ConvertBSONValueToJSON(bsonQuery)
			So(err, ShouldBeNil)
			jsonQueryBytes, err := json.Marshal(jsonQuery)
			So(err, ShouldBeNil)

			Convey("using --query for all the collections in the database", func() {
				md.InputOptions.Query = string(jsonQueryBytes)
				md.ToolOptions.Namespace.DB = testDB
				md.OutputOptions.Out = "backup"
				backupDir := testQuery(md, session)

				Reset(func() {
					So(session.DB(testRestoreDB).DropDatabase(), ShouldBeNil)
					So(os.RemoveAll(backupDir), ShouldBeNil)
				})

			})

			Convey("using --queryFile for all the collections in the database", func() {
				ioutil.WriteFile("example.json", jsonQueryBytes, 0777)
				md.InputOptions.QueryFile = "example.json"
				md.ToolOptions.Namespace.DB = testDB
				md.OutputOptions.Out = "backup"
				backupDir := testQuery(md, session)

				Reset(func() {
					So(session.DB(testRestoreDB).DropDatabase(), ShouldBeNil)
					So(os.RemoveAll(backupDir), ShouldBeNil)
					So(os.Remove("example.json"), ShouldBeNil)
				})

			})
		})

		Reset(func() {
			So(tearDownMongoBackupTestData(), ShouldBeNil)
		})
	})
}

func TestMongoBackupMetaData(t *testing.T) {
	testutil.VerifyTestType(t, testutil.IntegrationTestType)
	log.SetWriter(ioutil.Discard)

	Convey("With a MongoBackup instance", t, func() {
		err := setUpMongoBackupTestData()
		So(err, ShouldBeNil)

		Convey("testing that the backuped directory contains information about indexes", func() {
			md := simpleMongoBackupInstance()
			md.OutputOptions.Out = "backup"
			err = md.Init()
			So(err, ShouldBeNil)

			err = md.Backup()
			So(err, ShouldBeNil)

			path, err := os.Getwd()
			So(err, ShouldBeNil)
			backupDir := util.ToUniversalPath(filepath.Join(path, "backup"))
			backupDBDir := util.ToUniversalPath(filepath.Join(backupDir, testDB))
			So(fileDirExists(backupDir), ShouldBeTrue)
			So(fileDirExists(backupDBDir), ShouldBeTrue)

			Convey("having one metadata file per collection", func() {
				c1, err := countNonIndexBSONFiles(backupDBDir)
				So(err, ShouldBeNil)

				c2, err := countMetaDataFiles(backupDBDir)
				So(err, ShouldBeNil)

				So(c1, ShouldEqual, c2)

				Convey("and that the JSON in a metadata file is valid", func() {
					metaFiles, err := getMatchingFiles(backupDBDir, ".*\\.metadata\\.json")
					So(err, ShouldBeNil)
					So(len(metaFiles), ShouldBeGreaterThan, 0)

					oneMetaFile, err := os.Open(util.ToUniversalPath(filepath.Join(backupDBDir, metaFiles[0])))
					So(err, ShouldBeNil)
					contents, err := ioutil.ReadAll(oneMetaFile)
					var jsonResult map[string]interface{}
					err = json.Unmarshal(contents, &jsonResult)
					So(err, ShouldBeNil)

					Convey("and contains an 'indexes' key", func() {
						_, ok := jsonResult["indexes"]
						So(ok, ShouldBeTrue)
						So(oneMetaFile.Close(), ShouldBeNil)
					})

				})

			})

			Reset(func() {
				So(os.RemoveAll(backupDir), ShouldBeNil)
			})
		})

		Reset(func() {
			So(tearDownMongoBackupTestData(), ShouldBeNil)
		})

	})
}
