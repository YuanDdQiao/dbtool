package mongorsync

import (
	"fmt"
	// "io/ioutil"
	"mongorsync-1.1/common/db"
	"mongorsync-1.1/common/intents"
	"mongorsync-1.1/common/log"
	"mongorsync-1.1/common/progress"
	"mongorsync-1.1/common/util"
	"mongorsync-1.1/mgo.v2/bson"
	"strings"
	"time"

	// add
	"mongorsync-1.1/mgo.v2"
)

const (
	progressBarLength   = 24
	progressBarWaitTime = time.Second * 3
	insertBufferFactor  = 16
)

// RsyncIntents iterates through all of the intents stored in the IntentManager, and rsyncs them.
func (rsync *MongoRsync) RsyncIntents() error {
	// start up the progress bar manager
	rsync.progressManager = progress.NewProgressBarManager(log.Writer(0), progressBarWaitTime)
	rsync.progressManager.Start()
	defer rsync.progressManager.Stop()

	log.Logf(log.DebugLow, "syncing up to %v collections in parallel", rsync.OutputOptions.NumParallelCollections)

	if rsync.OutputOptions.NumParallelCollections > 0 {
		resultChan := make(chan error)

		// start a goroutine for each job thread
		for i := 0; i < rsync.OutputOptions.NumParallelCollections; i++ {
			go func(id int) {
				log.Logf(log.DebugHigh, "starting sync routine with id=%v", id)
				// var ioBuf []byte
				for {
					intent := rsync.manager.Pop()
					if intent == nil {
						// log.Logf(log.DebugHigh, "ending rsync routine with id=%v, no more work to do", id)
						resultChan <- nil // done
						return
					}
					// log.Logf(log.DebugHigh, "format......test.....%v.%v", intent.DB, intent.C)

					oplogFlag, err := rsync.RsyncIntent(intent)
					if err != nil {
						resultChan <- fmt.Errorf("%v: %v", intent.Namespace(), err)
						return
					}
					if !oplogFlag {
						continue
					}
					rsync.manager.Finish(intent)
				}
			}(i)
		}

		// wait until all goroutines are done or one of them errors out
		for i := 0; i < rsync.OutputOptions.NumParallelCollections; i++ {
			if err := <-resultChan; err != nil {
				return err
			}
		}
		return nil
	}

	// single-threaded
	for {
		intent := rsync.manager.Pop()
		if intent == nil {
			return nil
		}
		oplogFlag, err := rsync.RsyncIntent(intent)
		if err != nil {
			return fmt.Errorf("%v: %v", intent.Namespace(), err)
		}
		if !oplogFlag {
			// log.Logf(log.DebugHigh, "format......test.....%v.%v", intent.DB, intent.C)
			continue
		}
		rsync.manager.Finish(intent)

	}
	return nil
}

// RsyncIntent attempts to rsync a given intent into MongoDB.
func (rsync *MongoRsync) RsyncIntent(intent *intents.Intent) (bool, error) {
	var thatstrue bool
	thatstrue = false
	if intent.DB == "local" {
		// log.Logf(log.Always, "cannot drop collection from local %v, skipping", intent.Namespace())
		thatstrue = true
		return thatstrue, nil
	}
	collectionExists, err := rsync.CollectionExists(intent)
	if err != nil {
		return false, fmt.Errorf("error reading database: %v", err)
	}

	if rsync.safety == nil && !rsync.OutputOptions.Drop && collectionExists {
		log.Logf(log.Always, "syncing to existing collection %v without dropping", intent.Namespace())
		log.Log(log.Always, "Important: syncd data will be inserted without raising errors; check your server log")
	}
	if rsync.OutputOptions.Drop {
		if collectionExists {
			if strings.HasPrefix(intent.C, "system.") {
				log.Logf(log.Always, "cannot drop system collection %v, skipping", intent.Namespace())
			} else {
				log.Logf(log.Info, "dropping collection %v before syncing", intent.Namespace())
				err = rsync.DropCollection(intent)
				if err != nil {
					return false, err // no context needed
				}
				collectionExists = false
			}
		} else {
			log.Logf(log.DebugLow, "collection %v doesn't exist, skipping drop command", intent.Namespace())
		}
	}

	var options bson.D
	var indexes []IndexDocument

	// get indexes from system.indexes dump if we have it but don't have metadata files
	/*	if intent.MetadataFile == nil {
		if _, ok := rsync.dbCollectionIndexes[intent.DB]; ok {
			if indexes, ok = rsync.dbCollectionIndexes[intent.DB][intent.C]; ok {
				log.Logf(log.Always, "no metadata; falling back to system.indexes")
			}
		}
	}*/

	// logMessageSuffix := "with no metadata"
	// first create the collection with options from the metadata file
	/*	if intent.MetadataFile != nil {
			logMessageSuffix = "using options from metadata"
			err = intent.MetadataFile.Open()
			if err != nil {
				return err
			}
			defer intent.MetadataFile.Close()

			log.Logf(log.Always, "reading metadata for %v from %v", intent.Namespace(), intent.MetadataLocation)
			metadata, err := ioutil.ReadAll(intent.MetadataFile)
			if err != nil {
				return fmt.Errorf("error reading metadata from %v: %v", intent.MetadataLocation, err)
			}
			options, indexes, err = rsync.MetadataFromJSON(metadata)
			if err != nil {
				return fmt.Errorf("error parsing metadata from %v: %v", intent.MetadataLocation, err)
			}
			if rsync.OutputOptions.NoOptionsRsync {
				log.Log(log.Info, "not rsyncing collection options")
				logMessageSuffix = "with no collection options"
				options = nil
			}
		}
	*/
	// copy that metadata and options, indexes from top on

	/*
		from source fsession start
	*/
	/*
		from source fsession end
	*/
	// options, indexes, err = rsync.MetadataFromJSON(metadata)
	options, indexes, err = rsync.MetadataFromJSON(intent)
	// options, indexes, err = rsync.RsyncMetadata(intent)
	if err != nil {
		return false, fmt.Errorf("error parsing metadata from %v: %v", intent.C, err)
	}

	if !collectionExists {
		// log.Logf(log.Info, "creating collection %v %s", intent.Namespace(), logMessageSuffix)
		log.Logf(log.DebugHigh, "using collection options: %#v", options)
		err = rsync.CreateCollection(intent, options)
		if err != nil {
			return false, fmt.Errorf("error creating collection %v: %v", intent.Namespace(), err)
		}
	} else {
		log.Logf(log.Info, "collection %v already exists - skipping collection create", intent.Namespace())
	}

	var documentCount int64
	/*	if intent.BSONFile != nil {
		err = intent.BSONFile.Open()
		if err != nil {
			return err
		}
		defer intent.BSONFile.Close()

		log.Logf(log.Always, "rsyncing %v from %v", intent.Namespace(), intent.Location)

		bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(intent.BSONFile))
		defer bsonSource.Close()
	*/
	// bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(intent.BSONFile))

	documentCount, err = rsync.RsyncCollectionToDB(intent.DB, intent.C, intent)
	if err != nil {
		return false, fmt.Errorf("error syncing from %v: %v", intent.DB, err)
	}
	// }

	// finally, add indexes finished the db rsync ,but index and then.....
	// log.Logf(log.Always, "indexes  where ..%v....syncing indexes for collection %v from", indexes, intent.Namespace())
	if len(indexes) > 0 && !rsync.OutputOptions.NoIndexRsync {
		log.Logf(log.Always, "syncing indexes for collection %v", intent.Namespace())
		err = rsync.CreateIndexes(intent, indexes)
		if err != nil {
			return false, fmt.Errorf("error creating indexes for %v: %v", intent.Namespace(), err)
		}
	} else {
		log.Log(log.Always, "no indexes to sync")
	}

	log.Logf(log.Always, "finished syncing %v (%v %v)",
		intent.Namespace(), documentCount, util.Pluralize(int(documentCount), "document", "documents"))
	return false, nil
}

// RsyncCollectionToDB pipes the given BSON data into the database.
// Returns the number of documents rsyncd and any errors that occured.
func (rsync *MongoRsync) RsyncCollectionToDB(dbName, colName string, intent *intents.Intent) (int64, error) {

	var termErr error
	session, err := rsync.SessionProvider.GetSession()
	if err != nil {
		return int64(0), fmt.Errorf("error establishing connection: %v", err)
	}
	session.SetSafe(rsync.safety)
	defer session.Close()

	collection := session.DB(dbName).C(colName)

	documentCount := int64(0)

	/*
		queryiter set start
	*/
	fsession, err := rsync.FSessionProvider.GetSession()
	if err != nil {
		return int64(0), fmt.Errorf("error establishing connection: %v", err)
	}
	defer fsession.Close()
	// in mgo, setting prefetch = 1.0 causes the driver to make requests for
	// more results as soon as results are returned. This effectively
	// duplicates the behavior of an exhaust cursor.
	fsession.SetPrefetch(1.0)

	var findQuery *mgo.Query
	var iter *mgo.Iter
	var total int
	switch {
	case len(rsync.query) > 0:
		findQuery = fsession.DB(intent.DB).C(intent.C).Find(rsync.query)

	case rsync.InputOptions.TableScan:
		// ---forceTablesScan runs the query without snapshot enabled
		findQuery = fsession.DB(intent.DB).C(intent.C).Find(nil)
	default:
		findQuery = fsession.DB(intent.DB).C(intent.C).Find(nil).Snapshot()

	}

	if len(rsync.query) == 0 {
		total, err = findQuery.Count()
		if err != nil {
			return int64(0), fmt.Errorf("error reading from db: %v", err)
		}
		log.Logf(log.DebugLow, "counted %v %v in %v", total, docPlural(int64(total)), intent.Namespace())
	} else {
		log.Logf(log.DebugLow, "not counting query on %v", intent.Namespace())
	}
	iter = findQuery.Iter()
	/*
		queryiter stop end
	*/
	watchProgressor := progress.NewCounter(int64(total))
	bar := &progress.Bar{
		Name:      fmt.Sprintf("%v.%v", dbName, colName),
		Watching:  watchProgressor,
		BarLength: progressBarLength,
	}
	rsync.progressManager.Attach(bar)
	defer rsync.progressManager.Detach(bar)
	// _, dumpCount := watchProgressor.Progress()

	maxInsertWorkers := rsync.OutputOptions.NumInsertionWorkers
	if rsync.OutputOptions.MaintainInsertionOrder {
		maxInsertWorkers = 1
	}

	docChan := make(chan bson.Raw, insertBufferFactor)
	resultChan := make(chan error, maxInsertWorkers)

	// stream documents for this collection on docChan
	go func() {
		doc := bson.Raw{}
		// for bsonSource.Next(&doc) {
		for iter.Next(&doc) {
			select {
			case <-rsync.shutdownIntentsNotifier.notified:
				log.Logf(log.DebugHigh, "terminating writes to target ")
				termErr = util.ErrTerminated
				close(docChan)
				return
			default:
				rawBytes := make([]byte, len(doc.Data))
				copy(rawBytes, doc.Data)
				docChan <- bson.Raw{Data: rawBytes}
				documentCount++
			}
		}
		close(docChan)
	}()

	log.Logf(log.DebugLow, "using %v insertion workers", maxInsertWorkers)

	for i := 0; i < maxInsertWorkers; i++ {
		go func() {
			// get a session copy for each insert worker
			s := session.Copy()
			defer s.Close()

			coll := collection.With(s)
			bulk := db.NewBufferedBulkInserter(
				coll, rsync.ToolOptions.BulkBufferSize, !rsync.OutputOptions.StopOnError)
			for rawDoc := range docChan {
				if rsync.objCheck {
					err := bson.Unmarshal(rawDoc.Data, &bson.D{})
					if err != nil {
						resultChan <- fmt.Errorf("invalid object: %v", err)
						return
					}
				}
				if err := bulk.Insert(rawDoc); err != nil {
					if db.IsConnectionError(err) || rsync.OutputOptions.StopOnError {
						// Propagate this error, since it's either a fatal connection error
						// or the user has turned on --stopOnError
						resultChan <- err
					} else {
						// Otherwise just log the error but don't propagate it.
						log.Logf(log.Always, "error: %v", err)
					}
				}
				// right here ,right now,should be modify
				watchProgressor.Inc(1)
			}
			err := bulk.Flush()
			if err != nil {
				if !db.IsConnectionError(err) && !rsync.OutputOptions.StopOnError {
					// Suppress this error since it's not a severe connection error and
					// the user has not specified --stopOnError
					log.Logf(log.Always, "error: %v", err)
					err = nil
				}
			}
			resultChan <- err
			return
		}()

		// sleep to prevent all threads from inserting at the same time at start
		time.Sleep(time.Duration(i) * 10 * time.Millisecond)
	}

	// wait until all insert jobs finish
	for done := 0; done < maxInsertWorkers; done++ {
		err := <-resultChan
		if err != nil {
			return int64(0), fmt.Errorf("insertion error: %v", err)
		}
	}

	// final error check
	if err = iter.Err(); err != nil {
		return int64(0), fmt.Errorf("reading fsession db data : %v", err)
	}
	return documentCount, termErr
}

// shouldSkipCollection returns true when a collection name is excluded
// by the mongodump options.
func (rsync *MongoRsync) shouldSkipCollection(colName string) bool {
	for _, excludedCollection := range rsync.InputOptions.ExcludedCollections {
		if colName == excludedCollection {
			return true
		}
	}
	for _, excludedCollectionPrefix := range rsync.InputOptions.ExcludedCollectionPrefixes {
		if strings.HasPrefix(colName, excludedCollectionPrefix) {
			return true
		}
	}
	return false
}
