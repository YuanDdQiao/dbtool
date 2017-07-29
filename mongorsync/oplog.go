package mongorsync

import (
	"fmt"
	"mongorsync-1.1/common/db"
	// "mongorsync-1.1/common/intents"
	"mongorsync-1.1/common/log"
	"mongorsync-1.1/common/progress"
	"mongorsync-1.1/common/util"
	"mongorsync-1.1/mgo.v2"
	"mongorsync-1.1/mgo.v2/bson"
	"strconv"
	"strings"
	"time"
)

// oplogMaxCommandSize sets the maximum size for multiple buffered ops in the
// applyOps command. This is to prevent pathological cases where the array overhead
// of many small operations can overflow the maximum command size.
// Note that ops > 8MB will still be buffered, just as single elements.
const oplogMaxCommandSize = 1024 * 1024 * 8
const timer_count = 1 * 1000000000

// RsyncOplog attempts to rsync a MongoDB oplog.
func (rsync *MongoRsync) RsyncOplog() error {
	log.Log(log.Always, "replaying oplog")
	intent := rsync.managerOplog.Oplog()
	if intent == nil {
		// this should not be reached
		log.Log(log.Always, "no oplog.rs in root of the sync source, skipping oplog application")
		return nil
	}
	/*if err := intent.BSONFile.Open(); err != nil {
		return err
	}
	if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
		fileNeedsIOBuffer.TakeIOBuffer(make([]byte, db.MaxBSONSize))
	}
	defer intent.BSONFile.Close()
	*/
	// NewBufferlessBSONSource reads each bson document into its own buffer
	// because bson.Unmarshal currently can't unmarshal binary types without
	// them referencing the source buffer
	/*	bsonSource := db.NewDecodedBSONSource(db.NewBufferlessBSONSource(intent.BSONFile))
		defer bsonSource.Close()
	*/
	/*
	   from source oplog.rs raw start
	*/
	var findQuery *mgo.Query
	var iter *mgo.Iter
	// var total int

	// log.Logf(log.Always, "。。。。。。。。。。。%v,%v", intent.DB, intent.C)
	/*	if len(rsync.query) == 0 {
			total, err = findQuery.Count()
			if err != nil {
				return fmt.Errorf("error reading from oplog.rs : %v", err)
			}
			log.Logf(log.DebugLow, "counted %v %v in %v", total, docPlural(int64(total)), intent.Namespace())
		} else {
			log.Logf(log.DebugLow, "not counting query on %v", intent.Namespace())
		}

	*/
	/*
	   from source oplog.rs raw  end
	*/

	rawOplogEntry := &bson.Raw{}
	entryAsOplog := db.Oplog{}
	var totalOps int64
	var entrySize int
	var strStr bson.MongoTimestamp
	// var barStr string

	// oplogProgressor := progress.NewCounter(totalOps)
	oplogProgressor := progress.NewCounterOp(totalOps, strStr)
	bar := progress.BarOp{
		Name: "oplog\t",
		// "Last oplog " + string(entryAsOplog.Timestamp),
		WatchingOp: oplogProgressor,
		WaitTime:   3 * time.Second,
		Writer:     log.Writer(0),
		BarLength:  progressBarLength,
		IsBytes:    true,
	}
	bar.StartOp()
	defer bar.StopOp()

	session, err := rsync.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error establishing connection: %v", err)
	}
	defer session.Close()

	/*get session from source*/
	fsession, err := rsync.FSessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error establishing connection: %v", err)
	}
	defer fsession.Close()
	// in mgo, setting prefetch = 1.0 causes the driver to make requests for
	// more results as soon as results are returned. This effectively
	// duplicates the behavior of an exhaust cursor.
	fsession.SetPrefetch(1.0)

	// take IsStale
	rsync.oplogLastOptimeFor, err = rsync.getOplogLastTime()
	if err != nil {
		return fmt.Errorf("error current time : %v", err)
	}
	if rsync.oplogLastOptimeFor >= rsync.oplogStart {

		return fmt.Errorf("error,too stale to catch up: %v", rsync.oplogLastOptimeFor)
	}
	// fsession.DB("local").C("oplog.rs").Find(nil).One(result)
	// get last fsession.op
	// for {
	/*
		establishing options from source db
	*/
	// switch {
	// case len(rsync.query) > 0:
	// 	// findQuery = fsession.DB(intent.DB).C(intent.C).Find(rsync.query)
	// 	findQuery = fsession.DB("local").C("oplog.rs").Find(bson.M{"ns": intent.DB + "." + intent.C})
	// case len(rsync.query) == 0:
	// findQuery = fsession.DB(intent.DB).C(intent.C).Find(rsync.query)
	rsync.oplogStime, _ = ParseTimestampFlag(rsync.InputOptions.OSt)
	rsync.oplogEtime, _ = ParseTimestampFlag(rsync.InputOptions.OEt)

	switch {
	case len(rsync.InputOptions.OSt) != 0 && len(rsync.InputOptions.OEt) != 0:
		findQuery = fsession.DB("local").C("oplog.rs").Find(bson.M{"ts": bson.M{"$gte": rsync.oplogStime, "$lte": rsync.oplogEtime}})
	case len(rsync.InputOptions.OSt) != 0 && len(rsync.InputOptions.OEt) == 0:
		findQuery = fsession.DB("local").C("oplog.rs").Find(bson.M{"ts": bson.M{"$gte": rsync.oplogStime}})
	case len(rsync.InputOptions.OSt) == 0 && len(rsync.InputOptions.OEt) != 0:
		findQuery = fsession.DB("local").C("oplog.rs").Find(bson.M{"ts": bson.M{"$lte": rsync.oplogEtime}})
	default:
		findQuery = fsession.DB("local").C("oplog.rs").Find(nil)
	}
	// iter = findQuery.Iter()
	iter = findQuery.Sort("$natural").Tail(-1)
	/*
		end
	*/
	rsync.oplogLastOptimeFor, err = rsync.getOplogLastTime()
	if err != nil {
		return fmt.Errorf("error current time : %v", err)
	}
	if rsync.oplogLastOptimeFor >= rsync.oplogStart {

		return fmt.Errorf("error,too stale to catch up: %v", rsync.oplogLastOptimeFor)
	}

	for iter.Next(rawOplogEntry) {

		for len(rawOplogEntry.Data) == 0 {
			log.Logf(log.Always, "No new oplogs,the data is the newest ^_^.waiting.")
			time.Sleep(timer_count)
		}
		// entrySize = 0
		entrySize = len(rawOplogEntry.Data)

		err = bson.Unmarshal(rawOplogEntry.Data, &entryAsOplog)
		if err != nil {
			return fmt.Errorf("error reading oplog: %v", err)
		}
		if entryAsOplog.Operation == "n" {
			//skip no-ops
			continue
		}
		if !rsync.TimestampBeforeLimit(entryAsOplog.Timestamp) && len(rsync.InputOptions.OEt) != 0 {
			log.Logf(
				log.DebugLow,
				"timestamp %v is not below limit of %v; ending oplog sync",
				entryAsOplog.Timestamp,
				rsync.InputOptions.OEt,
			)
			break
		}

		totalOps++
		oplogProgressor.IncOp(int64(entrySize), entryAsOplog.Timestamp)
		err = rsync.ApplyOps(session, []interface{}{entryAsOplog})
		if err != nil {
			return fmt.Errorf("error applying oplog: %v", err)
		}
		// log.Logf(log.Always, "Synced up to optime:%v", entryAsOplog.Timestamp)
		// }
		// barStr = "Last oplog " + string(entryAsOplog.Timestamp) + "\t"
	}
	iter.Close()

	log.Logf(log.Info, "applied %v ops", totalOps)
	return nil

}

// ApplyOps is a wrapper for the applyOps database command, we pass in
// a session to avoid opening a new connection for a few inserts at a time.
func (rsync *MongoRsync) ApplyOps(session *mgo.Session, entries []interface{}) error {
	res := bson.M{}
	err := session.Run(bson.D{{"applyOps", entries}}, &res)
	if err != nil {
		return fmt.Errorf("applyOps: %v", err)
	}
	if util.IsFalsy(res["ok"]) {
		return fmt.Errorf("applyOps command: %v", res["errmsg"])
	}

	return nil
}

// TimestampBeforeLimit returns true if the given timestamp is allowed to be
// applied to mongorsync's target database.
func (rsync *MongoRsync) TimestampBeforeLimit(ts bson.MongoTimestamp) bool {

	if len(rsync.InputOptions.OEt) == 0 {
		// always valid if there is no --oplogLimit set
		return true
	}
	return ts < rsync.oplogEtime
}

// ParseTimestampFlag takes in a string the form of <time_t>:<ordinal>,
// where <time_t> is the seconds since the UNIX epoch, and <ordinal> represents
// a counter of operations in the oplog that occurred in the specified second.
// It parses this timestamp string and returns a bson.MongoTimestamp type.
func ParseTimestampFlag(ts string) (bson.MongoTimestamp, error) {
	var seconds, increment int
	timestampFields := strings.Split(ts, ":")
	if len(timestampFields) > 2 {
		return 0, fmt.Errorf("too many : characters")
	}
	seconds, err := strconv.Atoi(timestampFields[0])
	if err != nil {
		return 0, fmt.Errorf("error parsing timestamp seconds: %v", err)
	}

	// parse the increment field if it exists
	if len(timestampFields) == 2 {
		if len(timestampFields[1]) > 0 {
			increment, err = strconv.Atoi(timestampFields[1])
			if err != nil {
				return 0, fmt.Errorf("error parsing timestamp increment: %v", err)
			}
		} else {
			// handle the case where the user writes "<time_t>:" with no ordinal
			increment = 0
		}
	}

	timestamp := (int64(seconds) << 32) | int64(increment)
	return bson.MongoTimestamp(timestamp), nil
}

// source db oplog

func (rsync *MongoRsync) determineOplogCollectionName() error {
	masterDoc := bson.M{}
	err := rsync.FSessionProvider.Run("isMaster", &masterDoc, "admin")
	if err != nil {
		return fmt.Errorf("error running command: %v", err)
	}
	if _, ok := masterDoc["hosts"]; ok {
		log.Logf(log.DebugLow, "determined cluster to be a replica set")
		log.Logf(log.DebugHigh, "oplog located in local.oplog.rs")
		rsync.oplogCollection = "oplog.rs"
		return nil
	}
	if isMaster := masterDoc["ismaster"]; util.IsFalsy(isMaster) {
		log.Logf(log.Info, "mongodump is not connected to a master")
		return fmt.Errorf("not connected to master")
	}

	log.Logf(log.DebugLow, "not connected to a replica set, assuming master/slave")
	log.Logf(log.DebugHigh, "oplog located in local.oplog.$main")
	rsync.oplogCollection = "oplog.$main"
	return nil

}

// getOplogStartTime returns the most recent oplog entry
func (rsync *MongoRsync) getOplogStartTime() (bson.MongoTimestamp, error) {
	mostRecentOplogEntry := db.Oplog{}

	err := rsync.FSessionProvider.FindOne("local", "oplog.rs", 0, nil, []string{"-$natural"}, &mostRecentOplogEntry, 0)
	// err := rsync.FSessionProvider.FindOne("local", rsync.oplogCollection, 0, nil, []string{"-$natural"}, &mostRecentOplogEntry, 0)
	if err != nil {
		return 0, err
	}
	return mostRecentOplogEntry.Timestamp, nil
}
func (rsync *MongoRsync) getOplogLastTime() (bson.MongoTimestamp, error) {
	mostRecentOplogEntry := db.Oplog{}

	err := rsync.FSessionProvider.FindOne("local", "oplog.rs", 0, nil, []string{"$natural"}, &mostRecentOplogEntry, 0)
	// err := rsync.FSessionProvider.FindOne("local", rsync.oplogCollection, 0, nil, []string{"-$natural"}, &mostRecentOplogEntry, 0)
	if err != nil {
		return 0, err
	}
	return mostRecentOplogEntry.Timestamp, nil
}

// checkOplogTimestampExists checks to make sure the oplog hasn't rolled over
// since mongodump started. It does this by checking the oldest oplog entry
// still in the database and making sure it happened at or before the timestamp
// captured at the start of the dump.
func (rsync *MongoRsync) checkOplogTimestampExists(ts bson.MongoTimestamp) (bool, error) {
	oldestOplogEntry := db.Oplog{}
	err := rsync.SessionProvider.FindOne("local", rsync.oplogCollection, 0, nil, []string{"+$natural"}, &oldestOplogEntry, 0)
	if err != nil {
		return false, fmt.Errorf("unable to read entry from oplog: %v", err)
	}

	log.Logf(log.DebugHigh, "oldest oplog entry has timestamp %v", oldestOplogEntry.Timestamp)
	if oldestOplogEntry.Timestamp > ts {
		log.Logf(log.Info, "oldest oplog entry of timestamp %v is older than %v",
			oldestOplogEntry.Timestamp, ts)
		return false, nil
	}
	return true, nil
}

// DumpOplogAfterTimestamp takes a timestamp and writer and dumps all oplog entries after
// the given timestamp to the writer. Returns any errors that occur.
/*func (rsync *MongoRsync) DumpOplogAfterTimestamp(ts bson.MongoTimestamp) error {
	session, err := rsync.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()
	intent := rsync.manager.Oplog()
	err = intent.BSONFile.Open()
	if err != nil {
		return fmt.Errorf("error opening output stream for dumping oplog: %v", err)
	}
	defer intent.BSONFile.Close()
	session.SetPrefetch(1.0) // mimic exhaust cursor
	queryObj := bson.M{"ts": bson.M{"$gt": ts}}
	oplogQuery := session.DB("local").C(rsync.oplogCollection).Find(queryObj).LogReplay()
	oplogCount, err := rsync.dumpQueryToWriter(oplogQuery, rsync.manager.Oplog())
	if err == nil {
		log.Logf(log.Always, "\tdumped %v oplog %v",
			oplogCount, util.Pluralize(int(oplogCount), "entry", "entries"))
	}
	return err
}
*/
