package mongobackup

import (
	"fmt"
	"mongoIncbackup-1.1/common/db"
	"mongoIncbackup-1.1/common/log"
	"mongoIncbackup-1.1/common/util"
	"mongoIncbackup-1.1/mgo.v2/bson"
)

// determineOplogCollectionName uses a command to infer
// the name of the oplog collection in the connected db
func (backup *MongoBackup) determineOplogCollectionName() error {
	masterDoc := bson.M{}
	err := backup.sessionProvider.Run("isMaster", &masterDoc, "admin")
	if err != nil {
		return fmt.Errorf("error running command: %v", err)
	}
	if _, ok := masterDoc["hosts"]; ok {
		log.Logf(log.DebugLow, "determined cluster to be a replica set")
		log.Logf(log.DebugHigh, "oplog located in local.oplog.rs")
		backup.oplogCollection = "oplog.rs"
		return nil
	}
	if isMaster := masterDoc["ismaster"]; util.IsFalsy(isMaster) {
		log.Logf(log.Info, "mongobackup is not connected to a master")
		return fmt.Errorf("not connected to master")
	}

	log.Logf(log.DebugLow, "not connected to a replica set, assuming master/slave")
	log.Logf(log.DebugHigh, "oplog located in local.oplog.$main")
	backup.oplogCollection = "oplog.$main"
	return nil

}

// getOplogStartTime returns the most recent oplog entry
func (backup *MongoBackup) getOplogStartTime() (bson.MongoTimestamp, error) {
	mostRecentOplogEntry := db.Oplog{}

	err := backup.sessionProvider.FindOne("local", backup.oplogCollection, 0, nil, []string{"-$natural"}, &mostRecentOplogEntry, 0)
	if err != nil {
		return 0, err
	}
	return mostRecentOplogEntry.Timestamp, nil
}

// checkOplogTimestampExists checks to make sure the oplog hasn't rolled over
// since mongobackup started. It does this by checking the oldest oplog entry
// still in the database and making sure it happened at or before the timestamp
// captured at the start of the backup.
func (backup *MongoBackup) checkOplogTimestampExists(ts bson.MongoTimestamp) (bool, error) {
	oldestOplogEntry := db.Oplog{}
	err := backup.sessionProvider.FindOne("local", backup.oplogCollection, 0, nil, []string{"+$natural"}, &oldestOplogEntry, 0)
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

// BackupOplogAfterTimestamp takes a timestamp and writer and backups all oplog entries after
// the given timestamp to the writer. Returns any errors that occur.
func (backup *MongoBackup) BackupOplogAfterTimestamp(ts bson.MongoTimestamp) error {
	session, err := backup.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()
	intent := backup.manager.Oplog()
	err = intent.BSONFile.Open()
	if err != nil {
		return fmt.Errorf("error opening output stream for backuping oplog: %v", err)
	}
	defer intent.BSONFile.Close()
	session.SetPrefetch(1.0) // mimic exhaust cursor
	queryObj := bson.M{"ts": bson.M{"$gt": ts}}
	// right here got tail -1
	// oplogQuery := session.DB("local").C(backup.oplogCollection).Find(queryObj).LogReplay()
	oplogQuery := session.DB("local").C(backup.oplogCollection).Find(queryObj)
	oplogCount, err := backup.backupQueryToWriterM(oplogQuery, backup.manager.Oplog())
	if err == nil {
		log.Logf(log.Always, "\tbackuped %v oplog %v",
			oplogCount, util.Pluralize(int(oplogCount), "entry", "entries"))
	}
	return err
}
