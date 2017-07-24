package mongorsync

import (
	"fmt"
	"mongorsync-1.1/common/bsonutil"
	"mongorsync-1.1/common/db"
	"mongorsync-1.1/common/intents"
	"mongorsync-1.1/common/json"
	"mongorsync-1.1/common/log"
	"mongorsync-1.1/common/util"
	"mongorsync-1.1/mgo.v2"
	"mongorsync-1.1/mgo.v2/bson"
	"strings"
)

// Specially treated rsync collection types.
const (
	Users = "users"
	Roles = "roles"
)

// struct for working with auth versions
type authVersionPair struct {
	// Dump is the auth version of the users/roles collection files in the target dump directory
	Dump int
	// Server is the auth version of the connected MongoDB server
	Server int
}

// Metadata holds information about a collection's options and indexes.
type Metadata struct {
	Options bson.D          `json:"options,omitempty"`
	Indexes []IndexDocument `json:"indexes"`
}

// this struct is used to read in the options of a set of indexes
type metaDataMapIndex struct {
	Indexes []bson.M `json:"indexes"`
}

// IndexDocument holds information about a collection's index.
type IndexDocument struct {
	Options bson.M `bson:",inline"`
	Key     bson.D `bson:"key"`
}
type MetadataF struct {
	Options interface{}   `json:"options,omitempty"`
	Indexes []interface{} `json:"indexes"`
}

// MetadataFromJSON takes a slice of JSON bytes and unmarshals them into usable
// collection options and indexes for rsyncing collections.
// right here that to be continue
func (rsync *MongoRsync) MetadataFromJSON(intent *intents.Intent) (bson.D, []IndexDocument, error) {

	/*	if len(jsonBytes) == 0 {
			// skip metadata parsing if the file is empty
			return nil, nil, nil
		}

	*/
	meta := &Metadata{}
	metaf := MetadataF{
		// We have to initialize Indexes to an empty slice, not nil, so that an empty
		// array is marshalled into json instead of null. That is, {indexes:[]} is okay
		// but {indexes:null} will cause assertions in our legacy C++ mongotools
		Indexes: []interface{}{},
	}

	// get index start
	// log.Logf(log.DebugHigh, "\treading indexes for `%v`", nsID)

	fsession, err := rsync.FSessionProvider.GetSession()
	if err != nil {
		return nil, nil, err
	}
	defer fsession.Close()

	// get the indexes
	indexesIter, err := db.GetIndexes(fsession.DB(intent.DB).C(intent.C))
	if err != nil {
		return nil, nil, err
	}
	if indexesIter == nil {
		log.Logf(log.Always, "the collection %v appears to have been dropped after the sync started", intent.Namespace())
		return nil, nil, nil
	}
	indexOpts := &bson.D{}
	for indexesIter.Next(indexOpts) {
		convertedIndex, err := bsonutil.ConvertBSONValueToJSON(*indexOpts)
		if err != nil {
			return nil, nil, fmt.Errorf("error converting index (%#v): %v", convertedIndex, err)
		}
		metaf.Indexes = append(metaf.Indexes, convertedIndex)
	}

	if err := indexesIter.Err(); err != nil {
		return nil, nil, fmt.Errorf("error getting indexes for collection :%v", err)
	}

	// Finally, we send the results to the writer as JSON bytes
	jsonBytes, err := json.Marshal(metaf)
	if err != nil {
		return nil, nil, fmt.Errorf("error marshalling metadata json for collection :%v", err)
	}
	// get index end

	// err := json.Unmarshal(jsonBytes, meta)
	// if err != nil {
	// 	return nil, nil, err
	// }
	err = json.Unmarshal(jsonBytes, meta)
	if err != nil {
		return nil, nil, err
	}

	// first get the ordered key information for each index,
	// then merge it with a set of options stored as a map
	metaAsMap := metaDataMapIndex{}
	err = json.Unmarshal(jsonBytes, &metaAsMap)
	if err != nil {
		return nil, nil, fmt.Errorf("error unmarshalling metadata as map: %v", err)
	}
	for i := range meta.Indexes {
		// remove "key" from the map so we can decode it properly later
		delete(metaAsMap.Indexes[i], "key")

		// parse extra index fields
		meta.Indexes[i].Options = metaAsMap.Indexes[i]
		if err := bsonutil.ConvertJSONDocumentToBSON(meta.Indexes[i].Options); err != nil {
			return nil, nil, fmt.Errorf("extended json error: %v", err)
		}

		// parse the values of the index keys, so we can support extended json
		for pos, field := range meta.Indexes[i].Key {
			meta.Indexes[i].Key[pos].Value, err = bsonutil.ParseJSONValue(field.Value)
			if err != nil {
				return nil, nil, fmt.Errorf("extended json in '%v' field: %v", field.Name, err)
			}
		}
	}

	// parse the values of options fields, to support extended json
	meta.Options, err = bsonutil.GetExtendedBsonD(meta.Options)
	if err != nil {
		return nil, nil, fmt.Errorf("extended json in 'options': %v", err)
	}

	return meta.Options, meta.Indexes, nil
}

// LoadIndexesFromBSON reads indexes from the index BSON files and
// caches them in the MongoRsync object.
/*func (rsync *MongoRsync) LoadIndexesFromBSON() error {

	dbCollectionIndexes := make(map[string]collectionIndexes)

	for _, dbname := range rsync.manager.SystemIndexDBs() {
		dbCollectionIndexes[dbname] = make(collectionIndexes)
		intent := rsync.manager.SystemIndexes(dbname)
		err := intent.BSONFile.Open()
		if err != nil {
			return err
		}
		defer intent.BSONFile.Close()
		bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(intent.BSONFile))
		defer bsonSource.Close()

		// iterate over stored indexes, saving all that match the collection
		indexDocument := &IndexDocument{}
		for bsonSource.Next(indexDocument) {
			namespace := indexDocument.Options["ns"].(string)
			dbCollectionIndexes[dbname][stripDBFromNS(namespace)] =
				append(dbCollectionIndexes[dbname][stripDBFromNS(namespace)], *indexDocument)
		}
		if err := bsonSource.Err(); err != nil {
			return fmt.Errorf("error scanning system.indexes: %v", err)
		}
	}
	rsync.dbCollectionIndexes = dbCollectionIndexes
	return nil
}
*/
func stripDBFromNS(ns string) string {
	i := strings.Index(ns, ".")
	if i > 0 && i < len(ns) {
		return ns[i+1:]
	}
	return ns
}

// CollectionExists returns true if the given intent's collection exists.
func (rsync *MongoRsync) CollectionExists(intent *intents.Intent) (bool, error) {
	rsync.knownCollectionsMutex.Lock()
	defer rsync.knownCollectionsMutex.Unlock()

	// make sure the map exists
	if rsync.knownCollections == nil {
		rsync.knownCollections = map[string][]string{}
	}

	// first check if we haven't done listCollections for this database already
	if rsync.knownCollections[intent.DB] == nil {
		// if the database name isn't in the cache, grab collection
		// names from the server
		session, err := rsync.SessionProvider.GetSession()
		if err != nil {
			return false, fmt.Errorf("error establishing connection: %v", err)
		}
		defer session.Close()
		collections, err := session.DB(intent.DB).CollectionNames()
		if err != nil {
			return false, err
		}
		// update the cache
		rsync.knownCollections[intent.DB] = collections
	}

	// now check the cache for the given collection name
	exists := util.StringSliceContains(rsync.knownCollections[intent.DB], intent.C)
	return exists, nil
}

// CreateIndexes takes in an intent and an array of index documents and
// attempts to create them using the createIndexes command. If that command
// fails, we fall back to individual index creation.
func (rsync *MongoRsync) CreateIndexes(intent *intents.Intent, indexes []IndexDocument) error {
	// first, sanitize the indexes
	for _, index := range indexes {
		// update the namespace of the index before inserting
		index.Options["ns"] = intent.Namespace()

		// check for length violations before building the command
		fullIndexName := fmt.Sprintf("%v.$%v", index.Options["ns"], index.Options["name"])
		if len(fullIndexName) > 127 {
			return fmt.Errorf(
				"cannot sync index with namespace '%v': "+
					"namespace is too long (max size is 127 bytes)", fullIndexName)
		}

		// remove the index version, forcing an update,
		// unless we specifically want to keep it
		if !rsync.OutputOptions.KeepIndexVersion {
			delete(index.Options, "v")
		}
	}

	session, err := rsync.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error establishing connection: %v", err)
	}
	session.SetSafe(&mgo.Safe{})
	defer session.Close()

	// then attempt the createIndexes command
	rawCommand := bson.D{
		{"createIndexes", intent.C},
		{"indexes", indexes},
	}
	results := bson.M{}
	err = session.DB(intent.DB).Run(rawCommand, &results)
	if err == nil {
		return nil
	}
	if err.Error() != "no such cmd: createIndexes" {
		return fmt.Errorf("createIndex error: %v", err)
	}

	// if we're here, the connected server does not support the command, so we fall back
	log.Log(log.Info, "\tcreateIndexes command not supported, attemping legacy index insertion")
	for _, idx := range indexes {
		log.Logf(log.Info, "\tmanually creating index %v", idx.Options["name"])
		err = rsync.LegacyInsertIndex(intent, idx)
		if err != nil {
			return fmt.Errorf("error creating index %v: %v", idx.Options["name"], err)
		}
	}
	return nil
}

// LegacyInsertIndex takes in an intent and an index document and attempts to
// create the index on the "system.indexes" collection.
func (rsync *MongoRsync) LegacyInsertIndex(intent *intents.Intent, index IndexDocument) error {
	session, err := rsync.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error establishing connection: %v", err)
	}
	defer session.Close()

	// overwrite safety to make sure we catch errors
	session.SetSafe(&mgo.Safe{})
	indexCollection := session.DB(intent.DB).C("system.indexes")
	err = indexCollection.Insert(index)
	if err != nil {
		return fmt.Errorf("insert error: %v", err)
	}

	return nil
}

// CreateCollection creates the collection specified in the intent with the
// given options.
func (rsync *MongoRsync) CreateCollection(intent *intents.Intent, options bson.D) error {
	command := append(bson.D{{"create", intent.C}}, options...)

	session, err := rsync.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error establishing connection: %v", err)
	}
	defer session.Close()

	res := bson.M{}
	err = session.DB(intent.DB).Run(command, &res)
	if err != nil {
		return fmt.Errorf("error running create command: %v", err)
	}
	if util.IsFalsy(res["ok"]) {
		return fmt.Errorf("create command: %v", res["errmsg"])
	}
	return nil
}

// RsyncUsersOrRoles accepts a users intent and a roles intent, and rsyncs
// them via _mergeAuthzCollections. Either or both can be nil. In the latter case
// nothing is done.
func (rsync *MongoRsync) RsyncUsersOrRoles(users, roles *intents.Intent) error {

	type loopArg struct {
		intent             *intents.Intent
		intentType         string
		mergeParamName     string
		tempCollectionName string
	}

	if users == nil && roles == nil {
		return nil
	}

	if users != nil && roles != nil && users.DB != roles.DB {
		return fmt.Errorf("can't rsync users and roles to different databases, %v and %v", users.DB, roles.DB)
	}

	args := []loopArg{}
	mergeArgs := bson.D{}
	userTargetDB := ""

	if users != nil {
		args = append(args, loopArg{users, "users", "tempUsersCollection", rsync.tempUsersCol})
	}
	if roles != nil {
		args = append(args, loopArg{roles, "roles", "tempRolesCollection", rsync.tempRolesCol})
	}

	session, err := rsync.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error establishing connection: %v", err)
	}
	defer session.Close()

	// For each of the users and roles intents:
	//   build up the mergeArgs component of the _mergeAuthzCollections command
	//   upload the BSONFile to a temporary collection
	for _, arg := range args {

		/*		if arg.intent.Size == 0 {
				// MongoDB complains if we try and remove a non-existent collection, so we should
				// just skip auth collections with empty .bson files to avoid gnarly logic later on.
				log.Logf(log.Always, "%v file '%v' is empty; skipping %v restoration", arg.intentType, arg.intent.Location, arg.intentType)
			}*/
		// log.Logf(log.Always, "rsyncing %v from %v", arg.intentType, arg.intent.Location)
		mergeArgs = append(mergeArgs, bson.DocElem{arg.mergeParamName, "admin." + arg.tempCollectionName})

		/*		err := arg.intent.BSONFile.Open()
				if err != nil {
					return err
				}
				defer arg.intent.BSONFile.Close()
				bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(arg.intent.BSONFile))
				defer bsonSource.Close()
		*/
		tempCollectionNameExists, err := rsync.CollectionExists(&intents.Intent{DB: "admin", C: arg.tempCollectionName})
		if err != nil {
			return err
		}
		if tempCollectionNameExists {
			log.Logf(log.Info, "dropping preexisting temporary collection admin.%v", arg.tempCollectionName)
			err = session.DB("admin").C(arg.tempCollectionName).DropCollection()
			if err != nil {
				return fmt.Errorf("error dropping preexisting temporary collection %v: %v", arg.tempCollectionName, err)
			}
		}

		log.Logf(log.DebugLow, "syncing %v to temporary collection", arg.intentType)
		// if _, err = rsync.RsyncCollectionToDB("admin", arg.tempCollectionName, 0); err != nil {
		if _, err = rsync.RsyncCollectionToDB("admin", arg.tempCollectionName, roles); err != nil {
			return fmt.Errorf("error syncing %v: %v", arg.intentType, err)
		}

		// make sure we always drop the temporary collection
		defer func() {
			session, e := rsync.SessionProvider.GetSession()
			if e != nil {
				// logging errors here because this has no way of returning that doesn't mask other errors
				log.Logf(log.Info, "error establishing connection to drop temporary collection admin.%v: %v", arg.tempCollectionName, e)
				return
			}
			defer session.Close()
			log.Logf(log.DebugHigh, "dropping temporary collection admin.%v", arg.tempCollectionName)
			e = session.DB("admin").C(arg.tempCollectionName).DropCollection()
			if e != nil {
				log.Logf(log.Info, "error dropping temporary collection admin.%v: %v", arg.tempCollectionName, e)
			}
		}()
		userTargetDB = arg.intent.DB
	}

	if userTargetDB == "admin" {
		// _mergeAuthzCollections uses an empty db string as a sentinel for "all databases"
		userTargetDB = ""
	}

	// we have to manually convert mgo's safety to a writeconcern object
	writeConcern := bson.M{}
	if rsync.safety == nil {
		writeConcern["w"] = 0
	} else {
		if rsync.safety.WMode != "" {
			writeConcern["w"] = rsync.safety.WMode
		} else {
			writeConcern["w"] = rsync.safety.W
		}
	}

	command := bsonutil.MarshalD{}
	command = append(command,
		bson.DocElem{"_mergeAuthzCollections", 1})
	command = append(command,
		mergeArgs...)
	command = append(command,
		bson.DocElem{"drop", rsync.OutputOptions.Drop},
		bson.DocElem{"writeConcern", writeConcern},
		bson.DocElem{"db", userTargetDB})

	log.Logf(log.DebugLow, "merging users/roles from temp collections")
	res := bson.M{}
	err = session.Run(command, &res)
	if err != nil {
		return fmt.Errorf("error running merge command: %v", err)
	}
	if util.IsFalsy(res["ok"]) {
		return fmt.Errorf("_mergeAuthzCollections command: %v", res["errmsg"])
	}
	return nil
}

// GetDumpAuthVersion reads the admin.system.version collection in the dump directory
// to determine the authentication version of the files in the dump. If that collection is not
// present in the dump, we try to infer the authentication version based on its absence.
// Returns the authentication version number and any errors that occur.
/*func (rsync *MongoRsync) GetRsyncAuthVersion(fsession *mgo.Session) (int, error) {
	// first handle the case where we have no auth version
	intent := rsync.manager.AuthVersion()
	if intent == nil {
		if rsync.InputOptions.RsyncDBUsersAndRoles {
			// If we are using --rsyncDbUsersAndRoles, we cannot guarantee an
			// $admin.system.version collection from a 2.6 server,
			// so we can assume up to version 3.
			log.Logf(log.Always, "no system.version bson source found in '%v' database rsync", rsync.ToolOptions.FDB)
			log.Log(log.Always, "warning: assuming users and roles collections are of auth version 3")
			log.Log(log.Always, "if users are from an earlier version of MongoDB, they may not rsync properly")
			return 3, nil
		}
		log.Log(log.Info, "no system.version bson source found in rsync")
		log.Log(log.Always, "assuming users in the rsync source are from <= 2.4 (auth version 1)")
		return 1, nil
	}

	err := intent.BSONFile.Open()
	if err != nil {
		return 0, err
	}
	defer intent.BSONFile.Close()
	bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(intent.BSONFile))
	defer bsonSource.Close()

	versionDoc := bson.M{}
	for bsonSource.Next(&versionDoc) {
		id, ok := versionDoc["_id"].(string)
		if ok && id == "authSchema" {
			authVersion, ok := versionDoc["currentVersion"].(int)
			if ok {
				return authVersion, nil
			}
			return 0, fmt.Errorf("can't unmarshal system.version curentVersion as an int")
		}
		log.Logf(log.DebugLow, "system.version document is not an authSchema %v", versionDoc["_id"])
	}
	err = bsonSource.Err()
	if err != nil {
		log.Logf(log.Info, "can't unmarshal system.version document: %v", err)
	}
	return 0, fmt.Errorf("system.version bson file does not have authSchema document")
}

// ValidateAuthVersions compares the authentication version of the dump files and the
// authentication version of the target server, and returns an error if the versions
// are incompatible.
func (rsync *MongoRsync) ValidateAuthVersions() error {
	if rsync.authVersions.Dump == 2 || rsync.authVersions.Dump == 4 {
		return fmt.Errorf(
			"cannot rsync users and roles from a dump file with auth version %v; "+
				"finish the upgrade or roll it back", rsync.authVersions.Dump)
	}
	if rsync.authVersions.Server == 2 || rsync.authVersions.Server == 4 {
		return fmt.Errorf(
			"cannot rsync users and roles to a server with auth version %v; "+
				"finish the upgrade or roll it back", rsync.authVersions.Server)
	}
	switch rsync.authVersions {
	case authVersionPair{3, 5}:
		log.Log(log.Info,
			"rsyncing users and roles of auth version 3 to a server of auth version 5")
	case authVersionPair{5, 5}:
		log.Log(log.Info,
			"rsyncing users and roles of auth version 5 to a server of auth version 5")
	case authVersionPair{3, 3}:
		log.Log(log.Info,
			"rsyncing users and roles of auth version 3 to a server of auth version 3")
	case authVersionPair{1, 1}:
		log.Log(log.Info,
			"rsyncing users and roles of auth version 1 to a server of auth version 1")
	case authVersionPair{1, 5}:
		return fmt.Errorf("cannot rsync users of auth version 1 to a server of auth version 5")
	case authVersionPair{5, 3}:
		return fmt.Errorf("cannot rsync users of auth version 5 to a server of auth version 3")
	case authVersionPair{1, 3}:
		log.Log(log.Info,
			"rsyncing users and roles of auth version 1 to a server of auth version 3")
		log.Log(log.Always,
			"users and roles will have to be updated with the authSchemaUpgrade command")
	case authVersionPair{5, 1}:
		fallthrough
	case authVersionPair{3, 1}:
		return fmt.Errorf(
			"cannot rsync users and roles dump file >= auth version 3 to a server of auth version 1")
	default:
		return fmt.Errorf("invalid auth pair: dump=%v, server=%v",
			rsync.authVersions.Dump, rsync.authVersions.Server)
	}
	return nil

}
*/
// ShouldRsyncUsersAndRoles returns true if mongorsync should go through
// through the process of rsyncing collections pertaining to authentication.
func (rsync *MongoRsync) ShouldRsyncUsersAndRoles() bool {
	// If the user has done anything that would indicate the restoration
	// of users and roles (i.e. used --rsyncDbUsersAndRoles, -d admin, or
	// is doing a full rsync), then we check if users or roles BSON files
	// actually exist in the dump dir. If they do, return true.
	if rsync.InputOptions.RsyncDBUsersAndRoles ||
		rsync.ToolOptions.FDB == "" ||
		rsync.ToolOptions.FDB == "admin" {
		if rsync.managerUserRoleOther.Users() != nil || rsync.managerUserRoleOther.Roles() != nil {
			return true
		}
	}
	return false
}

// DropCollection drops the intent's collection.
func (rsync *MongoRsync) DropCollection(intent *intents.Intent) error {
	session, err := rsync.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error establishing connection: %v", err)
	}
	defer session.Close()
	err = session.DB(intent.DB).C(intent.C).DropCollection()
	if err != nil {
		return fmt.Errorf("error dropping collection: %v", err)
	}
	return nil
}
