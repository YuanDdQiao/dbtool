# dbtool
mongodb tools version 1.1 with backup and sync for 

微信号：ydq580

邮箱：2960428571@qq.com

QQ群：62163057


使用--help:
Usage:
  mongorsync <options>

mongosync the content of a running server into target server.

Specify a database with -d and a collection with -c to only sync that database or collection.

See https://github.com/YuanDdQiao/dbtool/blob/master/README.md for more information.

general options:
      /help                                                 print usage
      /version                                              print the tool version and exit

verbosity options:
  /v, /verbose:<level>                                      more detailed log output (include multiple times for more verbosity, e.g. -vvvvv, or specify a numeric value, e.g. --verbose=N)
      /quiet                                                hide all log output

connection options:
  /h, /host:<hostname>                                      mongodb host to connect to (setname/host1,host-、2 for replica sets)
      /port:<port>                                          server port (can also use --host hostname:port)
  /H, /fhost:<hostname>                                     mongodb host to connect to (setname/host1,host-2 for replica sets)
      /fport:<port>                                         server port (can also use --host hostname:port)

authentication options:
  /u, /username:<username>                                  username for authentication
  /p, /password:<password>                                  password for authentication
      /authenticationDatabase:<database-name>               database that holds the user's credentials
      /authenticationMechanism:<mechanism>                  authentication mechanism to use
  /U, /fusername:<username>                                 username for authentication
  /P, /fpassword:<password>                                 password for authentication
      /fauthenticationDatabase:<database-name>              database that holds the user's credentials
      /fauthenticationMechanism:<mechanism>                 authentication mechanism to use

namespace options:
  /d, /db:<database-name>                                   database to use
  /c, /collection:<collection-name>                         collection to use
  /D, /fdb:<database-name>                                  database to use
  /C, /fcollection:<collection-name>                        collection to use

InputOptions options:
  /q, /query:                                               query filter, as a JSON string, e.g., '{x:{$gt:1}}'
      /queryFile:                                           path to a file containing a query filter (JSON)
      /readPreference:<string>|<json>                       specify either a preference name or a preference json object
      /forceTableScan                                       force a table scan
      /oplog                                                use oplog for taking a point-in-time snapshot
      /oSt:<seconds>[:ordinal]                              only include oplog entries before the provided Timestamp
      /oEt:<seconds>[:ordinal]                              only include oplog entries before the provided Timestamp
      /rsyncDBUsersAndRoles                                 restore user and role definitions for the given database
      /excludeCollection:<collection-name>                  collection to exclude from the dump (may be specified multiple times to exclude additional collections)
      /excludeCollectionsWithPrefix:<collection-prefix>     exclude all collections from the dump that have the given prefix (may be specified multiple times to exclude additional prefixes)

OutputOptions options:
      /drop                                                 drop each collection before import
      /writeConcern:<write-concern>                         write concern options e.g. --writeConcern majority, --writeConcern '{w: 3, wtimeout: 500, fsync: true, j: true}' (defaults to 'majority')
      /noIndexRsync                                         don't rsync indexes
      /noOptionsRsync                                       don't rsync collection options
      /keepIndexVersion                                     don't update index version
      /maintainInsertionOrder                               preserve order of documents during restoration
  /j, /numParallelCollections:                              number of collections to rsync in parallel (4 by default)
      /numInsertionWorkersPerCollection:                    number of insert operations to run concurrently per collection (1 by default)
      /stopOnError                                          stop rsyncing if an error is encountered on insert (off by default)
      /bypassDocumentValidation                             bypass document validation
