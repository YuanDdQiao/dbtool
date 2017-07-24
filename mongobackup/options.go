package mongobackup

import (
	"fmt"
	"io/ioutil"
)

var Usage = `<options>

Export the content of a running server into .bson files.

Specify a database with -d and a collection with -c to only backup that database or collection.

See http://docs.mongodb.org/manual/reference/program/mongobackup/ for more information.`

// InputOptions defines the set of options to use in retrieving data from the server.
type InputOptions struct {
	Query          string `long:"query" short:"q" description:"query filter, as a JSON string, e.g., '{x:{$gt:1}}'"`
	QueryFile      string `long:"queryFile" description:"path to a file containing a query filter (JSON)"`
	ReadPreference string `long:"readPreference" value-name:"<string>|<json>" description:"specify either a preference name or a preference json object"`
	TableScan      bool   `long:"forceTableScan" description:"force a table scan"`
}

// Name returns a human-readable group name for input options.
func (*InputOptions) Name() string {
	return "query"
}

func (inputOptions *InputOptions) HasQuery() bool {
	return inputOptions.Query != "" || inputOptions.QueryFile != ""
}

func (inputOptions *InputOptions) GetQuery() ([]byte, error) {
	if inputOptions.Query != "" {
		return []byte(inputOptions.Query), nil
	} else if inputOptions.QueryFile != "" {
		content, err := ioutil.ReadFile(inputOptions.QueryFile)
		if err != nil {
			fmt.Errorf("error reading queryFile: %v", err)
		}
		return content, err
	}
	panic("GetQuery can return valid values only for query or queryFile input")
}

// OutputOptions defines the set of options for writing backup data.
type OutputOptions struct {
	Out                        string   `long:"out" value-name:"<directory-path>" short:"o" description:"output directory, or '-' for stdout (defaults to 'backup')"`
	Gzip                       bool     `long:"gzip" description:"compress archive our collection output with Gzip"`
	Repair                     bool     `long:"repair" description:"try to recover documents from damaged data files (not supported by all storage engines)"`
	Oplog                      bool     `long:"oplog" description:"use oplog for taking a point-in-time snapshot"`
	Archive                    string   `long:"archive" value-name:"<file-path>" optional:"true" optional-value:"-" description:"backup as an archive to the specified path. If flag is specified without a value, archive is written to stdout"`
	BackupDBUsersAndRoles        bool     `long:"backupDbUsersAndRoles" description:"backup user and role definitions for the specified database"`
	ExcludedCollections        []string `long:"excludeCollection" value-name:"<collection-name>" description:"collection to exclude from the backup (may be specified multiple times to exclude additional collections)"`
	ExcludedCollectionPrefixes []string `long:"excludeCollectionsWithPrefix" value-name:"<collection-prefix>" description:"exclude all collections from the backup that have the given prefix (may be specified multiple times to exclude additional prefixes)"`
	NumParallelCollections     int      `long:"numParallelCollections" short:"j" description:"number of collections to backup in parallel (4 by default)" default:"4" default-mask:"-"`
}

// Name returns a human-readable group name for output options.
func (*OutputOptions) Name() string {
	return "output"
}
