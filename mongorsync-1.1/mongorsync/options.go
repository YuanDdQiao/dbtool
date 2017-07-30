/*
===--- options.go ---------------------------------------------------===//
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See https://github.com/YuanDdQiao/dbtool/blob/master/README.md for more details.
===----------------------------------------------------------------------===
*/
package mongorsync

import (
	"fmt"
	"io/ioutil"
)

//Usage describes basic usage of mongorsync

var Usage = `<options>

rsync the content of a running server into target server.

Specify a database with -d and a collection with -c to only rsync that database or collection.

See https://github.com/YuanDdQiao/dbtool/blob/master/README.md for more information.`

// InputOptions defines the set of options to use in retrieving data from the server.
type InputOptions struct {
	Query          string `long:"query" short:"q" description:"query filter, as a JSON string, e.g., '{x:{$gt:1}}'"`
	QueryFile      string `long:"queryFile" description:"path to a file containing a query filter (JSON)"`
	ReadPreference string `long:"readPreference" value-name:"<string>|<json>" description:"specify either a preference name or a preference json object"`
	TableScan      bool   `long:"forceTableScan" description:"force a table scan"`
	// to target server
	Oplog bool   `long:"oplog" description:"use oplog for taking a point-in-time snapshot"`
	OSt   string `long:"oSt" value-name:"<seconds>[:ordinal]" description:"only include oplog entries before the provided Timestamp"`
	OEt   string `long:"oEt" value-name:"<seconds>[:ordinal]" description:"only include oplog entries before the provided Timestamp"`

	// OplogLimit                 string   `long:"oplogLimit" value-name:"<seconds>[:ordinal]" description:"only include oplog entries before the provided Timestamp"`
	RsyncDBUsersAndRoles       bool     `long:"RsyncDBUsersAndRoles" description:"restore user and role definitions for the given database"`
	ExcludedCollections        []string `long:"excludeCollection" value-name:"<collection-name>" description:"collection to exclude from the dump (may be specified multiple times to exclude additional collections)"`
	ExcludedCollectionPrefixes []string `long:"excludeCollectionsWithPrefix" value-name:"<collection-prefix>" description:"exclude all collections from the dump that have the given prefix (may be specified multiple times to exclude additional prefixes)"`
}

// Name returns a human-readable group name for input options.
func (*InputOptions) Name() string {
	return "InputOptions"
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

// OutputOptions defines the set of options for rsyncing source data.
type OutputOptions struct {
	Drop                     bool   `long:"drop" description:"drop each collection before import"`
	WriteConcern             string `long:"writeConcern" value-name:"<write-concern>" default:"majority" default-mask:"-" description:"write concern options e.g. --writeConcern majority, --writeConcern '{w: 3, wtimeout: 500, fsync: true, j: true}' (defaults to 'majority')"`
	NoIndexRsync             bool   `long:"noIndexRsync" description:"don't rsync indexes"`
	NoOptionsRsync           bool   `long:"noOptionsRsync" description:"don't rsync collection options"`
	KeepIndexVersion         bool   `long:"keepIndexVersion" description:"don't update index version"`
	MaintainInsertionOrder   bool   `long:"maintainInsertionOrder" description:"preserve order of documents during restoration"`
	NumParallelCollections   int    `long:"numParallelCollections" short:"j" description:"number of collections to rsync in parallel (4 by default)" default:"4" default-mask:"-"`
	NumInsertionWorkers      int    `long:"numInsertionWorkersPerCollection" description:"number of insert operations to run concurrently per collection (1 by default)" default:"1" default-mask:"-"`
	StopOnError              bool   `long:"stopOnError" description:"stop rsyncing if an error is encountered on insert (off by default)"`
	BypassDocumentValidation bool   `long:"bypassDocumentValidation" description:"bypass document validation"`
}

// Name returns a human-readable group name for output options.
func (*OutputOptions) Name() string {
	return "OutputOptions"
}
