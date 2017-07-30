/*
===--- main.go ---------------------------------------------------===//
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See https://github.com/YuanDdQiao/dbtool/blob/master/README.md for more details.
===----------------------------------------------------------------------===
*/
// Main package for the mongobackup tool.
package main

import (
	"mongoIncbackup-1.1/common/log"
	"mongoIncbackup-1.1/common/options"
	"mongoIncbackup-1.1/common/util"
	"mongoIncbackup-1.1/mongobackup"
	"os"
)

func main() {
	// initialize command-line opts
	opts := options.New("mongobackup", mongobackup.Usage, options.EnabledOptions{true, true, true})

	inputOpts := &mongobackup.InputOptions{}
	opts.AddOptions(inputOpts)
	outputOpts := &mongobackup.OutputOptions{}
	opts.AddOptions(outputOpts)

	args, err := opts.Parse()
	if err != nil {
		log.Logf(log.Always, "error parsing command line options: %v", err)
		log.Logf(log.Always, "try 'mongobackup --help' for more information")
		os.Exit(util.ExitBadOptions)
	}

	if len(args) > 0 {
		log.Logf(log.Always, "positional arguments not allowed: %v", args)
		log.Logf(log.Always, "try 'mongobackup --help' for more information")
		os.Exit(util.ExitBadOptions)
	}

	// print help, if specified
	if opts.PrintHelp(false) {
		return
	}

	// print version, if specified
	if opts.PrintVersion() {
		return
	}

	// init logger
	log.SetVerbosity(opts.Verbosity)

	// connect directly, unless a replica set name is explicitly specified
	_, setName := util.ParseConnectionString(opts.Host)
	opts.Direct = (setName == "")
	opts.ReplicaSetName = setName

	backup := mongobackup.MongoBackup{
		ToolOptions:   opts,
		OutputOptions: outputOpts,
		InputOptions:  inputOpts,
	}

	if err = backup.Init(); err != nil {
		log.Logf(log.Always, "Failed: %v", err)
		os.Exit(util.ExitError)
	}

	if err = backup.Backup(); err != nil {
		log.Logf(log.Always, "Failed: %v", err)
		if err == util.ErrTerminated {
			os.Exit(util.ExitKill)
		}
		os.Exit(util.ExitError)
	}
}
