/*
===--- main.go ---------------------------------------------------===//
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See https://github.com/YuanDdQiao/dbtool/blob/master/README.md for more details.
===----------------------------------------------------------------------===
*/
// Main package for the mongorsync tool.
package main

import (
	// "fmt"
	"mongorsync-1.1/common/db"
	"mongorsync-1.1/common/log"
	"mongorsync-1.1/common/options"
	"mongorsync-1.1/common/util"
	"mongorsync-1.1/mongorsync"
	"os"
	// "fmt"
	// "time"
)

func main() {
	// initialize command-line opts
	opts := options.New("mongorsync", mongorsync.Usage,
		options.EnabledOptions{Auth: true, Connection: true, FAuth: true, FConnection: true, FNamespace: true})
	inputOpts := &mongorsync.InputOptions{}
	opts.AddOptions(inputOpts)
	outputOpts := &mongorsync.OutputOptions{}
	opts.AddOptions(outputOpts)

	// fmt.Printf("test....%v\n", time.Now())
	// extraArgs, err := opts.Parse()
	_, err := opts.Parse()
	if err != nil {
		log.Logf(log.Always, "error parsing command line options: %v", err)
		log.Logf(log.Always, "try 'mongorsync --help' for more information")
		os.Exit(util.ExitBadOptions)
	}

	// print help or version info, if specified
	if opts.PrintHelp(false) {
		return
	}

	if opts.PrintVersion() {
		return
	}

	log.SetVerbosity(opts.Verbosity)

	_, setName := util.ParseConnectionString(opts.Host)
	opts.Direct = (setName == "")
	opts.ReplicaSetName = setName

	provider, err := db.NewSessionProvider(*opts)
	if err != nil {
		log.Logf(log.Always, "error connecting to host: %v", err)
		os.Exit(util.ExitError)
	}
	provider.SetBypassDocumentValidation(outputOpts.BypassDocumentValidation)
	// from souce url
	_, fsetName := util.ParseConnectionString(opts.FHost)
	opts.FDirect = opts.Direct
	opts.FReplicaSetName = fsetName

	fprovider, err := db.FNewSessionProvider(*opts)
	if err != nil {
		log.Logf(log.Always, "error connecting to Fhost: %v", err)
		os.Exit(util.ExitError)
	}
	fprovider.SetBypassDocumentValidation(outputOpts.BypassDocumentValidation)

	// disable TCP timeouts for rsync jobs
	provider.SetFlags(db.DisableSocketTimeout)
	rsync := mongorsync.MongoRsync{
		ToolOptions:   opts,
		OutputOptions: outputOpts,
		InputOptions:  inputOpts,
		// TargetDirectory: targetDir,
		FSessionProvider: fprovider,
		SessionProvider:  provider,
	}
	// log.Logf(log.Always, "init....: %v....%v", time.Now().Unix(), time.Now())
	if err = rsync.Rsync(); err != nil {
		log.Logf(log.Always, "Failed: %v", err)
		if err == util.ErrTerminated {
			os.Exit(util.ExitKill)
		}
		os.Exit(util.ExitError)
	}
}
