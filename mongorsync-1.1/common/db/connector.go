package db

import (
	"time"

	"mongorsync-1.1/common/db/kerberos"
	"mongorsync-1.1/common/options"
	"mongorsync-1.1/common/util"
	"mongorsync-1.1/mgo.v2"
)

// Interface type for connecting to the database.
type DBConnector interface {
	// configure, based on the options passed in
	Configure(options.ToolOptions) error
	FConfigure(options.ToolOptions) error

	// dial the database and get a fresh new session
	GetNewSession() (*mgo.Session, error)
}

// Basic connector for dialing the database, with no authentication.
type VanillaDBConnector struct {
	dialInfo *mgo.DialInfo
}

// Configure sets up the db connector using the options in opts. It parses the
// connection string and then sets up the dial information using the default
// dial timeout.
func (self *VanillaDBConnector) Configure(opts options.ToolOptions) error {
	// create the addresses to be used to connect
	connectionAddrs := util.CreateConnectionAddrs(opts.Host, opts.Port)

	timeout := time.Duration(options.DefaultDialTimeoutSeconds) * time.Second
	if opts.HiddenOptions != nil && opts.HiddenOptions.DialTimeoutSeconds != nil {
		timeout = time.Duration(*opts.HiddenOptions.DialTimeoutSeconds) * time.Second
	}
	// set up the dial info
	self.dialInfo = &mgo.DialInfo{
		Addrs:          connectionAddrs,
		Timeout:        timeout,
		Direct:         opts.Direct,
		ReplicaSetName: opts.ReplicaSetName,
		Username:       opts.Auth.Username,
		Password:       opts.Auth.Password,
		Source:         opts.GetAuthenticationDatabase(),
		Mechanism:      opts.Auth.Mechanism,
	}
	kerberos.AddKerberosOpts(opts, self.dialInfo)
	return nil
}
func (self *VanillaDBConnector) FConfigure(opts options.ToolOptions) error {
	// create the addresses to be used to connect
	connectionAddrs := util.CreateConnectionAddrs(opts.FHost, opts.FPort)

	timeout := time.Duration(options.DefaultDialTimeoutSeconds) * time.Second
	if opts.HiddenOptions != nil && opts.HiddenOptions.DialTimeoutSeconds != nil {
		timeout = time.Duration(*opts.HiddenOptions.DialTimeoutSeconds) * time.Second
	}
	// set up the dial info
	self.dialInfo = &mgo.DialInfo{
		Addrs:          connectionAddrs,
		Timeout:        timeout,
		Direct:         opts.Direct,
		ReplicaSetName: opts.FReplicaSetName,
		Username:       opts.Auth.FUsername,
		Password:       opts.Auth.FPassword,
		Source:         opts.FGetAuthenticationDatabase(),
		Mechanism:      opts.Auth.FMechanism,
	}
	kerberos.FAddKerberosOpts(opts, self.dialInfo)
	return nil
}

// GetNewSession connects to the server and returns the established session and any
// error encountered.
func (self *VanillaDBConnector) GetNewSession() (*mgo.Session, error) {
	return mgo.DialWithInfo(self.dialInfo)
}
