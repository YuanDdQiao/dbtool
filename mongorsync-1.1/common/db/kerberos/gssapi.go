// Package kerberos implements authentication to MongoDB using kerberos
package kerberos

// #cgo windows CFLAGS: -Ic:/sasl/include
// #cgo windows LDFLAGS: -Lc:/sasl/lib

import (
	"mongorsync-1.1/common/options"
	"mongorsync-1.1/mgo.v2"
)

const authMechanism = "GSSAPI"

func AddKerberosOpts(opts options.ToolOptions, dialInfo *mgo.DialInfo) {
	if dialInfo == nil {
		return
	}
	if opts.Kerberos == nil || opts.Kerberos.Service == "" ||
		opts.Kerberos.ServiceHost == "" {
		return
	}
	if opts.Auth == nil || (opts.Auth.Mechanism != authMechanism &&
		dialInfo.Mechanism != authMechanism) {
		return
	}
	dialInfo.Service = opts.Kerberos.Service
	dialInfo.ServiceHost = opts.Kerberos.ServiceHost
	dialInfo.Mechanism = authMechanism
}

func FAddKerberosOpts(opts options.ToolOptions, dialInfo *mgo.DialInfo) {
	if dialInfo == nil {
		return
	}
	if opts.Kerberos == nil || opts.Kerberos.FService == "" ||
		opts.Kerberos.FServiceHost == "" {
		return
	}
	if opts.Auth == nil || (opts.Auth.FMechanism != authMechanism &&
		dialInfo.Mechanism != authMechanism) {
		return
	}
	dialInfo.Service = opts.Kerberos.FService
	dialInfo.ServiceHost = opts.Kerberos.FServiceHost
	dialInfo.Mechanism = authMechanism
}
