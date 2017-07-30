// +build ssl

package db

import (
	"mongoIncbackup-1.1/common/db/openssl"
	"mongoIncbackup-1.1/common/options"
)

func init() {
	GetConnectorFuncs = append(GetConnectorFuncs, getSSLConnector)
}

// return the SSL DB connector if using SSL, otherwise, return nil.
func getSSLConnector(opts options.ToolOptions) DBConnector {
	if opts.SSL.UseSSL {
		return &openssl.SSLDBConnector{}
	}
	return nil
}
