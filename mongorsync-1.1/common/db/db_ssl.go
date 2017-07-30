// +build ssl

package db

import (
	"mongorsync-1.1/common/db/openssl"
	"mongorsync-1.1/common/options"
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
