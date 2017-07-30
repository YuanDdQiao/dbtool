// +build !solaris

package password

import (
	"mongoIncbackup-1.1/howeyc/gopass"
	"mongoIncbackup-1.1/x/crypto/ssh/terminal"
	"syscall"
)

// This file contains all the calls needed to properly
// handle password input from stdin/terminal on all
// operating systems that aren't solaris

func IsTerminal() bool {
	return terminal.IsTerminal(int(syscall.Stdin))
}

func GetPass() string {
	return string(gopass.GetPasswd())
}
