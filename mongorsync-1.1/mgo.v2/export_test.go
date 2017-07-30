package mgo

import (
	"time"
)

func HackPingDelay(newDelay time.Duration) (rsync func()) {
	globalMutex.Lock()
	defer globalMutex.Unlock()

	oldDelay := pingDelay
	rsync = func() {
		globalMutex.Lock()
		pingDelay = oldDelay
		globalMutex.Unlock()
	}
	pingDelay = newDelay
	return
}

func HackSyncSocketTimeout(newTimeout time.Duration) (rsync func()) {
	globalMutex.Lock()
	defer globalMutex.Unlock()

	oldTimeout := syncSocketTimeout
	rsync = func() {
		globalMutex.Lock()
		syncSocketTimeout = oldTimeout
		globalMutex.Unlock()
	}
	syncSocketTimeout = newTimeout
	return
}
