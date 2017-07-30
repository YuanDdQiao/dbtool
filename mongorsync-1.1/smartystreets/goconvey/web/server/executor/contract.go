package executor

import "mongorsync-1.1/smartystreets/goconvey/web/server/contract"

type Parser interface {
	Parse([]*contract.Package)
}

type Tester interface {
	SetBatchSize(batchSize int)
	TestAll(folders []*contract.Package)
}
