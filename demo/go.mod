module github.com/ralscha/govers/demo

go 1.25.4

require (
	github.com/ralscha/govers/core v0.0.0
	github.com/ralscha/govers/inmemory v0.0.0
)

replace (
	github.com/ralscha/govers/core => ../core
	github.com/ralscha/govers/inmemory => ../inmemory
)
