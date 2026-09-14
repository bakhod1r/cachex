module github.com/bakhod1r/cachex/redis

go 1.25.3

require (
	github.com/bakhod1r/cachex v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/bakhod1r/cachex => ../
