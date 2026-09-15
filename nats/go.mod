module github.com/bakhod1r/cachex/nats

go 1.25.3

replace github.com/bakhod1r/cachex => ../

require (
	github.com/bakhod1r/cachex v0.0.0-00010101000000-000000000000
	github.com/nats-io/nats.go v1.53.1
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
)
