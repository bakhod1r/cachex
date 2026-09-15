module github.com/bakhod1r/cachex/kafka

go 1.25.3

replace github.com/bakhod1r/cachex => ../

require (
	github.com/bakhod1r/cachex v0.2.0
	github.com/twmb/franz-go v1.21.7
	github.com/twmb/franz-go/pkg/kadm v1.18.0
)

require (
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.52.0 // indirect
)
