module github.com/bakhod1r/cachex/bench

go 1.25.3

replace github.com/bakhod1r/cachex => ../

require (
	github.com/bakhod1r/cachex v0.0.0-00010101000000-000000000000
	github.com/dgraph-io/ristretto/v2 v2.4.2
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/maypok86/otter/v2 v2.3.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	golang.org/x/sys v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
