// go.mod declares this directory to be the root of a Go *module*: a unit of
// versioning and dependency management. It is deliberately small, because Go
// resolves dependencies from the import paths in the source files rather than
// from a hand-maintained list.
//
// Run `mise run tidy` (i.e. `go mod tidy`) after adding or removing an import
// and Go will rewrite the require block below to match reality.

// The module path is the string other people will `import`. It must match the
// repository URL, because that is literally how `go get` finds the code — there
// is no central package registry to publish to, only version-control hosts.
module github.com/algo-one/binance-data-downloader

// The `go` directive is the MINIMUM Go version required to build this module,
// not the version we develop with. Those are deliberately different:
//
//   - This line (1.24) is a promise to consumers: anyone on Go 1.24 or newer
//     can import the library. Setting it to the newest release would lock out
//     users for no benefit, and a published library should be generous here.
//   - mise.toml pins the *development* toolchain to a specific patch release
//     so every machine and CI runner compiles with identical bits.
//
// The compiler enforces this: language features newer than 1.24 are rejected
// even when a newer toolchain is doing the compiling. That is the point — it
// stops us accidentally breaking the promise.
go 1.24

// The one third-party dependency so far. `go mod tidy` writes this block from
// the imports in the source, so it is a report rather than a wish list — the
// way to add a dependency is to import it and re-run tidy.
//
// udecimal carries the prices and volumes in binancedata.Kline. Binance quote
// volumes reach 20 significant digits, which float64 cannot hold and no int64
// fixed-point scale covers; udecimal keeps a 128-bit coefficient inline in the
// value, so it is exact without allocating. See kline.go for the measurements
// and docs/numbers.md for the comparison against the alternatives.
//
// Dependencies are a liability for a published library — every one of them
// becomes a constraint on everybody who imports us — so this list is meant to
// stay short. Everything else here is the standard library.
require github.com/quagmt/udecimal v1.10.1
