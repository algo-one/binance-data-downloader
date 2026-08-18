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
