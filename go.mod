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
//   - This line (1.24.9) is a promise to consumers: anyone on Go 1.24.9 or
//     newer can import the library. Setting it to the newest release would lock
//     out users for no benefit, and a published library should be generous here.
//   - mise.toml pins the *development* toolchain to a specific patch release
//     so every machine and CI runner compiles with identical bits.
//
// The compiler enforces this: language features newer than 1.24 are rejected
// even when a newer toolchain is doing the compiling. That is the point — it
// stops us accidentally breaking the promise.
//
// It said 1.24.0 until Stage 5. parquet-go declares `go 1.24.9` from v0.26.0
// onward, and a module's floor cannot sit below any of its dependencies' —
// `go get` therefore rewrote this line, exactly as `go get golang.org/x/sync`
// tried to in Stage 4. The difference is that this one is a *patch* bump within
// 1.24 rather than a move to 1.25: it excludes nobody who keeps their toolchain
// patched, Go's own support policy only covers the newest patch of each minor
// release anyway, and it was accepted deliberately rather than absorbed. The
// alternative was pinning parquet-go v0.25.1, the last release declaring
// `go 1.22`, and taking a year-old version of the library that owns this
// project's on-disk format.
go 1.24.9

// `go mod tidy` writes these blocks from the imports in the source, so they are
// a report rather than a wish list — the way to add a dependency is to import
// it and re-run tidy.
//
// Dependencies are a liability for a published library — every one of them
// becomes a constraint on everybody who imports us — so this list is meant to
// stay short. Everything not named here is the standard library.
require (
	// parquet-go writes and reads tier 2 of the cache: the columnar form of an
	// archive, which is what a backtest's repeat reads actually hit. It is pure
	// Go, so the library still cross-compiles and links statically — DuckDB was
	// ruled out for needing cgo, which would have forfeited both.
	//
	// It is the dependency that owns this project's on-disk format, which is
	// why it is on the newest release rather than pinned back: see the note on
	// the `go` directive above for the floor it brought with it, and
	// parquet.go for the writer settings that are pinned so an upgrade cannot
	// silently change what is written.
	github.com/parquet-go/parquet-go v0.32.0

	// udecimal carries the prices and volumes in binancedata.Kline. Binance
	// quote volumes reach 20 significant digits, which float64 cannot hold and
	// no int64 fixed-point scale covers; udecimal keeps a 128-bit coefficient
	// inline in the value, so it is exact without allocating. See kline.go for
	// the measurements and docs/numbers.md for the comparison against the
	// alternatives.
	github.com/quagmt/udecimal v1.10.1

	// x/sync is the Go team's own module, versioned separately from the
	// standard library. errgroup is a WaitGroup that also propagates the first
	// error and cancels a shared context; availability.go uses it to run two
	// listings at once, and Stage 7 uses its SetLimit as the bounded worker
	// pool. singleflight, in the same module, collapses duplicate in-flight
	// requests in Stage 5.
	//
	// Pinned to v0.18.0 rather than latest on purpose: from v0.20.0 the module
	// requires Go 1.25, and adopting it would silently drag this module's floor
	// up with it — breaking the promise the `go` line above makes to consumers
	// still on 1.24. `go get golang.org/x/sync@latest` does exactly that, with
	// nothing but a line in its output to say so.
	golang.org/x/sync v0.18.0
)

// Dependencies of dependencies. `go mod tidy` maintains this block; nothing
// here is imported by this module's own code.
//
// klauspost/compress is ahead of the v1.17.9 parquet-go asks for, deliberately:
// that version carries GO-2026-5841, and v1.18.7 is the fix. Go resolves a
// dependency to the highest version anyone in the graph requires, so raising it
// here is enough — and v1.18.7 still declares `go 1.24`, so it costs nothing at
// the floor. govulncheck reports it as uncalled either way; it is fixed rather
// than waived because it was free to fix.
//
// golang.org/x/sys is knowingly left at v0.38.0 with GO-2026-5024 outstanding.
// The fix, v0.44.0, declares `go 1.25.0` and would move this module's floor off
// 1.24 entirely — the same trade x/sync was pinned to avoid. govulncheck
// confirms nothing here calls it.
require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	golang.org/x/sys v0.38.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
