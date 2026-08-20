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
//   - This line (1.25.0) is a promise to consumers: anyone on Go 1.25 or newer
//     can import the library. Setting it to the newest release would lock out
//     users for no benefit, and a published library should be generous here.
//   - mise.toml pins the *development* toolchain to a specific patch release
//     so every machine and CI runner compiles with identical bits.
//
// The compiler enforces this: language features newer than 1.25 are rejected
// even when a newer toolchain is doing the compiling. That is the point — it
// stops us accidentally breaking the promise. CI has a job pinned to the floor
// for the same reason.
//
// # Why it moved, twice
//
// It said 1.24.0 until Stage 5, when parquet-go — which declares `go 1.24.9`
// from v0.26.0 — pushed it to 1.24.9. A module's floor cannot sit below any of
// its dependencies', so `go get` rewrote the line. That one was a patch bump
// inside 1.24 and excluded nobody who keeps their toolchain current.
//
// It moved to 1.25.0 in Stage 6, and that one is a real break for anyone still
// on 1.24. It was taken deliberately, for `testing/synctest`, which entered the
// stable API in 1.25 (it was `synctest.Run` behind GOEXPERIMENT in 1.24).
//
// The reasoning is worth recording, because a floor bump for a *test* facility
// looks like a poor trade until the knock-on effects are counted:
//
//   - synctest gives a test a private fake clock for the entire time package.
//     That let internal/vision/limiter.go drop a hand-rolled token bucket in
//     favour of golang.org/x/time/rate. The bucket existed only because this
//     package injects its clock so that tests assert on delays rather than
//     spending them, and rate.Limiter reads time.Now() internally — an
//     objection a bubble removes completely.
//   - x/sync could come off its v0.18.0 pin, which existed solely because
//     v0.20.0 onward requires 1.25.
//   - x/sys could move to v0.47.0, closing GO-2026-5024. It had been left
//     outstanding because the fix requires 1.25.
//
// So the bump bought a dependency we no longer maintain ourselves, two pins
// released, and an open advisory closed. The cost is consumers on 1.24, and Go
// supports only the two newest minor releases in any case.
go 1.25.0

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
	// It was pinned to v0.18.0 from Stage 4 until Stage 6, because v0.20.0
	// onward requires Go 1.25 and `go get golang.org/x/sync@latest` would have
	// dragged this module's floor up with it, with nothing but a line of output
	// to say so. Moving the floor to 1.25 deliberately made the pin pointless,
	// so it is gone.
	golang.org/x/sync v0.22.0

	// x/time/rate is the token bucket that paces calls to
	// data-api.binance.vision. That endpoint — unlike the archive bucket — has a
	// published quota of 6000 request-weight per minute per IP, and exceeding it
	// earns an HTTP 418 IP ban of two minutes to three days that punishes every
	// process on the address. See internal/vision/limiter.go.
	//
	// Stage 6 shipped a hand-rolled bucket first and replaced it in the same
	// stage. The only argument for owning the code was that this package injects
	// its clock so tests can assert on delays without spending them, and
	// rate.Limiter reads time.Now() itself; testing/synctest answers that
	// completely, and the library's cancellation path is more careful than the
	// hand-rolled one — it restores only tokens that later reservations have not
	// already claimed.
	golang.org/x/time v0.15.0
)

// Dependencies of dependencies. `go mod tidy` maintains this block; nothing
// here is imported by this module's own code.
//
// klauspost/compress is ahead of the v1.17.9 parquet-go asks for, deliberately:
// that version carries GO-2026-5841, and v1.18.7 is the fix. Go resolves a
// dependency to the highest version anyone in the graph requires, so raising it
// here is enough. govulncheck reports it as uncalled either way; it is fixed
// rather than waived because it was free to fix.
//
// golang.org/x/sys is on v0.47.0, which closes GO-2026-5024. It sat at v0.38.0
// with that advisory open through Stage 5, because the fix declares
// `go 1.25.0` and the floor was still 1.24 — govulncheck confirmed nothing here
// called it, so the trade was to wait. Stage 6 moved the floor for other
// reasons and this came along for free.
require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
