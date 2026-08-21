package binancedata

import "runtime/debug"

// DevVersion is reported by [Version] when the binary carries no version at
// all. That is what `go run` produces, and what a `go test` binary carries, so
// it is the string the test suite sees.
//
// It is *not* what a plain `go build` produces inside this repository. Since Go
// 1.24 the toolchain reads the version control system and stamps what it finds,
// so a build here carries a real version even with no release involved — see
// [Version] for the four cases, measured. DevVersion comes back from a build
// only when version control is unavailable or switched off with
// -buildvcs=false.
//
// The string itself is Go's own convention, so it matches what `go version -m`
// prints for the same binary.
const DevVersion = "(devel)"

// Version reports the version of this module that was linked into the running
// binary, for example "v0.3.1", or [DevVersion] when the binary carries none.
//
// There is no version constant to keep in sync anywhere in this repository, and
// no build script injecting one with -ldflags. The Go toolchain stamps the
// module version into every binary at link time and this function reads it back
// out, so a release is made by pushing a git tag and nothing in the source
// needs editing. A second source of truth is the thing this design exists to
// avoid: a constant and a tag that disagree is a bug nobody notices until they
// are debugging the wrong version.
//
// # What the toolchain actually stamps
//
// Worth spelling out, because it decides whether a release pipeline needs to
// inject anything. Measured with `go version -m` on Go 1.26:
//
//	go run ./cmd/bmd                     (devel)
//	go build with -buildvcs=false        (devel)
//	go build, untagged commit            v0.0.0-20260821111323-f4255484548a
//	go build, clean tree at a tag        v0.1.0
//	go build, dirty tree at a tag        v0.1.0+dirty
//
// The last two are why goreleaser is configured with no version ldflags: it
// builds from a checkout at the tag, so the tag arrives on its own. The +dirty
// suffix is a bonus — a binary built from uncommitted changes says so rather
// than impersonating the release.
func Version() string {
	return versionFrom(debug.ReadBuildInfo)
}

// versionFrom holds the real logic of [Version] with its one dependency —
// reading the embedded build metadata — passed in as a parameter.
//
// This is the standard Go answer to "how do I test something that talks to the
// runtime?". Go has no monkey-patching, so the dependency becomes a
// function-typed parameter instead: production passes debug.ReadBuildInfo, and
// the test passes a stub. The behaviour below is then fully tested without any
// global state being mutated, and the compiler checks that the stub has the
// right signature.
//
// The function is unexported (lowercase name), so it is invisible outside this
// package but still directly reachable from version_test.go, because tests
// live in the same package.
func versionFrom(read func() (*debug.BuildInfo, bool)) string {
	info, ok := read()
	// ReadBuildInfo reports ok == false when the binary was not built in
	// module mode. Rare, but it costs one line to handle rather than panic on
	// a nil pointer.
	if !ok || info == nil {
		return DevVersion
	}

	// Main is the module being built. Its Version is empty for a `go run` of
	// a local package and "(devel)" for an untagged build; normalise both to
	// the same answer so callers have one case to reason about.
	if info.Main.Version == "" {
		return DevVersion
	}

	return info.Main.Version
}
