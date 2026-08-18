package binancedata

import "runtime/debug"

// DevVersion is reported by [Version] when no released version is recorded in
// the binary, which is the normal case during local development and testing.
//
// The string is Go's own convention for this, so it matches what you would see
// from `go version -m` on a locally built binary.
const DevVersion = "(devel)"

// Version reports the version of this module that was linked into the running
// binary, for example "v0.3.1". It returns [DevVersion] when the binary was
// built from a working tree rather than from a tagged module.
//
// There is no version constant to keep in sync anywhere in this repository.
// The Go toolchain stamps the module version into every binary at link time,
// and this function reads it back out — so a release is created by pushing a
// git tag, and nothing in the source needs editing.
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
