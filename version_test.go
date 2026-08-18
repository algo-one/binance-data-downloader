package binancedata

import (
	"runtime/debug"
	"testing"
)

// Go's testing package is deliberately tiny. There is no assertion library in
// the standard library and no popular convention of adding one: a test is a
// plain function named TestXxx taking *testing.T, and it fails by calling
// t.Errorf. You write the comparison yourself; there is nothing to import and
// nothing to configure.
//
// This file also demonstrates the *table-driven test*, which is the dominant
// idiom in Go and the one worth internalising first: describe the cases as
// data in a slice, then run the same body over each one. Adding a case is one
// line, and every case reports its own name on failure.

func TestVersionFrom(t *testing.T) {
	// t.Parallel marks this test as safe to run alongside other parallel
	// tests. Combined with -race (see mise.toml) it is how Go surfaces
	// accidental shared state early.
	t.Parallel()

	// buildInfo is a small helper so each case below reads as one line rather
	// than four. Defining helpers inside the test function keeps them out of
	// the package's namespace entirely.
	buildInfo := func(version string) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) {
			return &debug.BuildInfo{
				Main: debug.Module{
					Path:    "github.com/algo-one/binance-data-downloader",
					Version: version,
				},
			}, true
		}
	}

	// The table. An anonymous struct type is used because it exists only here;
	// naming it would add a symbol to the package for no benefit.
	tests := []struct {
		name string
		read func() (*debug.BuildInfo, bool)
		want string
	}{
		{
			name: "tagged release returns the module version",
			read: buildInfo("v1.2.3"),
			want: "v1.2.3",
		},
		{
			name: "pseudo-version is passed through unchanged",
			read: buildInfo("v0.0.0-20260817120000-abcdef123456"),
			want: "v0.0.0-20260817120000-abcdef123456",
		},
		{
			name: "untagged build already reads as devel",
			read: buildInfo("(devel)"),
			want: DevVersion,
		},
		{
			name: "empty version falls back to devel",
			read: buildInfo(""),
			want: DevVersion,
		},
		{
			name: "missing build info falls back to devel",
			read: func() (*debug.BuildInfo, bool) { return nil, false },
			want: DevVersion,
		},
		{
			// Defensive: ok == true with a nil pointer should not panic.
			name: "nil build info falls back to devel",
			read: func() (*debug.BuildInfo, bool) { return nil, true },
			want: DevVersion,
		},
	}

	for _, tt := range tests {
		// t.Run creates a subtest. Failures are reported as
		// TestVersionFrom/tagged_release_returns_the_module_version, and you
		// can run one case in isolation with `go test -run`.
		//
		// Note that `tt` is used directly inside the closure. Before Go 1.22
		// every loop variable was reused across iterations, so this needed the
		// famous `tt := tt` line to avoid every subtest seeing the last case.
		// Go 1.22 gave each iteration its own variable, so that workaround is
		// gone — but you will still see it in older code and tutorials.
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := versionFrom(tt.read)

			// %q quotes the string, which makes an empty or whitespace-only
			// result visible in the failure message instead of invisible.
			if got != tt.want {
				t.Errorf("versionFrom() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestVersion checks the exported wrapper end to end. It cannot assert a
// specific value, because the answer legitimately depends on how the test
// binary was built — so it asserts the one property that must always hold.
func TestVersion(t *testing.T) {
	t.Parallel()

	if got := Version(); got == "" {
		t.Error("Version() returned an empty string; expected a version or DevVersion")
	}
}
