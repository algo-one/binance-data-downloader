package main

// Everything about BMD_CACHE_DIR: how it is resolved against the -cache-dir
// flag, what an empty one does, and which commands are allowed to see it.

import (
	"bytes"
	"flag"
	"io"
	"os"
	"slices"
	"testing"
)

// TestMain clears BMD_CACHE_DIR for every test in this package.
//
// It lives in this file rather than main_test.go because this is the file about
// the environment, and because the reason it exists is entirely about this one
// variable: without it, a developer who has exported BMD_CACHE_DIR in their own
// shell runs a different test suite from everybody else. Every assertion below
// that says "nothing was set" would be asserting on that shell instead.
//
// Go allows one TestMain per package and calls it instead of running the tests
// directly; m.Run() is what actually runs them, and its return value is the
// exit status the test binary must report. Replacing lookupEnv here rather than
// calling os.Unsetenv leaves the real process environment alone, which matters
// because os.UserCacheDir — the default underneath all of this — reads
// XDG_CACHE_HOME out of that same environment.
func TestMain(m *testing.M) {
	lookupEnv = func(name string) (string, bool) {
		if name == cacheDirEnv {
			return "", false
		}

		return os.LookupEnv(name)
	}

	os.Exit(m.Run())
}

// setCacheDirEnv makes lookupEnv report BMD_CACHE_DIR as set to value for one
// test, and restores the previous lookup afterwards.
//
// It is the same seam fakeLoader.install uses on newLoader, with the same
// consequence: two tests that both install a lookup must not run in parallel
// with each other, which is why none of the tests here call t.Parallel.
func setCacheDirEnv(t *testing.T, value string) {
	t.Helper()

	original := lookupEnv

	lookupEnv = func(name string) (string, bool) {
		if name == cacheDirEnv {
			return value, true
		}

		return original(name)
	}

	t.Cleanup(func() { lookupEnv = original })
}

// cacheDirFlags parses args through a FlagSet that registers -cache-dir, which
// is what the five cache-touching commands hand to commonFlags.options.
func cacheDirFlags(t *testing.T, args ...string) (*commonFlags, *flag.FlagSet) {
	t.Helper()

	var c commonFlags

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	c.registerCacheDir(fs)

	if err := fs.Parse(args); err != nil {
		t.Fatalf("Parse(%q): %v", args, err)
	}

	return &c, fs
}

// TestCacheDirResolution is the precedence rule, stated once as a table.
//
// The flag beats the environment variable, which beats the library's default.
// An empty return means the default: WithCacheDir rejects an empty directory,
// so "no preference" is expressed by not passing the option at all.
func TestCacheDirResolution(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     string
		envSet  bool
		want    string
		wantErr bool
	}{{
		name: "neither the flag nor the variable is given",
		want: "",
	}, {
		name: "the flag alone",
		args: []string{"-cache-dir", "/from/flag"},
		want: "/from/flag",
	}, {
		name:   "the variable alone",
		env:    "/from/env",
		envSet: true,
		want:   "/from/env",
	}, {
		// The whole point of having both: what was typed for this one run beats
		// what was exported once into a shell.
		name:   "the flag beats the variable",
		args:   []string{"-cache-dir", "/from/flag"},
		env:    "/from/env",
		envSet: true,
		want:   "/from/flag",
	}, {
		// `export BMD_CACHE_DIR="$CACHE_DIR"` with CACHE_DIR unset. Defaulting
		// quietly here aims `bmd evict -all` at the user's real cache.
		name:    "the variable is set but empty",
		env:     "",
		envSet:  true,
		wantErr: true,
	}, {
		// The same shell mistake with a stray quote in it.
		name:    "the variable holds only spaces",
		env:     "   ",
		envSet:  true,
		wantErr: true,
	}, {
		// TrimSpace decides whether the value is empty; it never edits the value.
		// A directory name may legally end in a space on Unix, and rewriting one
		// would send the cache somewhere its owner did not name.
		name:   "a path ending in a space is passed through as written",
		env:    "/from/env ",
		envSet: true,
		want:   "/from/env ",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSet {
				setCacheDirEnv(t, tt.env)
			}

			c, fs := cacheDirFlags(t, tt.args...)

			got, err := c.resolveCacheDir(fs)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveCacheDir = %q, nil; want a usage error", got)
				}

				if status := report(err, &bytes.Buffer{}); status != exitUsage {
					t.Errorf("exit status = %d, want %d", status, exitUsage)
				}

				return
			}

			if err != nil {
				t.Fatalf("resolveCacheDir: %v", err)
			}

			if got != tt.want {
				t.Errorf("resolveCacheDir = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestCacheDirEnvIsIgnoredWhereTheFlagIsNotRegistered is the rule the whole
// design turns on, checked at the level it is enforced.
//
// `bmd list` opens no cache, so it registers no -cache-dir. The environment is
// held to the same standard: a variable a command reads and never acts on is
// the accepted-and-ignored setting docs/architecture.md calls a defect, and it
// costs a debugging session to discover. fs.Lookup returning nil for an
// unregistered flag is what makes that automatic — a future command that does
// not register -cache-dir cannot accidentally start honouring BMD_CACHE_DIR.
func TestCacheDirEnvIsIgnoredWhereTheFlagIsNotRegistered(t *testing.T) {
	setCacheDirEnv(t, "/from/env")

	var c commonFlags

	// A FlagSet with no -cache-dir on it, which is exactly what `bmd list`
	// builds.
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	got, err := c.resolveCacheDir(fs)
	if err != nil {
		t.Fatalf("resolveCacheDir: %v", err)
	}

	if got != "" {
		t.Errorf("resolveCacheDir = %q, want \"\" for a command that registers no -cache-dir", got)
	}
}

// TestCacheDirEnvReachesTheLoader runs the variable through a real command.
//
// An Option is an opaque interface, so there is nothing to read out of one —
// but counting them tells a setting that produced an option from one that was
// quietly dropped somewhere between resolveCacheDir and newLoader. `bmd cache`
// with no flags produces no options at all, so the count here is the whole
// assertion.
func TestCacheDirEnvReachesTheLoader(t *testing.T) {
	setCacheDirEnv(t, t.TempDir())

	f := &fakeLoader{usage: fullCache()}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := len(f.gotOptions); got != 1 {
		t.Errorf("newLoader got %d options, want 1 (the cache directory from %s)", got, cacheDirEnv)
	}
}

// TestCacheDirEnvIsAbsentByDefault is the other half of the test above: without
// the variable, `bmd cache` passes no options, so the 1 up there is the
// variable's doing and not something the command always sends.
func TestCacheDirEnvIsAbsentByDefault(t *testing.T) {
	f := &fakeLoader{usage: fullCache()}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := len(f.gotOptions); got != 0 {
		t.Errorf("newLoader got %d options, want 0 when %s is unset", got, cacheDirEnv)
	}
}

// TestCacheDirEnvRejectsAnEmptyValue is TestCacheRejectsAnEmptyCacheDir's twin,
// checked through a whole command rather than through resolveCacheDir alone: the
// error has to survive the trip out of options and reach report as a usage
// error, or the shell sees exit 1 and treats a typing mistake as a failed run.
func TestCacheDirEnvRejectsAnEmptyValue(t *testing.T) {
	setCacheDirEnv(t, "")

	f := &fakeLoader{usage: fullCache()}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"cache"}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("run returned nil, want a usage error for an empty %s", cacheDirEnv)
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}
}

// TestCacheDirEnvIsIgnoredByList is the command-level version of the rule: the
// variable is set, `bmd list` runs, and the loader is built with nothing.
func TestCacheDirEnvIsIgnoredByList(t *testing.T) {
	setCacheDirEnv(t, "/from/env")

	f := &fakeLoader{available: availabilityWithAHole(t)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"list", "-symbol", "BTC/USDT", "-interval", "1mo"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := len(f.gotOptions); got != 0 {
		t.Errorf("newLoader got %d options, want 0 — `bmd list` opens no cache", got)
	}
}

// TestCacheDirFlagBeatsTheEnvOnEveryCommandThatTakesIt walks the five commands
// that register -cache-dir.
//
// Precedence is settled in one place, so this is not five copies of the same
// logic being re-tested — it is the check that all five reach that place. A
// command that built its loader without going through commonFlags.options would
// be invisible to every test above and would fail here.
func TestCacheDirFlagBeatsTheEnvOnEveryCommandThatTakesIt(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"cache", []string{"cache"}},
		{"prune", []string{"prune", "-n"}},
		{"evict", []string{"evict", "-all", "-n"}},
		{"verify", []string{"verify"}},
		{"download", []string{
			"download",
			"-symbol", "BTCUSDT",
			"-interval", "1h",
			"-start", "2024-01-15",
			"-end", "2024-01-15",
			"-quiet",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// An empty variable, which resolveCacheDir rejects — unless the flag
			// is there to win first. That makes the assertion a strong one: the
			// command succeeding proves the environment was never consulted,
			// rather than proving one path happened to agree with the other.
			setCacheDirEnv(t, "")

			f := &fakeLoader{
				usage:     fullCache(),
				available: availabilityWithAHole(t),
				klines:    testKlines(t, 1),
			}
			f.install(t)

			// download writes a file into the working directory.
			t.Chdir(t.TempDir())

			var stdout, stderr bytes.Buffer

			// slices.Concat rather than append(tt.args, ...): appending to a
			// slice from the table can write into its backing array, so one
			// subtest would be editing the arguments of the next.
			args := slices.Concat(tt.args, []string{"-cache-dir", t.TempDir()})

			if err := run(t.Context(), args, &stdout, &stderr); err != nil {
				t.Fatalf("run: %v", err)
			}
		})
	}
}
