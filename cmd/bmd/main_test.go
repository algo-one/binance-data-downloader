package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
)

// These tests exist mostly to demonstrate the payoff of the structure in
// main.go. Because run() takes its arguments and its two output streams as
// parameters instead of reading os.Args and writing to os.Stdout, the entire
// command-line surface can be exercised from an ordinary test — no subprocess,
// no temporary files, no os.Exit killing the test binary.

func TestRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string

		// wantErr is compared with errors.Is. A nil value means "expect
		// success". errors.Is walks the chain of wrapped errors, which is why
		// `fmt.Errorf("download: %w", errNotImplemented)` still matches
		// errNotImplemented — the %w verb is what records that link.
		wantErr error

		// wantErrContains checks the message of an error that has no sentinel
		// to match against.
		wantErrContains string

		// wantStdout and wantStderr assert that a fragment appears on the
		// given stream. Which stream matters: usage text on stdout would
		// corrupt `bmd download ... | jq`.
		wantStdout string
		wantStderr string
	}{
		{
			name:       "help writes usage to stdout",
			args:       []string{"help"},
			wantStdout: "Usage:",
		},
		{
			name:    "-h returns flag.ErrHelp",
			args:    []string{"-h"},
			wantErr: flag.ErrHelp,
			// The flag package prints usage itself, via the fs.Usage hook.
			wantStderr: "Usage:",
		},
		{
			name:       "-version writes a version to stdout",
			args:       []string{"-version"},
			wantStdout: "(devel)", // tests are never built from a tagged module
		},
		{
			name:            "no arguments is an error and usage goes to stderr",
			args:            []string{},
			wantErrContains: "no command given",
			wantStderr:      "Usage:",
		},
		{
			name:            "an unknown command names the offender",
			args:            []string{"frobnicate"},
			wantErrContains: `unknown command "frobnicate"`,
			wantStderr:      "Usage:",
		},
		{
			name:    "download is recognised but not implemented",
			args:    []string{"download"},
			wantErr: errNotImplemented,
		},
		{
			name:    "verify is recognised but not implemented",
			args:    []string{"verify"},
			wantErr: errNotImplemented,
		},
		{
			name:    "list is recognised but not implemented",
			args:    []string{"list"},
			wantErr: errNotImplemented,
		},
		{
			name:            "an unknown flag is reported",
			args:            []string{"-nope"},
			wantErrContains: "flag provided but not defined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// bytes.Buffer implements io.Writer, so it drops straight into
			// the slots that hold os.Stdout and os.Stderr in production.
			var stdout, stderr bytes.Buffer

			// t.Context() returns a context that is cancelled when the test
			// finishes, which is the right default for anything taking a ctx
			// in a test.
			err := run(t.Context(), tt.args, &stdout, &stderr)

			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("run() error = %v, want it to wrap %v", err, tt.wantErr)
				}
			case tt.wantErrContains != "":
				if err == nil {
					t.Fatalf("run() error = nil, want one containing %q", tt.wantErrContains)
				}
				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Errorf("run() error = %q, want it to contain %q", err, tt.wantErrContains)
				}
			default:
				if err != nil {
					t.Errorf("run() error = %v, want nil", err)
				}
			}

			if tt.wantStdout != "" && !strings.Contains(stdout.String(), tt.wantStdout) {
				t.Errorf("stdout = %q, want it to contain %q", stdout.String(), tt.wantStdout)
			}
			if tt.wantStderr != "" && !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantStderr)
			}
		})
	}
}

// TestRunKeepsUsageOffStdout guards a property that is easy to break later and
// annoying to debug: when a command succeeds, stdout must carry only the
// command's own output, so that `bmd download ... > file` produces a clean
// file.
func TestRunKeepsUsageOffStdout(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer

	// A deliberately invalid invocation.
	_ = run(t.Context(), []string{"frobnicate"}, &stdout, &stderr)

	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want it empty on an argument error", stdout.String())
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty; the error path should have explained itself")
	}
}
