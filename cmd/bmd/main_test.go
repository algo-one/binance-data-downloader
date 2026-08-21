package main

import (
	"bytes"
	"context"
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
	tests := []struct {
		name string
		args []string

		// wantErr is compared with errors.Is. A nil value means "expect
		// success". errors.Is walks the chain of wrapped errors, which is why
		// `fmt.Errorf("%w: %s", errUsage, ...)` still matches errUsage — the
		// %w verb is what records that link.
		wantErr error

		// wantErrContains checks the message of an error that has no sentinel
		// to match against.
		wantErrContains string

		// wantStdout and wantStderr assert that a fragment appears on the
		// given stream. Which stream matters: usage text on stdout would
		// corrupt `bmd download ... -out - | jq`.
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
			name:            "no arguments is a usage error and usage goes to stderr",
			args:            []string{},
			wantErr:         errUsage,
			wantErrContains: "no command given",
			wantStderr:      "Usage:",
		},
		{
			name:            "an unknown command names the offender",
			args:            []string{"frobnicate"},
			wantErr:         errUsage,
			wantErrContains: `unknown command "frobnicate"`,
			wantStderr:      "Usage:",
		},
		{
			name:            "an unknown flag is reported",
			args:            []string{"-nope"},
			wantErrContains: "flag provided but not defined",
		},
		{
			// Every command requires flags, so an empty invocation of one is
			// a usage error rather than a crash — and it must name the flag
			// rather than failing somewhere in the library.
			name:            "download with no flags asks for a symbol",
			args:            []string{"download"},
			wantErr:         errUsage,
			wantErrContains: "-symbol is required",
		},
		{
			name:            "list with no flags asks for a symbol",
			args:            []string{"list"},
			wantErr:         errUsage,
			wantErrContains: "-symbol is required",
		},
		{
			name:            "a positional argument is refused rather than ignored",
			args:            []string{"verify", "somewhere"},
			wantErr:         errUsage,
			wantErrContains: `unexpected argument "somewhere"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// bytes.Buffer implements io.Writer, so it drops straight into
			// the slots that hold os.Stdout and os.Stderr in production.
			var stdout, stderr bytes.Buffer

			// t.Context() returns a context that is cancelled when the test
			// finishes, which is the right default for anything taking a ctx
			// in a test.
			err := run(t.Context(), tt.args, &stdout, &stderr)

			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("run() error = %v, want it to wrap %v", err, tt.wantErr)
			}

			switch {
			case tt.wantErrContains != "":
				if err == nil {
					t.Fatalf("run() error = nil, want one containing %q", tt.wantErrContains)
				}

				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Errorf("run() error = %q, want it to contain %q", err, tt.wantErrContains)
				}
			case tt.wantErr == nil:
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
// annoying to debug: when a command fails, stdout must carry nothing, so that
// `bmd download ... -out - > file` never produces a file with an error message
// in the middle of it.
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

// TestReportExitStatuses pins the mapping a script depends on. The four codes
// answer different questions — did it work, did it fail, did I type it wrong,
// did somebody stop it — and a shell can only see the number.
func TestReportExitStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		want     int
		wantSaid string
	}{
		{name: "success", err: nil, want: exitOK},
		{name: "help is not a failure", err: flag.ErrHelp, want: exitOK},
		{
			name:     "a usage error is 2",
			err:      usagef("no command given"),
			want:     exitUsage,
			wantSaid: "no command given",
		},
		{
			name:     "an interrupt is 130 and says so",
			err:      context.Canceled,
			want:     exitInterrupted,
			wantSaid: "interrupted",
		},
		{
			// Wrapped rather than bare, because that is how it arrives: the
			// cancellation is detected several layers down and travels up
			// through %w. Comparing with == would miss it.
			name:     "a wrapped interrupt is still 130",
			err:      errors.Join(errors.New("fetching BTCUSDT"), context.Canceled),
			want:     exitInterrupted,
			wantSaid: "interrupted",
		},
		{
			name:     "anything else is 1",
			err:      errors.New("data not available"),
			want:     exitFailure,
			wantSaid: "data not available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stderr bytes.Buffer

			if got := report(tt.err, &stderr); got != tt.want {
				t.Errorf("report(%v) = %d, want %d", tt.err, got, tt.want)
			}

			if tt.wantSaid != "" && !strings.Contains(stderr.String(), tt.wantSaid) {
				t.Errorf("stderr = %q, want it to mention %q", stderr.String(), tt.wantSaid)
			}
		})
	}
}
