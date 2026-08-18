// Command bmd downloads historical Binance market data from the command line.
//
// It is a thin shell around the binancedata library: every flag maps onto a
// field of binancedata.Request or an option passed to binancedata.NewLoader,
// and the CLI itself holds no logic of its own. Anything you can do here you
// can do from Go code, and vice versa.
//
// Usage:
//
//	bmd [flags] <command> [command flags]
//
// The commands are stubs today; they arrive in a later stage. Run `bmd -h`
// for the current surface.
package main

// A directory under cmd/ that declares `package main` becomes an executable.
// Everything else in the repository is library code. This is Go's whole
// convention for the split — there is no equivalent of an entry_points table.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	// The import path ends in "binance-data-downloader" but the package it
	// declares is named "binancedata" — hyphens are not legal in a Go
	// identifier. Go would resolve this on its own, but spelling the name out
	// as an alias means a reader of this file does not have to open another
	// one to learn what "binancedata." refers to.
	binancedata "github.com/algo-one/binance-data-downloader"
)

// Process exit statuses. Distinct codes per failure class can be added later
// if scripts need to tell them apart.
const (
	exitOK      = 0
	exitFailure = 1
)

// errNotImplemented is returned by the command stubs. Declaring it once as a
// package-level variable — rather than building the same string inline in
// three places — is the Go convention for an error a caller might want to
// recognise with errors.Is.
var errNotImplemented = errors.New("not implemented yet")

// main is deliberately one line.
//
// os.Exit terminates the process immediately and — this is the gotcha worth
// remembering — does NOT run deferred functions. So any function that calls it
// cannot also use defer for cleanup. Isolating os.Exit in main, and returning
// an exit status up from a function that can use defer freely, sidesteps that
// entirely. It also keeps every piece of real logic in a function a test can
// call.
func main() {
	os.Exit(cli())
}

// cli installs signal handling, runs the requested command, and translates the
// outcome into a process exit status.
func cli() int {
	// signal.NotifyContext turns Ctrl-C (os.Interrupt) and SIGTERM into
	// cancellation of ctx. Because every function in this project takes a
	// context and passes it down, that single line is the entire shutdown
	// mechanism: an in-flight HTTP download sees ctx.Done() and returns.
	//
	// The second return value, stop, releases the signal handler. This defer
	// genuinely runs, because cli returns normally rather than calling
	// os.Exit itself.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// run takes its output streams as parameters rather than reaching for the
	// os globals, so a test can pass a bytes.Buffer and assert on what was
	// printed.
	//
	// This is a switch with an initialiser and no tag value: `err` is scoped
	// to the switch, and each case is a boolean expression.
	switch err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); {
	case err == nil:
		return exitOK

	case errors.Is(err, flag.ErrHelp):
		// The flag package returns ErrHelp when the user asked for help with
		// -h or -help. Usage has already been printed at that point, and
		// asking for help is not a failure.
		return exitOK

	default:
		// The blank assignment is deliberate and is how you tell both a
		// reader and the errcheck linter that an error is being ignored on
		// purpose. If writing the error message to stderr fails, there is no
		// second channel on which to report that — the reporting channel is
		// the thing that broke.
		_, _ = fmt.Fprintf(os.Stderr, "bmd: %v\n", err)
		return exitFailure
	}
}

// run parses arguments and dispatches to a command. It returns an error rather
// than exiting so that main can decide the process's fate in one place.
//
// Passing stdout and stderr in as io.Writer rather than using the os globals
// directly is a small habit with a large payoff: the whole CLI becomes
// testable from an ordinary Go test.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	// flag.NewFlagSet is used instead of the package-level flag functions
	// (flag.Bool, flag.Parse, ...) because those write into a single global
	// FlagSet. A global would make two calls to run() in one test process
	// interfere with each other, and it is what forces most Go CLIs to be
	// untestable.
	//
	// flag.ContinueOnError makes Parse return an error; the alternative,
	// flag.ExitOnError, calls os.Exit internally and would defeat the whole
	// arrangement above.
	fs := flag.NewFlagSet("bmd", flag.ContinueOnError)
	fs.SetOutput(stderr)

	// Flag values come back as pointers, because Parse fills them in later.
	showVersion := fs.Bool("version", false, "print the version and exit")

	fs.Usage = func() { writeUsage(stderr) }

	if err := fs.Parse(args); err != nil {
		// Includes flag.ErrHelp for -h, which main treats as success.
		return err
	}

	if *showVersion {
		// Unlike the usage text, this is real output a caller may be piping
		// into something. A failed write is a genuine failure, so it is
		// returned rather than ignored.
		_, err := fmt.Fprintln(stdout, binancedata.Version())
		return err
	}

	// Anything left after the global flags is the subcommand and its own
	// arguments.
	rest := fs.Args()
	if len(rest) == 0 {
		writeUsage(stderr)
		return errors.New("no command given")
	}

	// If a signal arrived while we were parsing, stop before doing any work.
	if err := ctx.Err(); err != nil {
		return err
	}

	// rest[0] is the command; rest[1:] will become the arguments each
	// subcommand parses with its own flag.FlagSet once they are implemented.
	command := rest[0]

	// Go's switch needs no `break`; cases do not fall through unless you write
	// an explicit `fallthrough`.
	switch command {
	case "download":
		// Stage 8: fetch a range and write it as csv, json or parquet.
		return fmt.Errorf("download: %w", errNotImplemented)

	case "verify":
		// Stage 8: re-hash every cached archive against its .CHECKSUM sidecar.
		return fmt.Errorf("verify: %w", errNotImplemented)

	case "list":
		// Stage 8: report what Binance actually publishes for a symbol and
		// interval, by querying the public S3 listing.
		return fmt.Errorf("list: %w", errNotImplemented)

	case "help":
		writeUsage(stdout)
		return nil

	default:
		writeUsage(stderr)
		// %q quotes the command so a stray quote or space in the argument is
		// visible in the message rather than silently confusing.
		return fmt.Errorf("unknown command %q", command)
	}
}

// writeUsage prints the help text. It takes an io.Writer so that `bmd help`
// can send it to stdout (where it can be piped) while an argument error sends
// it to stderr (where it will not corrupt piped output).
func writeUsage(w io.Writer) {
	// Blank assignment: ignoring this error is deliberate. Help text is
	// advisory, and every caller of writeUsage is already on an error path or
	// about to return, so there is nothing useful to do if the write fails.
	_, _ = fmt.Fprint(w, `bmd - download historical Binance market data

Usage:
  bmd [flags] <command> [command flags]

Commands:
  download    Download candles for a symbol and time range
  verify      Re-verify cached archives against their checksums
  list        Show what Binance publishes for a symbol and interval
  help        Show this help

Flags:
  -version    Print the version and exit
  -h, -help   Show this help

The commands are not implemented yet; this is the Stage 0 scaffold.
`)
}
