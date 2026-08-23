// Command bmd downloads historical Binance market data from the command line.
//
// It is a thin shell around the binancedata library: every flag maps onto a
// field of binancedata.Request or an option passed to binancedata.NewLoader,
// and the CLI itself holds no logic of its own beyond turning text into those
// values and candles into a file. Anything you can do here you can do from Go
// code, and vice versa.
//
// Usage:
//
//	bmd [flags] <command> [command flags]
//
// The six commands are download, list, cache, prune, evict and verify. Run
// `bmd help` for the summary and `bmd <command> -h` for a command's own flags.
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

// Process exit statuses.
//
// Four rather than two, because a script wrapping this tool wants to tell them
// apart. "I typed the command wrong" and "Binance is down" call for completely
// different responses, and a shell can only see the number.
const (
	exitOK = 0

	// exitFailure is the catch-all: the work was attempted and did not
	// succeed. A missing archive, a checksum mismatch, an unwritable output
	// file, a cache with bad files in it.
	exitFailure = 1

	// exitUsage is for a command line that could not be acted on at all — an
	// unknown flag, a missing required one, a date that will not parse. It is
	// 2 because that is what getopt, and therefore most of Unix, uses.
	exitUsage = 2

	// exitInterrupted is Ctrl-C. The shell convention is 128 plus the signal
	// number, and SIGINT is 2. Reporting it distinctly is what lets a script
	// tell "the user stopped it" from "it failed", which matters most in the
	// case where those look identical: a partial download.
	exitInterrupted = 130
)

// errUsage marks an error as being about the command line rather than about the
// work. It is never returned on its own — it is wrapped around a message that
// says what was wrong — and its only job is to select an exit status.
var errUsage = errors.New("usage")

// usagef builds a usage error, since every call site does the same two things.
func usagef(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errUsage, fmt.Sprintf(format, args...))
}

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
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)

	return report(err, os.Stderr)
}

// report prints an error if there is one and returns the exit status for it.
//
// It is split out of cli so that the mapping from error to status is one
// switch a test can drive directly, rather than something only reachable by
// starting a process.
func report(err error, stderr io.Writer) int {
	switch {
	case err == nil:
		return exitOK

	case errors.Is(err, flag.ErrHelp):
		// The flag package returns ErrHelp when the user asked for help with
		// -h or -help. Usage has already been printed at that point, and
		// asking for help is not a failure.
		return exitOK

	case errors.Is(err, context.Canceled):
		// Ctrl-C. Say something, because a command that stops mid-download and
		// prints nothing is indistinguishable from one that finished.
		//
		// The blank assignment is deliberate and is how you tell both a reader
		// and the errcheck linter that an error is being ignored on purpose.
		// If writing to stderr fails there is no second channel on which to
		// report that — the reporting channel is the thing that broke.
		_, _ = fmt.Fprintln(stderr, "bmd: interrupted")

		return exitInterrupted

	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "bmd: %v\n", err)

		return exitUsage

	default:
		_, _ = fmt.Fprintf(stderr, "bmd: %v\n", err)

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
		// Includes flag.ErrHelp for -h, which report treats as success.
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

		return usagef("no command given")
	}

	// If a signal arrived while we were parsing, stop before doing any work.
	if err := ctx.Err(); err != nil {
		return err
	}

	command, args := rest[0], rest[1:]

	// Go's switch needs no `break`; cases do not fall through unless you write
	// an explicit `fallthrough`.
	switch command {
	case "download":
		return download(ctx, args, stdout, stderr)

	case "list":
		return list(ctx, args, stdout, stderr)

	case "verify":
		return verify(ctx, args, stdout, stderr)

	case "cache":
		return cacheReport(ctx, args, stdout, stderr)

	case "prune":
		return prune(ctx, args, stdout, stderr)

	case "evict":
		return evict(ctx, args, stdout, stderr)

	case "help":
		writeUsage(stdout)

		return nil

	default:
		writeUsage(stderr)

		// %q quotes the command so a stray quote or space in the argument is
		// visible in the message rather than silently confusing.
		return usagef("unknown command %q", command)
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
  download    Download candles for symbols, intervals and a time range
  list        Show what Binance publishes for a symbol and interval
  cache       Show what the cache holds and what can be reclaimed
  prune       Delete cached archives the cache no longer reads
  evict       Delete cached data you no longer want
  verify      Re-hash cached archives against their checksums
  help        Show this help

Flags:
  -version    Print the version and exit
  -h, -help   Show this help

Run 'bmd <command> -h' for a command's own flags.

Examples:
  bmd download -symbol BTC/USDT -interval 1h -start 2024-01-01 -end 2024-03-31
  bmd download -symbol BTC/USDT -interval 1d -start 2024-01-01 -format json -out -
  bmd list -symbol BTC/USDT -interval 1h
  bmd cache
  bmd prune -n
  bmd evict -symbol BTC/USDT -before 2023-01-01
  bmd verify

Dates are UTC. A bare -end date includes that whole day.
Data goes to stdout only when -out is "-"; everything else goes to stderr.
`)
}
