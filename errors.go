package binancedata

import "errors"

// This file holds every sentinel error the package can return. Keeping them in
// one place is a deliberate choice: the set of things that can go wrong is part
// of the public API, and a reader should be able to learn it from a single
// screen rather than by grepping for errors.New.
//
// # What an error is in Go
//
// There are no exceptions. `error` is an ordinary interface with one method:
//
//	type error interface { Error() string }
//
// Any type implementing it is an error, and errors travel as ordinary return
// values. A function that can fail returns one as its last result, and the
// caller checks it. That is the whole mechanism.
//
// # What a sentinel is
//
// A sentinel is a package-level error variable that stands for one specific
// condition, so callers can ask "was it *this* problem?" rather than matching
// on message text. errors.New returns a pointer to a newly allocated struct,
// which matters more than it looks: two calls to errors.New("boom") produce two
// distinct, non-equal errors. Identity is the thing being compared, never the
// string — so the messages below can be reworded without breaking any caller.
//
// # Wrapping, and why %w
//
// A sentinel alone is too coarse: "invalid request" does not say which field.
// So the site that detects the problem adds context by wrapping:
//
//	return fmt.Errorf("interval %q: %w", s, ErrInvalidRequest)
//
// The %w verb (as opposed to %v) records the wrapped error inside the new one,
// building a chain that errors.Is can walk. The result prints as
// `interval "2h30m": invalid request` and still answers true to
// errors.Is(err, ErrInvalidRequest).
//
// # Always errors.Is, never ==
//
// Comparing with == tests only the outermost error, so it silently returns
// false the moment anyone wraps. errors.Is walks the whole chain, so it keeps
// working. The errorlint linter (see .golangci.yml) fails the build on ==,
// which is how this rule stays true as the codebase grows.
//
//	if errors.Is(err, ErrNotAvailable) { ... }   // correct
//	if err == ErrNotAvailable { ... }            // broken by any wrapping
//
// A `var` block groups related declarations the way an import block groups
// imports. It is one declaration, not six, and reads as a single unit.
var (
	// ErrInvalidRequest reports that a request was rejected before any I/O
	// happened, because something about the request itself is wrong: an
	// unknown interval, a malformed symbol, a start after its end, an interval
	// Binance does not publish at the granularity being asked for.
	//
	// This error is always the caller's to fix, and it is always cheap: no
	// network round trip is spent discovering it.
	ErrInvalidRequest = errors.New("invalid request")

	// ErrNotAvailable reports that Binance does not have the requested data —
	// an archive URL that answers 404, a symbol that was not yet listed on the
	// date asked for, or a day too recent to have been published.
	//
	// This is a fact about the world, not a failure of the program, and it is
	// routinely the correct answer. The download layer converts a 404 into
	// this error so that "no data here" arrives as a value the caller can
	// branch on.
	//
	// The Python implementation this library replaces returned a None
	// DataFrame for a 404 and relied on every call site remembering to check.
	// Several did not. Here, ignoring the error is a visible act.
	ErrNotAvailable = errors.New("data not available")

	// ErrChecksum reports that a downloaded or cached archive did not match
	// the SHA-256 in its .CHECKSUM sidecar. Either the transfer was corrupted
	// or the cached file was damaged on disk; in both cases the bytes are not
	// what Binance published and must not be parsed.
	//
	// The Python implementation wrote checksums to disk and never verified
	// them on read, which makes the sidecar decorative. Here a mismatch is
	// load-bearing: it discards the file and re-downloads.
	ErrChecksum = errors.New("checksum mismatch")

	// ErrCorruptArchive reports that bytes which passed transport and checksum
	// checks still could not be understood: a ZIP whose central directory will
	// not open, an archive holding no CSV member, a row with the wrong number
	// of fields, a price that will not parse as a decimal.
	//
	// It is kept distinct from ErrChecksum because the two call for different
	// responses. A checksum failure is transient and worth retrying; a corrupt
	// archive that verified correctly means Binance published something this
	// parser does not understand, and retrying will produce the same bytes.
	ErrCorruptArchive = errors.New("corrupt archive")

	// ErrRateLimited reports that Binance asked us to slow down — HTTP 429, or
	// 418 after repeated 429s. The REST tail of a request is the only place
	// this can occur; the bulk archives are static files and are not limited.
	//
	// It is a distinct sentinel because it is the one failure where the right
	// response is to wait rather than to give up or to try elsewhere. A 418
	// carries [ErrIPBanned] as well, for the case where waiting is not enough.
	ErrRateLimited = errors.New("rate limited")

	// ErrIPBanned reports an HTTP 418: Binance has barred this IP address
	// rather than merely asked it to slow down. It is the escalation applied
	// to a client that keeps ignoring 429s, and it lasts from two minutes to
	// three days, lengthening with repeat offences.
	//
	// Every error carrying it also carries ErrRateLimited, so a caller asking
	// only "should I slow down?" needs to know nothing about it. The reason it
	// exists as well is that the right response is different in kind: there is
	// no backoff short enough to ride out a ban, and retrying is what earns the
	// next, longer one. A pipeline that can stop should stop.
	//
	// The ban is on the address, not the process or the API key. One earned by
	// a history download also locks out anything else on the same host — a live
	// trading bot, most expensively — which is why internal/vision paces this
	// endpoint preventatively rather than reacting once this arrives.
	ErrIPBanned = errors.New("ip banned")
)
