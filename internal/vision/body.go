package vision

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"
)

// This file holds the two things every HTTP response in this package needs
// doing to it, and which are easy to get subtly wrong in ways nothing fails on
// until the system is under load.

const (
	// maxDrain is how much of an unwanted body is read before giving up on
	// reusing the connection. Error documents are kilobytes; anything larger
	// is not worth the transfer, and dropping the connection is the cheaper
	// mistake at that point.
	maxDrain = 64 << 10 // 64 KiB

	// maxSnippet is how much of an unrecognised body is quoted in an error.
	// Enough to see a proxy's HTML title or a JSON error field, short enough
	// to fit in a terminal.
	maxSnippet = 200

	// maxErrorDoc is how much of a body is read when it might be a structured
	// error document that wants parsing rather than quoting.
	//
	// It is deliberately far larger than [maxSnippet]. The two limits look
	// interchangeable and are not: a snippet may be cut anywhere, because its
	// job is to show a human the beginning of something, while a document cut
	// anywhere is invalid JSON and parses as nothing at all. Sharing one buffer
	// between the two uses is fine and reading it at the snippet's length is
	// not — the parse would then succeed or fail on how many characters the
	// server happened to put in its message.
	//
	// Eight kibibytes is far more than Binance's own error documents need — the
	// longest documented template comes to about 110 bytes — and the body is
	// being read and discarded either way, so the ceiling costs nothing until
	// something answers with an error document larger than any error document.
	maxErrorDoc = 8 << 10
)

// drain reads and discards what is left of a response body, so that the
// connection underneath it can be reused.
//
// # Why closing is not enough
//
// Finishing with a response looks like it should be one line —
// resp.Body.Close() — and that is the version almost everyone writes. It works,
// in the sense that nothing leaks. What it silently gives up is the connection:
// net/http can only return a connection to the idle pool once its body has been
// read to EOF, because a connection with unread bytes still on it cannot carry
// the next request. Closing a partially read body therefore closes the TCP
// connection instead of pooling it.
//
// That is invisible while everything succeeds, and it fires precisely when it
// hurts. Every non-200 answer — a burst of 503s from a struggling endpoint, a
// month of 404s for archives that were never published — would tear down its
// connection and force a fresh TCP and TLS handshake for the next request.
// Correctness requirement 8 in docs/architecture.md, "one http.Client is shared
// so that connections are reused", is defeated by the one line that looks
// harmless.
//
// The read is capped: a body large enough to be expensive to discard is not
// worth a connection.
func drain(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}

	// io.Discard is a writer that throws everything away without allocating,
	// and io.CopyN stops after n bytes. The error is deliberately ignored —
	// this function's whole job is to be best-effort, and there is nothing to
	// report about failing to throw bytes away. The explicit `_, _ =` is the
	// project's convention for "ignored on purpose", so that errcheck's
	// silence is a decision rather than an oversight.
	_, _ = io.CopyN(io.Discard, resp.Body, maxDrain)
}

// drainAndClose is [drain] followed by Close, for the paths that own the
// response rather than deferring its close to a caller.
//
// Close on an http response body is safe to call twice, so the two conventions
// can coexist without anyone having to trace which function took ownership.
func drainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}

	drain(resp)

	_ = resp.Body.Close()
}

// bodySnippet reads the beginning of a body for use in an error message, then
// drains and closes the rest.
func bodySnippet(resp *http.Response) string {
	return snippetOf(readBodyPrefix(resp, maxSnippet+utf8.UTFMax))
}

// readBodyPrefix reads up to limit bytes of a body and then drains and closes
// the rest, so that the connection underneath survives being finished with.
//
// It takes ownership of resp. A read that fails yields nil rather than an
// error: every caller is already on an error path and building a message, and
// "the body could not be read" is not a more useful thing to say there than
// saying nothing about the body at all.
//
// # Why this is a function
//
// The three lines it holds — a limited read, then [drainAndClose] — were
// written twice, once for the snippet and once for the error-document parse.
// Two copies of a limit is two limits, and the next change to body handling
// would silently apply to one endpoint only. The `+utf8.UTFMax` its snippet
// caller passes is the subtle half: it is what leaves [snippetOf] a whole rune
// to trim back through instead of a fragment of one.
func readBodyPrefix(resp *http.Response, limit int64) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}

	// LimitReader caps what is read; the drain below deals with whatever is
	// left on the connection.
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit))

	drainAndClose(resp)

	if err != nil {
		return nil
	}

	return b
}

// snippetOf trims b to something short enough, and safe enough, to end an error
// message with, and returns it quoted.
//
// # The UTF-8 trap
//
// Truncating a byte slice at a fixed length cuts wherever it lands, and if that
// is the middle of a multi-byte rune the result is not valid UTF-8. Printed, it
// ends in U+FFFD — the replacement character — so the one code path whose
// entire purpose is to explain what went wrong signs off with a mojibake box.
// Trimming back to the last rune boundary keeps the text intact and simply
// stops a byte or two earlier.
//
// # Why the result is quoted
//
// Trimming the cut only ever fixed the *tail*. The bytes in the middle were
// never examined, and a body short enough to need no truncation was never
// examined at all — so a misconfigured endpoint answering 500 with 150 bytes of
// a gzip frame, a TLS alert or a protobuf went into the error message verbatim,
// carrying invalid UTF-8 and raw control bytes into whatever terminal or log
// read it. A body claiming to be an error message is precisely the input least
// worth trusting about what it contains.
//
// strconv.Quote fixes both halves at once: it escapes control characters and
// invalid bytes as \x sequences, and the surrounding quotes delimit the
// server's words from ours in a message that concatenates the two. The
// delimiting matters more than it looks — an unquoted snippet ending in a colon
// reads as though the sentence continues into whatever the server sent.
//
// Quoting can expand the result by up to four times for binary input. That is
// the case where the expansion is the diagnosis: a screen of \x escapes says
// "this endpoint is not serving text" far faster than any status code.
//
// The bound is on bytes, not runes, because the input is untrusted and may not
// be text at all.
func snippetOf(b []byte) string {
	s := strings.TrimSpace(string(b))
	if s == "" {
		// Not strconv.Quote(""), which is a pair of quotes with nothing between
		// them. Callers test the result against "" to decide whether there is
		// anything worth appending at all, and `""` is not nothing.
		return ""
	}

	if len(s) > maxSnippet {
		s = s[:maxSnippet]

		// Walk back to a rune boundary. DecodeLastRuneInString reports
		// RuneError with a width of 1 for an invalid or truncated sequence,
		// which is exactly the case being trimmed away — while a genuine U+FFFD
		// in the input decodes with its real width of 3 and is left alone.
		for len(s) > 0 {
			r, size := utf8.DecodeLastRuneInString(s)
			if r != utf8.RuneError || size > 1 {
				break
			}

			s = s[:len(s)-1]
		}

		s += "..."
	}

	return strconv.Quote(s)
}
