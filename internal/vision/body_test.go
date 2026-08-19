package vision

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// unquoteSnippet undoes the quoting snippetOf applies, so that the assertions
// below can talk about the text itself rather than about its escaping.
//
// It doubles as an assertion in its own right: a result that does not unquote
// is one that was not quoted, and quoting is what keeps a body of raw bytes
// from reaching a terminal intact.
func unquoteSnippet(t *testing.T, s string) string {
	t.Helper()

	unquoted, err := strconv.Unquote(s)
	if err != nil {
		t.Fatalf("snippet %q is not a quoted string: %v", s, err)
	}

	return unquoted
}

// TestSnippetOfNeverEndsMidRune covers the failure that only ever appears in an
// error message, and only for a server that answers in a non-ASCII language.
//
// Truncating a byte slice at a fixed length cuts wherever it lands. If that is
// the middle of a multi-byte rune the result is not valid UTF-8, and printing
// it ends the message with U+FFFD — so the one code path whose entire purpose
// is to explain what went wrong signs off with a mojibake box.
func TestSnippetOfNeverEndsMidRune(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{"short ascii", "not an S3 endpoint"},
		{"exactly at the limit", strings.Repeat("a", maxSnippet)},
		{"one past the limit", strings.Repeat("a", maxSnippet+1)},
		// The euro sign is three bytes, so a 200-byte cut lands inside one of
		// them for two out of every three offsets. This is a real gateway
		// error page in any language that is not English.
		{"multi-byte, cut inside a rune", strings.Repeat("a", maxSnippet-2) + strings.Repeat("€", 20)},
		{"multi-byte throughout", strings.Repeat("é", maxSnippet)},
		{"emoji", strings.Repeat("🚫", maxSnippet)},
		{"cyrillic error page", "<html>" + strings.Repeat("Ошибка сервера ", 40) + "</html>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := unquoteSnippet(t, snippetOf([]byte(tt.body)))

			if !utf8.ValidString(got) {
				t.Errorf("snippet %q is not valid UTF-8", got)
			}

			// The input carries no replacement characters, so any in the
			// output were manufactured by the truncation.
			if strings.ContainsRune(got, utf8.RuneError) {
				t.Errorf("snippet %q ends in a replacement character", got)
			}

			// Trimming back to a rune boundary may drop up to three bytes,
			// and the ellipsis adds three.
			if len(got) > maxSnippet+len("...") {
				t.Errorf("snippet is %d bytes, want at most %d", len(got), maxSnippet+3)
			}
		})
	}
}

func TestSnippetOfKeepsShortBodiesWhole(t *testing.T) {
	t.Parallel()

	// Whitespace is trimmed — an error page is usually indented — but nothing
	// short is abbreviated, and no ellipsis is added to a complete message.
	if got, want := unquoteSnippet(t, snippetOf([]byte("\n  NoSuchBucket  \n"))), "NoSuchBucket"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// Empty is empty, not a pair of quotes with nothing between them. Callers
	// test the result against "" to decide whether to append anything at all,
	// and `""` at the end of an error message is worse than no snippet.
	if got := snippetOf(nil); got != "" {
		t.Errorf("got %q, want empty", got)
	}

	if got := snippetOf([]byte("   \n\t ")); got != "" {
		t.Errorf("a whitespace-only body gave %q, want empty", got)
	}
}

// TestSnippetOfEscapesBytesThatAreNotText is the regression test for a body that
// is not a message at all.
//
// A misconfigured endpoint answering 500 with a gzip frame, a TLS alert or a
// protobuf used to have those bytes copied verbatim into an error string, which
// then reached a terminal and a log file — invalid UTF-8, raw control
// characters, ANSI escapes and all. The truncation path never covered this: a
// body short enough to need no truncation was returned entirely unexamined, and
// a longer one had only its final rune boundary repaired.
func TestSnippetOfEscapesBytesThatAreNotText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body []byte
	}{
		// A gzip frame: the two magic bytes, then binary. Short enough that the
		// truncating branch never runs.
		{"gzip frame", []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xff, 0xfe}},
		{"tls alert", []byte{0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28}},
		// Lone continuation bytes: valid nowhere in UTF-8, and in the middle
		// rather than at the cut, which is the half the rune-boundary walk
		// never looked at.
		{"invalid utf-8 in the middle", []byte("before\xc3\x28\xa0\xa1after")},
		// An ANSI escape sequence rewrites the terminal it is printed to.
		{"ansi escape", []byte("\x1b[2J\x1b[31mnot really an error\x1b[0m")},
		{"control characters", []byte("line one\r\x00\x07line two")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := snippetOf(tt.body)

			if !utf8.ValidString(got) {
				t.Fatalf("snippet %q is not valid UTF-8", got)
			}

			// strconv.Quote escapes everything strconv.IsPrint rejects, so a
			// printable result is the whole assertion — no control byte, no
			// escape sequence and no undecodable byte survives to be printed.
			for _, r := range got {
				if !strconv.IsPrint(r) {
					t.Errorf("snippet %q carries the unprintable rune %U", got, r)
				}
			}

			if !strings.HasPrefix(got, `"`) || !strings.HasSuffix(got, `"`) {
				t.Errorf("snippet %q is not quoted", got)
			}
		})
	}
}

// TestSnippetOfLeavesRealReplacementCharactersAlone guards the trimming loop
// against eating valid input.
//
// U+FFFD may legitimately appear in a body that something upstream already
// decoded lossily, and the loop has to tell that apart from the U+FFFD its own
// truncation would create. utf8.DecodeLastRuneInString is what distinguishes
// them: an encoded U+FFFD reports its real width of 3, while a sequence cut
// short reports RuneError with a width of 1.
//
// The character is placed so that it ends exactly on the limit. One byte later
// and it would straddle the cut, at which point trimming it away is the correct
// answer rather than a bug — it is then genuinely half a rune.
func TestSnippetOfLeavesRealReplacementCharactersAlone(t *testing.T) {
	t.Parallel()

	body := strings.Repeat("a", maxSnippet-len("�")) + "�" + strings.Repeat("b", 50)

	got := unquoteSnippet(t, snippetOf([]byte(body)))
	if !strings.HasSuffix(got, "�...") {
		t.Errorf("snippet %q dropped a replacement character that was in the input", got)
	}
}
