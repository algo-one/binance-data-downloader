# Stage 6 code review

**Date:** 2026-08-20
**Branch:** `main` (uncommitted working tree, on top of `0e4a626 Restore the Stage 4 review doc`)
**Scope:** the Stage 6 diff — new `internal/vision/klines.go`, `internal/vision/limiter.go`,
`restapi.go` and their tests, the new fixture `testdata/BTCUSDT-1h-2024-01-15.klines.json`,
plus the touched support files (`download.go`, `cache.go`, `cache_test.go`,
`internal/vision/download.go`, `internal/vision/listing.go`, `.github/workflows/ci.yml`,
`CLAUDE.md`, `docs/architecture.md`, `testdata/README.md`, `go.mod`, `go.sum`).

15 findings. `mise run ci` is green — fmt, `golangci-lint` (0 issues), `-race` tests and
build all pass — so every finding here is a logic or design defect rather than something
the toolchain would have caught.

Thirteen of the fifteen were re-checked against the source before being written down and
are marked **Verified**. Two — #3 and #8 — could not be confirmed without a network call
to Binance or an injected filesystem fault, and are marked as such; treat their stated
consequence as a claim to test, not a fact.

Fix order recommendation: **#1**, **#2** and **#3** first. All three change how Stage 7's
orchestration will behave — the first two by handing the pool a wrong verdict about
whether to retry, the third by failing a fetch that was never wrong. **#14** and **#15**
are one-line documentation fixes worth doing in the same pass, because both actively
misdirect the next reader.

---

## Correctness

### 1. A Binance 5xx outage is classified as the caller's own bug

**`internal/vision/klines.go:397`** — *Verified*

`retryableStatus` (`internal/vision/retry.go:392`) includes 500, 502, 503 and 504. When
`doWithRetry` exhausts `Policy.MaxAttempts`, it deliberately returns the response rather
than a synthesised error, so the caller can report what the server actually said:

```go
if attempt >= p.MaxAttempts {
    return resp, nil
}
```

`statusError` (`klines.go:345`) then handles 429/418 and 404 explicitly and sends
everything else — including every 5xx — to its `default` branch:

```go
default:
    return fmt.Errorf("%s: %w", label, readAPIError(resp))
```

`readAPIError` parses the body and returns `*APIError` whenever `doc.Code != 0`.
Binance's documented 5xx shape carries exactly that document, e.g.
`{"code":-1001,"msg":"Internal error; unable to process your request."}`.
`(*APIError).Unwrap` returns `ErrBadRequest`, which `translateVisionError`
(`download.go:167`) maps to `ErrInvalidRequest`.

**Failure scenario.** Binance has a server-side outage. Four attempts fail with HTTP 500
and a `{"code","msg"}` body. The root package reports `ErrInvalidRequest`, which
`errors.go` documents as "always the caller's to fix" and "no network round trip is spent
discovering it". Stage 7 will treat a transient outage as an unfixable caller bug and
refuse to retry or fall back to another source.

The test suite does not catch this because its only non-4xx case — `an unexplained status
quotes what arrived`, a 502 — uses an HTML body, which fails the JSON parse and falls
through to the untyped snippet branch.

The fix belongs in `statusError`: a 5xx should not reach `readAPIError`'s typed branch at
all, or `readAPIError` should take the status into account before deciding the body
means `ErrBadRequest`.

---

### 2. A REST 404 is reported as "Binance does not have this data"

**`download.go:150`** — *Verified*

```go
case errors.Is(err, vision.ErrNotFound):
    // A 404 is a fact about the calendar, not a failure: the month is not
    // published yet, or the symbol had not been listed on that day.
    return fmt.Errorf("%w: %w", err, ErrNotAvailable)
```

That reading is right for `data.binance.vision`, which is a static file server: a missing
object genuinely is a fact about the calendar. It is wrong for the REST endpoint, which
answers a range it has no data for with `[]` and HTTP 200 — never a 404. A 404 from
`/api/v3/klines` means the base URL or the path is wrong.

**Failure scenario.** A caller sets an option pointing at the wrong host, or Binance moves
the endpoint path. Every REST chunk 404s, becomes `ErrNotAvailable`, and correctness
requirement 4 degrades the whole REST tail to nothing — returning a successful, silently
short result instead of a configuration error.

`restapi_test.go:523` (`404 is a fact about the calendar`) locks the wrong reading in for
the REST half. For that half a 404 should stay untyped, the way `translateVisionError`'s
`default` branch already handles anything it does not recognise.

---

### 3. The pagination cursor lands inside the previous candle

**`restapi.go:220`** — *Not verified: confirming it requires a live Binance call*

```go
cursor = prev.Add(time.Millisecond)
```

`prev` is the previous candle's **open** time, so the cursor is one millisecond past an
open — an instant that is still inside that candle's interval. This is only safe if
Binance's inclusive `startTime` filters strictly on open time.

**Failure scenario.** If `startTime` instead selects the kline whose *interval contains*
the timestamp, page 2 begins with the same candle at `T` that page 1 ended with.
`appendPage`'s strict-increase check then fails the entire fetch:

```go
if !prev.IsZero() && !k.OpenTime.After(*prev) {
    return false, fmt.Errorf("page %d row %d: open time %s does not follow ...: %w",
        page, i+1, ..., ErrCorruptArchive)
}
```

No test can catch this. `serveKlines` (`restapi_test.go:92`) filters on
`row.open.UnixMilli()` and therefore encodes the assumption under test as its own
behaviour.

The Python loader this is a port of advanced by `close_time+1` instead
(`my_engine/src/algo_one/history_data/fetch_ohlcv.py:148`) — exactly the next candle's
open, which is correct under either reading. `intervalEnd(prev, spec.Interval)` is the Go
equivalent, skips nothing, and removes the dependency on which semantics Binance uses.

Worth resolving by measurement against the live endpoint, the way the rest of this stage's
numbers were, and recording the answer next to the cursor.

---

### 4. `readAPIError` truncates the body at 204 bytes before parsing it

**`internal/vision/klines.go:384`** — *Verified*

```go
b, readErr := io.ReadAll(io.LimitReader(resp.Body, maxSnippet+utf8.UTFMax))
```

The same bytes serve both the JSON parse and the fallback snippet, which is a sensible
economy for the snippet and a bug for the parse. `maxSnippet+utf8.UTFMax` is 204 bytes.

**Failure scenario.** Binance returns HTTP 400 with a long message, e.g.
`{"code":-1102,"msg":"Mandatory parameter 'symbol' was not sent, was empty/null, or malformed. ..."}`.
`json.Unmarshal` fails on the truncated JSON, so the function falls through to the snippet
branch and returns an untyped `unexpected status 400 Bad Request: "..."` — no `*APIError`,
no `ErrBadRequest`, and therefore no `ErrInvalidRequest` at the root.

Whether a caller can branch on "this is my bug" currently depends on how many characters
Binance put in `msg`. The parse needs its own, larger read limit.

---

### 5. The limiter reserves once per call, but a call can send four requests

**`internal/vision/klines.go:308`** — *Verified*

```go
if err := a.limiter.WaitN(ctx, KlinesWeight); err != nil {
```

The doc comment justifies reserving once rather than per retry, on the grounds that
retries already carry their own backoff and honour `Retry-After`, so pacing them again
would apply two delays for one problem. That argument is sound **for pacing**. It does not
address **accounting**: `doWithRetry` sends up to `Policy.MaxAttempts` (4) HTTP requests
against a single 2-unit reservation, so the quota bookkeeping under-counts by up to 4x —
and it does so precisely when it matters, because 429 and every retryable 5xx are the
statuses that trigger the extra requests.

**Failure scenario.** Stage 7's bounded pool drives the configured 40 weight/second (20
klines calls/s). Binance starts answering 429/503. Each call becomes four requests: 80
requests/s × 2 weight = 160 weight/s against the 100 weight/s ceiling `limiter.go`'s own
file comment computes, i.e. 9,600/minute against a 6,000/minute quota. The limiter that
exists specifically to pre-empt the 418 escalation is what permits it.

`limiter_test.go` tests `rate.Limiter` in isolation and never exercises
Klines-with-retries, so nothing measures the real spend.

Options, in increasing order of intrusiveness: reserve per attempt inside `doWithRetry`;
reserve once and top up on each retry; or leave it and document the ceiling as
`MaxAttempts × DefaultWeightPerSecond`. The current comment should not be left implying
the accounting is exact.

---

### 6. Exceeding `maxRESTPages` is reported as `ErrCorruptArchive`

**`restapi.go:189`** — *Verified*

```go
if page > maxRESTPages {
    return nil, fmt.Errorf("%s: still incomplete after %d pages: %w",
        ref, maxRESTPages, ErrCorruptArchive)
}
```

The page cap is a resource bound. `ErrCorruptArchive` tells the caller Binance published
bytes this library cannot understand, and `errors.go` documents it as "retrying will
produce the same bytes".

**Failure scenario.** Binance stops publishing daily archives for two weeks — an outage,
or a new pair partway through a backfill — and the REST tail for a `1s` interval spans 15
days: 1,296,000 candles at 1,000 per page is 1,296 pages, past the 1,000 cap. The caller
is told to give up on data that is perfectly fine and merely large.

Needs its own condition. The distinction matters to Stage 7, which is the layer that could
decide to split the range instead of failing it.

---

### 7. Nothing validates the injected `now`

**`restapi.go:262`** — *Verified*

```go
if !f.includePartial && intervalEnd(k.OpenTime, spec.Interval).After(now) {
    return false, nil
}
```

`restRef.validate` and `decodeSpec.validate` both reject zero times. `now` passes through
unchecked.

**Failure scenario.** A future call site passes `time.Time{}` — a forgotten field in a
struct literal, a clock not yet wired. `intervalEnd(...).After(time.Time{})` is true for
every real candle, so `appendPage` returns `settled=false` on row 1 of page 1, the loop
breaks, and the caller receives `([], nil)`.

A failed read presented as an empty range is the exact conflation flagged as finding 1 of
the Stage 3 review and forbidden by the design. Only reachable through an internal
miswiring today, but the guard costs one line and the existing `validate` methods already
establish the convention.

---

### 8. `writeAtomic` creates directories before it knows the write will succeed

**`cache.go:301`, `cache.go:649`** — *Not verified: needs a fault-injected filesystem or a
missing archive to observe*

The Stage 6 cancellation fix added a `ctx.Err()` check before the `singleflight` loop,
which addresses the flaky-test symptom described in the comment above it. The claim here
is that it was applied at one call site rather than at the mechanism:

```go
func writeAtomic(path string, write func(io.Writer) error) error {
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, cacheDirPerm); err != nil { ... }
    f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
```

The deferred cleanup removes the temp file and never the directories.

**Failure scenario (no cancellation involved).** Fetch an archive Binance never published.
`writeAtomic` creates `data/spot/daily/klines/FOOUSDT/1h/`, `CreateTemp` makes a temp
file, the download 404s, and the directory tree is left behind on every failed load.

**Second scenario.** In the `continue` branch at `cache.go:325`, the caller's `ctx` can be
cancelled between `isForeignCancellation` returning false and the next `DoChan`,
reproducing the abandoned-goroutine directory leak the new guard was added to prevent.

Both disappear if `writeAtomic` defers `MkdirAll`/`CreateTemp` until it has bytes, or
cleans up the directories it created when the write fails.

---

## Design and API

### 9. `ErrIPBanned` cannot be reached by any consumer of the library

**`download.go:163`** — *Verified*

```go
// An HTTP 418 — the ban Binance escalates a persistent 429 into —
// arrives here too, because vision.RateLimitError wraps both sentinels
// and this case tests the coarser one. The distinction is not lost: it
// stays readable through errors.Is(err, vision.ErrIPBanned) and in the
// message, which is where a caller that can act on it will look.
```

`vision` is under `internal/`, so `errors.Is(err, vision.ErrIPBanned)` does not compile
outside this module — "use of internal package ... not allowed". `errors.As(err,
&vision.RateLimitError{})` fails the same way, so `RetryAfter` is unreachable too.
`errors.go` exports five sentinels and none of them is a ban.

**Failure scenario.** A downstream trading framework imports the library and wants to halt
rather than back off on an IP ban. Its only option is substring-matching the message,
which `errors.go:26` explicitly forbids: "Identity is the thing being compared, never the
string."

`docs/architecture.md` repeats the same unachievable claim in the new REST-tail section.

Either export a root-package sentinel for the ban, or correct both comments to say the
distinction is internal-only and visible solely in the message text.

---

### 10. `KlinesPage.UsedWeight` is decoded and then discarded

**`restapi.go:211`** — *Verified*

`usedWeight` parses `X-MBX-USED-WEIGHT-1M` on every page and `Klines` returns it
(`internal/vision/klines.go:338`). `restapi.go` never reads `got.UsedWeight`; the only
other reference in the tree is the test that asserts the field is populated.

`docs/architecture.md`'s new REST section states:

> `X-MBX-USED-WEIGHT-1M` is parsed and reported rather than acted on — when it climbs
> while the local accounting says otherwise, something else on the address is spending the
> quota, which is a diagnosis no local bookkeeping reaches.

No such report exists. By the rule the same document states at line 681 — "an
accepted-and-ignored setting is a defect, not a stub" — this is decoded-and-ignored
diagnostics, and the described diagnosis is impossible today.

Either surface it (a log line, a callback, a field on the result) or narrow the doc to say
the header is decoded and available to the layer above, which is all that is true.

---

## Cleanup

### 11. `jsonScalar` lets JSON booleans through, contradicting its own comment

**`internal/vision/klines.go:483`** — *Verified*

```go
case '{', '[', 'n':
    return "", fmt.Errorf("expected a number or a string, got %s", snippetOf(raw))

default:
    return string(raw), nil
```

The doc comment says "a null, object or array is refused rather than rendered ... turning
one into a plausible-looking string would hand the decimal parser something to fail on two
layers away from the cause". `true` and `false` hit `default` and become the literal text
`"true"` / `"false"`, which then fails at `udecimal.Parse` in `decodeRow` as
`open "true": ...: corrupt archive` — precisely the two-layers-away failure the function
exists to prevent.

Adding `'t', 'f'` to the refusing case makes the code match the comment.

---

### 12. `readAPIError` re-implements `bodySnippet`

**`internal/vision/klines.go:380`** — *Verified*

`internal/vision/body.go:82` already holds the read-limited-prefix / drain-and-close /
`snippetOf` sequence, including the identical `maxSnippet+utf8.UTFMax` expression whose
`+UTFMax` is subtle and load-bearing (it is what stops a multi-byte rune being cut in
half). There are now two copies.

The next change to body handling — a different cap, a `Content-Encoding` check — has to be
made twice or it silently applies to one endpoint only. This is the same argument
`ParseChecksum`'s doc comment makes for sharing one parser.

Splitting `bodySnippet` into a bytes-returning helper plus the quoting step removes the
copy. Note that finding #4 changes the read limit for one of the two call sites, so the
two findings should be fixed together.

---

### 13. A 192-byte array copy per row on the hottest loop in the file

**`restapi.go:240`** — *Verified*

```go
for i, row := range rows {
    k, err := decodeRow(row[:], spec)
```

`vision.RawKline` is `[12]string` — 192 bytes on a 64-bit build. Ranging by value copies
each row and forces the local array to be addressable so `row[:]` can slice it.
`rows[i][:]` does the same job with neither.

A day of `1s` candles is 86,400 rows. This matters by this project's own standard: Stage 5
rejected parquet-go's `GenericReader` specifically over 99,573 allocations versus 498 for
the same month of data.

---

## Documentation

### 14. README still promises Go 1.24.9

**`README.md:66`** — *Verified*

```
Requires Go 1.24.9 or newer.
```

`go.mod:56` says `go 1.25.0`. Stage 6 moved the floor deliberately, for
`testing/synctest`, and updated `go.mod`, `CLAUDE.md`, `.github/workflows/ci.yml` and
`docs/architecture.md` — but not the one user-facing statement of the floor.

A consumer on 1.24.9 reads the README, runs `go get`, and gets a toolchain error.

---

### 15. The plan's Stage 6 row contradicts itself about `limiter.go`

**`~/.claude/plans/let-s-implement-binance-historical-humble-matsumoto.md:841`** — *Verified*

The row describes the shipped artefact as:

> `internal/vision/limiter.go` (hand-rolled token bucket, injected clock, process-wide via
> `sync.OnceValue`, 40 of the 100 weight/second ceiling, tokens taken before the wait so
> concurrent callers queue instead of stampeding, refunded on cancellation)

and then, later in the same row:

> the stage shipped a hand-rolled bucket first and replaced it with `rate.Limiter` +
> `testing/synctest` within the same stage

The first passage describes code that was deleted. `CLAUDE.md` requires the plan's stage
table to be updated when a stage lands, and `MEMORY.md`'s anchor-scope rule makes that
table the first thing quoted at the start of a session — so a Stage 7 session will be
pointed at a hand-rolled bucket with an injected clock that does not exist.

The description of the artefact should read `golang.org/x/time/rate`, with the hand-rolled
version mentioned only in the finding that explains why it went.

---

## A note on the review itself

Findings #1, #2 and #9 are the ones that earn the exercise: each is a place where a
comment or a doc states a property the code does not have, and in #1 and #2 the wrong
property is the one Stage 7 will build on.

#3 is the only finding here that cannot be settled by reading. It should be resolved the
way the rest of this stage's numbers were — measured against the live endpoint, once, with
the answer written down next to the cursor.

#5 is a genuine gap but the existing comment is not wrong, only incomplete: it answers the
pacing question and is silent on the accounting one. Fixing the comment may be the whole
fix.

---

## Resolution

All fifteen were applied, in the commit after the one that landed Stage 6. Three
of them changed a verdict Stage 7 would have built on, and those are the ones
worth remembering:

| # | What changed |
| --- | --- |
| 1 | `statusCause` decides the sentinel from the status class rather than from the body, so a 5xx carries the new internal `vision.ErrServerError` and reaches the caller unrecognised rather than as `ErrInvalidRequest`. An unexplained 4xx is now typed too — whose fault a refusal is no longer depends on whether the server sent JSON |
| 2 | `translateRESTError` leaves a REST 404 untyped and says why in the message. The shared translation keeps the calendar reading, which is correct for the bucket |
| 3 | The cursor advances to `intervalEnd(prev)` — the next candle's open — which is correct under either reading of Binance's inclusive `startTime`, so the question never has to be measured. `restapi_test.go` runs the same range against a handler for each reading; the new one fails against the old cursor with "page 2 row 1: open time … does not follow" |
| 4 | `readAPIError` reads `maxErrorDoc` (8 KiB) rather than the snippet's 204 bytes. Finding 1's fix already removed the consequence — the sentinel no longer depends on the parse — so this now only affects whether Binance's own code and message are recovered |
| 5 | `Policy.Reserve` is consulted once per attempt inside `doWithRetry`, so a call that retries four times spends four reservations. `API` no longer holds the limiter; the closure does |
| 6 | The page cap is untyped, with a message naming the fix (split the range). Still untested: reaching it needs 1,001 full pages |
| 7 | A zero `now` is rejected with `ErrInvalidRequest` before any request is sent |
| 8 | `writeAtomic` creates nothing until the first byte is written, via `tempFile`; a write that never starts leaves no trace. The `ctx.Err()` guard moved inside the singleflight loop, which is what the foreign-cancellation retry needed. Directories are still not removed after a failed write, deliberately: concurrent writers share one, and a tidy-up races them |
| 9 | `ErrIPBanned` is a sixth public sentinel, attached alongside `ErrRateLimited` |
| 10 | `docs/architecture.md` now says the header is decoded and available to the layer above, and the field's comment names Stage 7 as its consumer |
| 11 | `'t'` and `'f'` join the refusing case in `jsonScalar` |
| 12 | `readBodyPrefix` holds the read-limit-then-drain sequence once |
| 13 | `rows[i][:]` |
| 14 | README says 1.25.0 |
| 15 | The plan's Stage 6 row describes `x/time/rate` |

One finding the review missed was fixed in the same pass: every failure in
`decodeKlines` was untyped, so a body that would not parse as JSON reached the
caller unrecognised while a bad decimal inside a well-formed row was
`ErrCorruptArchive` — the same condition answered two ways depending on which
layer noticed. `vision.ErrMalformedResponse` now covers the JSON half and
`translateVisionError` folds it onto `ErrCorruptArchive`.

`mise run ci` is green: 0 lint issues, 652 tests under `-race`, up from 633.
