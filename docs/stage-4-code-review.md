# Stage 4 code review

**Date:** 2026-08-19
**Branch:** `main` (uncommitted working tree, on top of `16db16d Stage 3: parsing`)
**Scope:** the Stage 4 diff — `availability.go`, `download.go`, `internal/vision/{listing,body,client,download,retry}.go` and their tests, plus `docs/architecture.md` and `docs/caching.md`.

13 findings. The substantive ones were spot-checked against the source and confirmed.

Fix order recommendation: **#1** and **#4** first — a silent empty index is the failure the whole availability design exists to prevent, and Stage 7's bounded pool is what turns #4 from a latency wart into a synchronised retry storm. **#6** is worth a look too, since `docs/architecture.md` now claims a guarantee the default constructor path does not deliver.

---

## Correctness

### 1. `fetchArchiveIndex` returns an empty index and a `nil` error for an invalid interval

**`availability.go:250`**

`Interval.HasMonthlyArchives()` and `Interval.HasDailyArchives()` both begin with `i.IsValid() &&` (`interval.go:314,321`). An interval that fails `IsValid` therefore launches zero errgroup goroutines, `g.Wait()` returns `nil`, and the function returns `archiveIndex{months:{}, days:{}, through:zero}, nil`.

**Failure scenario.** `fetchArchiveIndex(ctx, lister, MarketSpot, "BTCUSDT", Interval(200), since)` makes no requests and yields `through=0001-01-01, months=0, days=0, err=<nil>`. The planner reads that as "Binance has published no archives for this symbol" and sends a multi-year range to the REST API one page at a time, instead of returning `ErrInvalidRequest`. Market and aggregation are both rejected loudly on the same call; interval is not.

This is the exact "failed lookup read as an empty one" conflation the package forbids — and which `g.Wait()`'s own comment (`availability.go:266-270`) reasons about for the *partial* case while missing the *zero-goroutine* case.

---

### 2. `archiveRef.key()` validates market and aggregation but not interval

**`download.go:44`**

`archivePrefix` checks `marketPath` and `agg.valid()`, but nothing checks the interval. `Interval.String()` renders an invalid value through its `"Interval(" + strconv.Itoa(...) + ")"` fallback, which is a legal path segment.

**Failure scenario.** `archiveRef{Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval(0), Agg: aggDaily, Period: 2024-01-15}.key()` returns

```
data/spot/daily/klines/BTCUSDT/Interval(0)/BTCUSDT-Interval(0)-2024-01-15.zip, nil
```

`fetchArchive` then spends a round trip, gets a 404, and `translateVisionError` labels it `ErrNotAvailable` — "Binance does not have this data" for what is actually a caller bug.

The doc comment three lines above `key()` states: *"Both halves validate, so an unset market or aggregation is caught here rather than becoming a plausible-looking URL that 404s."* This is the identical failure mode the diff just fixed for aggregation at `availability.go:79`, applied to only one of the four parameters.

---

### 3. `archivePrefix` passes an unvalidated symbol to `path.Join`

**`availability.go:86`**

`path.Join` silently drops empty segments and runs `Clean` over `..`, so a malformed symbol produces a well-formed key pointing at a different directory than the one requested. Both cases return a `nil` error.

**Failure scenario.** Verified by running `key()`:

| symbol | resulting key |
| --- | --- |
| `""` | `data/spot/daily/klines/1h/-1h-2024-01-15.zip` — `path.Join` swallowed the empty segment, so the interval slid into the symbol's slot |
| `"../ETHUSDT"` | `data/spot/daily/ETHUSDT/1h/../ETHUSDT-1h-2024-01-15.zip` — `Clean` ate the `klines` segment, and `url.JoinPath` in `Downloader.get` cleans the remaining `..` again |

`NormalizeSymbol` (`symbol.go:45`) exists and rejects exactly these, but neither `archivePrefix` nor `archiveRef.key()` calls it, and there is no production caller yet to do it for them.

---

### 4. `Retry-After` replaces the computed delay wholesale, bypassing jitter and accepting zero

**`internal/vision/retry.go:306`**

```go
delay := p.backoff(attempt)
...
if d, ok := retryAfter(resp.Header.Get("Retry-After"), p.Now()); ok {
    delay = min(d, maxRetryAfter)
}
```

The header value replaces the jittered backoff entirely.

**Failure scenario A — thundering herd.** Forty pool workers hit the rate limiter at once and each receives `429 Retry-After: 1`. Every one sets `delay = 1s` exactly, waits, and re-fires at the same instant — precisely the lockstep retry the `Policy.Jitter` doc comment (`retry.go:71-81`) says *"recreates the overload it is backing off from"*.

**Failure scenario B — no backoff at all.** `retryAfter` returns `(0, true)` both for a literal `Retry-After: 0` and for any HTTP-date already in the past (`retry.go:371-375`). `delay` becomes `0`; `Policy.wait` short-circuits at `if d <= 0 { return ctx.Err() }` (`retry.go:217-221`) and returns `nil` without touching the timer. All four attempts then fire back-to-back with no backoff whatsoever, hammering a server that just said it was overloaded.

---

### 5. The replayability guard does not match what the retry loop does

**`internal/vision/retry.go:256`**

```go
if req.Body != nil && req.GetBody == nil {
    return nil, fmt.Errorf("retrying %s: request body cannot be replayed", req.URL)
}
```

The guard admits a request whenever `GetBody` is non-nil, but the loop retries via `req.Clone(ctx)` — and `http.Request.Clone` shallow-copies `Body`. It does not rewind it and never invokes `GetBody`.

**Failure scenario.** A future POST built by `http.NewRequest` with a `*bytes.Reader` (which sets `GetBody` automatically) passes the guard, sends its body on attempt 1, gets a 503, and on attempt 2 sends a request whose `Body` reader is already at EOF — a zero-length payload the server rejects, looking exactly like a server bug.

The comment claims the check *"states the requirement rather than assuming it"*. The requirement it states (`GetBody` present) is not the one the loop actually needs (`GetBody` **called**).

---

### 6. `NewHTTPClient` is wired into no production code

**`internal/vision/listing.go:92`, `internal/vision/download.go:88`**

Both constructors default a nil client to `http.DefaultClient`, whose `MaxIdleConnsPerHost` is 2. `grep -rn NewHTTPClient --include=*.go .` shows it referenced only by `client.go` and `client_test.go`.

**Failure scenario.** A consumer calls `NewDownloader("", nil, Policy{})` — the path `TestNewDownloaderDefaults` pins as supported — and gets `http.DefaultClient`. Eight workers fetching archives keep two idle connections and re-handshake TCP+TLS for the other six on every chunk: **bug 8, present in full, from the constructor the docs advertise.**

`docs/architecture.md` was updated in this diff to mark correctness requirement 8 as done. A package-level `sync.OnceValue(NewHTTPClient)` as the nil default would make the guarantee real instead of conditional on the caller knowing to ask.

---

### 7. Unbounded `io.ReadAll` on the 200 path

**`internal/vision/listing.go:307`**

Every other body read in the package is capped — 8 KiB in `statusError`, 64 KiB in `drain`, 4 KiB in `Checksum`. This one is not.

**Failure scenario.** `baseURL` is misconfigured (`TestListRejectsAnythingThatIsNotAListing` exists precisely because that happens) and points at a host answering 200 with a large payload — a CDN mirror, a proxy streaming a file, a bucket replaced by something else. `fetchPage` buffers the whole thing into memory before the `XMLName` check ever gets a chance to reject it. With the errgroup change two of these run concurrently per symbol, and Stage 7's bounded pool multiplies it further.

Wrapping `resp.Body` in an `io.LimitReader` sized for a 1000-key page — the bound the existing comment already reasons about — costs one line.

---

### 8. `snippetOf` only validates UTF-8 on the truncating branch

**`internal/vision/body.go:116`**

```go
s := strings.TrimSpace(string(b))
if len(s) <= maxSnippet {
    return s        // <- returned verbatim, never validated
}
s = s[:maxSnippet]
// rune-boundary walk happens only here
```

**Failure scenario.** A misconfigured endpoint answers 500 with 150 bytes of binary (a gzip frame, a TLS alert, a protobuf). `len(s) <= maxSnippet`, so it is returned unchanged and `Lister.statusError` embeds it in `listing %q: unexpected status %s: %s`. The result carries invalid UTF-8 and raw ANSI/control bytes straight into the operator's terminal and logs.

The function's own doc says its job is to trim `b` to *"something short enough, and valid enough, to end an error message with"*. A `utf8.ValidString` check — or `strconv.Quote` — on both paths would close it.

---

### 9. Every transport error is retried four times, including permanent ones

**`internal/vision/retry.go:270`**

A bad TLS certificate, a DNS NXDOMAIN for a mistyped host, or net/http's `stopped after 10 redirects` all land in `case err != nil`, set `lastErr`, and burn the full four attempts plus ~3.5 s of backoff before reporting.

`retryableStatus` exists because *"the list is short on purpose"* and *"retrying it wastes time to arrive at the same answer while looking, in a log, exactly like a network problem"*. The same reasoning applies to `x509.UnknownAuthorityError` and `*net.DNSError{IsNotFound: true}`, which are as much facts about the request as a 404 is.

---

## Quality / convention

### 10. New sentinels are not in `errors.go`

**`internal/vision/download.go:39`**

`ErrNotFound` and `ErrRateLimited` are declared mid-file; `internal/vision` has no `errors.go` at all. `ErrRateLimited` is raised from `statusError` in the same file, while `ErrNotFound` is also branched on from the root package.

`CLAUDE.md` (Conventions) requires: *"Errors are sentinels in `errors.go`, wrapped with `%w`, compared with `errors.Is`. Never `==`."* The rule's stated purpose — *"a reader should be able to learn [the set of things that can go wrong] from a single screen rather than by grepping for `errors.New`"* (`errors.go:5-8`) — is exactly what is lost as this package grows a REST client and a cache in Stages 5–6.

---

### 11. Two functions named `statusError`, with different body-ownership contracts

**`internal/vision/listing.go:328`**

```go
func (l *Lister) statusError(prefix string, resp *http.Response) error  // listing.go:328
func statusError(key string, resp *http.Response) error                 // download.go:220
```

The method never uses its receiver. Worse, the two differ in what they do with the body: the method drains but leaves closing to the caller's `defer`, the function drains **and** closes. A reader at a call site cannot tell which contract applies without checking whether a receiver is present, and a future refactor that moves a call between the two leaks or double-handles a connection.

Making the `Lister` one a plain function named `s3StatusError` removes both the phantom receiver and the ambiguity.

---

### 12. The test's race guard is asymmetric

**`availability_test.go:311`**

`newIndexServer` appends to `queries` under `mu` (line 290) but returns a raw `*[]string`. `TestFetchArchiveIndex` (line 332), `TestFetchArchiveIndexMonthlyOnly` (line 387) and `TestFetchArchiveIndexSeeks` (line 452) all read `*queries` directly with no lock.

It passes today only because net/http happens to establish a happens-before edge through the response read; nothing in the test guarantees that. The helper's own comment reasons at length about `-race` catching *"a genuine data race"* — returning a `func() []string` that snapshots under the mutex makes the guard symmetric.

---

### 13. `withDefaults` runs on every request

**`internal/vision/retry.go:254`**

`NewLister` (`listing.go:96`) and `NewDownloader` (`download.go:88`) both store `p.withDefaults()`. `doWithRetry` then calls `withDefaults` again per request, constructing a whole `DefaultPolicy()` — three func values plus four scalars — and running six nil/zero checks, on the hot path of every archive fetch in a bounded pool.

Either drop the call and document that callers must pass a defaulted `Policy`, or keep it and drop the constructors'. Doing both means the defaulting is paid for on every one of several thousand requests per backtest.

---

## Resolution

Verified against the source on 2026-08-19; findings 1, 2 and 3 were reproduced by executing the path builders. Every factual claim in the review held up. What follows is what was done about each, and why two were declined.

### Applied

| # | Change |
| --- | --- |
| 1 | `fetchArchiveIndex` rejects an interval published at no granularity before issuing a request. Guarded on the granularities rather than on `IsValid`, so a future interval Binance publishes at neither still reports rather than returning silence |
| 2, 3 | `archivePrefix` now validates all four of market, aggregation, interval and symbol. The symbol is *asserted* normalised via `checkNormalizedSymbol`, not normalised in passing — normalising in two places is how one of them stops happening |
| 4 | `Retry-After` became a floor instead of a replacement: clamped to `[BaseDelay, maxRetryAfter]`, then jittered on top. Fixes both the lockstep herd and the zero/past-dated header that produced no backoff at all |
| 5 | `doWithRetry` calls `GetBody` on every retry, so the guard's stated requirement is the one the loop actually satisfies |
| 6 | `sync.OnceValue(NewHTTPClient)` is the nil-client default for both constructors, replacing `http.DefaultClient` and its `MaxIdleConnsPerHost` of 2 |
| 7 | The 200-path listing read is bounded by `maxListingPage` (1 MiB), with the one-byte-past read that makes "too large" detectable |
| 8 | `snippetOf` returns `strconv.Quote`d output, which fixes both branches — the review's framing was slightly off here, since the truncating branch never validated UTF-8 either, it only repaired a broken trailing rune |
| 11 | The `Lister` method became the plain function `s3StatusError`, removing the phantom receiver and the same-name/different-body-contract ambiguity |
| 12 | `newIndexServer` returns a `func() []string` snapshotting under the mutex |
| — | **Missed by the review:** a 429 surviving every attempt discarded the server's `Retry-After` one frame below where Stage 7's pool needs it. `statusError` now returns a `*vision.RateLimitError` carrying it, unclamped; `errors.Is(err, ErrRateLimited)` still works through `Unwrap` |

New tests cover each: the zero-goroutine index, the four unvalidated path parameters, herd decorrelation across 40 concurrent workers, a zero and past-dated `Retry-After`, body replay across retries, the unreplayable-body refusal, the shared client default, the listing size cap, binary bodies in error messages, and `RateLimitError`'s duration. `mise run ci` is green — fmt, lint at 0 issues, tests under `-race`, build — and the new tests were run 20× under `-race` without a flake.

### Declined

**#9 — retrying permanent transport errors.** Presented as a defect; it is a judgment call, and the current behaviour is the better one. DNS `NXDOMAIN` and TLS failures are meaningfully more transient than a 404 — resolver hiccups, cert-rotation windows, captive portals — and 3.5 s on a genuinely dead host is cheap insurance. The fix also requires `errors.As` against `*net.DNSError` and `crypto/x509` internals that `net/http` makes no promises about wrapping.

**#13 — `withDefaults` on every request.** The observation is correct, the justification is not. `DefaultPolicy()` allocates nothing: func values of top-level functions are static, so this is a ~7-word stack copy and six branches, on the order of nanoseconds — measured against an HTTP round trip and, for a `1s` monthly archive, a 93 MB transfer. Calling it "the hot path" is off by roughly seven orders of magnitude. The redundancy is harmless and the defaulting stays at the point of use, where a nil `Jitter` would otherwise be a nil-pointer dereference.

### Left open

**#10 — sentinels not in `errors.go`.** A genuine coin flip rather than a defect. `errors.go`'s own rationale is that "the set of things that can go wrong is part of the **public** API"; `internal/vision` has no public API, and its two sentinels sit in the file that raises and consumes them. Worth revisiting if Stages 5–6 grow that package a third and fourth.

**A cosmetic wart, pre-existing.** `translateVisionError`'s two-`%w` wrap appends the root sentinel's text, so a rate-limit error reads `"key": rate limited, retry after 30s: rate limited`. That duplication predates these changes (`"key": rate limited: rate limited` before), and the two-`%w` behaviour is deliberate and documented. Not worth special-casing.
