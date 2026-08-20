# Stage 7 code review

**Date:** 2026-08-20
**Branch:** `main` (uncommitted working tree, on top of `f634de9 Stage 6: apply the code review`)
**Scope:** the Stage 7 diff — new `loader.go`, `loader_test.go`, `options.go`,
`options_test.go`, plus the touched files `codec.go` (`grid1w`, `alignUp`, `ceilFrom`),
`codec_test.go`, `internal/plan/plan.go` (the partial-month threshold, the split
publish/want condition, `Substitute`'s `dayStart` snap), `internal/plan/plan_test.go`,
`doc.go`, `README.md` and `docs/architecture.md`.

11 findings. `mise run ci` is green — `go vet` clean, `golangci-lint` 0 issues, `-race`
tests all pass — so every finding below is a logic or design defect rather than something
the toolchain would have caught.

Fix order recommendation: **#1** and **#2** first. Both let a `Fetch` return fewer candles
than were asked for with a `nil` error, which is the single failure mode this library's
whole error contract exists to prevent. **#3** next: it is a plan-quality regression that
fires in the ordinary monthly-publication lag window, not an exotic one. **#5** and **#6**
are the two places where the tests assert less than the prose says they do.

---

## Correctness

### 1. A cancellation between chunks truncates the range and reports no error

**`loader.go:634`, `loader.go:654–658`**

`stream`'s consumer leaves the loop when the group context is done:

```go
select {
case klines = <-out[i]:
case <-gctx.Done():
	break consume
}
...
<-producerDone

if err := g.Wait(); err != nil {
	yield(Kline{}, err)
}
```

`errgroup.Group.Wait` returns `g.err`, and `g.err` is only ever set by a function passed to
`g.Go` returning non-nil. A cancellation of the *caller's* context sets no `g.err` by
itself. So when `gctx` is done and every worker launched so far has already returned `nil`,
`g.Wait()` returns `nil`, the loop ends, and `Stream` finishes having yielded only part of
the range with no error at all. `Fetch` then returns those candles and `nil`.

The window is exactly "the producer is blocked in `l.sem.acquire(gctx)` for chunk *i* while
workers 0…*i*−1 have all finished successfully". Workers that are still in flight would
return `gctx.Err()` from their blocked send, which is why this usually reports correctly —
but the semaphore is shared across the whole `Loader`, so during a `FetchAll` (or any two
concurrent `Fetch` calls) one request's producer can sit in `acquire` for an arbitrarily
long time holding no workers at all.

**Failure scenario.** `FetchAll` over ten ranges with the default concurrency of 8. Request
A finishes its first three chunks and its producer blocks acquiring a permit for chunk 4,
all eight permits being held by requests B–D. The caller's `context.WithTimeout` expires.
A's consumer breaks at chunk 4, `g.Wait()` returns `nil` because A's first three workers
all returned `nil`, and `FetchAll` records A's partial slice as a success. The overall call
then fails only if some *other* request happened to have a worker in flight; if the timeout
lands in the same window for all of them, `FetchAll` returns a full map of short ranges and
`nil`. That is precisely the "a backtest cannot tell the market was quiet from two months
are missing" failure the package doc promises never happens.

**Fix.** On `break consume`, prefer `gctx.Err()` when `g.Wait()` is nil — or have the
producer record `ctx.Err()` into the group (`g.Go(func() error { return err })`) when
`acquire` fails.

**No test covers this.** `TestFetchHonoursCancellation` (`loader_test.go:1279`) cancels
*before* the call, so it only exercises `resolve`. There is also no test in which an error
arrives mid-stream after candles have already been yielded, which `Loader.Stream`'s doc
explicitly promises can happen.

---

### 2. "This chunk was empty" is judged on the whole chunk, not on the part that was requested

**`loader.go:679`, `loader.go:685`**

```go
if len(klines) == 0 && expectsCandles(c, req.Interval, now) {
	from, to := maxTime(c.Start, req.Start), minTime(c.End, req.End)
	return nil, fmt.Errorf("... [%s,%s): no candles from any source: %w", ...)
}
```

The *decision* uses the chunk's own extent; only the *message* is clamped to the request.
Before Stage 7, chunks and requests diverged only for `3d`/`1w`/`1mo`. The new threshold in
`plan.Expand` (finding #3) makes a whole-month chunk the normal way to serve a
partly-wanted month at *every* interval, so the two now diverge routinely, and both
directions of the mismatch are wrong.

**Failure scenario A — a short range returned as a success.** A pair is delisted on
2024-03-10; its `2024-03` monthly archive exists and holds ten days of candles. A caller
asks for `[2024-03-16, 2024-04-01)`. `worthWholeMonth` sees 16 of 31 days wanted and emits
one monthly chunk `[Mar 1, Apr 1)`. The archive is non-empty, so `len(klines) == 0` is
false and no error is raised; the reduce step then trims every candle away, and `Fetch`
returns an empty slice and `nil`. The pre-Stage-7 plan would have emitted sixteen daily
chunks, each empty, and failed with `ErrNotAvailable`.

**Failure scenario B — a spurious failure, with a reversed span in the message.** Same
request, but the `2024-03` monthly archive is one of Binance's holes. `route` substitutes
it into 31 daily chunks; `Mar 1`–`Mar 15` are outside the request, are not listed either,
and coalesce into one REST chunk `[Mar 1, Mar 16)` that Binance has nothing for.
`expectsCandles` says true, and the call fails even though every candle the caller asked
for is available. The message computes `from = max(Mar 1, Mar 16) = Mar 16` and
`to = min(Mar 16, Apr 1) = Mar 16`, so it names the degenerate span
`[2024-03-16T00:00:00Z,2024-03-16T00:00:00Z)`. With a single non-listed day rather than a
coalesced run it reads `[2024-03-16…,2024-03-04…)` — an end before its start.

**Fix.** Compute `from`/`to` first and test emptiness against the intersection: skip the
check entirely when `!from.Before(to)`, and count only the candles that fall inside it.

---

### 3. `monthPublished` asks `ArchivesThrough`, which cannot answer the question it is asked

**`internal/plan/plan.go:265`, `internal/plan/plan.go:279`**

```go
// Whether the month's archive *exists* is a fact about what Binance
// has published, and ArchivesThrough is the only thing that can answer it.
monthPublished := s.HasMonthly && !mEnd.After(s.ArchivesThrough)
```

`ArchivesThrough` is `archiveIndex.through` (`availability.go:347`), which is
`max(monthlyThrough, dailyThrough)` — in practice the *daily* frontier, because dailies lag
real time by about a day and monthlies by up to a month plus that day. So `monthPublished`
is true for every month that ends before the daily frontier, whether or not a monthly
archive for it exists. The comment states the opposite of what the value carries.

For the pre-existing case-1 branch this was harmless: the month was wanted in full, so the
mistaken monthly chunk is rerouted by `Loader.route` into exactly the days that were wanted
anyway. The new case-2 branch makes it costly, because it deliberately picks a chunk
*wider* than the request, and `plan.Substitute` then expands the whole month.

**Failure scenario.** It is 2024-03-03. Daily archives exist through 2024-03-01
(`dailyThrough = 2024-03-02`); the `2024-02` monthly archive has not been published yet.
A caller asks for `BTCUSDT 1h [2024-02-10, 2024-03-01)`. `monthPublished` is true (`Mar 1`
is not after `Mar 2`), `worthWholeMonth` sees 20 of 29 days, and the plan is one monthly
chunk `[Feb 1, Mar 1)`. `route` finds it is not listed and substitutes **all 29 days** — 58
HTTP requests and nine days of archives nobody asked for. The pre-Stage-7 plan emitted 20
daily chunks, 40 requests, no over-fetch. The same happens for any month that is one of
Binance's monthly holes, at any interval.

**Fix.** Either carry the two frontiers separately in `plan.Spec` (a `MonthlyThrough`
alongside `ArchivesThrough`), or — the deeper fix — apply the threshold in `Loader.route`,
where the listing is already in hand, instead of in the planner that cannot see it. As
written the trade-off is decided one layer above the only information that makes it sound.

---

### 4. `FetchAll`'s listing phase escapes the concurrency limit entirely

**`loader.go:285`, `loader.go:369`**

```go
// No SetLimit on this group. The limit that matters is the Loader's
// semaphore, taken per chunk inside each Fetch ...
// What this costs is one mostly-blocked goroutine per request, which is a few
// kilobytes of stack each.
g, gctx := errgroup.WithContext(ctx)
```

The semaphore is taken per *chunk*, inside `stream`. `resolve` runs before it and does I/O:
`fetchArchiveIndex` fires the monthly and daily listings concurrently. So `FetchAll` over
*N* requests opens up to **2N simultaneous listing requests** against the S3 host, and
`WithConcurrency` does not bound them. The goroutines are not "mostly blocked"; they are all
doing network I/O at once, at the one moment of the call when they are all at the same
stage.

Two requests for the same symbol and interval also repeat the same listing rather than
sharing it — the index is built per call and never memoised on the `Loader`, even though a
`Loader` is documented as long-lived and per-process. `FetchAll` over twenty ranges of
`BTCUSDT 1h` costs forty identical listings, each of which may itself be several paginated
round trips for an old range.

**Failure scenario.** A backtest calls `FetchAll` with 200 symbol/interval pairs and
`WithConcurrency(4)` because it is fetching 1s data and cannot afford the memory. The
listing phase opens 400 concurrent connections to `data.binance.vision` regardless, which is
the shape that earns a 429 and then the HTTP 418 that `l.pause` is explicitly written not to
wait out.

**Fix.** Acquire a permit around `resolve`, or give the index its own bounded group and a
per-`(market, symbol, interval)` singleflight on the `Loader`.

---

## Tests

### 5. `TestConcurrentRequestsFetchAnArchiveOnce` does not establish the claim it and the architecture doc make

**`loader_test.go:670`**

```go
go func() {
	defer wg.Done()
	queued.Done()                      // "signal arrival"
	results[i], errs[i] = l.Fetch(t.Context(), req)
}()
```

and, in the archive handler:

```go
once.Do(func() { queued.Wait(); close(release) })
```

`queued.Done()` is called before `Fetch`, so `queued.Wait()` returns as soon as all eight
goroutines have *started* — not when they have reached the cache and queued behind the
singleflight. The other seven callers are, at that moment, typically still doing their two
listing round trips. `release` closes, the first download completes, and the seven arrive to
find the archive already on disk.

That means the assertion `archiveCalls == 2` holds even with the deduplication removed: the
later callers would be tier-1 cache hits rather than singleflight waiters. The test as
written therefore does not exercise "a saturated pool let several tasks past the check
before any of them registered", which is what its own comment,
`docs/architecture.md`'s "Requirement 5 holds by construction" paragraph, and the correctness
requirement table all cite it for.

**Fix.** Gate `release` on something observable from inside the cache — e.g. release once
`cache.sf` reports the expected number of waiters, or have each caller signal from a
`WithProgress`/logger hook that only fires after the fetch has begun.

---

### 6. `TestAlignUpAgreesWithAligned` checks its third property for 2 of 16 intervals

**`codec_test.go:998`**

The test's own doc comment lists three properties, and `docs/architecture.md` says the test
"sweeps it across all sixteen intervals". The minimality property is guarded:

```go
if d, fixed := iv.Duration(); fixed && d <= time.Minute {
```

which is true only for `1s` and `1m`. For the other fourteen the test asserts only that the
answer is *on* the grid and *not before* the input — both of which an implementation that
overshot by a whole period would still satisfy. `Interval3d` is the weakest case of all:
`aligned` and `alignUp` both read the same `grid3d` variable, so a wrong anchor is invisible
to the cross-check, and nothing else pins it.

The stepping loop does not need to be per-second. Stepping by the interval's own duration
(or by `time.Hour` for intervals of a day or more) would cover every fixed-duration interval
at trivial cost, and `1mo` can be checked by asserting the answer is the 1st of `t`'s month
or the next one.

---

### 7. The rate-limit pause path and the runtime fallback ladder are effectively untested

**`loader.go:715` (`ladder`), `loader.go:774` (`pause`)**

Coverage over the whole new test file: `pause` **20.0 %**, `ladder` **21.4 %**. Nothing
exercises the branch where a chunk actually pauses the pipeline and is retried, so none of
this is covered by a test:

- `errors.As(err, &rl)` reaching `*vision.RateLimitError` through the two `%w` translations;
- the `min(max(after, minPipelinePause), maxPipelinePause)` clamp, including the zero
  `Retry-After` case that `options.go`/`architecture.md` spend two paragraphs justifying;
- `maxPipelinePauses` bounding the loop, so that a server answering 429 forever produces an
  error rather than a hang;
- `ladder` recovering a listed-then-404'd archive by falling through to dailies or REST.

`TestAnIPBanIsNotWaitedOut` covers only the *negative* branch (`pause` returns false).
`TestGate*` covers the gate in isolation but never the decision to close it. Given that this
is the machinery that stands between the pool and an IP ban, a test that serves 429 with a
`Retry-After` from `f.restHandler` and asserts both the retry count and the clamped delay is
worth the lines.

---

## Cleanup

### 8. `includePartial` is unreachable configuration, and its comment points at a note that does not exist

**`options.go:82–84`**

```go
// includePartial is passed through to [restFetcher]. It has no public
// option — see the note in NewLoader on why.
includePartial bool
```

`NewLoader`'s doc comment (`loader.go:118–130`) contains no such note — it discusses the
absence of I/O, the absence of directory creation, and why the constructor returns an error.
The cross-reference is dangling.

More to the point, the field has no public option *and* no test seam, so it is permanently
`false` and no code path can ever set it. The project's own rule — recorded in
`docs/architecture.md` as "an accepted-and-ignored setting is a defect, not a stub" — cuts
both ways: either add a `withPartial` seam (matching `withClock`, `withPolicy`,
`withLimiter`, which exist for exactly this reason) so the loader-level behaviour is
testable, or drop the field and construct `restFetcher` with the literal `false`.

---

### 9. `alignUp` hardcodes two durations the interval table already holds

**`codec.go:727`, `codec.go:731`**

```go
if iv == Interval1w {
	return ceilFrom(t, grid1w, 7*24*time.Hour), true
}

if iv == Interval3d {
	return ceilFrom(t, grid3d, 72*time.Hour), true
}
```

`intervalTable` (`interval.go:159–160`) already records `duration: 72 * time.Hour` for `3d`
and `7 * 24 * time.Hour` for `1w`, and `iv.Duration()` returns them. Both branches can call
`d, _ := iv.Duration()` and differ from the general case only in which anchor they pass —
which is what the function's own comment says is the point ("the same arithmetic … because a
grid is a grid: an anchor and a period"). As written, `1w`'s period is stated in two places
that must be changed together, and the function contradicts the design it documents.

---

### 10. `worthWholeMonth`'s lower bound is justified by a case that cannot occur

**`internal/plan/plan.go:275–277`**

```go
// How much of this month is still needed from an archive. The lower
// bound is cur rather than s.Start because earlier iterations may
// already have covered part of it, ...
wantedFrom, wantedTo := cur, minTime(mEnd, archiveEnd)
```

No iteration can leave `cur` part-way through a month. Every branch sets `cur = mEnd` (a
month boundary) or `cur = wantedTo`, and `wantedTo < mEnd` only when it equals `archiveEnd`,
which ends the loop. So `cur` differs from `monthStart(cur)` on the first iteration only,
where it is exactly `s.Start`. The code is right; the reason given for it is not, and
CLAUDE.md asks that justifications be verified rather than plausible. Either say "cur, which
is s.Start on the only iteration where the two can differ", or use `s.Start` directly.

---

### 11. Five tests construct a `Loader` aimed at the real Binance hosts

**`loader_test.go:892`, `loader_test.go:935`; `options_test.go:20`, `:209`, `:230`, `:246`**

`NewLoader()` with no `withTestHosts` leaves `listBaseURL`, `downloadBaseURL` and
`apiBaseURL` empty, which `internal/vision` reads as the production endpoints. None of these
tests currently performs I/O — `route`, `report` and field inspection are all offline, and
`newCache` deliberately creates nothing — so nothing reaches Binance today. But CLAUDE.md
states the rule without qualification ("Network paths use `httptest.Server` with committed
fixtures — **no test may touch Binance**"), and a live loader pointed at the real bucket is
one added `Fetch` away from breaking it silently in CI.

`TestNewLoaderDefaults` additionally asserts on `l.cache.root`, which is the developer's
real `~/Library/Caches/bmd` path. Passing `withTestHosts("", "", "")`-style stubs, or a
`t.TempDir()` cache dir where the assertion allows it, costs one line each and removes the
class.

---

## Checked and found correct

Recorded so the next reader does not re-derive them:

- **`ceilFrom`'s truncation argument holds.** Go's `/` truncates toward zero, so for
  `delta < 0` the quotient is already the ceiling and `delta%period > 0` is false; for
  `delta >= 0` the adjustment is exact. The `n * period` product stays within `int64`
  nanoseconds for any anchor in 1970 and any instant before ~2262.
- **`grid1w = 1970-01-05` is the first Monday at or after the epoch.** 1970-01-01 was a
  Thursday.
- **Case 1 of `Expand` is behaviour-preserving.** `monthPublished && wholeMonthWanted`
  reduces to `mEnd <= min(s.End, s.ArchivesThrough)`, which is the old `wholeMonthCovered`.
- **The `default` (daily) branch is unchanged.** `dayStart(wantedFrom)` is `dayStart(cur)`
  and `wantedTo` is the old `dayLimit`.
- **`verifyCoverage` still holds for the new branch.** Only the first iteration can see a
  `cur` that is not a month boundary, so the wide monthly chunk can only precede `s.Start`,
  which `chunks[0].Start.After(start)` permits.
- **`Substitute`'s `dayStart` snap cannot lose data.** The substitutes cover at least the
  chunk they replace, and the consumer's `!k.OpenTime.After(last)` dedup absorbs the
  overlap.
- **The listing marker still dominates every chunk `route` can produce.** `since` is
  `monthStart(resolved.Start)`, and the widest chunk the new threshold emits starts at
  exactly that instant.
- **The permits-in-chunk-order argument is sound.** Chunk *i*'s permit is taken before
  chunk *i*+1's, so the chunk the consumer is blocked on is always already running; the
  `deferred cancel → <-producerDone → g.Wait()` ordering also keeps `g.Go` from racing
  `g.Wait`.
- **`README.md`'s "20 significant digits"** matches CLAUDE.md and the measured worst value
  `118661604939.99255335`.

---

## Resolution

Verified 2026-08-20 against the code, not taken on trust. **Ten of the eleven
hold and were applied. One does not**, and it is worth recording why rather than
quietly skipping it.

Four findings were reproduced as failing tests before anything was changed, and
each of those tests was then watched failing again against the unfixed code —
including the two that needed the fix reverted by hand to do it.

| # | Verdict | What changed |
| --- | --- | --- |
| 1 | **Confirmed**, reproduced | A cancelled `Fetch` with the producer blocked in `acquire` and every launched worker already returned `nil` gave **0 candles and a nil error**. `stream` now falls back to `ctx.Err()` when `g.Wait()` is nil. `TestCancellationMidStreamIsReported` drives `stream` directly, holding the only permit, because the condition has to be arranged rather than waited for |
| 2 | **Confirmed**, both directions | Emptiness is decided on the intersection of the chunk with the request, in a new `checkGap`. An empty intersection is skipped entirely, which removes the spurious failure *and* the reversed span in the message. The lenient direction — a delisted pair's final month, non-empty but with nothing inside the request — returned an empty slice and `nil` |
| 3 | **Confirmed** | The threshold moved out of `Expand` into the new `plan.Consolidate`, called from `Loader.route` with the listing in hand. `Expand` is availability-blind again. Measured on the reported scenario: **29 daily chunks where 20 were wanted** |
| 4 | **Confirmed**, reproduced | `FetchAll` over 12 requests with `WithConcurrency(2)` opened **24 simultaneous listings**. The plan phase now runs under a permit, released before any chunk is fetched. The duplicate-listing half is *not* fixed — see below |
| 5 | **Does not hold** — see below | Barrier strengthened and the comment corrected anyway |
| 6 | **Confirmed** | The minimality property reached 2 of 16 intervals. Replaced by stepping *back* one grid point, which is the same statement in one comparison and covers all 16. Verified by making `alignUp` overshoot by a period: **104 failures**, against 16 under the old check |
| 7 | **Confirmed** | `ladder` 21.4% → **85.7%**, `pause` 20.0% → **100%**. New tests cover the clamp in both directions, the zero `Retry-After`, `maxPipelinePauses` bounding the loop, and the ladder recovering a listed-then-404'd archive. A `withPauseBounds` seam keeps them in milliseconds |
| 8 | **Confirmed** | `includePartial` dropped from `loaderConfig`; `restFetcher` is constructed without it and the dangling cross-reference is gone. The field stays on `restFetcher`, where `restapi_test.go` still exercises it |
| 9 | **Confirmed** | Both branches take the period from `iv.Duration()` |
| 10 | **Confirmed** | Dissolved by #3 — the comment and the code it justified are both gone with the threshold |
| 11 | **Confirmed** as hygiene | New `withOfflineHosts()` points the three transports at a port nothing listens on. No test reached Binance before; now none can start to |

### Why #5 does not hold

The finding claims `TestConcurrentRequestsFetchAnArchiveOnce` "does not exercise
the deduplication" and that its assertion "holds even with the deduplication
removed". Both halves were tested directly, and both are wrong.

**The stated mechanism does not occur.** The finding says the other seven callers
are "typically still doing their two listing round trips" when `release` closes.
Instrumenting the barrier to record `listCalls` at the moment it fires reports
**16 of 16** — every caller had finished both listings — on every run. The leader
makes two further round trips (its sidecar, then the archive) after its own
listings, and the others use that time.

**The conclusion is false.** Deduplication was defeated by making the
singleflight key unique per call, and the test **fails**: 16 archive requests
instead of 2, on all three runs. It catches exactly what it claims to.

What is fair in the finding is narrower, and worth fixing: `queued.Done()` was
called *before* `Fetch`, so `queued.Wait()` was satisfied the moment eight
goroutines existed and guaranteed nothing about where they had got to. The test
worked by timing rather than by construction. The barrier now waits for all
sixteen listings and settles briefly, and the comment no longer claims more than
it delivers. Registration on the singleflight itself is still not observable —
the group exposes no waiter count — and the comment says so.

### Not fixed, deliberately

The second half of #4: two requests for the same symbol and interval still list
the bucket twice. Memoising the index on the `Loader` is what the finding
suggests, and an index is a snapshot of two different kinds of fact — a month
that exists will exist forever, a month that does not may simply not have been
published yet. A cached "not yet" is how a process decides at 00:05 that today
has no data and believes it until restarted. Sharing it through `singleflight`
would also inherit the foreign-cancellation edge that `cache.klines` already has
to retry around. The unbounded *concurrency*, which is the part that earns a 429,
is fixed; the duplicate work is left, recorded here and in the plan file.
