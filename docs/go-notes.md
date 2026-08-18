# Go notes

The idioms this library leans on, in one place. The code is commented heavily
enough to stand alone; this document is the longer explanation of the handful of
constructs that recur everywhere — so a comment can say *"functional options,
see go-notes.md"* instead of re-deriving the pattern each time.

If you only read two sections, read [Errors are values](#errors-are-values) and
[Concurrency](#concurrency). Those two shape the structure of nearly every
package here.

---

## Contents

- [Errors are values](#errors-are-values)
- [Concurrency](#concurrency)
- [Cancellation and `context`](#cancellation-and-context)
- [Functions and arguments](#functions-and-arguments)
- [Structs are values](#structs-are-values)
- [Absence has four spellings](#absence-has-four-spellings)
- [Numbers](#numbers)
- [Interfaces](#interfaces)
- [Packages and visibility](#packages-and-visibility)
- [Iteration](#iteration)
- [Testing](#testing)
- [Standard library landmarks](#standard-library-landmarks)

---

## Errors are values

A Go function does not throw. It returns the failure alongside the result, and
the caller decides:

```go
data, err := download(ctx, url)
if err != nil {
    if errors.Is(err, ErrNotAvailable) {
        return nil, nil
    }
    return nil, err
}
```

The `if err != nil` block is famously repetitive. The compensation is that every
failure path is visible at the call site — nothing can fail three frames down
and surface somewhere unrelated, because there is nothing to unwind.

### Sentinel values

Failure *kinds* are package-level error values, declared once in `errors.go`:

```go
var (
    ErrNotAvailable   = errors.New("data not available")
    ErrChecksum       = errors.New("checksum mismatch")
    ErrCorruptArchive = errors.New("corrupt archive")
)
```

Callers branch on these rather than on strings or status codes.

### Wrapping with `%w`

Context is added by wrapping, using the `%w` verb in `fmt.Errorf`:

```go
if err := verify(path, want); err != nil {
    return fmt.Errorf("verify %s: %w", path, err)
}
```

`%w` records a link to the original error, so the sentinel underneath stays
findable. `%v` would only interpolate its text and break the chain — the
difference matters, and it is a common early mistake.

### Always `errors.Is`, never `==`

```go
if err == ErrNotAvailable { }             // WRONG: fails once the error is wrapped
if errors.Is(err, ErrNotAvailable) { }    // right: walks the whole chain
```

`errors.As` is the variant that also hands you the typed error, so you can read
fields off it:

```go
var httpErr *HTTPError
if errors.As(err, &httpErr) {
    log.Printf("status %d", httpErr.StatusCode)
}
```

The `errorlint` linter in `.golangci.yml` exists to catch the `==` form.

### Cleanup is `defer`

```go
resp, err := client.Do(req)
if err != nil {
    return err
}
defer resp.Body.Close()
```

`defer` runs when the *function* returns, not when the enclosing block ends, and
deferred calls run in reverse order. It sits immediately after the thing it
cleans up, which is why a forgotten close is easy to spot in review.

One trap worth knowing early: `os.Exit` does not run deferred functions. That is
why `cmd/bmd/main.go` isolates `os.Exit` in a one-line `main`.

---

## Concurrency

Any function can be run concurrently by prefixing a call with `go`. There is no
async colouring: a blocking call blocks only the goroutine that made it, so
there is never a second sync/async version of an API to maintain.

### Fan-out with `errgroup`

```go
g, ctx := errgroup.WithContext(ctx)
results := make([]Result, len(months))

for i, m := range months {
    g.Go(func() error {
        r, err := download(ctx, m)
        results[i] = r        // safe: each goroutine owns one distinct index
        return err
    })
}
if err := g.Wait(); err != nil {
    return nil, err
}
```

`errgroup.WithContext` earns its place here: the first error cancels the derived
context, so the remaining downloads stop instead of running to completion and
being discarded.

Writing into a preallocated slice at distinct indices needs no mutex — the
goroutines never touch the same memory. Anything less obvious than that should
use a channel or a mutex, and `mise run test` runs with `-race` precisely to
catch the cases where the reasoning was wrong.

### Bounding with `SetLimit`

```go
g.SetLimit(4)   // at most 4 goroutines from this group run at once
```

One limit over one queue of uniform work units. Nested limits are avoided
deliberately: when an outer unit holds a permit while merely waiting on inner
units, a slot is occupied by a task doing nothing. Flattening the work removes
that failure mode rather than tuning around it — see
[architecture.md](architecture.md).

### Collapsing duplicates with `singleflight`

```go
var group singleflight.Group

v, err, _ := group.Do(key, func() (any, error) {
    return download(ctx, key)
})
```

`singleflight.Do` guarantees that concurrent calls with the same key run the
function once and all receive the same result. Hand-rolling this — a map of
in-flight futures plus a mutex — is a well-known way to introduce a race between
checking the map and registering in it.

### Sharing memory

The Go proverb is *"do not communicate by sharing memory; share memory by
communicating"* — prefer passing values over a channel to guarding them with a
lock. In practice this codebase does both: channels where work flows between
stages, `sync.Mutex` where a map genuinely needs concurrent access. Neither is
wrong; use whichever makes the ownership obvious.

---

## Cancellation and `context`

The convention is absolute: **if a function does I/O, its first parameter is
`ctx context.Context`.**

```go
func (l *Loader) Fetch(ctx context.Context, req Request) ([]Kline, error)
```

A context carries a deadline and a cancellation signal down the call tree.
Cancel it at the top and every HTTP request, every worker goroutine, and every
sleep below it returns promptly. That is how Ctrl-C works in `bmd`:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
```

Two rules that are easy to get wrong:

- Never store a `Context` in a struct field. Pass it as an argument.
- Never pass a nil context. Use `context.Background()` at the top of `main`, or
  `t.Context()` in a test.

The `noctx` linter is enabled to catch HTTP requests built without one.

---

## Functions and arguments

Go has no named arguments and no default values. Two patterns cover what they
would have done.

### A parameter struct

For anything past about three parameters — especially several of the same type —
use a struct:

```go
type Request struct {
    Symbol   string
    Interval Interval
    Market   Market
    Start    time.Time
    End      time.Time
}

klines, err := loader.Fetch(ctx, Request{
    Symbol:   "BTC/USDT",
    Interval: Interval1h,
    Market:   MarketSpot,
    Start:    start,
    End:      end,
})
```

Fields you omit take their zero value, and deciding what that zero value means
is part of designing the struct. This codebase splits it deliberately:

- `Symbol`, `Interval` and `Market` have **no** usable zero value. Every enum in
  the package starts at `iota + 1`, so an unset field is invalid and the
  constructor rejects it. `Market` is the interesting one: spot is the only
  market today, so a default could only ever have meant spot — but once futures
  exists, every request written earlier would silently keep meaning spot, and
  the compiler's one chance to ask "which market?" is gone. Defaults are easy to
  add later and impossible to take back.
- `End` **does** have one. A zero `time.Time` is checked for inside the function
  and replaced with the current time **per call** — a default resolved at
  construction time would go stale in a long-running process.

The difference is whether the library can honestly guess. It can guess "now"; it
cannot guess which market you meant.

### Functional options

For a constructor with many optional settings, the idiom is a variadic list of
functions that mutate a private config:

```go
loader, err := binancedata.NewLoader(
    binancedata.WithCacheDir("~/.cache/bmd"),
    binancedata.WithConcurrency(8),
)
```

```go
type Option func(*config)

func WithConcurrency(n int) Option {
    return func(c *config) { c.concurrency = n }
}
```

It is wordy to define, but options can be added later without breaking a single
caller, and each one gets its own doc comment on pkg.go.dev.

---

## Structs are values

```go
type CacheKey struct {
    Symbol   string
    Interval Interval
    Period   Period
}
```

A struct whose fields are all comparable is itself comparable, so it works as a
map key directly — no hashing method to write, no tuple to build:

```go
cache := map[CacheKey][]Kline{}
```

Assignment copies. This is the single most common source of early confusion:

```go
a := CacheKey{Symbol: "BTCUSDT"}
b := a              // a full copy
b.Symbol = "ETHUSDT"
// a.Symbol is still "BTCUSDT"
```

Use a pointer (`*Loader`) when you want shared, mutable identity — which is why
methods that mutate state have pointer receivers.

There is no constructor hook that runs automatically. Validation goes in an
ordinary function returning `(T, error)`, which is better than a hook anyway:
a caller cannot forget it, because the error return is right there in the
signature and `errcheck` will not let it be discarded silently.

---

## Absence has four spellings

Picking the right one is a real design decision:

| Situation | Go |
| --- | --- |
| "No result, and that is normal" | `(T, bool)` — the comma-ok form |
| "No result, because something failed" | `(T, error)` |
| "Genuinely optional field" | a pointer, `*T`, which can be nil |
| "Absent means empty" | the zero value: `""`, `0`, `nil` slice |

The zero value is used far more than newcomers expect. A `nil` slice has length
0 and can be appended to; a `nil` map can be read from. A great deal of
defensive nil-checking turns out to be unnecessary.

A missing archive is the worked example. It could have been a nil result that
every caller has to remember to check; instead it is `ErrNotAvailable`, a
sentinel tested with `errors.Is` — the signature forces the caller to
acknowledge the error return, and the linter checks they did not discard it.

---

## Numbers

Prices and volumes are [`udecimal.Decimal`](https://github.com/quagmt/udecimal),
never `float64`. The reasoning is in [caching.md](caching.md) and the README.

Go has no operator overloading, so arithmetic is methods:

```go
total, err := a.Add(b.Mul(c))
```

Some operations return an error — division by zero, overflow past the 128-bit
inline representation. Prices from Binance are well inside the range, but the
error exists and should not be discarded blindly.

Comparison is a method too: `a.Cmp(b) < 0`, or `a.LessThan(b)`. `==` on a
Decimal compares the internal representation, not the numeric value, so avoid
it.

---

## Interfaces

Interfaces are structural and checked by the compiler. Nothing declares that it
implements one:

```go
type Fetcher interface {
    Fetch(ctx context.Context, chunk Chunk) ([]Kline, error)
}
```

Any type with that method satisfies `Fetcher` — no base type, no registration.
A test can define a two-line fake in the test file and pass it straight in.

Two conventions that shape the code:

- **Interfaces are small.** One or two methods is normal. `io.Reader` has one.
- **Interfaces are declared by the consumer, not the producer.** The package
  that *needs* a `Fetcher` defines `Fetcher`. That is what keeps packages from
  depending on each other unnecessarily.

The corollary — *accept interfaces, return structs* — is why `NewLoader`
returns `*Loader` rather than some `LoaderInterface`.

---

## Packages and visibility

A package is a *directory*, not a file. Every file in it shares one namespace
and sees the others' unexported symbols with no import — that is how
`version_test.go` calls the unexported `versionFrom`.

Capitalisation *is* the access modifier. `Kline` is exported, `kline` is not,
and there is no way to reach the second from another package.

`internal/` goes further: the compiler refuses any import of
`.../internal/vision` from outside this module. That is why the public API sits
in the root package and everything else is under `internal/` — the whole
implementation can be restructured between stages without breaking a single
consumer, and the boundary is enforced rather than documented.

---

## Iteration

Range-over-function (Go 1.23) lets a function be driven directly by a `for`
loop:

```go
for kline, err := range loader.Stream(ctx, req) {
    if err != nil {
        return err
    }
    process(kline)
}
```

`Stream` returns an `iter.Seq2[Kline, error]`. It matters here for a concrete
reason: a `Kline` measures 312 bytes, so five years of one-minute candles is
roughly 820 MB held at once, and a
backtest usually wants to consume them one at a time rather than materialise the
whole range.

Channels are the other option, and the right one when a producer goroutine is
genuinely running concurrently. `iter.Seq2` is better for a pull-based sequence,
because it needs no goroutine and so cannot leak one.

---

## Testing

Everything is in the standard library. A test is a function:

```go
func TestSomething(t *testing.T) {
    got := Something()
    if got != want {
        t.Errorf("Something() = %v, want %v", got, want)
    }
}
```

The pieces used throughout this repository:

| Need | Tool |
| --- | --- |
| Many input/output cases | a table-driven test — see `version_test.go` |
| A scratch directory | `t.TempDir()` — removed automatically |
| An environment variable | `t.Setenv()` — restored automatically |
| Teardown | `t.Cleanup(func(){ ... })` |
| A cancellable context | `t.Context()` |
| A fake HTTP server | `httptest.NewServer` |
| Faking anything else | **inject the dependency** — see below |

There is no monkey-patching in Go, so anything you want to fake must already be
a parameter or an interface. `versionFrom` in `version.go` takes its one
dependency — `debug.ReadBuildInfo` — as a function argument for exactly this
reason. The constraint pushes the seams into the signatures, where they are
visible, instead of leaving them to be discovered at test time.

Every network path in this library is tested against an `httptest.Server`
serving committed fixtures, so the suite runs offline and deterministically.

---

## Standard library landmarks

Where the recurring jobs live:

| Job | Package |
| --- | --- |
| String building | `fmt.Sprintf`, `strings.Builder` |
| Paths | `filepath.Join` — never manual `/` |
| Times and durations | `time.Now().UTC()`, `time.Hour`, `d.AddDate(0, 1, 0)` |
| Formatting a time | `d.Format("2006-01")` — see the warning below |
| JSON | `json.Unmarshal` into a typed struct |
| CSV | `encoding/csv` |
| ZIP archives | `archive/zip` |
| Hashing | `crypto/sha256` |
| Logging | `log/slog`, structured by default |
| HTTP | `net/http`, with `http.NewRequestWithContext` |

Time formatting deserves its own warning. Layouts are not `%`-codes; they are
written as the reference time `Mon Jan 2 15:04:05 MST 2006`, so a layout is an
*example* of the format you want. `2006-01` means "four-digit year, dash,
two-digit month". It reads oddly until it doesn't.
