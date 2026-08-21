# Stage 9 code review

**Date:** 2026-08-21
**Branch:** `stage-9`, reviewed as `git diff f425548..stage-9` (the whole stage, six commits)
**Commits:** `86420f8 Option becomes an interface`, `2970247 runnable examples`,
`4023726 goreleaser, config only`, `481e741 documentation`, `ac34487 pkg.go.dev polish`,
`aab97c1 correct the example count`
**Scope:** `options.go`, `options_test.go`, `loader.go`, `version.go`, `codec.go`,
`request.go`, `symbol.go`, `doc.go`, `example_test.go`, `.goreleaser.yaml`, `mise.toml`,
`.gitignore`, `README.md`, `docs/architecture.md`.

6 findings. `mise run lint`, `go vet ./...` and `go test ./...` are green, and
`goreleaser check` validates the config — so nothing below is something the toolchain
would have caught.

Fix order: **#1** first (a copy-paste from the README fails outright on the default macOS
shell), then **#3** (it is the exact defect this stage set out to remove). The rest are
comment-versus-reality drift.

## What was checked and holds up

The one behavioural change in the stage is `Option` becoming an interface, and it is
sound:

- Every `With*` signature is unchanged, so no caller moved. `cmd/bmd/flags.go:148` builds
  a `[]binancedata.Option` and is unaffected.
- The `opt == nil` guard in `NewLoader` still means what it did. Comparing an interface
  against the untyped `nil` tests the interface header, not the dynamic value, so an
  `optionFunc` closure never trips it and no comparison of two func values (which would
  panic) is reachable.
- The claim that a typed-nil `optionFunc` "cannot occur" is true today: `optionFunc` is
  unexported and every constructor returns a live closure.
- `var _ Option = optionFunc(nil)` in `options_test.go` is a compile-time assertion only;
  `apply` is never called on it.

The version design was verified against a real artefact rather than taken on trust. The
snapshot binary in `dist/` reports
`v0.0.0-20260821114356-2970247f722f+dirty` from `bmd -version`, and `go version -m` shows
`vcs=git` on it — so goreleaser's `-s -w -trimpath` build does preserve the toolchain's
VCS stamping, and the decision to drop goreleaser's default `-X main.version` ldflag is
correct, not merely plausible.

The example split is accurate as documented: sixteen examples, the seven pure ones carry
`// Output:` blocks and run, the nine holding a `Loader` do not. `Example_errors` covers
exactly the six sentinels in `errors.go`.

---

## Correctness

### 1. The GOPRIVATE line in the README fails on zsh

**`README.md:93`**

```bash
go env -w GOPRIVATE=github.com/algo-one/*
```

The `*` is unquoted. zsh — the default shell on macOS, and the shell this project is
developed in — treats an unmatched glob as an error rather than passing it through the
way bash does:

```
zsh:1: no matches found: GOPRIVATE=github.com/algo-one/*
```

The command does not run, and `go env -w` is never reached. A reader following the
install instructions on a Mac gets an error whose text says nothing about Go. Quote it:

```bash
go env -w 'GOPRIVATE=github.com/algo-one/*'
```

Severity: medium. It is the first command in the only install path the README offers.

### 2. The `insteadOf` rewrite is global and unscoped

**`README.md:96`**

```bash
git config --global url."git@github.com:".insteadOf "https://github.com/"
```

This rewrites *every* HTTPS GitHub URL on the machine to SSH, for every repository and
every Go module, not just this org. A reader who has no SSH key loaded — or whose key is
not on GitHub — has just broken `git clone https://github.com/...` for unrelated public
repositories, and the failure ("Permission denied (publickey)") does not point back here.

Scope it to the org that actually needs it, which also makes it match the `GOPRIVATE`
line directly above:

```bash
git config --global url."git@github.com:algo-one/".insteadOf "https://github.com/algo-one/"
```

Severity: low, but it is a `--global` write in a copy-paste block.

### 3. Two exported doc comments still link into `internal/`

**`options.go:248`** and **`availability.go:610`**

The stage's stated pkg.go.dev goal was that "an identifier the documentation names but
the reader cannot reach is worse than one it never mentions", and `ac34487` duly removed
`[collectKlines]`, `[restFetcher]`, `[defaultConcurrency]`, `[minSymbolLength]` and
`[Request.endExclusive]` from exported comments. Two of the same kind survived, and both
point at `internal/vision`, which no reader of the published documentation can open:

- `options.go:248` — `// The default is a package-wide client built by [vision.NewHTTPClient]`
  on `WithHTTPClient`.
- `availability.go:610` — `// ... see the note on [vision.Lister]'s three outcomes`
  on `Loader.Available`.

A sweep of every exported declaration's doc comment found these two and nothing else; the
other `[vision.*]` and `[plan.*]` links (`availability.go:250`, `cache.go:69`,
`download.go:96`, `loader.go:467-486`, `restapi.go:20`) are all on unexported
declarations or inside function bodies, so they never render.

Same fix as the others: spell the thing out in prose instead of linking it —
"a package-wide client built in `internal/vision`".

## Comment versus reality

### 4. The goreleaser platform list does not match the CI matrix

**`.goreleaser.yaml:41`** (and the same claim in `docs/architecture.md:1118`)

> This matches the CI matrix rather than reaching wider: an artefact for a platform
> nothing tests is a promise made without evidence.

`.github/workflows/ci.yml:35` is `os: [ubuntu-latest, macos-latest]` — that is
linux/amd64 and darwin/arm64, two platforms. The build block ships four: darwin and linux
crossed with amd64 and arm64. So `darwin/amd64` (Intel Macs) and `linux/arm64` are
released with no test run behind them — precisely the "promise made without evidence" the
comment disclaims.

Either shipping four is fine and the justification should say so honestly ("two are
tested, two are cross-compiled from the same pure-Go source and rely on that"), or the
list should drop to the two CI covers. What it cannot be is both.

### 5. The changelog filters cannot match this repository's commits

**`.goreleaser.yaml:122`**

```yaml
exclude:
  - "^docs:"
  - "^test:"
  - "^chore:"
  - "typo"
```

These are conventional-commit prefixes. Every commit in this repository is of the form
`Stage 9: documentation`, `Stage 8: the bmd CLI`, `Add illustrated architecture overview`
— nothing has ever begun with `docs:`, `test:` or `chore:`. The stated intent
("Documentation and test-only commits are noise in a user-facing changelog") is therefore
not achieved: `Stage 9: documentation` will appear in the v0.1.0 changelog.

Either adopt the prefixes in commit messages, or write filters against the style actually
used (`^Stage \d+: documentation`), or drop the block and admit the changelog is every
commit.

### 6. The `optionFunc` comment cites an analogy that is not one

**`options.go:102`**

> This is the standard Go move for giving an interface a function implementation —
> `http.HandlerFunc` is the same trick, and so is `slog.HandlerOptions`' cousin in every
> options-carrying library

`http.HandlerFunc` is exactly right. `slog.HandlerOptions` is a plain struct of settings
with no method and no interface behind it — it is not an adapter, and there is no
"cousin" of it doing this. In a codebase whose comments are the teaching text, a reader
who goes and looks up `slog.HandlerOptions` learns the opposite of what the sentence
implies. Drop the second half, or replace it with a real second example
(`sort.SliceStable`'s `sort.Interface` adapters, or `grpc.UnaryServerInterceptor`-style
option funcs).

---

## Not findings, recorded so they are not re-litigated

- **`cap(l.sem)` in `TestOptionsApplyInTheOrderWritten`** reads the semaphore's buffered
  channel capacity through the named `semaphore` type. That works and is the right thing
  to assert — it measures the setting as the loader holds it rather than as the config
  recorded it.
- **`docs/architecture.md` and `version.go` quote different untagged pseudo-versions**
  (`...111323-f4255484548a` versus `...114356-2970247f722f`). Both are real measurements
  taken at different commits during the stage; neither is wrong.
- **`example_test.go`'s file comment becomes the package comment for
  `binancedata_test`.** Harmless — it is the only file in that package, and external test
  packages are not published.
- **`ExampleRequest_Validate` uses `time.Local`** and asserts the request is rejected.
  This is deterministic even under `TZ=UTC`: `time.Local` is `&localLoc`, a different
  pointer from `time.UTC`'s `&utcLoc`, and `requireUTC` compares by pointer.
- **`ExampleWriteParquet` calls `os.Create` in the working directory.** It carries no
  `// Output:` block, so it is compiled and never run; no stray file is created.
