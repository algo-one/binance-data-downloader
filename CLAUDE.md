# CLAUDE.md

Instructions for Claude Code working in this repository. Loaded automatically at
the start of every session.

## What this is

A Go library and CLI that downloads and caches historical Binance market data.
It is built from scratch in Go; `docs/architecture.md` is the authoritative
description of what it does and how it is put together.

## Read these before starting a stage

| Source | Why |
| --- | --- |
| `docs/architecture.md` | The pipeline, package layout, stage table with completion status, and the ten correctness requirements each stage must satisfy |
| `docs/caching.md` | The two-tier cache and its invariants |
| `/Users/ivan/.claude/plans/let-s-implement-binance-historical-humble-matsumoto.md` | The full plan, with the measurements and rationale behind each decision. Not version-controlled; not loaded automatically |

**Work is done one stage per session.** Check the stage table in
`docs/architecture.md` for what is done and what is next. When a stage lands,
update that table *and* the one in the plan file.

## Hard constraints

- **No destructive commands. Always ask first.** This includes deleting or
  overwriting files, `git reset`, force pushes, and directory renames.
- **Do not commit or push unless asked.**
- **Heavy in-code comments are a primary deliverable, not polish.** Ivan is
  learning Go. Comments should explain *why*, and should explain Go idioms as
  they appear — the code reads as a teaching text. This is deliberately denser
  than idiomatic Go.
- **Spot klines only.** Futures and other data types are out of scope, but the
  three extension points in `docs/architecture.md` must be preserved.

## Conventions

- Module `github.com/algo-one/binance-data-downloader`, package `binancedata`.
- Public API in the root package; everything else under `internal/`, which the
  compiler forbids other modules from importing.
- **Prices and volumes are `udecimal.Decimal`, never `float64`.** Binance quote
  volumes reach 20 significant digits (worst real value:
  `118661604939.99255335`, BTCUSDT `1mo` quote volume). This was measured over
  1,751,352 real values, not assumed — see `docs/numbers.md` and the plan file
  before revisiting.
- Errors are sentinels in `errors.go`, wrapped with `%w`, compared with
  `errors.Is`. Never `==`.
- Every function that does I/O takes `ctx context.Context` first.
- Tests are table-driven. Network paths use `httptest.Server` with committed
  fixtures — **no test may touch Binance.** Time is injected, never read from
  `time.Now()` inside logic.
- `go.mod` declares Go 1.24 as the floor; mise pins 1.26.5 for development. A CI
  job builds with real 1.24, so do not use newer language or stdlib features
  without raising the floor deliberately.

## Commands

Tooling is managed with [mise](https://mise.jdx.dev/). Run `mise tasks` for the
full list.

```bash
mise run ci      # fmt:check + lint + test (-race) + build — must be green before finishing a stage
mise run test    # tests only
mise run lint    # golangci-lint
mise run build   # ./bin/bmd
mise run audit   # govulncheck
```

Prefer fixing a lint finding in the code over adding an exclusion to
`.golangci.yml`. When an error genuinely should be ignored, use an explicit
`_, _ =` assignment so the decision is visible at the call site.
