# letts dogfood

End-to-end validation of `dugdale`, the `letts` CLI, and `letts/php-client` on a developer machine. Six realistic PHP missions, six shell scenarios. No production data involved.

## Layout

| Path | Role |
|---|---|
| `_setup.sh` | Build `letts`/`dugdale`, install PHP deps, start dugdale, apply lanes. Writes `.runtime/env.sh` so subsequent scripts inherit `LETTS_CONFIG`. |
| `_teardown.sh` | Stop dugdale (TERM → KILL), wipe `.runtime/`. |
| `_lib.sh` | Shared bash helpers (`step`, `ok`, `fail`, `assert_equal`, `assert_contains`, `wait_for_status`). |
| `run-all.sh` | Setup → every `scenarios/NN_*.sh` → teardown. Exit 0 iff all green. |
| `dugdale.yaml.tpl` / `letts.yaml.tpl` | Templates substituted by `_setup.sh` into `.runtime/`. |
| `composer.json` / `composer.lock` | Path repo to `../../../letts-php`. Run `composer install` once; `vendor/` is gitignored. |
| `.gitignore` | Excludes `vendor/` and `.runtime/`. |
| `missions/` | PHP missions. Loaded by dugdale at dispatch time. |
| `scenarios/` | Numbered shell scenarios. Each standalone after `_setup.sh`. |

## Prerequisites

- Go (for building `letts`/`dugdale`)
- PHP 8.3+ with `ext-pcntl`, `ext-posix`, `ext-curl`, `ext-json`
- `composer`
- `jq`, `python3`, `curl`
- `~/Projects/letts-php` checked out as a sibling of `~/Projects/letts`

## Running

Full pass:
```bash
./run-all.sh
```

Individual scenario after setup:
```bash
./_setup.sh
./scenarios/03_parallel_lanes.sh
./_teardown.sh
```

## Scenario index

| # | What it exercises |
|---|---|
| 01 | `letts dispatch` and `letts events --follow`; `letts run` sync with return value |
| 02 | `letts run --file=role=PATH` inline staging upload, `--output-file=role=PATH` download, and sha verify |
| 03 | Dispatch 8 missions across `fast`/`normal`/`slow`; `ctl missions list --status=running` snapshot |
| 04 | Explicit fail / `--timeout` / `ctl missions kill` |
| 05 | `ctl dugdales/lanes list`, restart yields new id with `restarted_from`, delete and verify gone |
| 06 | `letts events --follow` resume with `--from <seq>` against completed mission — currently tests resume semantics against static stream because middleware Flusher is broken (see follow-up task) |
| 07 | `/v1/metrics` Prometheus exposition — assert success/failed mission counters, http_requests, lane_concurrency, dugdale_info populated |
| 08 | `letts logs <id> --follow` live stream — assert partial tail receives >=3 lines spanning >=600ms; full log re-fetch returns all 10 lines |

## Out of scope

Daemon `done` event compliance, perf/load benchmarks, real production missions, `letts ui` (separate `letts-ui` repo decision).

## Known daemon issues discovered by dogfood

- `GET /v1/missions/<id>` returns 200 with `status="deleting"` rather than a 404 → scenario 05 polls for actual SQL DELETE (~30s sweep) instead of relying on immediate 404.
- `internal/server/middleware/reqlog.go` `responseWriter` doesn't implement `http.Flusher` → events stream buffers until handler returns instead of flushing per event. Scenario 06 redesigned to test resume semantics against static stream.
