<video src="https://github.com/user-attachments/assets/2002d5f2-a434-4295-9b31-356c6877a77b" controls></video>

# letts

**letts** is a durable, self-hosted task queue and remote-execution system. It is the
successor to *jobd*. A long-running daemon called **dugdale** holds per-lane work
queues, runs jobs as ordinary operating-system processes, captures their structured
output, and exposes everything over a small HTTP API. A single command-line client
called **letts** talks to one or many dugdale instances to apply configuration,
dispatch work, follow it to completion, and inspect the results.

The whole system is two statically-linked Go binaries with no runtime dependencies.
All daemon state lives in a single SQLite database plus a directory of append-only
files, and every state transition is engineered to survive a crash or a hard kill
without losing or corrupting a job.

```
   ┌────────────┐   letts.yaml (client config, ${ENV}-aware)
   │  letts CLI │────────────────────────────────────────────────┐
   │ lettsclient│                                                │
   └─────┬──────┘                                                │
         │  HTTP/1.1 + Bearer tokens (dispatch / exec / admin)   │
         ▼                                                       ▼
   ┌─────────────────────────── dugdale daemon ────────────────────────────┐
   │  HTTP API ─► lane runners ─► spawn process ─► fd3 events + stdout/err │
   │                    │                                                  │
   │   SQLite state.db (WAL) ◄──► output/  staging/  work/  tombstone/     │
   └───────────────────────────────────────────────────────────────────────┘
                       dugdale.yaml (daemon config, literal values)
```

---

## Table of contents

- [Concepts and vocabulary](#concepts-and-vocabulary)
- [Architecture and data flow](#architecture-and-data-flow)
- [Installation and building](#installation-and-building)
- [Quick start](#quick-start)
- [Configuration](#configuration)
  - [Daemon configuration — `dugdale.yaml`](#daemon-configuration--dugdaleyaml)
  - [Client configuration — `letts.yaml`](#client-configuration--lettsyaml)
- [The dugdale daemon](#the-dugdale-daemon)
- [The letts CLI](#the-letts-cli)
- [Writing a mission](#writing-a-mission)
- [Remote exec](#remote-exec)
- [How it works: core mechanics and durability](#how-it-works-core-mechanics-and-durability)
- [HTTP API reference](#http-api-reference)
- [Observability](#observability)
- [Deployment and security](#deployment-and-security)
- [Development](#development)
- [License](#license)

---

## Concepts and vocabulary

A handful of terms recur throughout the system. Understanding them up front makes the
rest of the documentation much easier to follow.

- **dugdale** — one running instance of the daemon. It owns a data directory, a set of
  lanes, and an HTTP listener. A single physical host can run several dugdale instances
  side by side, each with its own data directory and configuration file.

- **mission** — the primary unit of work. A mission is a *configured* script: the
  operator registers a mission directory and a command template through `letts apply`,
  and then dispatches a mission by name with a JSON input payload. The daemon resolves
  the name to a script, runs it, and records the result.

- **exec** — an *ad-hoc* command. Where a mission runs a pre-registered script, an exec
  runs an arbitrary argv (or an uploaded script) that the caller supplies at dispatch
  time. Exec is a remote-code-execution feature and is therefore disabled by default;
  it must be explicitly turned on in the daemon configuration.

- **lane** — a named queue with a concurrency limit. Every mission and every exec is
  dispatched into a lane. The lane's concurrency limit caps how many of its jobs run at
  once; the rest wait in `queued` order. Lanes can be paused and resumed independently.

- **kind** — every job row is either `kind=mission` or `kind=exec`. Kinds are isolated
  from one another at the authorization layer: a dispatch token can only see missions,
  an exec token can only see execs, and only an admin token sees both.

- **staging** — a content-addressed file store inside the daemon. Mission and exec
  inputs (files, scripts, stdin) are uploaded to staging before dispatch, and outputs
  are committed back into staging when a job succeeds. Uploads are resumable and
  de-duplicated by content hash.

- **scope / token** — the daemon recognizes three Bearer-token scopes: **dispatch**
  (submit and read missions), **exec** (submit and read execs), and **admin**
  (everything, including configuration, lifecycle control, and listing). Admin is a
  superset of the other two.

- **the two configuration files** — these are easy to confuse, so it is worth stating
  plainly: `dugdale.yaml` configures the *daemon* (listen address, data directory,
  tokens, limits) and its values are literal. `letts.yaml` configures the *client*
  (which dugdales exist, how to reach them, which tokens to use, lane definitions) and
  its values may reference environment variables. Lanes, the mission directory, and the
  command template are **not** part of the daemon config — they are defined client-side
  in `letts.yaml` and pushed to the daemon with `letts apply`.

---

## Architecture and data flow

The lifecycle of a mission ties the pieces together:

1. The operator writes a `letts.yaml` that lists the dugdale servers, the lanes each one
   should run, and the command template used to turn a mission name into a script
   invocation. Running `letts apply` reconciles each daemon's lanes and runtime against
   that desired state.

2. A client calls `letts run --host <id> --lane <name> --mission <name> --input '{...}'`.
   The CLI resolves the target, validates and canonicalizes the input JSON, optionally
   uploads input files to staging, and issues `POST /v1/dispatch` with an
   `Idempotency-Key` header that becomes the new mission's identifier.

3. The daemon writes the mission's append-only events file to disk *before* inserting the
   database row (so a crash always leaves a recoverable artifact), inserts the row as
   `queued`, and notifies the lane's runner goroutine.

4. The lane runner picks the oldest queued row whenever it has spare concurrency, flips
   it to `running`, and spawns the script as a child process in its own process group.
   The script receives its input on standard input, a set of `LETTS_*` environment
   variables, and a dedicated file descriptor 3 on which it emits structured progress,
   success, and failure events as newline-delimited JSON.

5. While the process runs, the daemon streams its events file and captures its standard
   output and standard error (with size caps). Clients can follow either stream live.

6. When the process exits, the daemon computes a terminal outcome from the fd3 events,
   the exit code, and any signal, then durably finalizes the mission: it writes an
   intent row, renames any output files into staging, appends the terminal `done` event
   to the public stream, and finally updates the database row — all designed so that a
   crash at any point can be replayed correctly on the next start.

7. Background workers expire old missions and staging files according to retention
   policies, keep the SQLite database compact, and refresh Prometheus metrics.

### The data directory

Each dugdale owns one data directory (`data_dir`). The daemon creates and manages the
following layout inside it:

| Path | Contents |
|------|----------|
| `state.db` (+ `-wal`, `-shm`) | The SQLite database: missions, staging files, refs, runtime snapshots, finalize intents, applied config. Opened in WAL mode with a single serialized writer. |
| `dugdale.lock` | An advisory `flock` guaranteeing only one daemon process uses this data directory at a time. |
| `output/` | Per-mission append-only artifacts: the NDJSON `…-events` file and the `…-stdout` / `…-stderr` / `…-combined` capture files. Two-level sharded by the first four hex characters of the mission id. |
| `staging/` | Completed and in-progress staging files (mission/exec inputs, scripts, committed outputs). Sharded the same way. |
| `work/` | Per-job scratch directories. A mission's `$LETTS_WORKDIR` and an exec's materialized inputs/outputs live here for the duration of the run. |
| `tombstone/` | Staging files that have been garbage-collected are renamed here first and unlinked after a short grace period, so downloads already in flight keep working. |

State is persisted with SQLite via the pure-Go `modernc.org/sqlite` driver, so the
binaries need no cgo and no system SQLite library.

---

## Installation and building

### Prerequisites

- Go 1.26 or newer (the module targets `go 1.26`).
- A POSIX host for the daemon. Process-group kill handling and `/proc`-based start-time
  checks have Linux-specific implementations with portable fallbacks for development on
  macOS.

### Building from source

The project ships a `Makefile` that wraps the common tasks:

```sh
make build      # build both binaries into bin/ (dugdale and letts)
make dugdale    # build only the daemon  -> bin/dugdale
make letts      # build only the CLI     -> bin/letts
make check      # gofmt, go vet, go test -race ./...  (run before committing)
make test       # go test -race ./...
make vet        # go vet ./...
make fmt        # gofmt -l -w .
make clean      # remove bin/
```

You can equally build directly with the Go toolchain:

```sh
go build -o bin/dugdale ./cmd/dugdale
go build -o bin/letts   ./cmd/letts
```

### Version metadata

Build-time version information is injected through the linker into the
`letts/internal/version` package. Without overrides, the version reads `dev`:

```sh
go build -ldflags "\
  -X letts/internal/version.Version=$(scripts/build/version.sh) \
  -X letts/internal/version.Commit=$(git rev-parse --short HEAD) \
  -X letts/internal/version.BuiltAt=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  ./cmd/dugdale
```

The repository tracks a build number in the `VERSION` file, which the release tooling
turns into a `0.0.<N>` version string. `make bump` increments it; `make version` prints
the current value.

### Linux release artifacts and the Debian package

```sh
make linux      # cross-compile static linux/amd64 binaries into dist/
make deb        # build the linux binaries and package a .deb into dist/
```

The `.deb` is produced from `packaging/nfpm.yaml` and installs:

- `/usr/bin/dugdale` and `/usr/bin/letts`
- the systemd units `dugdale.service` (single default instance) and `dugdale@.service`
  (template for additional named instances)
- an annotated example config at `/etc/letts/dugdale/default.yaml.example`
- post-install / pre-remove / post-remove maintainer scripts

---

## Quick start

The fastest way to see the system run end to end is the development harness under
`scripts/dev`, which drives a local daemon with sample missions.

```sh
# Terminal 1 — start a daemon on 127.0.0.1:7180 with a /tmp data directory:
./scripts/dev/run.sh

# Terminal 2 — push lane and runtime config, then dispatch sample missions:
./scripts/dev/apply.sh
./scripts/dev/dispatch.sh hello         # stdout/stderr and an fd3 success with a return value
./scripts/dev/dispatch.sh progress      # a stream of fd3 progress events
./scripts/dev/dispatch.sh with_output   # produces a file and registers it as an output
./scripts/dev/dispatch.sh fail          # an explicit fd3 failure and a non-zero exit
```

To do the same thing by hand, the shape is:

```sh
# 1. Start the daemon with a config file (see the Configuration section):
dugdale --config /etc/letts/dugdale/default.yaml

# 2. Reconcile the daemon's lanes and runtime from your client config:
letts apply -f letts.yaml

# 3. Dispatch a mission and follow it to its terminal outcome:
letts run --host local --lane default --mission hello --input '{"name":"world"}'
```

`letts run` prints progress and log lines on standard error and the mission's structured
`return` value on standard output, then exits with a code that reflects the mission's
outcome.

---

## Configuration

There are two independent configuration files, parsed by two separate packages, with two
different philosophies about environment variables. Both are parsed strictly: an unknown
or misspelled key is a hard error rather than a silently ignored default.

### Daemon configuration — `dugdale.yaml`

The daemon configuration is deliberately small. It covers *how the daemon runs* — where
it listens, where it stores state, which tokens it accepts, and what resource limits it
enforces. It does **not** contain lanes, a mission directory, or a command template;
those are operational state that you push from the client side with `letts apply`.
Values are taken literally — `${ENV}` references are **not** expanded in `dugdale.yaml`.

The canonical location is `/etc/letts/dugdale/default.yaml`. A minimal, annotated example:

```yaml
listen:   127.0.0.1:7180 # a non-loopback listen requires network.behind_tls_proxy:
                         # true (firewalled / behind a TLS proxy) or, for dev only,
                         # network.insecure_plain_http: true
data_dir: /var/lib/letts/default

auth:
  tokens: ["CHANGE-ME-dispatch"] # dispatch-scope bearer tokens (a secret)

admin:
  tokens: ["CHANGE-ME-admin"] # admin-scope bearer tokens (a secret)

log:
  level:  info   # debug | info | warn | error
  format: json   # json | text
  output: stderr # stderr | stdout | - | /path/to/file

cleanup:
  success_ttl:    24h
  failed_ttl:     7d
  staging_ttl:    1h
  sweep_interval: 5m

exec:
  enabled: false # opt-in remote execution; leave off unless required
```

#### Full field reference

**Top-level / required.** Both must be set or the daemon refuses to start.

| Key | Type | Meaning |
|-----|------|---------|
| `listen` | string | TCP listen address, e.g. `127.0.0.1:7180`. |
| `data_dir` | string | Data directory; created `0755` on start. |

**`network`** — the daemon never terminates TLS itself; it only records whether it sits
behind a TLS-terminating proxy.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `network.behind_tls_proxy` | bool | `false` | Asserts a TLS proxy fronts the listener. Required to bind a non-loopback address while any token scope is configured. |
| `network.insecure_plain_http` | bool | `false` | Development escape hatch: permit plain-HTTP bearer tokens on a non-loopback address. |
| `network.trusted_proxies` | []string | `[]` | CIDRs of trusted proxies, for `X-Forwarded-For` parsing (single hosts as `/32` or `/128`; bare IPs are rejected). Invalid entries are logged and dropped rather than failing startup. |
| `network.use_x_forwarded_for` | bool | `false` | Honor `X-Forwarded-For` when computing the client IP for brute-force tracking. |

If `listen` is non-loopback **and** any token scope is configured (`auth.tokens`,
`admin.tokens`, or — only when `exec.enabled: true` — `exec.tokens`), the daemon refuses to
start unless `behind_tls_proxy` or `insecure_plain_http` is set — a guard against
accidentally exposing bearer tokens over plain HTTP.

**`auth` / `admin`** — bearer-token lists. Membership in a list grants that scope.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `auth.tokens` | []string | `[]` | Dispatch-scope tokens (submit/read missions). |
| `admin.tokens` | []string | `[]` | Admin-scope tokens (everything; superset of dispatch and exec). |

Admin access is over TCP with admin-scope bearer tokens — the daemon binds a TCP
listener only, with no Unix socket.

**`log`**

| Key | Type | Default | Allowed |
|-----|------|---------|---------|
| `log.level` | string | `info` | `debug`, `info`, `warn`, `error` |
| `log.format` | string | `json` | `json`, `text` |
| `log.output` | string | `stderr` | `stderr`, `stdout`, `-`, or a file path |

**`cleanup`** — retention durations. Durations accept Go's `time.ParseDuration` syntax
plus an `Nd` suffix meaning N×24h (for example `7d`). Negative values are rejected.

| Key | Default | Meaning |
|-----|---------|---------|
| `cleanup.success_ttl` | `24h` | Retain succeeded missions this long after they finish. |
| `cleanup.failed_ttl` | `7d` | Retain failed missions this long. |
| `cleanup.staging_ttl` | `1h` | Retain unreferenced staging uploads this long. |
| `cleanup.downloaded_grace` | `1h` | Extra grace before collecting an output that has been downloaded. |
| `cleanup.lost_cleanup_grace` | `10m` | Extra retention added for `lost` missions. |
| `cleanup.sweep_interval` | `5m` | How often the cleanup sweep runs. |

**`limits`** — resource caps. Byte sizes accept a plain integer or a `KiB`/`MiB`/`GiB`
suffix (1024-based). For several fields a value of `0` means *no cap*.

| Key | Default | Meaning |
|-----|---------|---------|
| `max_output_buffer` | `16MiB` | Combined byte budget shared by a mission's stdout, stderr, and combined capture. |
| `max_events_buffer` | `1MiB` | Cap on buffered progress events. |
| `max_event_line_size` | `1MiB` | Maximum size of a single events-file line (`0` = unlimited). |
| `max_return_value_size` | `768KiB` | Maximum size of a mission's `return` payload. |
| `max_fail_message_size` | `64KiB` | Maximum size of a failure message. |
| `max_fail_details_size` | `256KiB` | Maximum size of a failure details object. |
| `max_dispatch_body_size` | `2MiB` | Body cap for `POST /v1/dispatch`. |
| `max_mission_input_size` | `1MiB` | Cap on a mission's canonical input JSON. |
| `max_exec_body_size` | `1MiB` | Body cap for `POST /v1/exec/dispatch`. |
| `max_apply_body_size` | `1MiB` | Body cap for apply / kill / bulk endpoints. |
| `progress_buffer_size` | `256KiB` | Cap on buffered progress event bodies. |
| `max_output_file_size` | `0` (unlimited) | Per-output-file size cap. |
| `max_staging_upload_size` | `0` (unlimited) | Per-staging-upload size cap. |
| `max_incomplete_staging_bytes` | `0` (unlimited) | Total bytes of in-progress uploads before new ones are refused. |
| `max_data_dir_size` | `0` (unlimited) | Soft cap on total data-directory size; once exceeded, new work is refused with `503 disk_quota_exceeded`. |
| `max_progress_rate` | `50` | Progress events per second per mission (`0` = no cap). |
| `max_output_files_per_mission` | `32` | Maximum output files a mission may register (`0` = no cap). |
| `upload_idle_timeout` | `30s` | Idle upload streams past this are aborted so the partial can be reaped. |
| `max_incomplete_staging_uploads` | `128` | Number of concurrent in-progress uploads before new ones are refused (`0` = unlimited). |
| `max_queue_per_lane` | `0` | Maximum queued jobs per lane (`0` = unlimited). |
| `max_queue_total` | `0` | Maximum queued jobs across all lanes (`0` = unlimited). |
| `default_kill_grace` | `5s` | Grace between SIGTERM and SIGKILL when killing a job. |
| `reader_post_exit_grace` | `5s` | How long output readers keep draining after a process exits. |
| `cache_size` | `-16000` | SQLite `cache_size` pragma (negative means KiB). |

As a consistency guard, when `max_event_line_size > 0` the daemon refuses to start unless
the per-field caps leave room for the terminal `done` event envelope (otherwise a
successful mission could produce a `done` event too large to write, and the mission would
never finalize).

**`mission_env`** — environment passed to mission processes.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `mission_env.inherit` | []string | `[]` | Names of host environment variables to pass through to missions. |
| `mission_env.set` | map[string]string | `{}` | Literal environment variables set on missions (treated as a secret). |

No inherited name and no `set` key may begin with `LETTS_`, which is reserved for the
variables the daemon injects (`LETTS_MISSION_ID`, `LETTS_KIND`, `LETTS_LANE`,
`LETTS_WORKDIR`, `LETTS_TMPDIR`, `LETTS_IN_<role>`, and so on).

**`exec`** — the opt-in remote-execution feature.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `exec.enabled` | bool | `false` | Master switch. When off, `POST /v1/exec/dispatch` returns `404 feature_disabled` before authentication, so the surface is indistinguishable from a build without exec. |
| `exec.tokens` | []string | `[]` | Exec-scope bearer tokens (a secret). |
| `exec.allow_shell` | bool | `false` | Permit shell-form argv. |
| `exec.max_script_size` | bytes | `256KiB` | Maximum uploaded-script size. |
| `exec.max_inputs_per_exec` | int | `32` | Maximum staged inputs per exec (`0` = no cap). |
| `exec.max_outputs_per_exec` | int | `32` | Maximum declared outputs per exec (`0` = no cap). |
| `exec.exec_success_ttl` | duration | `1h` | Retain succeeded execs this long. |
| `exec.exec_failed_ttl` | duration | `24h` | Retain failed execs this long. |

When `exec.enabled` is true, the daemon refuses to start unless at least one of
`exec.tokens` or `admin.tokens` is non-empty — otherwise every exec request would simply
return `401`.

#### File permissions, discovery, and flags

- **Secret-file permissions.** If the configuration contains any secret — `auth.tokens`,
  `admin.tokens`, `exec.tokens`, or `mission_env.set` — the file must be mode `0600` or
  `0400` and owned by the daemon's user, or startup fails. Use
  `--insecure-config-permissions` to bypass this in development.
- **Discovery order.** The config path is resolved from the `--config` flag, then the
  `DUGDALE_CONFIG` environment variable, then `./dugdale.yaml`, then
  `/etc/letts/dugdale/default.yaml`. Additional named instances are pointed at their own
  file explicitly by the `dugdale@<name>.service` systemd unit.
- **Identifier rules.** Dugdale ids and route names match `^[a-z][a-z0-9_-]{0,63}$`; lane
  and label names match `^[a-z][a-z0-9_-]{0,31}$`; mission names match
  `^[A-Za-z0-9_][A-Za-z0-9_.-]*$` (up to 128 characters).

### Client configuration — `letts.yaml`

The client configuration describes the *fleet*: which dugdale servers exist, how to
reach each one, which tokens to present, and what lanes and runtime each should run.
Unlike the daemon config, `${VAR}` references in `letts.yaml` **are** expanded — lazily
and per scope, so a token is only resolved from the environment when a command actually
needs it.

A representative `letts.yaml`:

```yaml
auth:                                  # global token fallbacks (per-dugdale values win)
  token:       ${LETTS_DISPATCH_TOKEN} # dispatch scope
  admin_token: ${LETTS_ADMIN_TOKEN}    # admin scope
  exec_token:  ${LETTS_EXEC_TOKEN}     # exec scope

defaults:
  port: 7180

# reusable field clusters referenced via `extends`
templates:
  worker:
    mission_dir: /srv/missions
    runtime:
      command_template:      ["sh", "{mission_path}"]
      mission_path_template: "{mission}.sh"
      validate_mission_file: true
    lanes:
      default: {concurrency: 4}
      batch:   {concurrency: 1}

dugdales:
  - id: web1
    host: 10.0.0.11
    extends: worker
  - id: web2
    host: 10.0.0.12
    extends: worker
    lanes:
      batch: null # suppress the inherited `batch` lane on this host

aliases:
  primary: web1 # `--host=primary` resolves to web1

routes:
  nightly: {host: web1, lane: batch} # `--route=nightly` -> web1 with lane batch

selector:
  match: [prod] # default label filter for lane-only auto-select
```

#### Sections

- **`auth`** — global fallback tokens for the three scopes (`token`, `admin_token`,
  `exec_token`). A per-dugdale token, when present, takes precedence over the global one.
- **`defaults.port`** — the port used for any dugdale that gives a host but no port.
  Falls back to `7180` when unset.
- **`dugdales`** — the list of servers. Each entry has an `id` and reaches the daemon via
  either an explicit `url:` or a `host:` (with an optional `port:`, or a port embedded as
  `host:7180`). A `url:` always wins over host and port. Other fields: `extends` (a template
  name), `mission_dir`, `runtime` (`command_template`, `mission_path_template`,
  `validate_mission_file`), `labels` (used by selectors), per-dugdale `token` /
  `admin_token` / `exec_token`, and `lanes` (a map of lane name to
  `{concurrency, paused}`). Every dugdale must resolve to a `host` or a `url` after
  templates are applied.
- **`templates`** — named clusters of fields a dugdale can inherit via `extends`. Scalar
  fields fill in only where the dugdale left them empty; `labels` and `command_template`
  are replaced wholesale when the dugdale sets them; `lanes` are deep-merged, and a
  dugdale can suppress an inherited lane by setting it to `null`
  (`lanes: { <name>: null }`).
- **`aliases`** — a map from alias name to a dugdale id (or to another alias, forming a
  chain up to eight links deep). `--host=<alias>` resolves through the chain to a real
  dugdale id. Cycles and dead ends are rejected at load time.
- **`routes`** — named `{host, lane}` pairs. `--route=<name>` selects both a host and a
  lane at once.
- **`selector.match`** — a default set of labels used to auto-select a host when a
  command is given a lane but no explicit host.

#### Discovery, token resolution, and permissions

- **Discovery order.** The client config is found from `--config`, then `$LETTS_CONFIG`,
  then `./letts.yaml`, then `$XDG_CONFIG_HOME/letts/letts.yaml`, then
  `~/.letts/letts.yaml`, then `/etc/letts/letts.yaml`. The first existing file wins; if
  `--config` or `$LETTS_CONFIG` names a file that does not exist, that is a hard error.
- **Token resolution.** For a given dugdale and scope, the client prefers the
  dugdale-local token, falls back to the matching global `auth.*` token, and then expands
  any `${VAR}` reference. An empty result after all fallbacks is an error.
- **Environment substitution.** `${NAME}` is replaced from the environment. A value that
  is *purely* `${VAR}` is treated as a secret kept out of the file; any literal bytes
  (including a mixed `prefix-${VAR}`) make it a plain-text token.
- **File permissions.** If the config contains any plain-text token, the file must be
  mode `0600` or `0400`, or the client refuses to use it. Configs that keep all tokens in
  `${VAR}` form are exempt. `--insecure-config-permissions` bypasses the check, and
  `letts apply` additionally prints a warning for each plain-text token it finds.

---

## The dugdale daemon

### Command-line flags

```
dugdale [flags]

  --config <path>                  path to dugdale.yaml (else DUGDALE_CONFIG, then defaults)
  --check-config                   validate config (and referenced paths) and exit
  --migrate-only                   apply database migrations and exit
  --insecure-config-permissions    skip the 0600/0400 secret-file check
  --log-level <level>              override log.level (debug|info|warn|error)
  --log-format <format>            override log.format (json|text)
  --version                        print version and exit
```

`--check-config` parses the file, applies defaults, runs every validation, checks file
permissions, and probes the referenced paths (the data directory and the configured log
output) for real writability, then prints `ok`. Invalid log enums in the *file* are caught
here too; values passed via the `--log-level`/`--log-format` flag overrides are validated
only at real startup, not by `--check-config`.

The daemon's exit codes are `0` (clean), `1` (runtime failure such as a database or
listen error), `2` (flag parse error), and `3` (configuration error: discovery, parse,
permissions, paths, or log init).

### Startup sequence

On start the daemon discovers and loads its config, checks permissions and paths,
initializes logging, creates the data directory, and acquires the `dugdale.lock` advisory
lock so no second daemon can use the same data directory. It then opens and migrates the
SQLite database (or exits after migrating, with `--migrate-only`).

Critically, the **startup repair sweeps run synchronously before the HTTP listener
opens**, in order:

1. **Finalize-intent recovery** replays any durable finalize intent left behind by a
   crash, finishing partially-committed outputs or reverting them safely.
2. **Running-to-lost sweep** finalizes any mission still marked `running` from a previous
   life as `lost`, after best-effort killing its (possibly still-alive) process group.
3. **Orphan sweep** removes on-disk artifacts whose database row is gone and recomputes
   staging retention where needed.

Only then does the daemon replay applied configuration into live lane runners, start its
background workers, install the shutdown coordinator, register routes, and begin
listening.

### Background workers

While running, the daemon operates several background goroutines: a mission cleaner
(retention-based deletion), a staging garbage collector, a once-daily disk scanner that
catches leaked files, an hourly database vacuum/checkpoint, a metrics poller, and a
disk-usage monitor that feeds the `max_data_dir_size` soft cap.

### Single-instance safety

The `flock` on `dugdale.lock` is the primary guard against two daemons corrupting one
data directory. Because `flock` is tied to an inode, the daemon also periodically
verifies the lock file's integrity (frequently during the first minute of life, when a
manual `rm` followed by a restart is most likely) and shuts itself down if the lock is
lost.

### Graceful shutdown

Shutdown is a two-stage drain driven by signals:

- The **first `SIGTERM`** moves the daemon to *draining*: all lanes pause so no new job
  starts, `POST /v1/dispatch`, `POST /v1/exec/dispatch`, and staging uploads return
  `503 draining`, and `/v1/readyz` flips to `503 awaiting_drain` so a load balancer
  routes traffic away. The daemon then waits for running jobs to finish, logging a status
  table as it goes.
- A **second `SIGTERM`/`SIGINT`** escalates to *aggressive*: every running job is signaled
  to shut down (SIGTERM, grace, then SIGKILL on its process group), re-signaling
  periodically until the set is empty.

A transient database error during the drain is deliberately *not* treated as "drain
complete," so jobs still running are recorded as `killed/dugdale_shutdown` rather than
silently becoming `lost`.

---

## The letts CLI

### Global flags and output

Every subcommand inherits these persistent flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `--config <path>` | auto-discovery | Path to `letts.yaml`. |
| `-o, --output <format>` | `text` | Output format: `text`, `json`, `yaml`, or `ndjson`. |
| `--insecure-config-permissions` | `false` | Skip the `letts.yaml` `0600`/`0400` check (development only). |
| `-v, --verbose` | `false` | Print debug diagnostics to stderr: the resolved config path, and the base URL and scope each request targets. |
| `-q, --quiet` | `false` | Suppress informational stderr output (`run`'s event/log tailing, `apply`'s plain-token warning). Results on stdout are unaffected. |

Not every command honors every output format — for instance, `events` always emits
NDJSON, and several `ctl` text renderers fall back to JSON or YAML for nested records.
The global `--quiet`/`-q` is what suppresses `run`'s live event and log tailing; `run`
also has its own `--no-progress` (hide progress events) and `--logs` (show captured
stderr even under `--output=json`).

### Exit codes

The CLI maps typed errors to a stable set of exit codes:

| Code | Meaning |
|------|---------|
| `0` | Success. |
| `1` | Generic failure (including a mission whose outcome is `failed`). |
| `2` | Bad usage (invalid flags, mutually-exclusive options, malformed input). |
| `3` | Configuration error. |
| `4` | Network error. |
| `5` | Authentication/authorization error (HTTP 401/403). |
| `124` | Client wait timeout (`--wait-timeout` elapsed). |
| `125` | Abnormal mission outcome (`killed`, `timeout`, `oom`, `crashed`, `lost`, or no terminal event). |
| `255` | Exec transport error before any terminal outcome. |

For `exec`, a successful remote command's own exit code is propagated verbatim (so a
child exiting `7` makes `letts exec` exit `7`), except that the reserved codes `124`,
`125`, and `255` collapse to `125` to keep their meaning unambiguous.

### Targeting a host and lane

Commands that act on a server resolve their target with a clear precedence:

1. **`--route <name>`** selects a host and lane together. It is mutually exclusive with
   `--host` and `--lane`.
2. **`--host <id>` plus `--lane <name>`** names a host explicitly. `--host` resolves
   through alias chains to a real dugdale id, and `--lane` is required when `--host` is
   given.
3. **`--lane <name>` alone** auto-selects a host: every dugdale exposing that lane and
   matching the `--match` labels (or `selector.match` when `--match` is absent) is a
   candidate, and one is chosen at random.

`--host` accepts a comma-separated list (`s1,s2,s7`) for parallel fan-out across several
servers; duplicates are silently removed. For the read and lifecycle subcommands under
`ctl missions` and `ctl exec`, omitting `--host` entirely triggers **by-id fan-out**: the
operation runs concurrently against every configured dugdale, succeeding if exactly one
host owns the id, reporting "not found" if none do, and refusing with a conflict if more
than one does. The exception is `ctl exec restart`, which always requires an explicit
`--host`.

The `--selector` syntax used by bulk `restart`/`delete` is a comma-separated list of
`key=value` pairs. Supported keys are `status`, `outcome`, `lane`, `mission`,
`mission_prefix`, `since`, and `until`. A `since`/`until` value may be an absolute Unix
millisecond timestamp or a relative duration such as `-1h`, `-30m`, or `-7d`.

### Command reference

#### `letts apply`

Reconciles each selected dugdale's lanes and runtime against the desired state in one or
more `letts.yaml` files. By default it preserves lanes that are not mentioned; `--prune`
removes them (failing if they hold queued or running jobs) and `--force-prune` removes
them and terminates their jobs. `--dry-run` prints a diff instead of applying.

| Flag | Default | Meaning |
|------|---------|---------|
| `-f, --file` | (required) | A `letts.yaml` to apply; repeatable. |
| `--host` | all | Comma-separated dugdale ids to target. |
| `--match` | — | Label filter (AND). |
| `--dry-run` | `false` | Show the diff without applying. |
| `--force` | `false` | Allow destructive runtime changes. |
| `--prune` | `false` | Remove lanes absent from the applied config. |
| `--force-prune` | `false` | With prune: also terminate jobs in removed lanes. |

#### `letts dispatch`

Fire-and-forget submission of a mission. Resolves the target, loads and validates the
input JSON, uploads any `--file role=path` inputs to staging, and posts the dispatch.
Prints `mission_id<TAB>status`. It does not follow the mission.

| Flag | Meaning |
|------|---------|
| `--route` / `--host` / `--lane` / `--match` | Target selection (see above). |
| `--mission` | Mission name (required). |
| `--input` / `--input-file` | Input JSON literal, or a file (`-` for stdin); mutually exclusive; default `{}`. |
| `--file role=path` | Upload an input file to staging; repeatable. |
| `--timeout` | Server-side mission timeout, e.g. `5m`. |
| `--mission-id` | Override the generated UUIDv7 id. |

#### `letts run`

A superset of `dispatch` that follows the mission to its terminal outcome: it streams the
events, optionally tails standard output and standard error, downloads declared outputs,
and maps the outcome to its exit code. Accepts a comma-separated `--host` list for
parallel fan-out (the aggregate exit code reflects the worst outcome).

It adds, on top of the `dispatch` flags: `--output-file role=path` (download an output on
success, repeatable), `--wait-timeout` (client-side deadline; `0` means wait forever),
`--no-progress`, and `--logs` (show captured stderr even in `--output=json`); the global
`--quiet` suppresses all tailing. The wait deadline defaults sensibly from `--timeout`
plus a grace when not given explicitly. With multiple hosts, `--output-file` and
`--mission-id` are rejected.

#### `letts exec`

Runs an ad-hoc command on one or more dugdales (requires `exec.enabled` on the daemon and
an exec-scope token). The command argv follows `--`. `--lane` is required, and exactly one
of `--host`, `--match`, or `--all` must be given — exec deliberately does not auto-select
from `selector.match`.

| Flag | Meaning |
|------|---------|
| `--host` / `--match` / `--all` | Target one host (or a comma list), a label set, or every host with the lane. |
| `--lane` | Lane name (required). |
| `--script` | A local script uploaded once per dugdale via content-addressed staging. |
| `--in role=path` | Upload an input; materialized at `$LETTS_IN_<role>`; repeatable. |
| `--out role=path` | Download an output after success; refuses to overwrite; repeatable. |
| `--stdin` | `none`, `single`, or `broadcast` (default inferred from a TTY and host count). |
| `--timeout` / `--wait-timeout` | Server-side and client-side deadlines. |
| `--detach` | Print the `exec_id` (or `group_id`) and exit. |
| `--output` | `raw`, `prefix`, `json`, or `ndjson` (default `raw` for one host, `prefix` for several). |
| `--output-buffer` | Per-host buffer cap for `json` mode (bytes; default 65536). |
| `--allow-shell` | Permit shell-form argv (the daemon still gates on `exec.allow_shell`). |
| `--mission-id` / `--group-id` | Override the `exec_id` (single host) or `group_id` (multi-host). |

#### `letts events <mission_id>`

A low-level helper that streams a mission's event file as NDJSON, one event per line,
always as NDJSON regardless of `--output`. `--host` is required. Flags: `--follow`
(tail live events) and `--from <seq>` (resume from a sequence cursor).

#### `letts logs <mission_id>`

An alias for `ctl missions output`: streams a job's `stdout`, `stderr`, or `combined`
capture. Without `--host` it uses by-id fan-out. Flags: `--host`, `--match`,
`--stream` (`stdout`|`stderr`|`combined`, default `combined`), and `--follow`.

#### `letts ctl …`

The `ctl` group holds control and inspection subcommands:

- **`ctl dugdales list`** — list dugdales from the local `letts.yaml` (no network).
  **`ctl dugdales info --host <id>`** — fetch a daemon's runtime info (version, uptime,
  queue summary). **`ctl dugdales config --host <id>`** — fetch the daemon's applied
  state.
- **`ctl lanes list --host <id>`** — list lanes with concurrency and queue counts.
  **`ctl lanes pause --host <id> --lane <name>`** and **`ctl lanes continue …`** — pause
  or resume a lane.
- **`ctl missions list --host <id>`** — list missions with rich filters (`--status`,
  `--outcome`, `--lane`, `--mission`, `--kind`, `--since`, `--until`, `--limit`,
  `--cursor`). **`ctl missions show <id>`**, **`output <id>`**, **`restart [<id>]`**,
  **`kill <id>`**, and **`delete [<id>]`** operate on a single mission (with by-id fan-out
  when `--host` is omitted) or, for `restart`/`delete`, on a bulk `--selector` set with an
  interactive confirmation that `--yes` skips.
- **`ctl staging upload <path>`**, **`download <id>`**, **`delete <id>`**, and **`list`** —
  manage staging artifacts. Staging ids are not portable across hosts, so these are always
  per-host (`--host` required).
- **`ctl exec list|show|output|restart|kill|delete`** — the same read surface as
  `ctl missions`, pinned to `kind=exec` and adding a `--group` filter on `list`. Differences:
  `restart` requires an explicit `--host` (no by-id fan-out for re-executing remote
  commands), there is no bulk `--selector` mode for exec, and `kill` adds a
  `--signal TERM|KILL` flag.

#### `letts version`

Prints `letts <version>` from the build-time version metadata.

---

## Writing a mission

A mission is just an executable script that the daemon runs. The contract between the
daemon and the script is small and language-agnostic.

**Environment.** The daemon injects a set of `LETTS_*` variables: `LETTS_MISSION_ID`,
`LETTS_KIND`, `LETTS_LANE`, `LETTS_WORKDIR` (a per-mission scratch directory),
`LETTS_TMPDIR`, and `LETTS_IN_<role>` for each input file. Any `mission_env.inherit` /
`mission_env.set` values from the daemon config are added as well.

**Input.** The mission's JSON input is delivered on standard input.

**Structured events on file descriptor 3.** The script reports progress and its terminal
result by writing newline-delimited JSON objects to file descriptor 3. The recognized
event types are:

| Event | Fields | Meaning |
|-------|--------|---------|
| `progress` | `value` (0–1), `message` | A progress update; rate-limited and forwarded to the live events stream. |
| `output_file` | `key` | Declares that the script produced `"$LETTS_WORKDIR"/out/<key>`, to be collected and committed to staging on success. |
| `success` | `return` (object or null) | The terminal success event; `return` becomes the mission's result payload. |
| `fail` | `message`, `reason`, `details`, `exit_code` | The terminal failure event. |

**Output and exit code.** Whatever the script writes to standard output and standard error
is captured (subject to the shared byte budget). The exit code matters: a `success` event
followed by a non-zero exit, or a missing terminal event, is reconciled into the final
outcome.

A complete example — print to both streams, produce an output file, and succeed with a
return value:

```sh
#!/bin/sh
# Read input JSON from stdin, write a result file, register it, and succeed.
echo "starting mission $LETTS_MISSION_ID"          # captured as stdout
echo "a warning" >&2                               # captured as stderr

mkdir -p "$LETTS_WORKDIR/out"
date > "$LETTS_WORKDIR/out/result"

printf '{"event":"progress","value":0.5,"message":"halfway"}\n' >&3
printf '{"event":"output_file","key":"result"}\n' >&3
printf '{"event":"success","return":{"ok":true}}\n' >&3
exit 0
```

To register that script as the mission `report`, the client `letts.yaml` would set a
`runtime` whose `command_template` is `["sh", "{mission_path}"]` and whose
`mission_path_template` is `"{mission}.sh"`, with `mission_dir` pointing at the directory
that contains `report.sh`. The mission then runs as `letts run --mission report …`.

---

## Remote exec

Exec is the ad-hoc sibling of a mission. Instead of running a pre-registered script, the
caller supplies the command at dispatch time. Because that is remote code execution, the
feature is off by default and must be enabled with `exec.enabled: true` plus an
exec-scope token (or an admin token).

An exec runs through a completely separate runtime from a mission: there is no fd3
protocol and no command template. The daemon builds a clean environment (it does not
inherit the daemon's own environment — only the `LETTS_*` variables and a fixed `PATH`),
materializes any uploaded script and inputs read-only into a per-exec working directory,
runs the argv, captures standard output and standard error, and derives the outcome from
the exit code, any signal, and the timeout.

Inputs are uploaded to staging and exposed as `LETTS_IN_<role>` (and `LETTS_SCRIPT` for an
uploaded script); declared outputs are collected only on success and committed to staging
through the same durable two-phase finalize used by missions. Related execs can share a
`group_id` (surfaced to the command as `LETTS_GROUP_ID` and filterable with
`ctl exec list --group`), and an optional `display_name` provides operator-facing labeling.
Neither the group id nor the display name participates in the idempotency fingerprint.

```sh
# Run a one-off command on every prod host with the `ops` lane:
letts exec --match prod --lane ops -- df -h /var/lib/letts

# Upload a script and an input, capture an output file, on one host.
# The staged script always materializes at script/script under the work
# directory (which is also the process cwd), so reference it by that path:
letts exec --host web1 --lane ops \
  --script ./backup.sh --in db=./dump.sql --out archive=backup.tar.gz \
  -- sh script/script
```

Note that the daemon executes the argv verbatim — there is no shell and no environment
expansion in the command itself, so `-- sh "$LETTS_SCRIPT"` would not work (your local
shell would expand the variable to an empty string before letts ever saw it, and an
escaped `$LETTS_SCRIPT` would be passed to `sh` as a literal filename). `LETTS_SCRIPT`
and the other `LETTS_*` variables are available *inside* the spawned process — e.g. in
the script body or within an `sh -c '...'` string.

---

## How it works: core mechanics and durability

This section describes the guarantees that make letts safe to run unattended.

### Identifiers

Every mission, exec, staging file, and exec group is identified by an RFC 9562 **UUIDv7** —
a time-ordered identifier whose 48-bit millisecond prefix makes ids naturally sortable by
creation time. The client supplies the id as the `Idempotency-Key` header on dispatch, and
that value *becomes* the job's primary key, so the key and the id are one and the same.

### Missions, statuses, and outcomes

A job row carries a **kind** (`mission` or `exec`) and progresses through a small set of
persisted **statuses**: `queued` → `running` → `done`, plus `deleting` as a cleanup
tombstone. The "did it succeed" question is answered by a separate **outcome**, set only
when the status reaches `done`: `success`, `failed`, `killed`, `timeout`, `crashed`,
`lost`, or `oom`. Note that `lost` is an outcome, never a status.

When an outcome is not a plain success or failure, a free-text **fail reason** records why:
for example `killed_by_api`, `lane_removed`, `dugdale_shutdown`, `force_delete`,
`php_memory_limit` (out of memory), `unknown_sigkill`, the various signal crashes
(`segfault`, `sigbus`, and so on), and protocol violations such as `success_then_failed_exit`,
`fail_then_zero_exit`, or `no_event_nonzero_exit`. The `lost` and `timeout` outcomes
deliberately leave the fail reason empty.

The SQLite database is opened in WAL mode with `synchronous=NORMAL`, foreign keys on, and
incremental auto-vacuum. All writes go through a single serialized writer (a pinned-connection
immediate transaction), while reads share a connection pool — a simple model that keeps the
durability reasoning tractable.

### Lanes

Each lane has one runner goroutine that loops: pick the oldest queued row, atomically flip
it to `running` (with placeholder pid values, so two runners can never claim the same row),
spawn the process, and then fill in the real process id. Concurrency is governed by an
in-flight counter compared against the lane's current limit. Resizing a lane is immediate
for new pickups but never preempts a running job; shrinking simply blocks pickups until the
in-flight count drains.

Pausing and resuming a lane stops and starts new pickups while letting running jobs finish.
A lane's paused state carries a **provenance** — `yaml` when it came from applied
configuration, `ctl` when an operator paused it at runtime. The distinction matters during
reconciliation: a runtime (`ctl`) pause survives a later `apply` that sets `paused: false`,
so an operator's manual pause is never silently undone, whereas a configuration (`yaml`)
pause clears when the configuration unpauses it. Provenance is always derived on the server;
a value sent in a request body is stripped and recomputed.

`apply` is fully declarative. It reads the current applied state, computes a diff, starts,
stops, and resizes lane runners to match, and persists the new desired state. Removing a
lane that still holds jobs requires `--force-prune`, which marks the lane "removing," waits
for its runner to acknowledge before terminating anything (so a concurrent dispatch cannot
slip a fresh job into a lane that is going away), kills queued rows through the durable
queued-kill path, and signals running rows to stop.

### Staging

Staging is the daemon's file store for inputs, scripts, and committed outputs. Each file is
identified by a UUIDv7 and described by its content hash (sha256) and size. Uploads via
`PUT /v1/staging/{id}` are **resumable**: the client declares the full sha256 and total size,
and on a resumed upload the server verifies the on-disk prefix, re-hashes it, and appends so
the final hash always covers the complete content. The server streams and hashes
incrementally, fsyncs the file and its parent directory on completion, and rejects a hash
mismatch by deleting the partial. A per-id lock serializes concurrent uploads and aborts
idle ones.

A staging file moves through the states `uploading` → `complete`, with the additional
`pending_output` → `committing` states used while a mission commits its outputs, and
`deleting` as a garbage-collection tombstone. A `mission_staging_refs` row links a job to a
file with a ref kind of `input`, `output`, or `script`. Clients can avoid re-uploading a
blob they have already stored by looking it up with `GET /v1/staging/by-content/{sha}?size=`.

Retention is computed by a TTL formula: a file referenced by any live (`queued`, `running`,
or `deleting`) job is pinned alive; otherwise its expiry is the latest of its referencing
jobs' finish-times plus the relevant TTL; an unreferenced file expires `staging_ttl` after
creation. Garbage collection runs in three passes — expire, tombstone (rename into
`tombstone/`, which keeps any in-flight download working thanks to the open-file-descriptor
guarantee), and unlink after a short grace.

### Events, output, and the fd3 protocol

Each mission has an append-only **events file**, written as NDJSON, where every line carries
a monotonically increasing sequence number and an event kind (`queued`, `running`, `progress`,
`done`). The events file is created on disk *before* the database row, so a crash always
leaves a recoverable artifact. The terminal `done` event is durable (fsynced) and idempotent:
re-appending the same `done` is a no-op, while a *conflicting* `done` is refused — because the
public stream may already have been consumed with the old outcome, the daemon would rather
flag a critical error than rewrite history.

A mission process speaks to the daemon over **file descriptor 3**, writing NDJSON events
(`progress`, `output_file`, `success`, `fail`). Standard output and standard error are
captured into separate files plus an interleaved `combined` NDJSON file, all sharing one byte
budget; on overflow the daemon writes a single truncation marker and flags the row. Standard
error is additionally watched for the PHP out-of-memory marker, which maps the outcome to `oom`.

### Idempotency and fingerprints

Both dispatch endpoints require an `Idempotency-Key` (a valid UUIDv7) that becomes the job
id. The daemon computes an **input fingerprint** — `sha256` over an RFC 8785 (JSON
Canonicalization Scheme) serialization of the request's salient fields — and compares it
against any existing row with the same id:

- same fingerprint, row not being deleted → **200**, the current state is returned (an
  idempotent replay);
- same fingerprint, row being deleted → **410**;
- different fingerprint → **409 idempotency_conflict** (the key was reused for a different
  request).

The canonicalization sorts object keys, formats numbers per ECMA-262, preserves exact 64-bit
integers, and rejects duplicate JSON keys, so two genuinely different request bodies cannot
collide to the same fingerprint. For execs the fingerprint binds to input *content*
(sha256 and size), not just staging ids, and excludes the mutable group id and display name.

### Durability and crash recovery

Finalization is a two-phase commit. First the daemon writes a durable *intent* row together
with the precomputed terminal `done` event and any pending output rows, all in one write
transaction. Then, for a mission with outputs, it renames each temporary output file into its
final staging location and fsyncs the parent directory, appends the public `done` event, and
in a single final transaction marks the mission `done`, completes the staging rows, inserts
the output refs, and deletes the intent. A failure to rename an output is recorded durably as
`output_commit_failed` rather than lost.

Because the intent and the public `done` event are durable, the startup repair sweeps can
replay any interrupted finalize exactly: a prepared intent with no outputs is committed, one
with outputs present continues the rename-and-commit, one whose temporary files are gone is
reverted, and a half-renamed set is caught up (re-verifying the sha256 of anything already
renamed). A mission still marked `running` from a previous life is killed (guarding against
process-id reuse with the stored start time) and finalized as `lost`.

If a finalize or repair ever finds a terminal `done` event that disagrees with its intent — or
an intent whose output metadata is corrupt — the daemon sets a sticky, process-scoped
**"manual repair required"** flag. While set, `/v1/readyz` returns `503 awaiting_manual_repair`
until an operator resolves the offending intent and restarts; the daemon refuses to overwrite
the row, because doing so would lie about what the public stream already showed.

### Cleanup and retention

Old jobs and staging files are reclaimed by retention policy. The mission cleaner promotes
`done` rows past their TTL to `deleting` (never racing an in-flight finalize), removes their
on-disk artifacts, and deletes the rows in a transaction that cascades to their refs, runtime,
and intents. `lost` rows get an extra grace on top of the failed TTL. A once-daily disk scan
catches any file leaked by an interrupted delete. The disk-usage monitor enforces the
`max_data_dir_size` soft cap by refusing new dispatches, execs, and uploads with
`503 disk_quota_exceeded` once the directory grows too large. An hourly vacuum returns freed
pages to the filesystem and truncates the write-ahead log.

---

## HTTP API reference

The daemon serves a small JSON/HTTP API on its `listen` address (TCP only). Authentication
is by `Authorization: Bearer <token>`; there are no cookies and no CSRF tokens. Every
non-2xx response uses the envelope `{"error":"<machine_code>","message":"<human>","details":<object>}`,
where the machine-readable code is in the `error` field.

### Authentication and scopes

Three scopes map from the three token lists: `auth.tokens` → **dispatch**, `exec.tokens` →
**exec**, `admin.tokens` → **admin**. Admin is a superset and satisfies any required scope.
Tokens are compared in constant time. Beyond the scope check, **kind isolation** applies: a
dispatch token may touch only `kind=mission` rows and an exec token only `kind=exec` rows; a
mismatch returns `403 forbidden_kind` (existence checks run first, so a record's existence
never leaks across scopes). A missing or unknown token yields `401 unauthorized`; a valid
token with the wrong scope yields `403 forbidden`.

Admin- and exec-scoped endpoints are protected by a per-client-IP brute-force backoff: after
five failures, requests are delayed with exponential backoff (capped at 30 seconds) and
answered `429 rate_limited` with a `Retry-After`. Pure dispatch endpoints are intentionally
exempt. The client IP is taken from `X-Forwarded-For` only when
`network.use_x_forwarded_for` is enabled **and** the request arrives from an address listed
in `network.trusted_proxies`; otherwise the direct peer address is used.

### Routes

**No authentication:**

| Method · Path | Description |
|---|---|
| `GET /v1/healthz` | Liveness. Always `200 {"status":"ok"}`; never touches the database. |
| `GET /v1/readyz` | Readiness. `200` once a config has been applied; `503 awaiting_apply` before that, `503 awaiting_drain` while shutting down, `503 awaiting_manual_repair` if a critical consistency error is set. |
| `GET /v1/version` | Build metadata: `{version, commit, built_at}`. |
| `GET /v1/metrics` | Prometheus exposition (expected to be firewalled). |

**Dispatch / exec:**

| Method · Path | Scope | Description |
|---|---|---|
| `POST /v1/dispatch` | dispatch | Enqueue a mission. Requires an `Idempotency-Key` (UUIDv7) that becomes the `mission_id`. `202 {"mission_id","status":"queued"}` for a new key; replay semantics otherwise. |
| `POST /v1/exec/dispatch` | exec | Run an arbitrary command (only when `exec.enabled`). `202 {"exec_id","status":"queued"}`. Returns `404 feature_disabled` when exec is off. |

**Mission read and streaming** (scope: dispatch *or* exec, kind-isolated; admin sees all):

| Method · Path | Description |
|---|---|
| `GET /v1/missions/{id}` | The full mission resource, including joined input/output staging metadata. |
| `GET /v1/missions/{id}/events` | The NDJSON events stream. `?from=<seq>` resumes from a cursor; `?follow=true` tails until the terminal `done`. |
| `GET /v1/missions/{id}/output` | The captured output. `?stream=` selects `stdout`, `stderr`, or `combined` (required); `?follow=true` tails. |
| `GET /v1/missions` | **admin** — list with cursor pagination and filters (`status`, `outcome`, `lane`, `mission`, `kind`, `group_id`, `order`, `since`, `until`, `limit` ≤ 1000, `cursor`). |

**Mission lifecycle** (scope: admin):

| Method · Path | Description |
|---|---|
| `POST /v1/missions/{id}/restart` | Clone a `done` mission into a new queued one (a new id, carrying the input refs). |
| `POST /v1/missions/{id}/kill` | Kill a queued or running mission. |
| `DELETE /v1/missions/{id}` | Mark the mission `deleting`; `?force=true` kills a running one first. |
| `POST /v1/missions/bulk-restart`, `POST /v1/missions/bulk-delete` | Per-id batch operations (up to 1000 ids). |

**Admin configuration and lanes** (scope: admin):

| Method · Path | Description |
|---|---|
| `POST /v1/admin/apply` | Apply desired lane/runtime state (`?prune`, `?force_prune`). |
| `GET /v1/admin/state` | The current applied configuration. |
| `POST /v1/admin/lanes/{name}/pause`, `.../continue` | Pause or resume a lane (provenance `ctl`). |

**Inspect** (scope: dispatch *or* exec, so an exec-only token can still enumerate lanes):

| Method · Path | Description |
|---|---|
| `GET /v1/dugdale` | Daemon summary: version, uptime, queue counts (kind-filtered per scope). |
| `GET /v1/lanes` | Per-lane status: concurrency, paused, queued, running. |

**Staging:**

| Method · Path | Scope | Description |
|---|---|---|
| `PUT /v1/staging/{id}` | dispatch or exec | Resumable upload (`X-Letts-Sha256`, `Content-Length`/`Content-Range`). |
| `HEAD /v1/staging/{id}` | dispatch or exec | Resume probe; returns received/total byte headers. |
| `GET /v1/staging/{id}` | dispatch or exec | Download (Range-capable). |
| `DELETE /v1/staging/{id}` | admin | Delete; `?force=true` cascades dependent jobs to `deleting`. |
| `GET /v1/staging` | admin | List a mission's staging rows (`mission_id` required). |
| `GET /v1/staging/by-content/{sha}` | exec | De-dup lookup by content hash and `size`. |

---

## Observability

The daemon exposes Prometheus metrics at `GET /v1/metrics`, including:

- `letts_dugdale_info{version,commit}` and `letts_dugdale_uptime_seconds`;
- `letts_missions_total{kind,lane,outcome}` and `letts_mission_duration_seconds` (a histogram);
- per-lane gauges `letts_lane_queued`, `letts_lane_running`, `letts_lane_concurrency`,
  `letts_lane_paused`;
- `letts_storage_bytes{kind=db|output|staging}`;
- `letts_http_requests_total{route,method,status}` and
  `letts_http_request_duration_seconds{route,method}`;
- `letts_admin_auth_failures_total` and `letts_fsync_failures_total{transition}`.

To keep label cardinality bounded, the `route` label is the routing *template*
(`/v1/missions/{id}/events`) rather than the raw URL, the `method` label is restricted to a
known set of verbs, and lane labels collapse to `overflow` beyond 100 distinct lanes.

For health checks, point liveness probes at `/v1/healthz` and readiness probes at
`/v1/readyz`. The latter is the right signal for a load balancer: it reports not-ready before
any configuration is applied, while the daemon is draining for shutdown, and when a critical
consistency error needs an operator.

---

## Deployment and security

The Debian package installs both binaries, two systemd units, and an example configuration:

- **`dugdale.service`** runs a single default instance from `/etc/letts/dugdale/default.yaml`
  with `data_dir` `/var/lib/letts/default`.
- **`dugdale@.service`** is a template for additional named instances: `dugdale@foo` reads
  `/etc/letts/dugdale/foo.yaml`. Each named instance **must** use its own `data_dir`
  (`/var/lib/letts/<name>`) — the data-directory flock makes a second daemon on the same
  directory refuse to start. Run either the plain service or `dugdale@default`, not both.

Security recommendations that the daemon's own guards reinforce:

- **Keep the listener private.** Bind a loopback address, or put a TLS-terminating reverse
  proxy in front and set `network.behind_tls_proxy: true`. The daemon refuses to bind a
  public address with tokens configured unless you assert one of those. The metrics endpoint
  has no authentication and should be firewalled.
- **Protect the configuration files.** Any config that contains a plain-text token must be
  `0600`/`0400`; both the daemon and the client enforce this. Prefer keeping client tokens in
  the environment (`${VAR}`) so the file itself holds no secret.
- **Treat exec as remote code execution.** Leave `exec.enabled` off unless you need it, scope
  its tokens narrowly, and remember that `exec.allow_shell` widens the attack surface.
- **Rotate tokens** by editing the token lists and **restarting the daemon** — there is no
  config hot-reload or SIGHUP handling. Multiple tokens per scope still make rotation
  non-disruptive *for clients*: add the new token, restart, migrate the clients, remove the
  old token, restart again. Keep in mind that a restart waits for running jobs to drain
  (see "Stopping under systemd" below).

### First start on a server

The Debian package creates the `letts` system user but deliberately does **not** enable or
start the unit. The expected bootstrap is:

```sh
cp /etc/letts/dugdale/default.yaml.example /etc/letts/dugdale/default.yaml
$EDITOR /etc/letts/dugdale/default.yaml        # set real tokens, e.g. openssl rand -base64 24
chown letts:letts /etc/letts/dugdale/default.yaml && chmod 0600 /etc/letts/dugdale/default.yaml
systemctl enable --now dugdale
curl -s 127.0.0.1:7180/v1/healthz
```

A config containing secrets must be `0600`/`0400` *and owned by the daemon's own user*, or
the daemon refuses to start. A freshly started daemon answers `503 awaiting_apply` on
`/v1/readyz` and rejects dispatches with `412 no_lanes_configured` until the first
`letts apply` pushes lanes and a runtime to it.

### Backups

All state lives under `data_dir`: `state.db` (plus its `-wal`/`-shm` sidecars) and the
`output/`, `staging/`, and `work/` trees. For a consistent backup, stop the daemon (the
`dugdale.lock` flock guarantees there is no writer) and copy the whole directory; restoring
is copying it back and starting the daemon — startup repair replays any interrupted
finalization. A hot copy of `state.db` alone is **not** a valid backup: rows reference
files in `output/` and `staging/`.

### Stopping under systemd

The shipped units use `KillMode=process`, `SendSIGKILL=no`, and `TimeoutStopSec=infinity`,
so `systemctl stop dugdale` delivers exactly one SIGTERM and then waits — possibly forever —
for running jobs to finish (the daemon prints a drain table to its log every 10 seconds).
To escalate to the aggressive phase while systemd is waiting, send the second signal
yourself:

```sh
systemctl kill --signal=SIGTERM --kill-whom=main dugdale
```

Running jobs are then TERM'd, given the kill grace, KILL'd if needed, and recorded as
`killed` / `dugdale_shutdown`.

### Clients and UI

letts itself ships no web UI. The companion project **arby** (separate repository and
binary) provides a dashboard — missions, lanes, live tails, exec history — by aggregating
the same HTTP API across a fleet of dugdales. Official client libraries: Go
(`pkg/lettsclient` in this repository) and PHP (the separate `letts-php` package, used by
the dogfood harness in `scripts/dogfood`).

---

## Development

### Repository layout

| Path | Contents |
|------|----------|
| `cmd/dugdale` | The daemon binary: startup, route wiring, signal handling. |
| `cmd/letts` | The CLI binary: every cobra command and its rendering. |
| `internal/server` | HTTP handlers, auth/body-limit/logging middleware, and the error envelope. |
| `internal/mission` | Process spawn, the fd3 protocol, output capture, outcome computation, and the two-phase finalize. |
| `internal/lane` | Lane runners and the lane manager. |
| `internal/apply` | The declarative apply/diff engine and the DB replay on startup. |
| `internal/storage` | The SQLite schema, migrations, and typed query layer. |
| `internal/repair`, `internal/criticalerr` | Startup crash recovery and the manual-repair signal. |
| `internal/cleanup` | Retention, staging GC, disk scanning, and vacuuming. |
| `internal/eventfile`, `internal/outputfile` | The append-only events file and the truncating output writers. |
| `internal/fingerprint`, `internal/jcs` | Idempotency fingerprints and RFC 8785 canonicalization. |
| `internal/metrics`, `internal/shutdown` | Prometheus collectors and the graceful-shutdown coordinator. |
| `internal/config` | Daemon (`dugdale.yaml`) parsing and validation. |
| `pkg/lettsclient` | A reusable Go client for the HTTP API. |
| `pkg/lettsconfig` | A reusable Go parser/resolver for the client `letts.yaml`. |
| `packaging` | nfpm spec, systemd units, and the example daemon config. |
| `scripts/dev` | The local smoke-test harness and sample missions. |
| `scripts/dogfood` | An end-to-end test harness exercising the system through a real PHP client. |

The two `pkg/` libraries are deliberately public: `pkg/lettsclient` and `pkg/lettsconfig`
are the supported way for other Go programs to talk to a dugdale and to read the same
client configuration the CLI uses.

### Testing

```sh
make test     # go test -race ./...
make check    # gofmt, go vet, go test -race ./...
```

The test suite includes unit tests, package-level integration tests that wire a complete
in-process daemon against a real `httptest.Server`, end-to-end tests that boot the daemon as
a subprocess, and a crash-consistency coverage map (in `internal/integration`) that pairs
each crash/recovery scenario with the test that exercises it. The `scripts/dogfood` harness
drives a running daemon through a real client to catch integration regressions.

---

## License

MIT. See the package metadata in `packaging/nfpm.yaml`.
