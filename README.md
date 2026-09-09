# valkey-roaring — external validation suite

An independent, end-to-end battle-testing harness for
**[valkey-roaring](https://github.com/fjmaco/valkey-roaring)**, a Valkey
module providing Roaring Bitmap data structures.

It lives in its own repository on purpose. The module repository carries
the tests that belong to the code — unit, property, fuzz, integration and
benchmark — and those answer *"does the code do what its authors
intended?"*. This repository answers a different question: *"does the
running system keep every promise it makes, at real-world scale, against
independent references?"* It contains no module code, shares no helpers
with it, and validates strictly through public surfaces: the wire
protocol, the Docker image, and the published binary format.

That separation is also what makes the answer worth anything. A suite that
imports the implementation's own helpers inherits the implementation's own
misconceptions; this one cannot.

## What it has caught

Run against the module during development, these suites found and drove
the fixes for real bugs the module's own tests had missed, among them:

- `COPY` failed on module keys (missing type `copy` callback)
- `R.CLEARBITS` reply shape diverged from redis-roaring (`OK` + optional
  `COUNT` flag upstream)
- `R.SETRANGE` treated `end` as inclusive; redis-roaring (via CRoaring
  `add_range`) is end-**exclusive**
- variadic `R.BITOP` accepted a single source; upstream requires two

## Quick start

```bash
git clone https://github.com/fjmaco/-valkey-roaring-testing.git valkey-roaring-testing
cd valkey-roaring-testing
bash run_all.sh
```

Clone into an explicit directory name as above: the repository name begins
with a dash, and a directory named `-valkey-roaring-testing` cannot be
`cd`'d into without a path prefix (`cd ./-valkey-roaring-testing`) because
the shell reads the name as options.

That is the whole setup. The runner fetches the module source, builds the
image, starts the server, creates the Python virtualenv, downloads the
corpora, and runs all twelve suites.

Requirements: `docker` with Compose v2, `python3` with `venv`, and `git`.

```bash
bash run_all.sh              # everything (~3 minutes plus first-run downloads)
FULL=1 bash run_all.sh       # adds the large datasets to suite 01
bash run_all.sh 02 09        # only suites 02 and 09
VR_KEEP=1 bash run_all.sh    # leave the server up after a passing run
```

A failing run leaves its server running so it can be inspected; a passing
run tears itself down. Clear a leftover server with
`docker compose down --volumes`.

## Where the module comes from

This repository holds no module code, so `run_all.sh` resolves a checkout
of it and hands that to Compose as the image build context. In precedence
order:

| Source | When it applies |
|---|---|
| `VR_SOURCE=/path/to/valkey-roaring` | set explicitly; built exactly as it is on disk |
| `../valkey-roaring` | a sibling checkout exists — the layout to use when developing both together |
| `.module-src/` | otherwise: cloned from `VR_REPO` at `VR_REF` (default `main`), gitignored |

```bash
VR_SOURCE=~/src/valkey-roaring bash run_all.sh   # test a working tree
VR_REF=v1.1.1 bash run_all.sh                    # test a released tag
VR_REF=my-branch bash run_all.sh                 # test a branch
```

Testing an uncommitted change is the first form: point `VR_SOURCE` at the
working tree and the image is built from it directly.

## Architecture

```
.
├── run_all.sh              orchestrator (resolves source, starts server, runs suites)
├── docker-compose.yml      the server under test
├── requirements.txt        pyroaring — real CRoaring bindings for interop
├── lib/
│   ├── valkey_client.py    stdlib-only binary-safe RESP2/RESP3 client
│   ├── harness.py          assertion counters and reporting
│   ├── compose.py          locating and driving the compose project
│   ├── datasets.py         real-roaring-datasets download/cache/parse
│   └── reference.py        naive pure-Python model of every command
├── datasets/               downloaded corpora (cached, gitignored)
└── suites/                 twelve independent suites, detailed below
```

The client is stdlib-only and deliberately not a Redis client library, so
the suites see replies that client libraries normalize away — error
classes, verbatim types, RESP3 frames.

Three independent references are used, chosen so a shared bug is
implausible:

1. **A naive Python-set model** of every command (`lib/reference.py`)
2. **CRoaring itself** through pyroaring — the C implementation the
   portable format is defined by
3. **The published redis-roaring module** (`aviggiano/redis-roaring`
   Docker image) — the project this module promises drop-in
   compatibility with

## Datasets

All corpora come from
[RoaringBitmap/real-roaring-datasets](https://github.com/RoaringBitmap/real-roaring-datasets),
the standard cross-implementation benchmark corpus. Each was picked to
push a different physical container shape:

| Dataset | Character | Exercises |
|---|---|---|
| `census1881` | clustered, moderate | mixed containers |
| `census-income` | dense runs | run containers (`FULL=1`) |
| `wikileaks-noquotes` | scattered, sparse | array containers |
| `uscensus2000` | tiny, extremely sparse | degenerate cases |
| `weather_sept_85` | largest, mixed | scale (`FULL=1`) |

Downloads happen once into `datasets/` (~5 MB standard, ~50 MB with
`FULL=1`) and are cached there.

## The suites

| # | Suite | Validates | Escape class it targets |
|---|---|---|---|
| 01 | `dataset_semantics` | every read/write command over full real datasets, both widths (64-bit copies shifted above 2³²), vs the Python model | container-boundary and aggregate bugs unreachable by hand-picked values |
| 02 | `interop_croaring` | EXPORT/IMPORT byte-compatibility with CRoaring in both directions, both widths (values > 2⁶³ included), OR-merge semantics, the Lua path | serialization drift from the portable spec — invisible to self-round-trips |
| 03 | `persistence` | RDB across hard restart, DUMP/RESTORE (+REPLACE), AOF replay and rewrite, all at dataset scale | rdb_load/rdb_save pairing bugs that two-value tests can't reach |
| 04 | `replication` | live replica attached mid-load; byte-identical EXPORT of every key on both sides | verbatim-propagation gaps and ordering effects |
| 05 | `concurrency` | 8 threads of pipelined mixed ops + background BGSAVE/OPTIMIZE churn; per-thread models must match | state corruption under interleaving and fork pressure |
| 06 | `boundaries` | values and ranges straddling every seam: 65536-multiples, u32::MAX, 2³², 2⁶³, u64::MAX; contents stable across OPTIMIZE; STAT structure | off-by-ones at container seams; representation-dependent results |
| 07 | `keyspace` | TYPE, SCAN-by-type, RENAME, COPY, DEL/UNLINK, EXPIRE/PERSIST, MEMORY USAGE, keyspace events | seams between module type callbacks and server machinery |
| 08 | `resp3` | identical command battery over RESP2 and RESP3 (HELLO 3) connections | protocol-version-dependent reply encoding |
| 09 | `differential_upstream` | thousands of seeded-random commands against the real redis-roaring image, reply-for-reply | any semantic drift from the compatibility promise |
| 10 | `cluster` | cluster-enabled server: hash-tag same-slot ops work, `NOT ... last` is not treated as a key, cross-slot rejected | getkeys regressions (upstream's v1.7.3 bug class) |
| 11 | `protocol_torture` | 30k commands of hostile argument soup; server must reply, stay alive, keep a control key bit-identical | parser/dispatch crashes and cross-key corruption |
| 12 | `snapshot_export_workflow` | the producer/consumer raw-blob pattern: full refreshes, bit flips with exact replies, blob determinism across construction histories, decode fidelity via CRoaring, no torn blobs under concurrent refresh+export, byte-stability across restart, 2000-partition fleet | anything that would corrupt, skew, or destabilize a raw-blob handoff pipeline |

Every suite is a self-contained script with a module docstring stating its
contract and targeted escape class — those docstrings are the per-suite
documentation. Each is runnable on its own against an already-running
server:

```bash
.venv/bin/python suites/test_06_boundaries.py
```

Suites 03, 04 and 10 start and remove their own helper containers
(`vt-aof`, `vt-replica`, `vt-cluster`); suite 09 pulls and runs
`aviggiano/redis-roaring:latest` as `vt-upstream`.

## Scope limits, on purpose

- `R64.SETFULL` is never executed — it allocates the full u64 range and is
  documented as memory-exhausting in the module README.
- Suite 09 stays inside upstream's `RANGEINTARRAY` 100M-wide-range cap and
  skips EXPORT/IMPORT, which do not exist upstream.

## Adding a suite

New end-to-end tests belong here rather than in the module repository.
Add `suites/test_NN_name.py`, opening with a docstring that states the
contract it enforces and the escape class it targets, and use `Suite` from
`lib.harness` for assertions so the run summary stays uniform. `run_all.sh`
picks up `suites/test_*.py` automatically and the number is the filter key.

## Related

- **[fjmaco/valkey-roaring](https://github.com/fjmaco/valkey-roaring)** —
  the module under test, and its own unit, property, fuzz, integration and
  benchmark suites
- [RoaringBitmap/real-roaring-datasets](https://github.com/RoaringBitmap/real-roaring-datasets)
  — the corpora
- [aviggiano/redis-roaring](https://github.com/aviggiano/redis-roaring) —
  the upstream module whose semantics suite 09 holds this one to
