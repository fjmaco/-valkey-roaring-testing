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
corpora, and runs every suite (twenty-four; three of them only when asked,
see below).

Budget about ten minutes for a full run, most of it suite 09: it replays
thousands of commands against the upstream module and pulls that ~2 GB
image the first time. The other default suites together take about three
minutes.

Requirements: `docker` with Compose v2, `python3` with `venv`, and `git`.

```bash
bash run_all.sh              # everything (~10 minutes; longer on a first run)
FULL=1 bash run_all.sh       # adds the large datasets to suite 01
bash run_all.sh 02 09        # only suites 02 and 09
VR_KEEP=1 bash run_all.sh    # leave the server up after a passing run
VT_LOAD=1 bash run_all.sh 17 18   # load/soak suite 18 (minutes), longer suite 17
VT_HEAVY=1 bash run_all.sh 19     # 10M-value keys, lazy free, output-buffer limit
VT_COMPAT=1 bash run_all.sh 21    # data written by valkey-roaring 1.1.1 (pulls its image)
VT_PARITY_SEEDS=10 bash run_all.sh 20   # a longer independent parity fuzz
```

Suites 18, 19 and 21 are heavy or need an extra image and skip themselves
unless their flag is set, so a default run stays around ten minutes (suites
20 and 23 add about a minute each). Suite 18 also reads
`VT_LOAD_CLIENTS` (default 32), `VT_LOAD_PIPELINE` (8), `VT_SOAK_SECONDS`
(60) and `VT_PORT` (6379; point it at another server to compare builds).

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
VR_REF=b96eb7cdaa27df5ccabd3a2a1389156d2940afe5 bash run_all.sh   # exact commit
```

A commit must be given in full: the fetch asks the remote for that object
by name, and GitHub only serves full 40-character SHAs — an abbreviated one
fails with `Could not fetch`.

Testing an uncommitted change is the first form: point `VR_SOURCE` at the
working tree and the image is built from it directly.

## Architecture

```
.
├── run_all.sh              orchestrator (resolves source, starts server, runs suites)
├── docker-compose.yml      the server under test
├── requirements.txt        pyroaring — real CRoaring bindings for interop
├── .github/workflows/      CI: per-push, nightly against the module's main
├── lib/
│   ├── valkey_client.py    stdlib-only binary-safe RESP2/RESP3 client
│   ├── harness.py          assertion counters and reporting
│   ├── compose.py          locating and driving the compose project
│   ├── datasets.py         real-roaring-datasets download/cache/parse
│   └── reference.py        naive pure-Python model of every command
├── datasets/               downloaded corpora (cached, gitignored)
└── suites/                 independent suites, detailed below
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
| 04 | `replication` | live replica attached mid-load; byte-identical EXPORT of every key on both sides; no-op writes add nothing to the stream | verbatim-propagation gaps, ordering effects, and writes wrongly skipped as no-ops |
| 05 | `concurrency` | 8 threads of pipelined mixed ops + background BGSAVE/OPTIMIZE churn; per-thread models must match | state corruption under interleaving and fork pressure |
| 06 | `boundaries` | values and ranges straddling every seam: 65536-multiples, u32::MAX, 2³², 2⁶³, u64::MAX; contents stable across OPTIMIZE; STAT structure | off-by-ones at container seams; representation-dependent results |
| 07 | `keyspace` | TYPE, SCAN-by-type, RENAME, COPY, DEL/UNLINK, EXPIRE/PERSIST, MEMORY USAGE, keyspace events; WATCH and client-tracking invalidation fire on real changes only | seams between module type callbacks and server machinery |
| 08 | `resp3` | identical command battery over RESP2 and RESP3 (HELLO 3) connections | protocol-version-dependent reply encoding |
| 09 | `differential_upstream` | thousands of seeded-random commands against the real redis-roaring image, reply-for-reply with exact error texts; then about 1,250 exact-reply rows (argument grammars, check order, case-sensitive tokens, JACCARD and STAT formats, 64-bit positions) compared as raw bytes under RESP2 and RESP3; documented divergences asserted against valkey-roaring's intended reply | any semantic drift from the compatibility promise, down to reply types and error wording |
| 10 | `cluster` | cluster-enabled server: hash-tag same-slot ops work, `NOT ... last` is not treated as a key, cross-slot rejected | getkeys regressions (upstream's v1.7.3 bug class) |
| 11 | `protocol_torture` | 30k commands of hostile argument soup; server must reply, stay alive, keep a control key bit-identical | parser/dispatch crashes and cross-key corruption |
| 12 | `snapshot_export_workflow` | the producer/consumer raw-blob pattern: full refreshes, bit flips with exact replies, blob determinism across construction histories, decode fidelity via CRoaring, no torn blobs under concurrent refresh+export, byte-stability across restart, 2000-partition fleet | anything that would corrupt, skew, or destabilize a raw-blob handoff pipeline |
| 13 | `canonical_export` | every test set built through ~20 unrelated histories (bulk, bit-by-bit, ranges, deletes from a superset, IMPORT of optimized and unoptimized blobs, merges, every BITOP shape, DIFF, SETBITARRAY, OPTIMIZE, COPY, DUMP/RESTORE) exports exactly CRoaring's canonical blob; tie-sized containers at key 0, 0xFFFF, the top of u64, beside bitsets, across sub-bitmaps; EXPORT never fires WATCH or dirties; first-export cost of tie-heavy keys, all-tied and mixed with untied containers, bounded (regression guard) | history-dependent encodings leaking into blobs |
| 14 | `write_signals` | every write command of both widths, no-op variant vs real change, each checked for dirty counter, WATCH abort and client-tracking invalidation; key creation counts as a change; reads (EXPORT included) never signal; no-op MULTI/EXEC and scripts propagate nothing | a change skipped as a no-op (replica/AOF divergence) or a no-op that still invalidates |
| 15 | `streamed_replies` | streamed array replies (GETINTARRAY, RANGEINTARRAY, GETBITS) match a Python model at every page edge, on multi-container, multi-sub-bitmap and >2^63 keys, with a PING sentinel after each reply, over RESP2/RESP3, MULTI/EXEC and Lua | header/element count drift that desynchronizes the connection |
| 16 | `import_and_restore` | R64 blobs with empty sub-bitmaps through IMPORT and through RESTORE of a hand-built DUMP payload (the RDB load path): equality, subset, cardinality, canonical export, no-op re-import; trailing bytes and repeated or decreasing 64-bit high words refused on both paths (CRoaring, via pyroaring, rejects the same high-word layouts); truncated, bit-flipped and random blobs rejected cleanly or merged consistently | deserialization admitting states the in-memory invariants forbid, or silently dropping values |
| 17 | `memory_accounting` | MEMORY USAGE vs allocator growth for a dozen key shapes (ratios printed); create/refresh/merge/export/delete cycles return used memory to its start (`VT_LOAD=1`: 10x cycles) | estimates far from reality; leaks on value-replacement paths |
| 18 | `load_soak` | **`VT_LOAD=1` only** — 32 pipelined client processes for 60 s over 1M-value shared keys and per-client owned keys; models must match, shared keys unchanged, no error replies, memory returns; p50/p99 per command class | interleaving corruption, latency cliffs, growth under sustained load |
| 19 | `large_keys_and_limits` | **`VT_HEAVY=1` only** — 10M-value keys (full replies, 1M pages, BITOP, EXPORT/IMPORT); UNLINK of a 3M-container key stays off the main thread; GETINTARRAY of R.SETFULL refused up front, and a 100M-value GETINTARRAY into a client that never reads, in a 2 GB container with an output-buffer limit, leaves the server alive and bounded | stalls and memory blow-ups that only appear at scale |
| 20 | `parity_fuzz` | an independent raw-byte differential against redis-roaring: random commands with malformed numbers, bits, case/NUL-truncated tokens, wrong-type and missing keys in every slot (RESP2/RESP3, both widths); an exhaustive check-order matrix (reply and resulting state); CONTAINS's echoed token at its edges; JACCARD at %.17g ties; R.STAT after short write histories: every field but the per-encoding container breakdown must match at each step (the breakdown is a documented divergence), and the whole STAT after R.OPTIMIZE; regression guard for trailing CR/LF in the CONTAINS echo | reply drift the curated rows of suite 09 do not cover |
| 21 | `upgrade_from_1_1_1` | **`VT_COMPAT=1` only** — keys of every shape written by the 1.1.1 image survive DUMP/RESTORE, its dump.rdb at startup, its AOF replay and replication from a 1.1.1 primary; writes only 1.1.1 accepts (lenient IMPORT blobs, "+5"/"007"/"01", lowercase BITOP), hand-picked and as a random 2,500-command stream of plain commands, MULTI/EXEC blocks and EVAL effects, must replay and replicate to exactly 1.1.1's sets with no CRITICAL log lines (regression guard) | upgrade-path data loss from tightened decoding |
| 22 | `small_write_costs` | per-call server time of single-bit writes, short ranges, point reads and small GETBITS on a one-million-container R64 key and a 65,536-container R key vs a one-container key; the ratio must stay small (regression guard: R64.SETRANGE once walked the whole key) | hidden whole-key passes on hot paths that small-key benchmarks never show |
| 23 | `operation_routes` | commands that pick an algorithm by input shape give the same answer on every route: BITOP NOT's direct complement vs XOR (R and per-R64-sub-bitmap, full blocks, `last` at chunk edges and near the top), AND/DIFF copy-then-filter vs fresh build (R64 deciding from its first sixteen sub-bitmaps, steered both ways), two-source ANDOR/DIFF1, destination aliasing, R64 CONTAINS past sixteen sub-bitmaps with mixed encodings; model, canonical EXPORT and upstream reply bytes; overwrite/UNLINK loops around the 1,024-container in-place free threshold keep PING fast (`VT_LOAD=1`: every shape pair at full size, ~20 min) | a fast path wrong only for the shapes that select it; frees that stall the server |
| 24 | `limits_and_encoding` | the two limit configs (`valkey-roaring.max-reply-elements`, `valkey-roaring.max-write-values`): defaults with byte-identical refusals under RESP2/RESP3, every reply and write command at a changed limit's exact boundary on both widths with the value named in the refusal, out-of-range values refused by CONFIG SET and failing a server start, values given at startup; the maxmemory check: range writes that would cross maxmemory refused with the server's OOM reply before allocating or creating anything, smaller ones accepted, MULTI/EXEC and Lua, eviction policies (only writes larger than maxmemory refused, the server evicts for the rest); a replica and an AOF replay with a lower write limit and a tiny maxmemory apply every write the primary accepted; `EXPORT/IMPORT ... BASE64`: the raw blob's standard Base64 on real datasets, CRoaring blobs via Python's base64, canonical texts, strict decoding (padding, stray characters, URL-safe alphabet, unused bits), exact tokens, arity, GETKEYS, valkey-cli paste, no-op signals | limits that drift from their documented defaults or ignore their setting; memory refusals that leave partial state or fire on replicas and replay; a text encoding that is not the exact inverse of the binary one |

Every suite is a self-contained script with a module docstring stating its
contract and targeted escape class — those docstrings are the per-suite
documentation. Each is runnable on its own against an already-running
server:

```bash
.venv/bin/python suites/test_06_boundaries.py
```

Suites 03, 04, 10, 19, 21 and 24 start and remove their own helper
containers (`vt-aof`, `vt-replica`, `vt-cluster`, `vt-capped`,
`vt-old`/`vt-new`, `vt-lim-*`);
suites 09 and 20 pull and run `aviggiano/redis-roaring:latest` as
`vt-upstream` and `vt-upstream20`.

## Scope limits, on purpose

- `R64.SETFULL`, wide `R64.SETRANGE` and `R64.BITOP NOT` over universes
  past 2^38 are refused by the module and only ever sent to it, never to
  upstream: redis-roaring materializes them until it is killed. Suite 09
  asserts the refusals among its documented divergences.
- Suite 09 skips EXPORT/IMPORT, which do not exist upstream (suites 12, 13
  and 16 cover them).

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
