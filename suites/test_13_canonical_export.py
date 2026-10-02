"""Suite 13 — Canonical EXPORT: one logical set, one byte sequence.

R.EXPORT / R64.EXPORT promise a canonical blob: whatever sequence of writes
built a set, its export is the same bytes. This suite builds each test set
through a dozen unrelated histories (bulk, shuffled, bit-by-bit, range
writes, delete-from-superset, IMPORT of optimized and unoptimized CRoaring
blobs, merges, every BITOP shape, DIFF, SETBITARRAY, OPTIMIZE, COPY,
DUMP/RESTORE) and requires every export to equal one independent oracle:
CRoaring's own canonical form, `BitMap(values).run_optimize().serialize()`
(BitMap64 for R64), computed by pyroaring from the plain value list.

The shapes target the case roaring-rs' optimize() leaves open — containers
whose run and array encodings tie in size (cardinality = 2 * runs + 1) —
placed at container key 0, 0xFFFF, the top of u64, next to bitset
containers, across R64 sub-bitmap borders, and in blobs below and above the
four-container offset-table threshold. It also requires that an export is
a pure read (it re-encodes in place, yet must not fire WATCH) and repeats
byte-identically.

Finally it bounds the cost of the first export of a tie-heavy key, a
regression guard: an earlier tie demotion removed and re-inserted every
tied container, shifting the container vector (R) or walking every
sub-bitmap (R64), so a key with 65,536 tied containers blocked the server
for seconds where 1.1.1 took ~2 ms (an R64 key with 256 x 1,024 tied
containers took ~40 s). The demotion is now linear in the key.

Escape class targeted: history-dependent encodings leaking into the blob,
which breaks consumers that hash, dedupe or diff blobs.
"""

import random
import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from pyroaring import BitMap, BitMap64

from lib.harness import Suite
from lib.valkey_client import Client

CHUNK = 20_000


def runs_of(vals):
    out, start, prev = [], None, None
    for v in vals:
        if start is None:
            start = prev = v
        elif v == prev + 1:
            prev = v
        else:
            out.append((start, prev))
            start = prev = v
    if start is not None:
        out.append((start, prev))
    return out


def tie_container(base, runs):
    """Values in one container: one 3-run, then 2-runs: 2*runs+1 values."""
    vals, pos = [], base + 10
    for i in range(runs):
        n = 3 if i == 0 else 2
        vals.extend(range(pos, pos + n))
        pos += n + 3
    return vals


def shapes(rng, wide):
    top = (1 << 64) if wide else (1 << 32)
    s = {}
    s["tie_one"] = [5, 6, 7]
    s["tie_keys_0_and_ffff"] = tie_container(0, 1) + tie_container(0xFFFF << 16, 4)
    s["tie_next_to_bitset"] = (tie_container(0, 2) + sorted(rng.sample(range(65536, 131072), 5000))
                               + tie_container(2 << 16, 9))
    s["tie_three_containers"] = tie_container(0, 1) + tie_container(1 << 16, 2) + tie_container(2 << 16, 3)
    s["tie_many_containers"] = [v for k in range(0, 300, 3) for v in tie_container(k << 16, 1 + k % 7)]
    s["tie_big_runs"] = tie_container(5 << 16, 500)          # 1001 values, 500 runs
    s["single"] = [top - 1]
    s["clustered"] = sorted({b + i for b in rng.sample(range(1 << 22), 300) for i in range(rng.randrange(1, 12))})
    s["dense_and_sparse"] = sorted(set(range(70_000, 200_000)) | set(rng.sample(range(1 << 24), 3000)))
    s["u32_top"] = tie_container((1 << 32) - 65536, 3) + [(1 << 32) - 1]
    if wide:
        s["subbitmap_border"] = sorted(set(range((1 << 32) - 2, (1 << 32) + 3)) | {1 << 33, (1 << 33) + 1, (1 << 33) + 2})
        s["many_subbitmaps"] = [v for h in (0, 1, 7, 1 << 31, (1 << 32) - 1) for v in tie_container(h << 32, 2)]
        s["u64_top"] = tie_container(((1 << 32) - 1 << 32) | (0xFFFF << 16), 3) + [(1 << 64) - 1]
        s["above_2_63"] = sorted({(1 << 63) + b + i for b in rng.sample(range(1 << 20), 50) for i in range(3)})
    return s


def oracle(vals, wide):
    b = (BitMap64 if wide else BitMap)(vals)
    b.run_optimize()
    return b.serialize()


def put(c, P, key, vals, shuffle_rng=None):
    vals = list(vals)
    if shuffle_rng:
        shuffle_rng.shuffle(vals)
    c.cmd("DEL", key)
    if not vals:
        c.cmd(f"{P}.SETBIT", key, 1, 1)
        c.cmd(f"{P}.SETBIT", key, 1, 0)
        return
    for i in range(0, len(vals), CHUNK):
        c.cmd(f"{P}.SETINTARRAY" if i == 0 else f"{P}.APPENDINTARRAY", key, *vals[i:i + CHUNK])


def histories(c, P, vals, rng, wide):
    """Yield (name, key) after building `vals` at `key` through one history."""
    top = (1 << 64) if wide else (1 << 32)
    cls = BitMap64 if wide else BitMap
    noise = sorted(set(rng.randrange(top) for _ in range(200)) - set(vals))
    key = "h"

    put(c, P, key, vals)
    yield "setintarray sorted", key
    put(c, P, key, vals, rng)
    yield "setintarray shuffled", key
    if len(vals) <= 3000:
        c.cmd("DEL", key)
        c.cmd(f"{P}.SETBIT", key, 0, 0)                  # create empty
        order = list(vals)
        rng.shuffle(order)
        c.pipeline([[f"{P}.SETBIT", key, v, 1] for v in order])
        yield "setbit one by one", key
    c.cmd("DEL", key)
    c.cmd(f"{P}.SETBIT", key, 0, 0)
    rs = runs_of(vals)
    c.pipeline([[f"{P}.SETRANGE", key, a, b + 1] if b + 1 < top else [f"{P}.SETBIT", key, b, 1] for a, b in rs])
    # SETRANGE is end-exclusive, so a run ending at the type's max needs
    # its last value set separately.
    c.pipeline([[f"{P}.SETRANGE", key, a, b] for a, b in rs if b + 1 >= top and b > a])
    yield "setrange per run", key
    put(c, P, key, sorted(set(vals) | set(noise)))
    c.cmd(f"{P}.DELETEINTARRAY", key, *noise)
    yield "superset minus DELETEINTARRAY", key
    put(c, P, key, sorted(set(vals) | set(noise)))
    c.cmd(f"{P}.CLEARBITS", key, *noise)
    yield "superset minus CLEARBITS", key
    c.cmd("DEL", key)
    c.cmd(f"{P}.IMPORT", key, cls(vals).serialize())
    yield "import unoptimized croaring blob", key
    c.cmd("DEL", key)
    c.cmd(f"{P}.IMPORT", key, oracle(vals, wide))
    yield "import run-optimized croaring blob", key
    half = len(vals) // 2
    a_runs = cls(vals[:half])
    a_runs.run_optimize()
    c.cmd("DEL", key)
    c.cmd(f"{P}.IMPORT", key, a_runs.serialize())
    c.cmd(f"{P}.IMPORT", key, cls(vals[half:]).serialize())
    yield "import merge of halves", key
    put(c, P, "x1", vals[:half])
    put(c, P, "x2", vals[half:])
    c.cmd(f"{P}.BITOP", "OR", key, "x1", "x2")
    yield "bitop OR of halves", key
    put(c, P, "x1", sorted(set(vals) | set(noise[:100])))
    put(c, P, "x2", sorted(set(vals) | set(noise[100:])))
    c.cmd(f"{P}.BITOP", "AND", key, "x1", "x2")
    yield "bitop AND of supersets", key
    put(c, P, "x1", sorted(set(vals) | set(noise)))
    put(c, P, "x2", noise)
    c.cmd(f"{P}.BITOP", "XOR", key, "x1", "x2")
    yield "bitop XOR", key
    c.cmd(f"{P}.BITOP", "DIFF", key, "x1", "x2")
    yield "bitop DIFF", key
    c.cmd(f"{P}.DIFF", key, "x1", "x2")
    yield "R.DIFF", key
    c.cmd(f"{P}.BITOP", "ONE", key, "x1", "x2", "x2", "x2")
    yield "bitop ONE", key
    if vals and vals[-1] < (1 << 22):
        c.cmd(f"{P}.BITOP", "NOT", "x3", key, vals[-1])
        c.cmd(f"{P}.BITOP", "NOT", key, "x3", vals[-1])
        yield "bitop NOT twice", key
        c.cmd("DEL", key)
        bits = bytearray(b"0" * (vals[-1] + 1))
        for v in vals:
            bits[v] = ord("1")
        c.cmd(f"{P}.SETBITARRAY", key, bytes(bits))
        yield "setbitarray", key
    c.cmd("DEL", key)
    c.cmd(f"{P}.SETBIT", key, 0, 0)
    c.pipeline([[f"{P}.SETRANGE", key, a, b + 1] if b + 1 < top else [f"{P}.SETBIT", key, b, 1] for a, b in rs])
    c.pipeline([[f"{P}.SETRANGE", key, a, b] for a, b in rs if b + 1 >= top and b > a])
    c.cmd(f"{P}.OPTIMIZE", key)
    yield "setrange then OPTIMIZE", key
    c.cmd("COPY", key, "cp", "REPLACE")
    yield "COPY of the range-built key", "cp"
    c.cmd("DEL", key)
    c.cmd(f"{P}.SETBIT", key, 0, 0)
    c.pipeline([[f"{P}.SETRANGE", key, a, b + 1] if b + 1 < top else [f"{P}.SETBIT", key, b, 1] for a, b in rs])
    c.pipeline([[f"{P}.SETRANGE", key, a, b] for a, b in rs if b + 1 >= top and b > a])
    payload = c.cmd("DUMP", key)
    c.cmd("RESTORE", "rs", 0, payload, "REPLACE")
    yield "DUMP/RESTORE of the range-built key", "rs"


def main():
    s = Suite("13 canonical export")
    c = Client(timeout=120)
    c.cmd("FLUSHALL")
    rng = random.Random(0xCA70)

    for P, wide in (("R", False), ("R64", True)):
        c.cmd("FLUSHALL")
        s.section(f"{P}: every history exports the CRoaring canonical blob")
        for name, vals in shapes(rng, wide).items():
            vals = sorted(set(vals))
            want = oracle(vals, wide)
            bad = []
            n = 0
            for hist, key in histories(c, P, vals, rng, wide):
                n += 1
                blob = c.cmd(f"{P}.EXPORT", key)
                again = c.cmd(f"{P}.EXPORT", key)
                if blob != want or again != blob:
                    bad.append(hist)
            s.check(f"{P} {name}: {n} histories, all canonical", [], bad)
        # an empty (existing) key
        c.cmd("DEL", "e")
        c.cmd(f"{P}.SETBIT", "e", 3, 1)
        c.cmd(f"{P}.SETBIT", "e", 3, 0)
        s.check(f"{P} empty key exports the empty canonical blob", oracle([], wide),
                c.cmd(f"{P}.EXPORT", "e"))

        s.section(f"{P}: export is a read, even when it re-encodes")
        c.cmd("DEL", "w")
        c.cmd(f"{P}.SETRANGE", "w", 5, 8)                # a tied run container
        w = Client()
        w.cmd("WATCH", "w")
        dirty0 = int(c.cmd("INFO", "persistence").decode().split("rdb_changes_since_last_save:")[1].split()[0])
        c.cmd(f"{P}.EXPORT", "w")
        dirty1 = int(c.cmd("INFO", "persistence").decode().split("rdb_changes_since_last_save:")[1].split()[0])
        w.cmd("MULTI")
        w.cmd(f"{P}.BITCOUNT", "w")
        s.check(f"{P} WATCH survives an export that demotes a tie", [3], w.cmd("EXEC"))
        s.check(f"{P} export adds nothing to the dirty counter", 0, dirty1 - dirty0)
        w.close()

    s.section("first export of a tie-heavy key is not quadratic (regression guard)")
    # 1.1.1 exports both keys below in ~2 ms (without re-encoding the ties);
    # the linear demotion takes a few ms more. Bounds are 100x+ looser, so
    # only an algorithmic regression trips them.
    c.cmd("DEL", "tq")
    cmds = [["R.SETRANGE", "tq", i << 16, (i << 16) + 3] for i in range(65536)]
    for i in range(0, len(cmds), 5000):
        c.pipeline(cmds[i:i + 5000])
    t0 = time.monotonic()
    c.cmd("R.EXPORT", "tq")
    dt = time.monotonic() - t0
    s.check_true("regression guard: R.EXPORT of 65,536 tied containers under 0.5 s", dt < 0.5,
                 f"took {dt:.2f} s")
    c.cmd("DEL", "tq")
    cmds = [["R64.SETRANGE", "tq", (h << 32) | 5, (h << 32) | 8] for h in range(16384)]
    for i in range(0, len(cmds), 5000):
        c.pipeline(cmds[i:i + 5000])
    t0 = time.monotonic()
    c.cmd("R64.EXPORT", "tq")
    dt = time.monotonic() - t0
    s.check_true("regression guard: R64.EXPORT of 16,384 sub-bitmaps with one tie each under 0.25 s",
                 dt < 0.25, f"took {dt:.2f} s")

    # The guards above are all-tied keys, which the demotion short-cuts (the
    # tied arrays are the whole set). Tied containers interleaved with
    # untied runs, arrays and bitsets take the general merge path: they must
    # be just as fast and still export the canonical blob.
    mixed = [
        ("R", "32,768 tied + 32,768 untied runs",
         [(i << 16, (i << 16) + (3 if i % 2 == 0 else 20)) for i in range(65536)], []),
        ("R", "tied runs / arrays / long runs in thirds",
         [(i << 16, (i << 16) + (3 if i % 3 == 0 else 100)) for i in range(30000) if i % 3 != 1],
         [[(i << 16) | (j * 7) for j in range(20)] for i in range(1, 30000, 3)]),
        ("R64", "1,024 sub-bitmaps x 64 half-tied containers",
         [((h << 32) | (i << 16), ((h << 32) | (i << 16)) + (3 if i % 2 else 20))
          for h in range(1024) for i in range(64)], []),
    ]
    for P, name, ranges, arrays in mixed:
        c.cmd("DEL", "mx")
        cmds = [[f"{P}.SETRANGE", "mx", a, b] for a, b in ranges]
        cmds += [[f"{P}.APPENDINTARRAY", "mx", *vals] for vals in arrays]
        for i in range(0, len(cmds), 5000):
            c.pipeline(cmds[i:i + 5000])
        t0 = time.monotonic()
        blob = c.cmd(f"{P}.EXPORT", "mx")
        dt = time.monotonic() - t0
        s.check_true(f"regression guard: {P} {name}: first EXPORT under 0.25 s", dt < 0.25,
                     f"took {dt:.2f} s")
        vals = sorted({v for a, b in ranges for v in range(a, b)} | {v for vs in arrays for v in vs})
        s.check(f"{P} {name}: canonical blob", oracle(vals, P == "R64"), blob)

    c.cmd("FLUSHALL")
    s.finish()


main()
