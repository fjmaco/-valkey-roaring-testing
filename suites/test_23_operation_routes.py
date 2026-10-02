"""Suite 23 — Operations that pick an algorithm by shape: same answer either way.

Several commands choose between algorithms from the shape of their inputs:

  * BITOP NOT complements clustered sources directly (a blob built and
    decoded) and XORs everything else; R64 flips sub-bitmap by sub-bitmap
    and fills missing blocks with full ones;
  * AND and DIFF copy-then-filter well-filled sources and build sparse or
    clustered ones fresh; R64 decides from its first sixteen sub-bitmaps;
  * two-source ANDOR and DIFF1 are a single intersection / difference;
  * CONTAINS EQ / ALL_STRICT compare R64 sub-bitmaps past the sixteenth by
    a cheaper path;
  * values of up to 1,024 containers are freed in place, larger ones on
    the lazy-free thread.

This suite drives each choice from both sides with source shapes built to
land on either route (dense full-span, clustered, sparse, runs, mixed,
values at the top of the range and across 2^32 borders, more than sixteen
sub-bitmaps with the sample pointing the wrong way), and requires the
answer of a plain model (pyroaring), canonical EXPORT of every result, and
reply bytes identical to redis-roaring for NOT and the two-source forms.
Destination aliasing is covered. For freeing, it requires that overwrite-
and delete-heavy loops on keys just under and far over the threshold keep
the main thread responsive.

By default the route matrix uses moderate sizes and a fixed set of shape
pairs (about two minutes); VT_LOAD=1 runs every pair at full size (about
twenty minutes).

Escape class targeted: a fast path that is wrong only for the shapes that
select it, and frees that stall the server.
"""

import random
import subprocess
import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from pyroaring import BitMap, BitMap64

from lib.harness import Suite, env_flag
from lib.raw_resp import RawClient
from lib.valkey_client import Client

UP_PORT = 6407
FULL = env_flag("VT_LOAD")
K = 1 if FULL else 5          # size divisor for the default run
H = 1 << 32
U32 = H - 1


def start_upstream():
    subprocess.run(["docker", "rm", "-f", "vt-upstream23"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "vt-upstream23", "--memory=4g", "-p", f"{UP_PORT}:6379",
                    "aviggiano/redis-roaring:latest"], check=True, capture_output=True)
    for _ in range(30):
        try:
            if Client(port=UP_PORT, timeout=3).cmd("PING") == "PONG":
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError("upstream image did not start")


def load(c, P, key, vals, ranges=False):
    c.cmd("DEL", key)
    if not vals:
        c.cmd(f"{P}.SETBIT", key, 1, 1)
        c.cmd(f"{P}.SETBIT", key, 1, 0)
        return
    if not ranges:
        for i in range(0, len(vals), 50_000):
            c.cmd(f"{P}.SETINTARRAY" if i == 0 else f"{P}.APPENDINTARRAY", key, *vals[i:i + 50_000])
        return
    c.cmd(f"{P}.SETBIT", key, vals[0], 1)
    i = 0
    while i < len(vals):
        j = i
        while j + 1 < len(vals) and vals[j + 1] == vals[j] + 1:
            j += 1
        c.cmd(f"{P}.SETRANGE", key, vals[i], vals[j] + 1)
        i = j + 1


def clustered(rng, n_runs, run_len, span):
    return sorted({s + i for s in rng.sample(range(span), n_runs) for i in range(run_len)})


def matches(c, P, key, want, cls):
    if c.cmd(f"{P}.BITCOUNT", key) != len(want):
        return False
    if not len(want):
        return True
    blob = c.cmd(f"{P}.EXPORT", key)
    if cls.deserialize(blob) != want:
        return False
    c.cmd("DEL", "canon")
    c.cmd(f"{P}.IMPORT", "canon", want.serialize())
    return c.cmd(f"{P}.EXPORT", "canon") == blob


def main():
    s = Suite("23 operation routes")
    c = Client(timeout=300)
    c.cmd("FLUSHALL")
    start_upstream()
    rng = random.Random(0x23)

    s.section("BITOP NOT: direct complement and XOR routes")
    sources32 = {
        "clustered": clustered(rng, 300, 60, 1 << 24),
        "clustered_long": clustered(rng, 50, 4000, 1 << 26),
        "short_runs": clustered(rng, 2000, 2, 1 << 24),
        "dense": sorted(rng.sample(range(1 << 18), 120_000)),
        "sparse": sorted(rng.sample(range(1 << 30), 2000)),
        "full_containers": list(range(0, 3 * 65536)) + list(range(7 * 65536, 8 * 65536)),
        "near_top": list(range(U32 - 100_000, U32 - 50_000)) + list(range(U32 - 10, U32 + 1)),
        "empty": [],
    }
    bad, parity = [], []
    a, b = RawClient(), RawClient(port=UP_PORT)
    for name, vals in sources32.items():
        mx = vals[-1] if vals else 0
        for last in (None, mx, min(mx + 1, U32), min(mx + 70_000, U32 - 1), 65535, rng.randrange(1 << 31)):
            load(c, "R", "src", vals)
            args = ["R.BITOP", "NOT", "dst", "src"] + ([last] if last is not None else [])
            top = max(last if last is not None else -1, mx if vals else -1)
            want = BitMap.flip(BitMap(vals), 0, top + 1) if top >= 0 else BitMap()
            if c.cmd(*args) != len(want) or not matches(c, "R", "dst", want, BitMap):
                bad.append((name, last))
            if top < U32 and len(want) <= 3_000_000:
                for conn in (a, b):
                    conn.frames("DEL", "src", "dst")
                    if vals:
                        for i in range(0, len(vals), 50_000):
                            conn.frames("R.SETINTARRAY" if i == 0 else "R.APPENDINTARRAY", "src", *vals[i:i + 50_000])
                for pc in (args, ["R.RANGEINTARRAY", "dst", 0, 999], ["R.MIN", "dst"], ["R.MAX", "dst"],
                           ["R.BITPOS", "dst", 0], ["R.GETBITS", "dst", *[rng.randrange(top + 2) for _ in range(40)]]):
                    if a.frames(*pc) != b.frames(*pc):
                        parity.append((name, last, pc[0]))
    s.check("R NOT matches the model and exports canonically on every route", [], bad)
    s.check("R NOT replies are byte-identical to upstream", [], parity)

    bad = []
    c.cmd("FLUSHALL")
    sources64 = {
        "clustered_two_subs": [h * H + v for h in (0, 2) for v in clustered(rng, 80, 40, 1 << 30)],
        "border": list(range(H - 3000, H + 3000)),
        "bitset_sub": sorted(set(rng.sample(range(1 << 18), 100_000)) | {H + 5}),
        "sub_tops": [h * H + U32 for h in range(4)] + [h * H for h in range(4)],
    }
    for name, vals in sources64.items():
        vals = sorted(set(vals))
        for last in (None, vals[-1] + 1, vals[-1] + H, (1 << 38) - 1):
            load(c, "R64", "src", vals)
            top = max(last or 0, vals[-1])
            want = BitMap64.flip(BitMap64(vals), 0, top + 1)
            card = c.cmd("R64.BITOP", "NOT", "dst", "src", *([last] if last is not None else []))
            ok = card == len(want) and c.cmd("R64.BITCOUNT", "dst") == len(want)
            probes = [rng.randrange(top + 2) for _ in range(200)] + [h * H + d for h in range(8) for d in (0, U32)]
            ok = ok and c.cmd("R64.GETBITS", "dst", *probes) == [int(x in want) for x in probes]
            if len(want) <= 20_000_000:
                ok = ok and matches(c, "R64", "dst", want, BitMap64)
            if not ok:
                bad.append((name, last))
    s.check("R64 NOT matches the model per sub-bitmap, blocks filled and cut", [], bad)
    c.cmd("R.SETINTARRAY", "al", *sources32["clustered"])
    want = BitMap.flip(BitMap(sources32["clustered"]), 0, sources32["clustered"][-1] + 1)
    c.cmd("R.BITOP", "NOT", "al", "al")
    s.check_true("NOT with destination = source", matches(c, "R", "al", want, BitMap))

    s.section("AND / DIFF / ANDOR / DIFF1 on both routes, aliasing included")
    shapes = {
        "dense_fullspan": sorted(rng.sample(range((1 << 21) // K), 300_000 // K)),
        "clustered_thin": clustered(rng, 200 // K, 60, 1 << 28),
        "sparse": sorted(rng.sample(range(1 << 30), 10_000 // K)),
        "runs": [x for k in range(100 // K) for x in range(k * 131072, k * 131072 + 20_000)],
    }
    bad, parity = [], []
    for P, cls, lift in (("R", BitMap, lambda v: v), ("R64", BitMap64, lambda v: (v >> 20 << 32) | v)):
        c.cmd("FLUSHALL")
        for conn in (a, b):
            conn.frames("FLUSHALL")
        lifted = {k: sorted({lift(v) for v in vs}) for k, vs in shapes.items()}
        if P == "R64":   # first 16 sub-bitmaps say "dense", the rest are sparse, and the reverse
            lifted["dense16_then_sparse"] = sorted({h * H + v for h in range(16) for v in rng.sample(range(1 << 20), 15_000 // K)}
                                                   | {h * H + rng.randrange(H) for h in range(16, 50) for _ in range(20)})
            lifted["sparse16_then_dense"] = sorted({h * H + rng.randrange(H) for h in range(16) for _ in range(20)}
                                                   | {h * H + v for h in range(16, 30) for v in rng.sample(range(1 << 20), 15_000 // K)})
        names = list(lifted)
        pairs = [(x, y) for x in names for y in names]
        if not FULL:   # every shape on each side at least once, both routes meeting
            pairs = [(x, y) for x, y in pairs if x == y or (names.index(x) + names.index(y)) % 3 == 0]
        for x, y in pairs:
                for ranges in ((False, True) if FULL else (x != y,)):
                    load(c, P, "A", lifted[x], ranges)
                    load(c, P, "B", lifted[y])
                    A, B = cls(lifted[x]), cls(lifted[y])
                    for op, want in (("AND", A & B), ("DIFF", A - B), ("ANDOR", A & B), ("DIFF1", B - A)):
                        if c.cmd(f"{P}.BITOP", op, "D", "A", "B") != len(want) or not matches(c, P, "D", want, cls):
                            bad.append((P, op, x, y, ranges))
                        load(c, P, "A2", lifted[x], ranges)
                        if c.cmd(f"{P}.BITOP", op, "A2", "A2", "B") != len(want) or not matches(c, P, "A2", want, cls):
                            bad.append((P, op, "dest=first", x, y, ranges))
                        load(c, P, "B2", lifted[y])
                        if c.cmd(f"{P}.BITOP", op, "B2", "A", "B2") != len(want) or not matches(c, P, "B2", want, cls):
                            bad.append((P, op, "dest=second", x, y, ranges))
                    c.cmd(f"{P}.DIFF", "E", "A", "B")
                    if not matches(c, P, "E", A - B, cls):
                        bad.append((P, "R.DIFF", x, y, ranges))
        # two-source forms vs upstream, raw bytes
        for x, y in (("clustered_thin", "sparse"), ("dense_fullspan", "runs"), ("sparse", "dense_fullspan")):
            for conn in (a, b):
                for key, nm in (("A", x), ("B", y)):
                    conn.frames("DEL", key)
                    vs = lifted[nm]
                    for i in range(0, len(vs), 50_000):
                        conn.frames(f"{P}.SETINTARRAY" if i == 0 else f"{P}.APPENDINTARRAY", key, *vs[i:i + 50_000])
            for op in ("ANDOR", "DIFF1", "AND", "DIFF"):
                for pc in ([f"{P}.BITOP", op, "D", "A", "B"], [f"{P}.RANGEINTARRAY", "D", 0, 999], [f"{P}.BITCOUNT", "D"]):
                    if a.frames(*pc) != b.frames(*pc):
                        parity.append((P, op, x, y, pc[0]))
    s.check("every route gives the model's set, canonically exported", [], bad[:10])
    s.check("two-source ANDOR / DIFF1 and AND / DIFF replies identical to upstream", [], parity)

    s.section("R64 CONTAINS past sixteen sub-bitmaps, mixed encodings")
    c.cmd("FLUSHALL")
    bad = []
    for nsub in (17, 40, 300):
        vals = sorted({h * H + st + i for h in range(nsub) for st in rng.sample(range(1 << 24), 3) for i in range(rng.choice([1, 3, 50]))})
        load(c, "R64", "X", vals)                 # array containers
        load(c, "R64", "Y", vals, ranges=True)    # run containers
        q = lambda x, y, *m: c.cmd("R64.CONTAINS", x, y, *m)
        if (q("X", "Y", "EQ"), q("Y", "X", "EQ"), q("X", "Y", "ALL"), q("X", "Y", "ALL_STRICT"), q("X", "Y")) != (1, 1, 1, 0, 1):
            bad.append(("equal sets", nsub))
        late = (nsub - 1) * H + 11
        had = late in set(vals)
        c.cmd("R64.SETBIT", "Y", late, 0 if had else 1)
        got = (q("X", "Y", "EQ"), q("X", "Y", "ALL"), q("Y", "X", "ALL"), q("X", "Y", "ALL_STRICT"), q("Y", "X", "ALL_STRICT"))
        if got != ((0, 1, 0, 1, 0) if had else (0, 0, 1, 0, 1)):
            bad.append(("late difference", nsub, got))
    s.check("EQ / ALL / ALL_STRICT match the model", [], bad)

    s.section("freeing: in place up to 1,024 containers, lazily above")
    lat, stop = [], [False]

    def pinger():
        p = Client(timeout=60)
        while not stop[0]:
            t0 = time.perf_counter()
            p.cmd("PING")
            lat.append(time.perf_counter() - t0)
            time.sleep(0.002)

    c.cmd("FLUSHALL")
    th = threading.Thread(target=pinger)
    th.start()
    under = [k << 16 | 7 for k in range(1000)]                 # 1,000 containers: freed in place
    over = [k << 16 | 7 for k in range(65_536)]                 # 65,536 containers: lazily
    big64 = [k << 16 for k in range(1_500_000 // K)]            # 1.5M (default: 300k) containers
    worst = {}
    for label, P, vals, reps in (("1,000 containers", "R", under, 300), ("65,536 containers", "R", over, 20),
                                 ("R64 big", "R64", big64, 3)):
        lat.clear()
        t0 = time.perf_counter()
        for i in range(reps):
            load(c, P, "ow", vals)                              # each load overwrites (frees) the last value
            if i % 2:
                c.cmd("UNLINK", "ow")
        dt = (time.perf_counter() - t0) / reps
        worst[label] = max(lat or [0])
        print(f"    {label}: {1000 * dt:.2f} ms per overwrite cycle, worst PING {1000 * worst[label]:.1f} ms")
    stop[0] = True
    th.join()
    # Building a 1.5M-container value itself takes the main thread a while;
    # a synchronous free of the previous one would add about as much again.
    s.check_true("overwrites of small values keep PING under 20 ms", worst["1,000 containers"] < 0.02, worst)
    s.check_true("overwrites of 65,536-container values keep PING under 50 ms", worst["65,536 containers"] < 0.05, worst)
    c.cmd("DEL", "ow")
    load(c, "R64", "huge", big64)
    t0 = time.perf_counter()
    c.cmd("UNLINK", "huge")
    s.check_true(f"UNLINK of {len(big64):,} containers returns within 15 ms", time.perf_counter() - t0 < 0.015)

    subprocess.run(["docker", "rm", "-f", "vt-upstream23"], capture_output=True)
    c.cmd("FLUSHALL")
    s.finish()


main()
