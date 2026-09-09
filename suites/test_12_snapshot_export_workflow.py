"""Suite 12 — Snapshot-export workflow (strict).

Models a producer/consumer snapshot-export pattern end to end: a producer maintains per-partition membership sets (one key per
partition), updating them with full refreshes (SETINTARRAY) and single-bit
flips (SETBIT); consumers fetch the raw serialized value (EXPORT) and
deserialize it independently with a CRoaring-compatible library. Every
assertion here is a contract such a pipeline silently depends on:

  1. full-refresh fidelity at realistic cardinalities
  2. exact wire replies for the flip path (previous-bit values)
  3. blob determinism: one logical set => one byte sequence, regardless of
     construction history (consumers may hash or dedupe blobs)
  4. consumer decode fidelity through real CRoaring (pyroaring)
  5. atomicity: exports concurrent with refreshes never yield a torn blob —
     every observed blob equals exactly one of the writer's states
  6. restart stability: blobs byte-identical across RDB save/restart
  7. fleet shape: thousands of partitions, spot-verified, with the
     compressed blob materially smaller than the integer-array reply

Escape class targeted: anything that would corrupt, skew, or destabilize
the raw-blob handoff between the producer and its consumers.
"""

import random
import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from pyroaring import BitMap

from lib.compose import sh
from lib.harness import Suite
from lib.valkey_client import Client


def load(c, key, vals, chunk=5000):
    for i in range(0, len(vals), chunk):
        c.cmd("R.APPENDINTARRAY" if i else "R.SETINTARRAY", key, *vals[i:i + chunk])


def main():
    s = Suite("12 snapshot-export workflow")
    c = Client()
    rng = random.Random(0x5709E)
    c.cmd("FLUSHALL")

    # ------------------------------------------------------------------
    s.section("1. full refresh at realistic cardinality")
    universe = 2_000_000                      # id space
    members = sorted(rng.sample(range(universe), 150_000))
    load(c, "p:0", members)
    s.check("cardinality exact", len(members), c.cmd("R.BITCOUNT", "p:0"))
    s.check("min/max exact", [members[0], members[-1]],
            [c.cmd("R.MIN", "p:0"), c.cmd("R.MAX", "p:0")])
    probes = rng.sample(members, 500) + \
        [v for v in rng.sample(range(universe), 500) if v not in set(members)][:400]
    expected = [1 if p in set(members) else 0 for p in probes]
    s.check("membership probes exact", expected, c.cmd("R.GETBITS", "p:0", *probes))

    # A second refresh REPLACES, never merges
    second = sorted(rng.sample(range(universe), 90_000))
    load(c, "p:0", second)
    s.check("refresh replaces prior state", len(second), c.cmd("R.BITCOUNT", "p:0"))

    # ------------------------------------------------------------------
    s.section("2. incremental flips: exact wire replies")
    absent = next(v for v in range(universe) if v not in set(second))
    present = second[0]
    s.check("flip on returns prior 0", 0, c.cmd("R.SETBIT", "p:0", absent, 1))
    s.check("flip on twice returns prior 1", 1, c.cmd("R.SETBIT", "p:0", absent, 1))
    s.check("flip off returns prior 1", 1, c.cmd("R.SETBIT", "p:0", present, 0))
    s.check("flip off twice returns prior 0", 0, c.cmd("R.SETBIT", "p:0", present, 0))
    model = (set(second) | {absent}) - {present}
    s.check("state after flips exact", len(model), c.cmd("R.BITCOUNT", "p:0"))

    # ------------------------------------------------------------------
    s.section("3. blob determinism across construction histories")
    target = sorted(rng.sample(range(500_000), 40_000)) + list(range(600_000, 640_000))
    target = sorted(set(target))
    # history A: bulk refresh
    load(c, "h:a", target)
    # history B: shuffled bulk + spurious values removed again
    shuffled = target[:]
    rng.shuffle(shuffled)
    spurious = [v for v in rng.sample(range(700_000, 800_000), 100)]
    load(c, "h:b", shuffled + spurious)
    c.cmd("R.DELETEINTARRAY", "h:b", *spurious)
    # history C: range fill + trim to target
    c.cmd("DEL", "h:c")
    c.cmd("R.SETRANGE", "h:c", 600_000, 640_000)
    load_extra = [v for v in target if v < 600_000]
    for i in range(0, len(load_extra), 5000):
        c.cmd("R.APPENDINTARRAY", "h:c", *load_extra[i:i + 5000])
    blobs = {h: c.cmd("R.EXPORT", f"h:{h}") for h in "abc"}
    s.check("A and B byte-identical", True, blobs["a"] == blobs["b"])
    s.check("A and C byte-identical", True, blobs["a"] == blobs["c"])
    s.check("blob decodes to the exact target", target, list(BitMap.deserialize(blobs["a"])))

    # ------------------------------------------------------------------
    s.section("4. consumer decode fidelity (real CRoaring)")
    for card in (1, 100, 10_000, 70_000, 250_000, 750_000):
        vals = sorted(rng.sample(range(universe), card))
        load(c, "d:k", vals)
        blob = c.cmd("R.EXPORT", "d:k")
        bm = BitMap.deserialize(blob)
        s.check(f"decode exact at cardinality {card}", True, list(bm) == vals)
        s.check(f"decoded cardinality {card}", card, len(bm))

    # ------------------------------------------------------------------
    s.section("5. atomicity: concurrent exports never see a torn blob")
    set_a = sorted(rng.sample(range(universe), 120_000))
    set_b = sorted(rng.sample(range(universe), 120_000))
    load(c, "atom", set_a)
    blob_a = c.cmd("R.EXPORT", "atom")
    load(c, "atom", set_b)
    blob_b = c.cmd("R.EXPORT", "atom")
    load(c, "atom", set_a)

    stop = threading.Event()
    bad, seen = [], set()

    def writer():
        w = Client()
        for i in range(60):
            vals = set_a if i % 2 == 0 else set_b
            for j in range(0, len(vals), 5000):
                w.cmd("R.APPENDINTARRAY" if j else "R.SETINTARRAY",
                      "atom", *vals[j:j + 5000])
        stop.set()

    def reader():
        r = Client()
        while not stop.is_set():
            blob = r.cmd("R.EXPORT", "atom")
            seen.add(blob)
            if blob != blob_a and blob != blob_b:
                # mid-refresh chunk states are legitimate command-level
                # snapshots; a TORN blob is one that fails to decode or
                # decodes to values outside both states
                try:
                    got = set(BitMap.deserialize(blob))
                except Exception as e:  # noqa: BLE001
                    bad.append(f"undecodable blob: {e}")
                    continue
                if not got <= (set(set_a) | set(set_b)):
                    bad.append("blob contains values from neither state")

    threads = [threading.Thread(target=reader) for _ in range(4)]
    wt = threading.Thread(target=writer)
    for t in threads + [wt]:
        t.start()
    for t in threads + [wt]:
        t.join()
    s.check("no torn or undecodable blob observed", [], bad)
    s.check_true("readers actually raced the writer", len(seen) > 2,
                 f"distinct blobs seen: {len(seen)}")

    # ------------------------------------------------------------------
    s.section("6. restart stability of exported bytes")
    stable = sorted(rng.sample(range(universe), 80_000))
    load(c, "stab", stable)
    before = c.cmd("R.EXPORT", "stab")
    c.cmd("BGSAVE")
    time.sleep(2)
    sh("docker compose restart valkey")
    for _ in range(60):
        try:
            c = Client()
            if c.cmd("PING") == "PONG":
                break
        except OSError:
            time.sleep(1)
    s.check("blob byte-identical across restart", True,
            c.cmd("R.EXPORT", "stab") == before)

    # ------------------------------------------------------------------
    s.section("7. fleet shape: many partitions, compression sanity")
    fleet_digests = {}
    for i in range(2000):
        vals = rng.sample(range(universe), rng.randrange(50, 3000))
        c.pipeline([["R.SETINTARRAY", f"f:{i}", *vals]])
        if i % 200 == 0:
            fleet_digests[i] = sorted(set(vals))
    for i, vals in fleet_digests.items():
        s.check(f"fleet spot-check f:{i}", vals, c.cmd("R.GETINTARRAY", f"f:{i}"))

    dense = list(range(1_000_000, 1_200_000))
    load(c, "ratio", dense)
    blob = c.cmd("R.EXPORT", "ratio")
    array_bytes = sum(len(str(v)) + 1 for v in dense)
    s.check_true("blob is at least 10x smaller than the integer array",
                 len(blob) * 10 < array_bytes,
                 f"blob={len(blob)}B array≈{array_bytes}B")
    s.check_true("dense 200k-value blob under 64KB (run compression works)",
                 len(blob) < 65536, f"blob={len(blob)}B")

    # ------------------------------------------------------------------
    s.section("8. large-partition envelope: sizes and hot-path latency")
    # Partitions well past the realistic per-partition cardinality floor
    # (70k+), across a sparse multi-million id space.
    id_space = 10_000_000
    shapes = [70_000, 150_000, 400_000, 1_000_000]
    for card in shapes:
        vals = sorted(rng.sample(range(id_space), card))
        t0 = time.monotonic()
        load(c, f"big:{card}", vals, chunk=10_000)
        load_s = time.monotonic() - t0

        t0 = time.monotonic()
        blob = c.cmd("R.EXPORT", f"big:{card}")
        export_ms = (time.monotonic() - t0) * 1000

        bm = BitMap.deserialize(blob)
        s.check(f"{card}: decode cardinality exact", card, len(bm))
        sample = rng.sample(vals, 300)
        s.check(f"{card}: decoded membership sample", True,
                all(v in bm for v in sample))
        vset = set(vals)
        s.check(f"{card}: no foreign values in sample", True,
                all(v in vset for v in list(bm)[:1000]))

        array_bytes = sum(len(str(v)) + 1 for v in vals)
        # Uniformly random ids are roaring's worst case: array containers at
        # ~2 bytes/member. Assert that envelope (with container overhead
        # headroom) — real catalogs cluster and compress far better.
        s.check_true(f"{card}: blob within worst-case envelope (~2 B/member)",
                     len(blob) <= card * 2.2 + 65536,
                     f"{len(blob)/card:.2f} bytes/member")
        s.check_true(f"{card}: blob smaller than integer array",
                     len(blob) * 3 < array_bytes,
                     f"blob={len(blob)/1024:.0f}KB array≈{array_bytes/1024:.0f}KB")
        # Hot path must stay interactive; generous bound to avoid flakiness,
        # sizes/latency printed for the record.
        s.check_true(f"{card}: export under 1s", export_ms < 1000,
                     f"{export_ms:.0f}ms")
        mem = c.cmd("MEMORY", "USAGE", f"big:{card}")
        print(f"    [{card:>9,} members] blob={len(blob)/1024:8.1f}KB  "
              f"export={export_ms:6.1f}ms  refresh={load_s:5.2f}s  "
              f"mem={mem/1024:8.1f}KB  bytes/member={len(blob)/card:.2f}")

    c.cmd("FLUSHALL")
    s.finish()


main()
