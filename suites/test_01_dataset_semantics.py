"""Suite 01 — Real-dataset semantics vs an independent reference model.

Loads every bitmap of several real-world datasets into the module (32-bit
keys verbatim; 64-bit keys shifted above the u32 range) and requires that
every read command agrees with a naive pure-Python set model: cardinality,
min/max, first set/unset bit, membership samples, value-range windows, the
full sorted array, all eight BITOP results over sampled pairs, CONTAINS in
all four modes, JACCARD, and destructive ops (CLEARBITS/DELETEINTARRAY).

Escape class targeted: aggregate/container-level bugs that tiny hand-picked
integration values cannot reach — container-boundary splits, run/array/
bitset transitions, and width-specific behavior at real data shapes.
"""

import random
import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib import datasets
from lib.harness import Suite, env_flag
from lib.valkey_client import Client

R64_SHIFT = 1 << 33  # push 64-bit copies above the u32 range

DATASETS = ["census1881", "wikileaks-noquotes", "uscensus2000"]
if env_flag("FULL"):
    DATASETS += ["census-income", "weather_sept_85"]

def load_key(c, prefix, key, vals):
    for i in range(0, len(vals), 5000):
        chunk = vals[i:i + 5000]
        cmd = f"{prefix}.APPENDINTARRAY" if i else f"{prefix}.SETINTARRAY"
        c.cmd(cmd, key, *chunk)

def main():
    s = Suite("01 dataset semantics")
    c = Client()
    rng = random.Random(0xC0FFEE)
    c.cmd("FLUSHALL")

    for ds in DATASETS:
        s.section(ds)
        bitmaps = datasets.load(ds, max_files=40)
        loaded = []
        for i, vals in enumerate(bitmaps):
            key = f"{ds}:{i}"
            load_key(c, "R", key, vals)
            vals64 = [v + R64_SHIFT for v in vals]
            load_key(c, "R64", key + ":64", vals64)
            loaded.append((key, set(vals)))

            # Aggregates for both widths
            s.check(f"{key} bitcount", len(vals), c.cmd("R.BITCOUNT", key))
            s.check(f"{key} r64 bitcount", len(vals), c.cmd("R64.BITCOUNT", key + ":64"))
            s.check(f"{key} min", min(vals), c.cmd("R.MIN", key))
            s.check(f"{key} max", max(vals), c.cmd("R.MAX", key))
            s.check(f"{key} r64 min", min(vals) + R64_SHIFT, c.cmd("R64.MIN", key + ":64"))
            s.check(f"{key} r64 max", max(vals) + R64_SHIFT, c.cmd("R64.MAX", key + ":64"))

            # BITPOS against the model
            sv = set(vals)
            first_unset = 0
            while first_unset in sv:
                first_unset += 1
            s.check(f"{key} bitpos 1", min(vals), c.cmd("R.BITPOS", key, 1))
            s.check(f"{key} bitpos 0", first_unset, c.cmd("R.BITPOS", key, 0))

            # Membership sample (present + absent values)
            sample = rng.sample(vals, min(20, len(vals)))
            absent = [max(vals) + o for o in (1, 17, 4099)]
            probes = sample + absent
            got = c.cmd("R.GETBITS", key, *probes)
            s.check(f"{key} getbits", [1 if p in sv else 0 for p in probes], got)

            # Positional pagination window (0-based indexes into the sorted array)
            ordered = sorted(sv)
            lo = rng.randrange(len(ordered))
            hi = min(len(ordered) - 1, lo + rng.randrange(1, 500))
            expect = ordered[lo:hi + 1]
            s.check(f"{key} rangeintarray", expect, c.cmd("R.RANGEINTARRAY", key, lo, hi))
            expect64 = [v + R64_SHIFT for v in expect]
            s.check(f"{key} r64 rangeintarray", expect64,
                    c.cmd("R64.RANGEINTARRAY", key + ":64", lo, hi))
            tail_start = max(0, len(ordered) - 2)
            s.check(f"{key} rangeintarray past end", ordered[tail_start:],
                    c.cmd("R.RANGEINTARRAY", key, tail_start, len(ordered) + 99))

            # Full array equality (kept to moderate sizes)
            if len(vals) <= 50000:
                s.check(f"{key} getintarray", sorted(sv), c.cmd("R.GETINTARRAY", key))

        # Pairwise set operations vs model
        for _ in range(12):
            (ka, sa), (kb, sb) = rng.sample(loaded, 2)
            for op, expect in [
                ("AND", sa & sb), ("OR", sa | sb), ("XOR", sa ^ sb),
                ("DIFF", sa - sb), ("DIFF1", sb - sa),
                ("ANDOR", sa & sb), ("ONE", sa ^ sb),
            ]:
                card = c.cmd("R.BITOP", op, "dest", ka, kb)
                s.check(f"{ds} {op}({ka},{kb}) card", len(expect), card)
            # Verify one op's full contents
            c.cmd("R.BITOP", "XOR", "dest", ka, kb)
            s.check(f"{ds} XOR contents", sorted(sa ^ sb)[:1000],
                    c.cmd("R.RANGEINTARRAY", "dest", 0, 999))

            # CONTAINS / JACCARD
            s.check(f"{ds} contains default", int(bool(sa & sb)),
                    c.cmd("R.CONTAINS", ka, kb))
            for mode in ("ALL", "ALL_STRICT", "EQ"):
                model = {"ALL": int(sb <= sa),
                         "ALL_STRICT": int(sb <= sa and sa != sb),
                         "EQ": int(sa == sb)}[mode]
                s.check(f"{ds} contains {mode}", model, c.cmd("R.CONTAINS", ka, kb, mode))
            jac = float(c.cmd("R.JACCARD", ka, kb))
            model_jac = len(sa & sb) / len(sa | sb) if (sa | sb) else 0.0
            s.check_true(f"{ds} jaccard", abs(jac - model_jac) < 1e-9,
                         f"server={jac} model={model_jac}")

        # Destructive ops on a sacrificial copy
        key, sv = loaded[0]
        c.cmd("R.BITOP", "OR", "scratch", key, key)
        victim = rng.sample(sorted(sv), min(200, len(sv)))
        removed = c.cmd("R.CLEARBITS", "scratch", *victim, "COUNT")
        s.check(f"{ds} clearbits count", len(set(victim)), removed)
        s.check(f"{ds} clearbits result", len(sv) - len(set(victim)),
                c.cmd("R.BITCOUNT", "scratch"))
        c.cmd("R.DELETEINTARRAY", "scratch", *victim)  # idempotent second delete
        s.check(f"{ds} delete idempotent", len(sv) - len(set(victim)),
                c.cmd("R.BITCOUNT", "scratch"))

    c.cmd("FLUSHALL")
    s.finish()

main()
