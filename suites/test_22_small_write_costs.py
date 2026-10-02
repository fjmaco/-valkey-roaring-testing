"""Suite 22 — Small operations cost the same on small and huge keys.

A single-bit write, a short range, a point read or a ten-offset GETBITS
touches one or two containers, so its cost must not grow with the size of
the key. This suite builds an R64 key of one million containers (spread
over sixteen sub-bitmaps) and an R key of 65,536, measures each cheap
command's server time per call (INFO commandstats, reset per command) on
them and on a one-container key, and requires the ratio to stay small.

Regression guard: R64.SETRANGE once cost O(containers in the whole key):
its no-op check summed the cardinality of every sub-bitmap
(RoaringTreemap::len) and walked the sub-bitmaps from the first one. On the
one-million-container key a 5-value SETRANGE took ~0.7 ms per call where
1.1.1 took ~0.3 us.

Escape class targeted: hidden whole-key passes on hot write paths, which
benchmarks on small keys never show.
"""

import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client

MAX_RATIO = 20      # huge-key cost / small-key cost; a whole-key pass is 1000x+
CALLS = 500


def usec_per_call(c, cmd):
    for line in c.cmd("INFO", "commandstats").decode().split("\r\n"):
        if line.startswith(f"cmdstat_{cmd}:"):
            return float(line.split("usec_per_call=")[1].split(",")[0])
    return 0.0


def main():
    s = Suite("22 small write costs")
    c = Client(timeout=300)
    c.cmd("FLUSHALL")
    for P, n in (("R64", 1_000_000), ("R", 65_536)):
        vals = [i << 16 for i in range(n)]               # one container per value
        for j in range(0, n, 50_000):
            c.cmd(f"{P}.SETINTARRAY" if j == 0 else f"{P}.APPENDINTARRAY", "huge", *vals[j:j + 50_000])
        ops = {
            "SETBIT": lambda k, i: [f"{P}.SETBIT", k, (i << 16) + 9, 1],
            "GETBIT": lambda k, i: [f"{P}.GETBIT", k, i << 16],
            "GETBITS": lambda k, i: [f"{P}.GETBITS", k, *[(i << 16) + j for j in range(10)]],
            "CLEARBITS": lambda k, i: [f"{P}.CLEARBITS", k, (i << 16) + 9],
            "APPENDINTARRAY": lambda k, i: [f"{P}.APPENDINTARRAY", k, (i << 16) + 11, (i << 16) + 12],
            "DELETEINTARRAY": lambda k, i: [f"{P}.DELETEINTARRAY", k, (i << 16) + 11],
            "SETRANGE": lambda k, i: [f"{P}.SETRANGE", k, (i << 16) + 20, (i << 16) + 25],
            "MIN": lambda k, i: [f"{P}.MIN", k],
            "MAX": lambda k, i: [f"{P}.MAX", k],
        }
        s.section(f"{P}: one-container key vs {n:,} containers")
        for name, f in ops.items():
            cost = {}
            for key, span in (("small", 1), ("huge", n)):
                c.cmd("DEL", "small")
                c.cmd(f"{P}.SETINTARRAY", "small", 0)
                c.cmd("CONFIG", "RESETSTAT")
                c.pipeline([f(key, (i * 7919) % span) for i in range(CALLS)])
                cost[key] = usec_per_call(c, f"{P}.{name}")
            ratio = cost["huge"] / max(cost["small"], 0.05)
            label = f"{P}.{name}: huge/small cost {cost['huge']:.2f}/{cost['small']:.2f} us"
            if P == "R64" and name == "SETRANGE":
                label = "regression guard: " + label
            s.check_true(label, ratio < MAX_RATIO, f"ratio {ratio:.0f}x")
        c.cmd("DEL", "huge", "small")
    c.cmd("FLUSHALL")
    s.finish()


main()
