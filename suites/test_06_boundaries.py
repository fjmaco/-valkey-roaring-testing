"""Suite 06 — Type and container boundary sweeps.

Roaring bitmaps switch container representations at 16-bit chunk borders
(multiples of 65536) and by density (array <-> bitset <-> run). This suite
plants values and ranges straddling every interesting border — chunk edges,
u32::MAX, 2^32, 2^63, u64::MAX — and checks exact behavior against the
reference model, before AND after R.OPTIMIZE (representation change must
never change contents). STAT output is sanity-checked for both widths.

Escape class targeted: off-by-one bugs at container seams and extreme
values, and any operation whose result depends on physical representation.
"""

import json
import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client

U32, U64 = (1 << 32) - 1, (1 << 64) - 1


def main():
    s = Suite("06 boundaries")
    c = Client()
    c.cmd("FLUSHALL")

    s.section("chunk-border values (multiples of 65536)")
    border_vals = []
    for k in range(1, 6):
        border_vals += [k * 65536 - 1, k * 65536, k * 65536 + 1]
    border_vals += [0, 1, U32 - 1, U32]
    c.cmd("R.SETINTARRAY", "b", *sorted(border_vals))
    s.check("border contents", sorted(set(border_vals)), c.cmd("R.GETINTARRAY", "b"))
    s.check("border count", len(set(border_vals)), c.cmd("R.BITCOUNT", "b"))
    for v in border_vals:
        s.check(f"getbit {v}", 1, c.cmd("R.GETBIT", "b", v))
    c.cmd("R.OPTIMIZE", "b")
    s.check("contents stable across OPTIMIZE", sorted(set(border_vals)),
            c.cmd("R.GETINTARRAY", "b"))

    s.section("ranges straddling chunk borders (SETRANGE is end-exclusive)")
    c.cmd("R.SETRANGE", "span", 65530, 131080)  # [65530, 131080)
    s.check("span count", 131080 - 65530, c.cmd("R.BITCOUNT", "span"))
    s.check("span edges", [65530, 131079], [c.cmd("R.MIN", "span"), c.cmd("R.MAX", "span")])
    s.check("span bitpos 0", 0, c.cmd("R.BITPOS", "span", 0))
    c.cmd("R.SETRANGE", "span", 0, 65530)  # extend to contiguous [0, 131080)
    s.check("contiguous bitpos 0", 131080, c.cmd("R.BITPOS", "span", 0))

    s.section("u32 top edge")
    c.cmd("DEL", "top")
    c.cmd("R.SETRANGE", "top", U32 - 5, U32)   # [U32-5, U32) — end-exclusive
    s.check("top range", list(range(U32 - 5, U32)), c.cmd("R.GETINTARRAY", "top"))
    c.cmd("R.SETBIT", "top", U32, 1)           # the max bit itself via SETBIT
    s.check("max bit set", 1, c.cmd("R.GETBIT", "top", U32))
    card = c.cmd("R.BITOP", "NOT", "topnot", "top")
    s.check("NOT of top range card", U32 + 1 - 6, card)
    s.check("NOT excludes top values", [0] * 6,
            c.cmd("R.GETBITS", "topnot", *range(U32 - 5, U32 + 1)))
    s.check("NOT includes below", 1, c.cmd("R.GETBIT", "topnot", U32 - 6))
    err = c.cmd_err("R.SETBIT", "top", 1 << 32, 1)
    s.check_true("u32 overflow rejected", "out of range" in err, err)

    s.section("u64 giants: 2^32, 2^63, u64::MAX")
    giants = [(1 << 32) - 1, 1 << 32, (1 << 32) + 1, (1 << 63) - 1, 1 << 63, U64 - 1, U64]
    c.cmd("R64.SETINTARRAY", "g", *giants)
    s.check("giants count", len(giants), c.cmd("R64.BITCOUNT", "g"))
    got = c.cmd("R64.GETINTARRAY", "g")
    got = [int(x) if isinstance(x, (bytes, str)) else x for x in got]
    s.check("giants contents (mixed int/string replies)", giants, got)
    s.check("giants max is decimal string", str(U64).encode(), c.cmd("R64.MAX", "g"))
    s.check("giant getbit u64::MAX", 1, c.cmd("R64.GETBIT", "g", U64))
    c.cmd("R64.CLEARBITS", "g", U64)
    s.check("clear u64::MAX", 0, c.cmd("R64.GETBIT", "g", U64))
    err = c.cmd_err("R64.SETBIT", "g", "18446744073709551616", 1)  # 2^64
    s.check_true("u64 overflow rejected", "invalid" in err or "out of range" in err, err)

    s.section("bit-array codec round trip at borders")
    bits = "1" + "0" * 65534 + "11"  # length 65537, crosses a chunk border
    c.cmd("R.SETBITARRAY", "ba", bits)
    s.check("bitarray contents", [0, 65535, 65536], c.cmd("R.GETINTARRAY", "ba"))
    s.check("bitarray round trip", bits.encode(), c.cmd("R.GETBITARRAY", "ba"))

    s.section("STAT structural sanity (both widths)")
    stat = json.loads(c.cmd("R.STAT", "b", "JSON"))
    s.check("stat cardinality", str(len(set(border_vals))), stat["cardinality"])
    s.check_true("stat containers > 0", int(stat["number_of_containers"]) > 0, stat)
    stat64 = json.loads(c.cmd("R.STAT", "g", "JSON"))
    s.check("stat64 type", "bitmap64", stat64["type"])
    s.check_true("stat64 containers > 0", int(stat64["number_of_containers"]) > 0, stat64)
    s.check("stat64 max", str(U64 - 1), stat64["max_value"])

    c.cmd("FLUSHALL")
    s.finish()


main()
