"""Suite 15 — Streamed array replies: exact length, exact elements, no desync.

GETINTARRAY, RANGEINTARRAY and GETBITS write their array header first and
then stream one element per value straight into the reply buffer. If the
element count ever differs from the header, the connection desynchronizes:
every later reply on it is read as part of the wrong command. This suite
pins the header to the elements and the elements to a plain Python model:

  * RANGEINTARRAY pages at every edge (start 0, 1, card-1, card, card+1,
    end past the end, inverted, single element, widths up to the 100M cap)
    on multi-container keys, R64 keys spanning many 2^32 sub-bitmaps, and
    values at and above 2^63 (bulk strings in the middle of an array);
  * GETINTARRAY of keys up to a million values; GETBITS with 100k offsets
    and on a missing key;
  * each reply followed by a PING sentinel in the same pipeline, over
    RESP2 and RESP3, inside MULTI/EXEC and through Lua.

Escape class targeted: off-by-one counts in streamed replies, which corrupt
the connection rather than one reply, and pagination drift across
container and sub-bitmap boundaries.
"""

import bisect
import random
import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client, ReplyError

CAP = 100_000_000


def load(c, P, key, vals, chunk=20_000):
    c.cmd("DEL", key)
    for i in range(0, len(vals), chunk):
        c.cmd(f"{P}.SETINTARRAY" if i == 0 else f"{P}.APPENDINTARRAY", key, *vals[i:i + chunk])


def as_int(x):
    return int(x) if isinstance(x, bytes) else x


def page(vals, start, end):
    if start > end:
        return "empty"
    if end - start + 1 > CAP:
        return "too large"
    return vals[start:end + 1]


def main():
    s = Suite("15 streamed replies")
    c = Client(timeout=120)
    c.cmd("FLUSHALL")
    rng = random.Random(0x57EA)

    keysets = {
        "R": {
            "sparse": sorted(rng.sample(range(1 << 32), 50_000)),
            "dense": sorted(set(range(1_000_000, 1_400_000)) | set(rng.sample(range(1 << 26), 20_000))),
            "edges": [0, 1, 65535, 65536, 65537, (1 << 31), (1 << 32) - 2, (1 << 32) - 1],
        },
        "R64": {
            "subbitmaps": sorted({(h << 32) | rng.randrange(1 << 32) for h in rng.sample(range(1 << 20), 3000)
                                  for _ in range(5)}),
            "border": sorted(set(range((5 << 32) - 3000, (5 << 32) + 3000)) | {(6 << 32) + 1, (9 << 32)}),
            "above_2_63": sorted({(1 << 63) - 2, (1 << 63) - 1, 1 << 63, (1 << 63) + 1, (1 << 64) - 2,
                                  (1 << 64) - 1, 0, 5} | {(1 << 63) + rng.randrange(1 << 40) for _ in range(5000)}),
        },
    }

    for P, sets in keysets.items():
        s.section(f"{P}: RANGEINTARRAY pages match the model")
        for name, vals in sets.items():
            load(c, P, name, vals)
            card = len(vals)
            s.check(f"{P} {name}: cardinality", card, c.cmd(f"{P}.BITCOUNT", name))
            starts = sorted({0, 1, 2, card // 2, card - 2, card - 1, card, card + 1}
                            | {rng.randrange(card) for _ in range(20)})
            widths = (0, 1, 2, 99, 4095, 4096, 65536, 70_001)
            cmds, want = [], []
            for st in starts:
                if st < 0:
                    continue
                for w in widths:
                    cmds.append([f"{P}.RANGEINTARRAY", name, st, st + w])
                    want.append(page(vals, st, st + w))
                    cmds.append(["PING"])
                    want.append("PONG")
            for st, en in ((5, 4), (card, card), (card - 1, card + CAP - 2), (0, CAP - 1), (0, CAP), (1, CAP + 1)):
                if st < 0:
                    continue
                cmds.append([f"{P}.RANGEINTARRAY", name, st, en])
                want.append(page(vals, st, en))
                cmds.append(["PING"])
                want.append("PONG")
            bad = 0
            for i in range(0, len(cmds), 40):
                for cmd, exp, got in zip(cmds[i:i + 40], want[i:i + 40], c.pipeline(cmds[i:i + 40])):
                    if exp == "empty":
                        ok = got == []
                    elif exp == "too large":
                        ok = isinstance(got, ReplyError) and "range too large" in str(got)
                    elif exp == "PONG":
                        ok = got == "PONG"
                    else:
                        ok = isinstance(got, list) and [as_int(x) for x in got] == exp
                    if not ok:
                        bad += 1
                        if bad <= 3:
                            print(f"    {cmd[:4]} -> {str(got)[:120]}")
            s.check(f"{P} {name}: {len(cmds) // 2} pages exact, stream in sync", 0, bad)

            # Values above i64::MAX are bulk strings mid-array; RESP3 too.
            got = c.cmd(f"{P}.GETINTARRAY", name)
            s.check(f"{P} {name}: GETINTARRAY exact", vals, [as_int(x) for x in got])
            s.check(f"{P} {name}: in sync after GETINTARRAY", "PONG", c.cmd("PING"))

    s.section("big replies under every reply path")
    big = sorted(rng.sample(range(1 << 30), 1_000_000))
    load(c, "R", "big", big)
    r3 = Client(timeout=120)
    r3.cmd("HELLO", "3")
    for label, conn in (("RESP2", c), ("RESP3", r3)):
        got = conn.pipeline([["R.GETINTARRAY", "big"], ["PING"],
                             ["R.RANGEINTARRAY", "big", 999_000, 1_000_500], ["PING"],
                             ["MULTI"], ["R.RANGEINTARRAY", "big", 10, 20], ["R.GETINTARRAY", "missing"],
                             ["R.GETBITS", "missing", 1, 2], ["R.GETBITS", "big", big[0], big[0] + 1],
                             ["EXEC"], ["PING"]])
        s.check(f"{label} GETINTARRAY of 1M values", big, got[0])
        s.check(f"{label} page at the tail", big[999_000:], got[2])
        s.check(f"{label} MULTI/EXEC replies", [big[10:21], [], [], [1, 0]], got[9])
        s.check(f"{label} sentinels in sync", ["PONG", "PONG", "PONG"],
                [x.decode() if isinstance(x, bytes) else x for x in (got[1], got[3], got[10])])
    lua = c.cmd("EVAL", "local t = redis.call('R.RANGEINTARRAY', KEYS[1], 0, 99999) "
                "local m = redis.call('R.GETBITS', KEYS[1], 0, ARGV[1]) return {#t, t[1], t[#t], #m, m[2]}",
                1, "big", big[5])
    s.check("Lua sees the streamed arrays as tables", [100_000, big[0], big[99_999], 2, 1], lua)
    offs = [rng.randrange(1 << 30) for _ in range(100_000)]
    bigset = set(big)
    got = c.pipeline([["R.GETBITS", "big", *offs], ["PING"]])
    s.check("GETBITS with 100k offsets", [1 if o in bigset else 0 for o in offs], got[0])
    s.check("in sync after 100k-offset GETBITS", "PONG", got[1])
    r64 = [(1 << 63) + i for i in range(0, 3000, 3)]
    load(c, "R64", "hi", r64)
    got = r3.pipeline([["R64.GETINTARRAY", "hi"], ["R64.RANGEINTARRAY", "hi", 990, 2000], ["PING"]])
    s.check("RESP3 R64 values above 2^63", r64, [as_int(x) for x in got[0]])
    s.check("RESP3 R64 tail page", r64[990:], [as_int(x) for x in got[1]])
    s.check("RESP3 in sync", "PONG", got[2])
    r3.close()

    c.cmd("FLUSHALL")
    s.finish()


main()
