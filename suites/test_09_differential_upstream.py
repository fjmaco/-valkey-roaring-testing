"""Suite 09 — Differential testing against the real redis-roaring module.

Runs the published aviggiano/redis-roaring image next to valkey-roaring and
drives both with identical seeded-random command sequences over the shared
command surface, requiring reply-for-reply agreement, error wording
included.

Then the exact-reply rows: one row per edge case (argument grammars, check
order, error texts, case-sensitive tokens, JACCARD formatting, GETBITARRAY
and STAT layouts, 64-bit positions), each compared as the raw reply bytes
under RESP2 and RESP3, so a bulk string never passes for a simple string
or a double.

Known, documented divergences are not sent to upstream; their rows assert
valkey-roaring's intended reply instead (DIVERGENCES below): upstream
bugs (an inserted bit for SETBIT value 0 on a missing key, NOT at
4294967295, a doubled WRONGTYPE reply, an out-of-memory error for the
full-width R64.RANGEINTARRAY) and the materialization guards, where
upstream hangs, crashes or overflows. One battery-level sidestep: every
key is primed at battery start, so SETBIT never takes upstream's
missing-key path. EXPORT/IMPORT are valkey-roaring additions (suites 12,
13 and 16).

Escape class targeted: any semantic drift from upstream that hand-written
parity tests didn't think to encode — this is the promise "applications
built against redis-roaring's commands work here unchanged" under fire.
"""

import random
import subprocess
import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.raw_resp import RawClient
from lib.valkey_client import Client, ReplyError

DOMAIN = 1 << 20          # value domain small enough to collide often
KEYS = [f"k{i}" for i in range(8)]
OPS = ["AND", "OR", "XOR", "ANDOR", "DIFF", "DIFF1", "ONE"]


def start_upstream():
    subprocess.run("docker rm -f vt-upstream", shell=True, capture_output=True)
    subprocess.run(
        "docker run -d --name vt-upstream -p 6392:6379 aviggiano/redis-roaring:latest",
        shell=True, check=True, capture_output=True)
    for _ in range(30):
        try:
            c = Client(port=6392, timeout=3)
            if c.cmd("PING") == "PONG":
                return c
        except OSError:
            time.sleep(1)
    raise RuntimeError("upstream image did not start")


def normalize(reply):
    if isinstance(reply, ReplyError):
        return ("err", str(reply))
    if isinstance(reply, bytes):
        return reply
    if isinstance(reply, list):
        return [normalize(r) for r in reply]
    return reply


def gen_commands(rng, prefix):
    def key():
        return rng.choice(KEYS)

    def val():
        return rng.randrange(DOMAIN)

    cmds = []
    for _ in range(2500):
        pick = rng.randrange(20)
        if pick < 4:
            cmds.append([f"{prefix}.SETBIT", key(), val(), rng.randrange(2)])
        elif pick < 6:
            cmds.append([f"{prefix}.GETBIT", key(), val()])
        elif pick < 7:
            cmds.append([f"{prefix}.SETINTARRAY", key(),
                         *[val() for _ in range(rng.randrange(1, 40))]])
        elif pick < 8:
            cmds.append([f"{prefix}.APPENDINTARRAY", key(),
                         *[val() for _ in range(rng.randrange(1, 40))]])
        elif pick < 9:
            cmds.append([f"{prefix}.DELETEINTARRAY", key(),
                         *[val() for _ in range(rng.randrange(1, 10))]])
        elif pick < 10:
            cmds.append([f"{prefix}.GETINTARRAY", key()])
        elif pick < 11:
            lo = val()
            cmds.append([f"{prefix}.RANGEINTARRAY", key(), lo,
                         lo + rng.randrange(1 << 16)])
        elif pick < 12:
            cmds.append([f"{prefix}.BITCOUNT", key()])
        elif pick < 13:
            cmds.append([f"{prefix}.BITPOS", key(), rng.randrange(2)])
        elif pick < 14:
            cmds.append([f"{prefix}.MIN" if rng.randrange(2) else f"{prefix}.MAX", key()])
        elif pick < 15:
            cmds.append([f"{prefix}.GETBITS", key(),
                         *[val() for _ in range(rng.randrange(1, 8))]])
        elif pick < 16:
            cmds.append([f"{prefix}.CLEARBITS", key(),
                         *[val() for _ in range(rng.randrange(1, 8))]])
        elif pick < 17:
            op = rng.choice(OPS)
            n = 1 if rng.randrange(4) == 0 else rng.randrange(2, 4)
            cmds.append([f"{prefix}.BITOP", op, key(),
                         *[key() for _ in range(n)]])
        elif pick < 18:
            args = [f"{prefix}.BITOP", "NOT", key(), key()]
            if rng.randrange(2):
                args.append(val())
            cmds.append(args)
        elif pick < 19:
            mode = rng.choice(["NONE", "ALL", "ALL_STRICT", "EQ"])
            cmds.append([f"{prefix}.CONTAINS", key(), key(), mode])
        else:
            cmds.append([f"{prefix}.SETRANGE", key(), (v := val()), v + rng.randrange(500)])
    return cmds


def large_key_commands(rng, prefix):
    """Deterministic battery over multi-container keys: the small random
    battery above rarely leaves one container, so this one drives paging,
    full-array replies, membership probes and set algebra across hundreds of
    containers, dense runs included."""
    def ids(n, span):
        return sorted(rng.sample(range(span), n))

    sets = {
        "L0": ids(60_000, 1 << 24),                          # sparse arrays
        "L1": ids(60_000, 1 << 24),
        "L2": sorted(set(ids(20_000, 1 << 24)) | set(range(500_000, 700_000))),
    }
    cmds = [[f"{prefix}.SETINTARRAY", k, *v] for k, v in sets.items()]
    cmds.append([f"{prefix}.SETRANGE", "L3", 0, 300_000])   # dense prefix
    cmds.append([f"{prefix}.APPENDINTARRAY", "L3", *ids(5_000, 1 << 24)])
    for k in ("L0", "L2", "L3"):
        card = len(sets.get(k, ())) or 305_000
        for start in (0, 1, card // 2, card - 1000, card - 1, card, card + 5):
            cmds.append([f"{prefix}.RANGEINTARRAY", k, max(start, 0), max(start, 0) + 999])
        cmds.append([f"{prefix}.GETINTARRAY", k])
        cmds.append([f"{prefix}.BITCOUNT", k])
        cmds.append([f"{prefix}.BITPOS", k, 0])
        cmds.append([f"{prefix}.BITPOS", k, 1])
        cmds.append([f"{prefix}.MIN", k])
        cmds.append([f"{prefix}.MAX", k])
        cmds.append([f"{prefix}.GETBITS", k, *[rng.randrange(1 << 24) for _ in range(3000)]])
    for a, b in (("L0", "L1"), ("L0", "L2"), ("L2", "L3"), ("L3", "L3")):
        cmds.append([f"{prefix}.JACCARD", a, b])
        cmds.append([f"{prefix}.CONTAINS", a, b])
        for mode in ("ALL", "ALL_STRICT", "EQ"):
            cmds.append([f"{prefix}.CONTAINS", a, b, mode])
    for op in OPS:
        for srcs in (["L0", "L1"], ["L0", "L1", "L2"], ["L2", "L3", "L0", "L1"]):
            cmds.append([f"{prefix}.BITOP", op, "D", *srcs])
            cmds.append([f"{prefix}.GETINTARRAY", "D"])
    # destination aliasing a source
    cmds.append([f"{prefix}.BITOP", "OR", "L1", "L1", "L0"])
    cmds.append([f"{prefix}.GETINTARRAY", "L1"])
    cmds.append([f"{prefix}.BITOP", "NOT", "N", "L0"])
    cmds.append([f"{prefix}.BITCOUNT", "N"])
    cmds.append([f"{prefix}.RANGEINTARRAY", "N", 1_000_000, 1_000_999])
    cmds.append([f"{prefix}.DIFF", "L2", "L2", "L0"])
    cmds.append([f"{prefix}.GETINTARRAY", "L2"])
    # SETBITARRAY reads raw bytes: byte i == '1' sets bit i, whatever the
    # surrounding bytes (non-UTF-8 included).
    for raw in (b"\xff1", b"1\xc3\xa91", b"\x80\x80" + b"01" * 40, bytes(range(256)) + b"1"):
        cmds.append([f"{prefix}.SETBITARRAY", "BA", raw])
        cmds.append([f"{prefix}.GETINTARRAY", "BA"])
        cmds.append([f"{prefix}.GETBITARRAY", "BA"])
    return cmds


# ---------------------------------------------------------------------------
# Exact-reply rows
# ---------------------------------------------------------------------------
NUMS = ["5", "+5", "005", "0", "-0", "-1", " 5", "5 ", "", "abc", "1e3", "0x10", "1.0",
        "4294967295", "4294967296", "9223372036854775807", "9223372036854775808",
        "18446744073709551615", "18446744073709551616", "\uff15"]
BOOLS = ["0", "1", "2", "+1", "01", "-0", "abc", "", "1 "]
# Upstream materializes these 64-bit ranges (hours of CPU, or an OOM kill):
# their rows live in DIVERGENCES, against valkey-roaring's guard.
HUGE_64 = {"9223372036854775807", "9223372036854775808", "18446744073709551615"}

ROW_SETUP = [
    ["SET", "str", "x"],
    ["R.SETINTARRAY", "r", 1, 2, 3, 100], ["R64.SETINTARRAY", "r64", 1, 2, 3, 100],
    ["R.SETINTARRAY", "e", 1], ["R.CLEAR", "e"],
    ["R64.SETINTARRAY", "e64", 1], ["R64.CLEAR", "e64"],
    ["R.SETINTARRAY", "q", 1, 2], ["R64.SETINTARRAY", "q64", 1, 2],
    # JACCARD ratios: exact decimals, %.17g fractions, exponent forms.
    ["R.SETINTARRAY", "j1", 0], ["R.SETRANGE", "j3", 0, 3], ["R.SETRANGE", "j7", 0, 7],
    ["R.SETRANGE", "j8", 0, 8], ["R.SETRANGE", "j30k", 0, 30000],
    ["R.SETRANGE", "j3m", 0, 3000000], ["R.SETRANGE", "j1g", 0, 1000000000],
    ["R64.SETINTARRAY", "J1", 0], ["R64.SETRANGE", "J3", 0, 3],
    ["R64.SETRANGE", "J30k", 0, 30000], ["R64.SETRANGE", "J1g", 0, 1000000000],
    # STAT shapes whose container encodings both libraries agree on.
    ["R.SETRANGE", "s1", 10, 21], ["R.SETRANGE", "s2", 0, 100000],
    ["R.SETRANGE", "s3", 5, 70000], ["R.APPENDINTARRAY", "s3", 200000, 300000],
    ["R.SETINTARRAY", "s4", *range(0, 10000, 2)], ["R.SETINTARRAY", "s5", *range(5000)],
    ["R.SETINTARRAY", "s6", *range(5000)], ["R.OPTIMIZE", "s6"],
    ["R.SETBITARRAY", "s7", "0111111111111111111111111111111110001"],
    ["R.SETFULL", "s8"], ["R.SETRANGE", "s11", 0, 65536], ["R.DELETEINTARRAY", "s11", 7],
    ["R.SETINTARRAY", "s9", 1, 2, 3], ["R.BITOP", "NOT", "s10", "s9", "70000"],
    ["R.OPTIMIZE", "s10"],
    ["R64.SETRANGE", "t1", 4294967290, 4294967306],
    ["R64.SETINTARRAY", "t2", 1, 5000000000, 18446744073709551615],
    ["R64.SETRANGE", "t3", 0, 100000], ["R64.OPTIMIZE", "t3"],
    ["R64.SETINTARRAY", "t4", *range(5000)],
]


def row_cases():
    """Every exact-reply row, as a command."""
    out = []
    for P, key, e in (("R", "r", "e"), ("R64", "r64", "e64")):
        q = "q" if P == "R" else "q64"
        nums = NUMS if P == "R" else [n for n in NUMS if n not in HUGE_64]
        for k in (key, "missing", "str"):
            for n in NUMS:
                out += [[f"{P}.GETBIT", k, n], [f"{P}.GETBITS", k, "1", n],
                        [f"{P}.RANGEINTARRAY", k, n, "10"]]
                if not (P == "R64" and k == key and n == "18446744073709551615"):
                    out.append([f"{P}.RANGEINTARRAY", k, "0", n])
            for n in NUMS[:8]:
                out.append([f"{P}.SETBIT", f"{k}sb" if k != "str" else k, n, "1"])
            for b in BOOLS:
                out.append([f"{P}.BITPOS", k, b])
                # SETBIT 0 on a missing key is a documented divergence.
                if k != "missing":
                    out.append([f"{P}.SETBIT", k, "7", b])
        for n in NUMS:
            out += [[f"{P}.SETINTARRAY", "si", "1", n], [f"{P}.APPENDINTARRAY", "ai", n],
                    [f"{P}.DELETEINTARRAY", key, n], [f"{P}.DELETEINTARRAY", "missing_del", n],
                    [f"{P}.CLEARBITS", key, n], [f"{P}.CLEARBITS", "missing", n],
                    [f"{P}.CLEARBITS", key, n, "COUNT"], [f"{P}.CLEARBITS", key, n, "count"],
                    [f"{P}.SETRANGE", "sr", n, "10"]]
        for n in nums:
            out.append([f"{P}.SETRANGE", "sr", "0", n])
            if not (P == "R" and n == "4294967295"):
                out.append([f"{P}.BITOP", "NOT", "dn", key, n])
        out += [
            [f"{P}.SETRANGE", "sr", "10", "5"], [f"{P}.SETRANGE", "str", "abc", "1"],
            [f"{P}.SETINTARRAY", "str", "abc"], [f"{P}.APPENDINTARRAY", "str", "abc"],
            [f"{P}.DELETEINTARRAY", "str", "abc"], [f"{P}.CLEARBITS", "str", "abc"],
            [f"{P}.SETBITARRAY", "str", "01"],
            [f"{P}.GETBITARRAY", key], [f"{P}.GETBITARRAY", e],
            [f"{P}.GETBITARRAY", "missing"], [f"{P}.GETBITARRAY", "str"],
            [f"{P}.GETINTARRAY", key], [f"{P}.GETINTARRAY", e],
            [f"{P}.GETINTARRAY", "missing"], [f"{P}.GETINTARRAY", "str"],
            [f"{P}.OPTIMIZE", "missing"], [f"{P}.OPTIMIZE", key], [f"{P}.OPTIMIZE", key, "MEM"],
            [f"{P}.OPTIMIZE", key, "mem"], [f"{P}.OPTIMIZE", "str"],
            [f"{P}.JACCARD", e, e], [f"{P}.JACCARD", key, key], [f"{P}.JACCARD", key, q],
            [f"{P}.JACCARD", key, e], [f"{P}.JACCARD", "missing", key], [f"{P}.JACCARD", key, "str"],
            [f"{P}.CONTAINS", key, q], [f"{P}.CONTAINS", key, key, "ALL"],
            [f"{P}.CONTAINS", key, q, "ALL_STRICT"], [f"{P}.CONTAINS", key, key, "EQ"],
            [f"{P}.CONTAINS", key, key, "all"], [f"{P}.CONTAINS", key, key, "Eq"],
            [f"{P}.CONTAINS", key, key, "NONE"], [f"{P}.CONTAINS", key, key, "xyz"],
            [f"{P}.CONTAINS", key, key, "x" * 400], [f"{P}.CONTAINS", key, key, "ALL\x00junk"],
            [f"{P}.CONTAINS", key, key, b"\xff\xfeALL"], [f"{P}.CONTAINS", key, key, "a\r\nb"],
            [f"{P}.CONTAINS", key, key, "bad\x00\xff"], [f"{P}.CONTAINS", key, key, ""],
            [f"{P}.CONTAINS", key, key, "abc\n"], [f"{P}.CONTAINS", key, key, "abc\r\n"],
            [f"{P}.CONTAINS", key, key, "\r"], [f"{P}.CONTAINS", key, key, "a\r\nb\n"],
            [f"{P}.CONTAINS", key, key, "x" * 226 + "y\n\n"],
            [f"{P}.CONTAINS", "missing", key, "ALL"], [f"{P}.CONTAINS", key, "str"],
            [f"{P}.BITOP", "and", "d", key, key], [f"{P}.BITOP", "Or", "d", key, key],
            [f"{P}.BITOP", "not", "d", key], [f"{P}.BITOP", "AND", "d", key],
            [f"{P}.BITOP", "FOO", "d", key, key], [f"{P}.BITOP", "AND", "str", key, key],
            [f"{P}.BITOP", "AND", "str", key, "str"], [f"{P}.BITOP", "NOT", "d", "str"],
            [f"{P}.BITOP", "NOT", "str", key], [f"{P}.BITOP", "NOT", "str", key, "abc"],
            [f"{P}.BITOP", "NOT", "d", key, "1", "2"], [f"{P}.BITOP", "NOT", "d", "missing"],
            [f"{P}.BITOP", "NOT", "d", "missing", "3"], [f"{P}.BITOP", "OR", "d", key, q, "missing"],
            [f"{P}.DIFF", "str", "missing", key], [f"{P}.DIFF", "d", "missing", key],
            [f"{P}.DIFF", "d", key, "missing"], [f"{P}.DIFF", "d", key, "str"],
            [f"{P}.SETFULL", key], [f"{P}.SETFULL", "str"],
            [f"{P}.CLEAR", "missing"], [f"{P}.CLEAR", "str"], [f"{P}.BITCOUNT", "str"],
            [f"{P}.MIN", "str"], [f"{P}.MIN", e], [f"{P}.MAX", e],
            ["R.STAT", key], ["R.STAT", key, "JSON"], ["R.STAT", key, "json"],
            ["R.STAT", key, "TEXT"], ["R.STAT", e], ["R.STAT", e, "JSON"],
            ["R.STAT", "missing"], ["R.STAT", "str"],
        ]
    for a, b in (("j1", "j3"), ("j1", "j7"), ("j1", "j8"), ("j3", "j8"), ("j1", "j30k"),
                 ("j1", "j3m"), ("j1", "j1g"), ("j3", "j1g"), ("j30k", "j3m"), ("j7", "j3m")):
        out.append(["R.JACCARD", a, b])
    for a, b in (("J1", "J3"), ("J1", "J30k"), ("J1", "J1g"), ("J3", "J1g")):
        out.append(["R64.JACCARD", a, b])
    for k in ("s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11",
              "t1", "t2", "t3", "t4"):
        out += [["R.STAT", k], ["R.STAT", k, "JSON"]]
    out += [
        ["R.RANGEINTARRAY", "r", "0", "4294967295"],
        ["R.RANGEINTARRAY", "r", "1", "4294967295"],
        ["R.RANGEINTARRAY", "r", "0", "4294967294"],
        ["R64.RANGEINTARRAY", "r64", "9223372036854775808", "18446744073709551615"],
        ["R64.RANGEINTARRAY", "r64", "9223372036854775808", "9223372036854775810"],
        ["R64.RANGEINTARRAY", "missing", "0", "18446744073709551615"],
        ["R64.SETBIT", "big", "18446744073709551615", "1"], ["R64.MAX", "big"],
        ["R64.MIN", "big"], ["R64.BITPOS", "big", "1"], ["R64.BITPOS", "big", "0"],
        ["R64.GETINTARRAY", "big"], ["R64.BITCOUNT", "big"],
        ["R64.RANGEINTARRAY", "big", "0", "0"],
        ["R.GETINTARRAY", "s11"], ["R.GETBITARRAY", "s7"],
    ]
    return out


WRONGTYPE = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
RANGE_100M = b"-Roaring: range too large: maximum 100000000 elements\r\n"
RANGE_2_38 = b"-Roaring: range too large: maximum 274877906944 elements\r\n"

# (label, setup, command, valkey-roaring's reply bytes): never sent upstream.
DIVERGENCES = [
    ("upstream bug: SETBIT value 0 on a missing key sets the bit there",
     [], ["R.SETBIT", "nb", "7", "0"], b":0\r\n"),
    ("  ... and the bit stays clear", [], ["R.GETBIT", "nb", "7"], b":0\r\n"),
    ("  ... the key exists, empty", [], ["R.BITCOUNT", "nb"], b":0\r\n"),
    ("upstream bug: NOT up to 4294967295 overflows to a copy of the source",
     [["R.SETINTARRAY", "dv", 1, 2, 3, 100]], ["R.BITOP", "NOT", "dvn", "dv", "4294967295"],
     b":4294967292\r\n"),
    ("upstream bug: variadic BITOP with a wrong-type source replies twice",
     [["SET", "dvstr", "x"]], ["R.BITOP", "AND", "dvd", "dv", "dvstr"], WRONGTYPE),
    ("upstream: full-width R64.RANGEINTARRAY replies ERR out of memory",
     [["R64.SETINTARRAY", "dv64", 1, 2, 18446744073709551615]],
     ["R64.RANGEINTARRAY", "dv64", "0", "18446744073709551615"],
     b"*3\r\n:1\r\n:2\r\n$20\r\n18446744073709551615\r\n"),
    ("guard: R64.SETFULL (2^64 values) refused, upstream allocates until killed",
     [], ["R64.SETFULL", "dvfull"], RANGE_2_38),
    ("guard: R64.SETRANGE over 2^38 values refused",
     [], ["R64.SETRANGE", "dvsr", "0", "274877906945"], RANGE_2_38),
    ("guard: R64.SETRANGE to the top of the space refused",
     [], ["R64.SETRANGE", "dvsr", "9223372036854775808", "18446744073709551615"], RANGE_2_38),
    ("guard: R64.BITOP NOT with last past 2^38 refused",
     [], ["R64.BITOP", "NOT", "dvn64", "dvmissing", "274877906944"], RANGE_2_38),
    ("guard: R64.BITOP NOT up to 2^64 (upstream's last + 1 wraps to a copy) refused",
     [], ["R64.BITOP", "NOT", "dvn64", "dv64"], RANGE_2_38),
    ("guard: a refused NOT leaves the destination alone", [], ["EXISTS", "dvn64"], b":0\r\n"),
    ("guard: R64.BITOP NOT of a full 32-bit sub-bitmap works",
     [], ["R64.BITOP", "NOT", "dvn32", "dvmissing", "4294967295"], b":4294967296\r\n"),
    ("guard: GETINTARRAY past 100M values refused (upstream streams 2^32 values)",
     [["R.SETFULL", "dvf"]], ["R.GETINTARRAY", "dvf"], RANGE_100M),
    ("guard: full-width RANGEINTARRAY past 100M values refused",
     [], ["R.RANGEINTARRAY", "dvf", "0", "4294967295"], RANGE_100M),
    ("guard: GETBITARRAY of a 2^32-bit string refused (upstream crashes)",
     [], ["R.GETBITARRAY", "dvf"], RANGE_100M),
    ("guard: R.SETFULL still works", [], ["R.BITCOUNT", "dvf"], b":4294967296\r\n"),
    ("guard: the server is still up", [], ["PING"], b"+PONG\r\n"),
]


def run_battery(c, cmds):
    out = []
    for i in range(0, len(cmds), 100):
        out.extend(normalize(r) for r in c.pipeline(cmds[i:i + 100]))
    return out


def main():
    s = Suite("09 differential vs upstream")
    ours = Client()
    ours.cmd("FLUSHALL")
    theirs = start_upstream()
    theirs.cmd("FLUSHALL")

    for prefix in ("R", "R64"):
        for seed in range(2):
            rng = random.Random(0xD1FF + seed)
            cmds = gen_commands(rng, prefix)
            ours.cmd("FLUSHALL")
            theirs.cmd("FLUSHALL")
            # Prime all keys so SETBIT never takes the missing-key path,
            # where upstream has a known set-regardless-of-value bug.
            for k in KEYS:
                ours.cmd(f"{prefix}.SETINTARRAY", k, 0)
                theirs.cmd(f"{prefix}.SETINTARRAY", k, 0)
            a = run_battery(ours, cmds)
            b = run_battery(theirs, cmds)
            mismatches = [
                (i, cmds[i], x, y) for i, (x, y) in enumerate(zip(a, b)) if x != y
            ]
            for i, cmd, x, y in mismatches[:10]:
                print(f"  DIVERGE #{i} {cmd}\n    ours:     {x!r}\n    upstream: {y!r}")
            s.check(f"{prefix} seed {seed}: reply-identical over {len(cmds)} commands",
                    0, len(mismatches))

    for prefix in ("R", "R64"):
        cmds = large_key_commands(random.Random(0x1A26E), prefix)
        ours.cmd("FLUSHALL")
        theirs.cmd("FLUSHALL")
        a = run_battery(ours, cmds)
        b = run_battery(theirs, cmds)
        mismatches = [
            (i, cmds[i][:3], x, y) for i, (x, y) in enumerate(zip(a, b)) if x != y
        ]
        for i, cmd, x, y in mismatches[:10]:
            print(f"  DIVERGE #{i} {cmd}\n    ours:     {str(x)[:200]}\n"
                  f"    upstream: {str(y)[:200]}")
        s.check(f"{prefix} large keys: reply-identical over {len(cmds)} commands",
                0, len(mismatches))

    cases = row_cases()
    for resp in (2, 3):
        s.section(f"exact-reply rows, RESP{resp}")
        a, b = RawClient(resp=resp), RawClient(port=6392, resp=resp)
        for c in (a, b):
            c.reply("FLUSHALL")
            for cmd in ROW_SETUP:
                c.reply(*cmd)
        for case in cases:
            label = " ".join(x if isinstance(x, str) else str(x) for x in case)
            s.check(f"RESP{resp} {label[:120]}", b.reply(*case), a.reply(*case))
        a.close()
        b.close()

    s.section("documented divergences (valkey-roaring only)")
    a = RawClient()
    a.reply("FLUSHALL")
    for label, setup, cmd, want in DIVERGENCES:
        for step in setup:
            a.reply(*step)
        s.check(f"{label}: {' '.join(cmd)}", want, a.reply(*cmd))
    a.close()

    subprocess.run("docker rm -f vt-upstream", shell=True, capture_output=True)
    ours.cmd("FLUSHALL")
    s.finish()


main()
