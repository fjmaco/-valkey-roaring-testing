"""Suite 20 — Independent parity fuzz against redis-roaring, raw bytes.

A second, independently written differential next to suite 09. Where 09
replays a curated battery and fixed rows, this suite generates its own
commands and arguments and compares every reply as raw wire bytes (RESP2
and RESP3, both widths), error texts included:

  1. random commands over the whole shared surface with argument
     generators aimed at every parse and check-order path (malformed
     numbers in both grammars, bits, case and NUL-truncated tokens,
     wrong-type and missing keys in every key slot, arity errors);
  2. an exhaustive check-order matrix: every command x key state
     (existing, missing, string, other width) x argument validity,
     comparing the reply and the state the command leaves behind;
  3. CONTAINS's echoed mode token at its edges (CR/LF anywhere, the
     255-byte cut through multibyte text, invalid UTF-8, NUL);
  4. JACCARD's text at %.17g ties (ratios i / 2^k) and random ratios;
  5. R.STAT after short write histories, both widths.

Documented divergences (docs/commands/index.md) are recognized and not
counted: the doubled upstream WRONGTYPE, the full-width R64.RANGEINTARRAY,
the listing and materialization caps (those commands are not sent to
upstream at all, since it would build multi-GB replies or allocate until
killed), SETBIT value 0 on a missing key (kept out of the random stream),
and R.STAT's per-encoding container breakdown, which reflects each
library's container encodings and can differ after range inserts and set
operations (`R.SETRANGE x 5 7`: CRoaring a run, roaring-rs an array; XOR
results the other way round). Section 5 still requires every other STAT
field to match at each step, and the whole STAT text to match after
R.OPTIMIZE.

Regression guard: a mode token ending in CR/LF was once echoed with
trailing spaces; the server trims trailing CR/LF from upstream's text.

VT_PARITY_SEEDS (default 2) scales section 1; each seed is 2,000
commands per width and RESP version.

Escape class targeted: reply drift the curated rows of suite 09 do not
happen to cover.
"""

import itertools
import os
import random
import subprocess
import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.raw_resp import RawClient
from lib.valkey_client import Client

UP_PORT = 6398
U32, U64 = 2**32 - 1, 2**64 - 1
OPS = ["AND", "OR", "XOR", "ANDOR", "DIFF", "DIFF1", "ONE"]
WT = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
LIST_CAP = b"-Roaring: range too large: maximum 100000000 elements"
WRITE_CAP = b"-Roaring: range too large: maximum 274877906944 elements"
BAD_NUM = ["", "+5", "005", "00", "-0", "-1", " 5", "5 ", "1e3", "0x10", "1.0", "abc", "\x00", "1\x00", b"\xff",
           "4294967296", "18446744073709551615", "18446744073709551616", "99999999999999999999", "+", "++1", "+0",
           "٥", "0005", "+00", "1 2", "\t1", "-18446744073709551615", "9223372036854775808"]
BAD_BIT = ["2", "-1", "+1", "01", "00", "", "1\x00", "true", "0 ", " 1", "0x1", "+0", "-0", "1.0"]
BAD_OP = ["and", "Or", "NOT\x00", "AND\x00x", "", "BOGUS", "not", "ONE ", "DIFF2", "\x00"]
BAD_MODE = ["NONE", "all", "EQ\x00", "EQ\x00junk", "", "x" * 300, "é" * 150, "a\r\nb", b"\xff\xfe", "ALL ",
            "eq", "ALL_strict", "€" * 90, "y" * 228, "z" * 229, "%s%n", "NONE\x00"]
BAD_TOKEN = ["count", "COUNT\x00", "COUNT\x00x", "COUNT ", "", "C", "COUNTX"]
STAT_FMT = ["JSON", "json", "TEXT", "JSON\x00", "", "x", "JSON ", "PLAIN"]


def start_upstream():
    subprocess.run("docker rm -f vt-upstream20", shell=True, capture_output=True)
    subprocess.run(f"docker run -d --name vt-upstream20 --memory=4g -p {UP_PORT}:6379 aviggiano/redis-roaring:latest",
                   shell=True, check=True, capture_output=True)
    for _ in range(30):
        try:
            if Client(port=UP_PORT, timeout=3).cmd("PING") == "PONG":
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError("upstream image did not start")


class Gen:
    def __init__(self, rng, P):
        self.r, self.P, self.wide = rng, P, P == "R64"

    def val(self, bad=0.06, big=0.1):
        r, x = self.r, self.r.random()
        if x < bad:
            return r.choice(BAD_NUM)
        if x < bad + big:
            if self.wide:
                return r.choice([U32, U32 + 1, 2**32, 2**33 + 5, 2**63 - 1, 2**63, 2**63 + 1, U64, U64 - 1,
                                 "+7", "007", "+18446744073709551615", "00000000000000000000001"])
            return r.choice([U32, U32 - 1, 2**31, 65535, 65536, 0, 1])
        return r.randrange(1 << 20)

    def bit(self):
        return self.r.choice(["0", "1"] * 8 + BAD_BIT)

    def key(self):
        x = self.r.random()
        if x < 0.82:
            return self.r.choice(["k0", "k1", "k2", "k3", "k4", "k5"])
        if x < 0.90:
            return self.r.choice(["m0", "m1"])
        return "str" if x < 0.95 else "oth"


def gen(rng, P, n):
    g = Gen(rng, P)
    out = []
    for _ in range(n):
        p, k = rng.randrange(36), g.key()
        if p < 4:
            c = [f"{P}.SETBIT", k, g.val(), g.bit()]
        elif p < 6:
            c = [f"{P}.GETBIT", k, g.val()]
        elif p < 7:
            c = [f"{P}.GETBITS", k, *[g.val() for _ in range(rng.randrange(1, 8))]]
        elif p < 8:
            args = [g.val() for _ in range(rng.randrange(0, 6))]
            if rng.random() < 0.6:
                args.append("COUNT" if rng.random() < 0.7 else rng.choice(BAD_TOKEN))
            c = [f"{P}.CLEARBITS", k, *args]
        elif p < 9:
            c = [f"{P}.CLEAR", k]
        elif p < 10:
            c = [f"{P}.SETINTARRAY", k, *[g.val(bad=0.03) for _ in range(rng.randrange(1, 30))]]
        elif p < 11:
            c = [f"{P}.GETINTARRAY", k]
        elif p < 13:
            c = [f"{P}.APPENDINTARRAY", k, *[g.val(bad=0.03) for _ in range(rng.randrange(1, 30))]]
        elif p < 14:
            c = [f"{P}.DELETEINTARRAY", k, *[g.val(bad=0.05) for _ in range(rng.randrange(1, 10))]]
        elif p < 16:
            x = rng.random()
            if x < 0.15:
                st, en = g.val(bad=0.5, big=0.3), g.val(bad=0.5, big=0.3)
            elif x < 0.25:
                st, en = 0, (U64 if g.wide else U32)
            else:
                st = rng.randrange(3000)
                en = st + rng.choice([-1, 0, 1, 5, 100, 1000, 99_999_999, 100_000_000])
            c = [f"{P}.RANGEINTARRAY", k, st, en]
        elif p < 17:
            s = "".join(rng.choice("0001") for _ in range(rng.randrange(0, 200)))
            if rng.random() < 0.2:
                s = rng.choice(["", "1\x001", "\xff1", "abc1", "é1"]) + s
            c = [f"{P}.SETBITARRAY", k, s]
        elif p < 18:
            c = [f"{P}.GETBITARRAY", k]
        elif p < 20:
            if rng.random() < 0.15:
                st, en = g.val(bad=0.6, big=0.2), g.val(bad=0.6, big=0.2)
            else:
                st = rng.randrange(1 << 20)
                en = st + rng.choice([-5, -1, 0, 1, 2, 3, 7, 100, 5000, 70000, 200000])
            c = [f"{P}.SETRANGE", k, st, en]
        elif p < 21:
            c = [f"{P}.BITCOUNT", k]
        elif p < 22:
            c = [f"{P}.BITPOS", k, g.bit()]
        elif p < 23:
            c = [rng.choice([f"{P}.MIN", f"{P}.MAX"]), k]
        elif p < 24:
            c = [f"{P}.OPTIMIZE", k] + ([rng.choice(["MEM", "mem", "x"])] if rng.random() < 0.3 else [])
        elif p < 26:
            c = [f"{P}.CONTAINS", k, g.key()]
            if rng.random() < 0.7:
                c.append(rng.choice(["ALL", "ALL_STRICT", "EQ"]) if rng.random() < 0.75 else rng.choice(BAD_MODE))
        elif p < 27:
            c = [f"{P}.JACCARD", k, g.key()]
        elif p < 28:
            c = [f"{P}.DIFF", g.key(), g.key(), g.key()]
        elif p < 31:
            op = rng.choice(OPS) if rng.random() < 0.85 else rng.choice(BAD_OP)
            c = [f"{P}.BITOP", op, k, *[g.key() for _ in range(rng.choice([1, 2, 2, 3, 4]))]]
        elif p < 32:
            # NOT only over the n* keys, which hold small values
            c = [f"{P}.BITOP", "NOT", rng.choice(["n2", "n3", "str"]), rng.choice(["n0", "n1", "m0", "str", "oth"])]
            if rng.random() < 0.6:
                c.append(rng.choice([rng.randrange(1 << 18), rng.randrange(1 << 18), "abc", "", "-1", "+5", "005"]))
        elif p < 33:
            c = ["R.STAT", k] + ([rng.choice(STAT_FMT)] if rng.random() < 0.5 else [])
        elif p < 34:
            c = [f"{P}.SETINTARRAY", rng.choice(["n0", "n1"]), *[rng.randrange(1 << 18) for _ in range(rng.randrange(1, 40))]]
        else:
            name = rng.choice(["SETBIT", "GETBIT", "GETBITS", "CLEARBITS", "CLEAR", "SETINTARRAY", "GETINTARRAY",
                               "APPENDINTARRAY", "DELETEINTARRAY", "RANGEINTARRAY", "SETBITARRAY", "GETBITARRAY",
                               "SETRANGE", "BITCOUNT", "BITPOS", "MIN", "MAX", "OPTIMIZE", "CONTAINS", "JACCARD",
                               "DIFF", "BITOP"])
            c = [f"{P}.{name}", k] + [rng.randrange(1 << 20) for _ in range(rng.choice([0, 1, 2, 3, 4, 5]))]
            if rng.random() < 0.3:
                c = c[:1]
        out.append(c)
    return out


def documented(cmd, ra, rb):
    name = cmd[0].split(".")[-1]
    if name == "BITOP" and ra == [WT] and rb == [WT, WT]:
        return True
    if name == "RANGEINTARRAY" and cmd[0].startswith("R64.") and rb == [b"-ERR out of memory\r\n"]:
        return True
    if name == "STAT" and cmd[1] in ("n2", "n3"):
        return True
    return False


def setup_keys(c, P):
    other = "R64" if P == "R" else "R"
    c.frames("FLUSHALL")
    for k in ["k0", "k1", "k2", "k3", "k4", "k5", "n0", "n1", "n2", "n3"]:
        c.frames(f"{P}.SETINTARRAY", k, 0)
    c.frames("SET", "str", "x")
    c.frames(f"{other}.SETINTARRAY", "oth", 1)


def random_battery(seed, P, n, resp):
    rng = random.Random(seed)
    a, b = RawClient(resp=resp), RawClient(port=UP_PORT, resp=resp)
    setup_keys(a, P), setup_keys(b, P)
    undocumented, skipped = [], 0
    for cmd in gen(rng, P, n):
        if cmd[0].endswith(".SETBIT") and len(cmd) == 4 and cmd[1] in ("m0", "m1"):
            continue
        ra = a.frames(*cmd)
        if ra and (ra[0].startswith(LIST_CAP) or ra[0].startswith(WRITE_CAP)):
            skipped += 1                       # documented caps: never sent upstream
            continue
        rb = b.frames(*cmd)
        if ra != rb and not documented(cmd, ra, rb):
            undocumented.append((cmd, ra, rb))
        if ra != rb or any(k in ("m0", "m1") for k in cmd[1:3] if isinstance(k, str)):
            # keep the keyspaces identical and the missing keys missing
            for k in cmd[1:4]:
                if isinstance(k, str) and k[:1] in ("k", "n", "m") and len(k) == 2 and k[1:].isdigit():
                    for c in (a, b):
                        c.frames("DEL", k)
                        if k[0] in "kn":
                            c.frames(f"{P}.SETINTARRAY", k, 0)
    a.close(), b.close()
    return undocumented, skipped


def order_cases(P):
    num = [5, "x5", "-1", "4294967296" if P == "R" else "18446744073709551616", "+5", "005"]
    bits = ["1", "2", "0"]
    keys = {"E": "ex", "M": "missing", "S": "str", "O": "oth", "E2": "ex2"}
    cases = []
    for K in ("ex", "missing", "str", "oth"):
        for n in num:
            cases += [[f"{P}.SETBIT", K, n, b] for b in bits]
            cases += [[f"{P}.GETBIT", K, n], [f"{P}.GETBITS", K, 1, n], [f"{P}.GETBITS", K, n, 1],
                      [f"{P}.CLEARBITS", K, n], [f"{P}.CLEARBITS", K, n, "COUNT"], [f"{P}.SETINTARRAY", K, 1, n],
                      [f"{P}.APPENDINTARRAY", K, n], [f"{P}.DELETEINTARRAY", K, n]]
            for m in num:
                cases += [[f"{P}.RANGEINTARRAY", K, n, m], [f"{P}.SETRANGE", K, n, m]]
        cases += [[f"{P}.BITPOS", K, b] for b in bits]
        cases += [[f"{P}.SETRANGE", K, 9, 3], [f"{P}.RANGEINTARRAY", K, 9, 3], [f"{P}.CLEAR", K],
                  [f"{P}.GETINTARRAY", K], [f"{P}.GETBITARRAY", K], [f"{P}.BITCOUNT", K], [f"{P}.MIN", K],
                  [f"{P}.MAX", K], [f"{P}.OPTIMIZE", K], [f"{P}.OPTIMIZE", K, "MEM"], [f"{P}.SETBITARRAY", K, "0101"],
                  ["R.STAT", K], ["R.STAT", K, "JSON"]]
        if P == "R":
            cases.append([f"{P}.SETFULL", K])
    for A, B in itertools.product(list(keys.values()), repeat=2):
        cases += [[f"{P}.CONTAINS", A, B, *m] for m in ([], ["ALL"], ["EQ"], ["ALL_STRICT"], ["bogus"], ["NONE"], ["eq"])]
        cases.append([f"{P}.JACCARD", A, B])
        cases += [[f"{P}.DIFF", D, A, B] for D in keys.values()]
        for op in OPS + ["and", "BOGUS"]:
            cases += [[f"{P}.BITOP", op, "dst", A, B], [f"{P}.BITOP", op, A, B, "ex"], [f"{P}.BITOP", op, A, B]]
        cases += [[f"{P}.BITOP", "NOT", A, B, *last] for last in ([], [5], ["x"], ["-1"], [num[3]], [5, 6])]
    for K in ("ex", "missing", "str", "oth"):
        for name in ("SETBIT", "GETBIT", "RANGEINTARRAY", "SETRANGE", "BITPOS", "CONTAINS", "JACCARD", "DIFF"):
            cases += [[f"{P}.{name}", K], [f"{P}.{name}", K, 1, 2, 3, 4, 5]]
    return cases


def order_matrix(P, resp):
    other = "R64" if P == "R" else "R"
    a, b = RawClient(resp=resp), RawClient(port=UP_PORT, resp=resp)

    def setup(c):
        c.frames("FLUSHALL")
        c.frames(f"{P}.SETINTARRAY", "ex", 1, 2, 3, 5, 100)
        c.frames(f"{P}.SETINTARRAY", "ex2", 2, 3)
        c.frames("SET", "str", "v")
        c.frames(f"{other}.SETINTARRAY", "oth", 1)

    def state(c):
        return [c.frames("EXISTS", k) + c.frames(f"{P}.BITCOUNT", k) + c.frames(f"{P}.MIN", k)
                + c.frames(f"{P}.MAX", k) for k in ("missing", "dst", "ex", "str")]

    bad = []
    cases = order_cases(P)
    for case in cases:
        setup(a), setup(b)
        ra, rb = a.frames(*case), b.frames(*case)
        documented_ = (ra == [WT] and rb == [WT, WT] and case[0].endswith("BITOP")) or \
                      (case[0].endswith(".SETBIT") and len(case) == 4 and case[1] == "missing" and case[3] == "0")
        if documented_:
            continue
        if ra != rb or state(a) != state(b):
            bad.append((case, ra, rb))
    a.close(), b.close()
    return len(cases), bad


STAT_HEADER_LINES = 5   # type, cardinality, containers, max, min


def stat_histories(P, trials, rng):
    """Short random write histories, R.STAT after each step, then R.OPTIMIZE.
    Returns (encoding-only divergences by op signature, histories where a
    field other than the per-encoding breakdown differs, histories whose
    STAT still differs after R.OPTIMIZE)."""
    def op(key):
        p, base = rng.randrange(12), rng.choice([0, 65536, 3 << 16])
        rv = lambda span: base + rng.randrange(span)
        if p == 0:
            s = rv(60000)
            return [f"{P}.SETRANGE", key, s, s + rng.choice([1, 2, 3, 4, 5, 8, 100, 3000, 5000, 70000])]
        if p == 1:
            return [f"{P}.SETINTARRAY", key, *[rv(65536) for _ in range(rng.choice([1, 2, 3, 50, 5000]))]]
        if p == 2:
            return [f"{P}.APPENDINTARRAY", key, *[rv(65536) for _ in range(rng.choice([1, 2, 3, 50, 5000]))]]
        if p == 3:
            return [f"{P}.DELETEINTARRAY", key, *[rv(65536) for _ in range(rng.choice([1, 3, 50, 3000]))]]
        if p == 4:
            return [f"{P}.CLEARBITS", key, *[rv(65536) for _ in range(rng.choice([1, 3, 50]))]]
        if p == 5:
            return [f"{P}.SETBIT", key, rv(65536), rng.randrange(2)]
        if p == 6:
            return [f"{P}.SETBITARRAY", key, "".join(rng.choice("01") for _ in range(rng.choice([3, 100, 9000, 70000])))]
        if p == 7:
            return [f"{P}.OPTIMIZE", key]
        if p == 8:
            return [f"{P}.BITOP", rng.choice(OPS), key, key, "b"]
        if p == 9:
            return [f"{P}.BITOP", rng.choice(["AND", "OR", "XOR"]), key, "b", "c"]
        if p == 10:
            return [f"{P}.DIFF", key, key, "b"]
        return [f"{P}.CLEAR", key]

    a, b = RawClient(), RawClient(port=UP_PORT)
    found, hard, after_optimize = {}, [], []

    def header(frame):
        # The payload after the RESP length line (which differs whenever
        # the breakdown does), then its first lines.
        payload = frame.split(b"\r\n", 1)[-1]
        return payload.split(b"\n")[:STAT_HEADER_LINES]

    for _ in range(trials):
        setup = []
        for k in ("b", "c"):
            kind = rng.randrange(3)
            if kind == 0:
                s = rng.randrange(100000)
                setup.append([f"{P}.SETRANGE", k, s, s + rng.choice([3, 5, 70000, 200000])])
            elif kind == 1:
                setup.append([f"{P}.SETINTARRAY", k, *[rng.randrange(200000) for _ in range(rng.choice([3, 300, 6000]))]])
            else:
                setup.append([f"{P}.SETBITARRAY", k, "".join(rng.choice("01") for _ in range(rng.choice([50, 70000])))])
        setup.append([f"{P}.SETINTARRAY", "a", rng.randrange(1 << 18)])
        for c in (a, b):
            c.frames("FLUSHALL")
            for s in setup:
                c.frames(*s)
        hist = []
        for _ in range(4):
            o = op("a")
            hist.append(o)
            a.frames(*o), b.frames(*o)
            sa, sb = a.frames("R.STAT", "a"), b.frames("R.STAT", "a")
            if sa != sb:
                if [header(f) for f in sa] != [header(f) for f in sb]:
                    hard.append(setup + hist)
                sig = tuple(h[0].split(".")[1] + (":" + h[1] if h[0].endswith("BITOP") else "") for h in hist)
                found.setdefault(sig, setup + hist)
        if a.frames("EXISTS", "a") != [b":0\r\n"]:
            a.frames(f"{P}.OPTIMIZE", "a"), b.frames(f"{P}.OPTIMIZE", "a")
            for fmt in ([], ["JSON"]):
                if a.frames("R.STAT", "a", *fmt) != b.frames("R.STAT", "a", *fmt):
                    after_optimize.append(setup + hist)
    a.close(), b.close()
    return found, hard, after_optimize


def main():
    s = Suite("20 parity fuzz")
    start_upstream()
    seeds = int(os.environ.get("VT_PARITY_SEEDS", "2"))

    s.section("1. random commands, raw replies")
    for resp in (2, 3):
        for P in ("R", "R64"):
            undocumented, skipped = [], 0
            for seed in range(seeds):
                u, k = random_battery(seed * 31 + resp, P, 2000, resp)
                undocumented += u
                skipped += k
            # R.STAT encodings are checked in section 5; count the rest here.
            other = [d for d in undocumented if d[0][0] != "R.STAT"]
            for cmd, ra, rb in other[:5]:
                print(f"    {[str(x)[:30] for x in cmd[:6]]}\n      new: {[f[:150] for f in ra]}\n      up : {[f[:150] for f in rb]}")
            s.check(f"RESP{resp} {P}: {seeds * 2000} random commands match upstream "
                    f"(documented caps skipped: {skipped})", [], [d[0][:3] for d in other][:5])

    s.section("2. check-order matrix")
    for resp in (2, 3):
        for P in ("R", "R64"):
            n, bad = order_matrix(P, resp)
            for case, ra, rb in bad[:5]:
                print(f"    {case}\n      new: {ra}\n      up : {rb}")
            s.check(f"RESP{resp} {P}: {n} key-state x argument cases match upstream", 0, len(bad))

    s.section("3. CONTAINS echoes the mode token as upstream does")
    modes = ["a\r\nb", "\r\nabc", "x" * 300, "x" * 228, "x" * 229, "é" * 150, "a" + "é" * 114,
             "€" * 90, b"\xff\xfe\xfd", "EQ\x00junk", "\x00EQ", "%s%d%n", "\U0001F600" * 60,
             "a" * 226 + "\U0001F600", "all", "NONE", ""]
    trailing = ["abc\n", "abc\r\n", "EQ\n", "\r", "\n", "x" * 227 + "\r" + "tail"]
    for P in ("R", "R64"):
        for resp in (2, 3):
            a, b = RawClient(resp=resp), RawClient(port=UP_PORT, resp=resp)
            for c in (a, b):
                c.frames("FLUSHALL"), c.frames(f"{P}.SETINTARRAY", "a", 1), c.frames(f"{P}.SETINTARRAY", "b", 1)
            bad = [m for m in modes if a.frames(f"{P}.CONTAINS", "a", "b", m) != b.frames(f"{P}.CONTAINS", "a", "b", m)]
            s.check(f"{P} RESP{resp}: mode echo at the edges", [], bad)
            bad = [m for m in trailing if a.frames(f"{P}.CONTAINS", "a", "b", m) != b.frames(f"{P}.CONTAINS", "a", "b", m)]
            s.check(f"regression guard: {P} RESP{resp}: mode token ending in CR/LF echoed like upstream", [], bad)
            a.close(), b.close()

    s.section("4. JACCARD text at %.17g ties and random ratios")
    rng = random.Random(0x7A)
    pairs = [(i, 1 << k) for k in range(20, 39) for i in (1, 3, 7, 33, (1 << (k // 2)) + 1, rng.randrange(1, 1 << k) | 1)]
    pairs += [(rng.randrange(1, u), u) for u in (rng.randrange(2, 1 << 36) for _ in range(40))]
    a, b = RawClient(), RawClient(port=UP_PORT)
    mism, last_u = [], None
    for i, u in pairs:
        for c in (a, b):
            if u != last_u:
                c.frames("DEL", "B"), c.frames("R64.SETRANGE", "B", 0, u)
            c.frames("DEL", "A"), c.frames("R64.SETRANGE", "A", 0, i)
        last_u = u
        if a.frames("R64.JACCARD", "A", "B") != b.frames("R64.JACCARD", "A", "B"):
            mism.append((i, u))
    a.close(), b.close()
    s.check(f"{len(pairs)} JACCARD ratios byte-identical", [], mism)

    s.section("5. R.STAT after short write histories")
    for P in ("R", "R64"):
        found, hard, after_optimize = stat_histories(P, 150, random.Random(0x57A7 + (P == "R64")))
        # Documented divergence: the per-encoding breakdown alone.
        print(f"    {P}: {len(found)} op signatures differ only in the container breakdown "
              f"(documented divergence)")
        for hist in (hard + after_optimize)[:3]:
            print(f"    {[[str(x)[:12] for x in h[:5]] for h in hist]}")
        s.check(f"{P}: every STAT field but the per-encoding breakdown matches upstream", 0, len(hard))
        s.check(f"{P}: the whole STAT (text and JSON) matches upstream after R.OPTIMIZE", 0,
                len(after_optimize))

    subprocess.run("docker rm -f vt-upstream20", shell=True, capture_output=True)
    Client().cmd("FLUSHALL")
    s.finish()


main()
