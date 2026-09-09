"""Suite 08 — RESP2 vs RESP3 reply parity.

Runs an identical command battery over one RESP2 connection and one RESP3
connection (HELLO 3) and requires semantically identical replies for every
command family, including the mixed integer/bulk-string replies used for
values above 2^63 and error replies.

Escape class targeted: protocol-version-dependent reply encoding — clients
increasingly default to RESP3, which the repo suite never exercises.
"""

import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client, ReplyError


def battery(c):
    out = []
    c.cmd("DEL", "r3", "r3b", "r3dest", "r3big")
    out.append(c.cmd("R.SETINTARRAY", "r3", 1, 5, 100000))
    out.append(c.cmd("R.BITCOUNT", "r3"))
    out.append(c.cmd("R.GETINTARRAY", "r3"))
    out.append(c.cmd("R.GETBITS", "r3", 1, 2, 5))
    out.append(c.cmd("R.MIN", "r3"))
    # RESP3 replies doubles as a native frame; RESP2 as a bulk string.
    # Normalize to float — the values must agree.
    out.append(float(c.cmd("R.JACCARD", "r3", "r3")))
    out.append(c.cmd("R.SETINTARRAY", "r3b", 5, 6))
    out.append(c.cmd("R.BITOP", "AND", "r3dest", "r3", "r3b"))
    out.append(c.cmd("R.STAT", "r3"))
    out.append(c.cmd("R.GETBITARRAY", "r3b"))
    out.append(c.cmd("R64.SETBIT", "r3big", (1 << 63) + 9, 1))
    out.append(c.cmd("R64.MAX", "r3big"))          # decimal-string reply path
    out.append(c.cmd("R64.GETINTARRAY", "r3big"))
    try:
        c.cmd("R.BITOP", "NOPE", "x", "y", "z")
        out.append("no-error")
    except ReplyError as e:
        out.append(("err", str(e)))
    try:
        c.cmd("R.GETBIT", "r3")
        out.append("no-error")
    except ReplyError as e:
        out.append(("err", str(e)))
    return out


def main():
    s = Suite("08 RESP3 parity")
    c2 = Client()
    c3 = Client()
    hello = c3.cmd("HELLO", 3)
    s.check_true("HELLO 3 accepted", hello.get("proto") == 3, hello.get("proto"))

    r2 = battery(c2)
    r3 = battery(c3)
    labels = ["setintarray", "bitcount", "getintarray", "getbits", "min", "jaccard",
              "setintarray b", "bitop and", "stat", "getbitarray",
              "r64 setbit big", "r64 max big", "r64 getintarray big",
              "invalid op error", "wrong arity error"]
    for label, a, b in zip(labels, r2, r3):
        s.check(f"RESP2 == RESP3: {label}", a, b)

    c2.cmd("DEL", "r3", "r3b", "r3dest", "r3big")
    s.finish()


main()
