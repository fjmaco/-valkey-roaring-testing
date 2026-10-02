"""Suite 14 — Write signals: every write command, no-op vs real change.

A write that changes nothing must be invisible: no dirty-counter increment
(so nothing is replicated, AOF-logged or counted toward save points), no
WATCH abort, no client-tracking invalidation. A write that changes
anything — including one that only creates an empty key — must do all
three. This suite drives every write command of both widths through a
no-op variant and a real-change variant, each on its own key, and checks
the three signals per command. Reads (EXPORT included) must never signal.
OPTIMIZE, the bulk setters, BITOP and DIFF always rewrite their key and
must always signal.

Escape class targeted: a write wrongly classified as a no-op (replica and
AOF silently diverge from the primary) or a no-op that still invalidates
caches and aborts transactions.
"""

import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client, ReplyError

U32 = (1 << 32) - 1
U64 = (1 << 64) - 1


def dirty(c):
    info = c.cmd("INFO", "persistence").decode()
    return int(info.split("rdb_changes_since_last_save:")[1].split()[0])


def cases(P, blob_sub, blob_super, blob_empty, blob_empty_sub):
    """(name, setup commands, command, expect_signal). {k} is the key."""
    hi = (1 << 63) + 5 if P == "R64" else U32
    base = [[f"{P}.SETINTARRAY", "{k}", 1, 2, 3, hi]]
    empty = [[f"{P}.SETBIT", "{k}", 1, 1], [f"{P}.SETBIT", "{k}", 1, 0]]
    ranged = [[f"{P}.SETRANGE", "{k}", 100, 70_000]]
    c = []
    # -- no-ops
    c += [("SETBIT already set", base, [f"{P}.SETBIT", "{k}", 2, 1], False),
          ("SETBIT high already set", base, [f"{P}.SETBIT", "{k}", hi, 1], False),
          ("SETBIT already clear", base, [f"{P}.SETBIT", "{k}", 9, 0], False),
          ("CLEARBITS absent", base, [f"{P}.CLEARBITS", "{k}", 9, 10], False),
          ("CLEARBITS absent COUNT", base, [f"{P}.CLEARBITS", "{k}", 9, "COUNT"], False),
          ("CLEARBITS missing key", [], [f"{P}.CLEARBITS", "{k}", 1], False),
          ("CLEAR empty key", empty, [f"{P}.CLEAR", "{k}"], False),
          ("CLEAR missing key", [], [f"{P}.CLEAR", "{k}"], False),
          ("APPENDINTARRAY present", base, [f"{P}.APPENDINTARRAY", "{k}", 3, 2, 1, 2], False),
          ("DELETEINTARRAY absent", base, [f"{P}.DELETEINTARRAY", "{k}", 50, 51], False),
          ("SETRANGE inside a range", ranged, [f"{P}.SETRANGE", "{k}", 200, 60_000], False),
          ("SETRANGE same range", ranged, [f"{P}.SETRANGE", "{k}", 100, 70_000], False),
          ("SETRANGE empty range", base, [f"{P}.SETRANGE", "{k}", 50, 50], False),
          ("SETRANGE over listed values", base, [f"{P}.SETRANGE", "{k}", 1, 4], False),
          ("IMPORT subset blob", base, [f"{P}.IMPORT", "{k}", blob_sub], False),
          ("IMPORT empty blob", base, [f"{P}.IMPORT", "{k}", blob_empty], False),
          ("SETFULL existing (error)", base, [f"{P}.SETFULL", "{k}"], False),
          ("OPTIMIZE missing key", [], [f"{P}.OPTIMIZE", "{k}"], False),
          ("SETBIT parse error", base, [f"{P}.SETBIT", "{k}", "x", 1], False),
          ("BITOP wrong-type source", base + [["SET", "{k}s", "x"]],
           [f"{P}.BITOP", "OR", "{k}", "{k}", "{k}s"], False)]
    if blob_empty_sub is not None:
        c.append(("IMPORT subset with empty sub-bitmap", base, [f"{P}.IMPORT", "{k}", blob_empty_sub], False))
    # -- reads
    for r in ([f"{P}.GETBIT", "{k}", 1], [f"{P}.GETBITS", "{k}", 1, 9], [f"{P}.GETINTARRAY", "{k}"],
              [f"{P}.RANGEINTARRAY", "{k}", 0, 2], [f"{P}.BITCOUNT", "{k}"], [f"{P}.BITPOS", "{k}", 0],
              [f"{P}.MIN", "{k}"], [f"{P}.MAX", "{k}"], [f"{P}.GETBITARRAY", "{k}"],
              [f"{P}.CONTAINS", "{k}", "{k}"], [f"{P}.JACCARD", "{k}", "{k}"], ["R.STAT", "{k}"]):
        c.append((f"read {r[0]}", base, r, False))
    c.append(("read EXPORT (re-encodes a tie)", [[f"{P}.SETRANGE", "{k}", 5, 8]], [f"{P}.EXPORT", "{k}"], False))
    # -- real changes
    c += [("SETBIT set new", base, [f"{P}.SETBIT", "{k}", 9, 1], True),
          ("SETBIT clear present", base, [f"{P}.SETBIT", "{k}", 2, 0], True),
          ("SETBIT 0 creates key", [], [f"{P}.SETBIT", "{k}", 9, 0], True),
          ("CLEARBITS present", base, [f"{P}.CLEARBITS", "{k}", 9, 2], True),
          ("CLEARBITS present COUNT", base, [f"{P}.CLEARBITS", "{k}", 2, "COUNT"], True),
          ("CLEAR non-empty", base, [f"{P}.CLEAR", "{k}"], True),
          ("APPENDINTARRAY new value", base, [f"{P}.APPENDINTARRAY", "{k}", 1, 50], True),
          ("APPENDINTARRAY creates key", [], [f"{P}.APPENDINTARRAY", "{k}", 50], True),
          ("DELETEINTARRAY present", base, [f"{P}.DELETEINTARRAY", "{k}", 50, 1], True),
          ("DELETEINTARRAY creates empty key", [], [f"{P}.DELETEINTARRAY", "{k}", 50], True),
          ("SETRANGE partly new", ranged, [f"{P}.SETRANGE", "{k}", 60_000, 80_000], True),
          ("SETRANGE empty range creates key", [], [f"{P}.SETRANGE", "{k}", 7, 7], True),
          ("SETINTARRAY same contents", base, [f"{P}.SETINTARRAY", "{k}", 1, 2, 3, hi], True),
          ("SETBITARRAY", base, [f"{P}.SETBITARRAY", "{k}", "0111"], True),
          ("OPTIMIZE existing", base, [f"{P}.OPTIMIZE", "{k}"], True),
          ("IMPORT superset blob", base, [f"{P}.IMPORT", "{k}", blob_super], True),
          ("IMPORT empty blob creates key", [], [f"{P}.IMPORT", "{k}", blob_empty], True),
          ("BITOP OR onto itself", base, [f"{P}.BITOP", "OR", "{k}", "{k}", "{k}"], True),
          ("BITOP NOT", [[f"{P}.SETINTARRAY", "{k}", 1, 2, 3]], [f"{P}.BITOP", "NOT", "{k}", "{k}", 10], True),
          ("DIFF onto itself", base, [f"{P}.DIFF", "{k}", "{k}", "{k}"], True)]
    if P == "R":
        c.append(("SETFULL missing key", [], ["R.SETFULL", "{k}"], True))
    return c


def fmt(cmd, key):
    return [a.replace("{k}", key) if isinstance(a, str) else a for a in cmd]


def main():
    s = Suite("14 write signals")
    c = Client()
    c.cmd("FLUSHALL")
    saved = c.cmd("CONFIG", "GET", "save")[1]
    c.cmd("CONFIG", "SET", "save", "")            # keep the dirty counter from resetting

    blobs = {}
    for P in ("R", "R64"):
        hi = (1 << 63) + 5 if P == "R64" else U32
        c.cmd(f"{P}.SETINTARRAY", "b:sub", 2, hi)
        c.cmd(f"{P}.SETINTARRAY", "b:super", 2, 77)
        c.cmd(f"{P}.SETBIT", "b:empty", 1, 1)
        c.cmd(f"{P}.SETBIT", "b:empty", 1, 0)
        sub, sup, emp = (c.cmd(f"{P}.EXPORT", k) for k in ("b:sub", "b:super", "b:empty"))
        empty_sub = None
        if P == "R64":
            # {2} plus an empty 32-bit sub-bitmap under high word 9: the
            # portable format allows it, and 1.1.1 kept it in memory.
            empty32 = (12346).to_bytes(4, "little") + (0).to_bytes(4, "little")
            c.cmd("R.SETINTARRAY", "b:two", 2)
            two32 = c.cmd("R.EXPORT", "b:two")
            empty_sub = ((2).to_bytes(8, "little") + (0).to_bytes(4, "little") + two32
                         + (9).to_bytes(4, "little") + empty32)
        blobs[P] = (sub, sup, emp, empty_sub)
        c.cmd("DEL", "b:sub", "b:super", "b:empty", "b:two")

    inv = Client()
    inv_id = inv.cmd("CLIENT", "ID")
    inv.cmd("SUBSCRIBE", "__redis__:invalidate")
    tracker = Client()
    tracker.cmd("CLIENT", "TRACKING", "on", "REDIRECT", inv_id, "BCAST", "PREFIX", "sig:")

    def drain(wait, until=None):
        keys = []
        deadline = time.time() + wait
        while time.time() < deadline and until not in keys:
            try:
                inv.sock.settimeout(max(0.05, deadline - time.time()))
                msg = inv._read_reply()
            except OSError:
                break
            if msg and msg[0] == b"message" and msg[1] == b"__redis__:invalidate":
                keys.extend(k.decode() for k in (msg[2] or []))
        return keys

    w = Client()
    for P in ("R", "R64"):
        s.section(f"{P} write commands")
        for i, (name, setup, cmd, expect) in enumerate(cases(P, *blobs[P])):
            key = f"sig:{P}:{i}"
            for st in setup:
                c.cmd(*fmt(st, key))
            drain(0.05)                               # discard setup invalidations
            w.cmd("WATCH", key)
            d0 = dirty(c)
            try:
                c.cmd(*fmt(cmd, key))
            except ReplyError:
                pass
            d1 = dirty(c)
            # Invalidations are pushed before the command's reply is sent;
            # a short wait suffices to see one that should not be there.
            invalidated = key in drain(1.0 if expect else 0.15, until=key)
            w.cmd("MULTI")
            w.cmd("PING")
            aborted = w.cmd("EXEC") is None
            got = (d1 > d0, aborted, invalidated)
            s.check(f"{P} {name}: (dirty, WATCH abort, invalidation)", (expect,) * 3, got)

    s.section("a transaction of no-ops propagates nothing")
    c.cmd("R.SETINTARRAY", "sig:tx", 1, 2, 3)
    d0 = dirty(c)
    c.cmd("MULTI")
    c.cmd("R.SETBIT", "sig:tx", 1, 1)
    c.cmd("R.APPENDINTARRAY", "sig:tx", 2)
    c.cmd("R.CLEARBITS", "sig:tx", 99)
    c.cmd("R.EXPORT", "sig:tx")
    c.cmd("EXEC")
    s.check("no-op MULTI/EXEC leaves the dirty counter alone", 0, dirty(c) - d0)
    d0 = dirty(c)
    c.cmd("EVAL", "redis.call('R.SETBIT', KEYS[1], 1, 1) redis.call('R.SETRANGE', KEYS[1], 1, 3) "
          "return redis.call('R.IMPORT', KEYS[1], redis.call('R.EXPORT', KEYS[1]))", 1, "sig:tx")
    s.check("no-op script leaves the dirty counter alone", 0, dirty(c) - d0)

    tracker.cmd("CLIENT", "TRACKING", "off")
    for conn in (inv, tracker, w):
        conn.close()
    c.cmd("CONFIG", "SET", "save", saved)
    c.cmd("FLUSHALL")
    s.finish()


main()
