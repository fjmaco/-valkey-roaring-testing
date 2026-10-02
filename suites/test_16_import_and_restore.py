"""Suite 16 — IMPORT and RESTORE of unusual but legal blobs, and of garbage.

The portable 64-bit format allows a 32-bit sub-bitmap to be empty, and
valkey-roaring 1.1.1 kept such entries in memory after R64.IMPORT; treemap
equality compares sub-bitmaps key by key, so R64.CONTAINS EQ then reported
two equal sets as different. The fix drops empty sub-bitmaps wherever a
blob is deserialized: IMPORT and the RDB load path. This suite feeds both
paths — RESTORE gets a hand-built DUMP payload (module opcode stream plus
CRC-64), the shape an older RDB file carries — and checks equality, subset,
cardinality, canonical EXPORT and that an import adding nothing is a no-op.

It then throws truncations, bit flips and random bytes at R.IMPORT /
R64.IMPORT: each must either be rejected with the bad-binary error and
leave the target key untouched, or be accepted as a set whose EXPORT
decodes in CRoaring to exactly what GETINTARRAY returns; the server must
stay up throughout.

A blob must be exactly one valid bitmap, on both paths: trailing bytes
after a complete blob are refused (1.1.1 ignored them), and so is a 64-bit
blob whose high words do not strictly increase (1.1.1 kept the last of a
repeated word, silently dropping the values under the others). CRoaring,
through pyroaring, is the reference for the latter: it rejects the same
blobs.

Escape class targeted: deserialization accepting states the in-memory
invariants do not allow, and parser crashes on hostile blobs.
"""

import random
import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from pyroaring import BitMap, BitMap64

from lib.harness import Suite
from lib.valkey_client import Client, ReplyError

EMPTY32 = (12346).to_bytes(4, "little") + (0).to_bytes(4, "little")


def crc64(data):
    """CRC-64/Jones, the checksum ending every DUMP payload."""
    poly, crc = 0x95AC9329AC4BC9B5, 0
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ (poly if crc & 1 else 0)
    return crc


def lenenc(n):
    if n < 64:
        return bytes([n])
    if n < 16384:
        return bytes([0x40 | (n >> 8), n & 0xFF])
    if n <= 0xFFFFFFFF:
        return b"\x80" + n.to_bytes(4, "big")
    return b"\x81" + n.to_bytes(8, "big")


def r64_blob(parts):
    """parts: [(high word, 32-bit portable blob)] in the given order."""
    return len(parts).to_bytes(8, "little") + b"".join(h.to_bytes(4, "little") + b for h, b in parts)


def splice_dump(payload, old, new):
    """Replace the module's saved string `old` by `new` in a DUMP payload
    taken with rdbcompression off, and re-seal it."""
    i = payload.find(lenenc(len(old)) + old)
    assert i >= 0, "blob not found in payload"
    body = payload[:i] + lenenc(len(new)) + new + payload[i + len(lenenc(len(old))) + len(old):-8]
    return body + crc64(body).to_bytes(8, "little")


def import_error(c, cmd, key, blob):
    try:
        c.cmd(cmd, key, blob)
        return "accepted"
    except ReplyError as e:
        return "bad binary" if "bad binary" in str(e) else str(e)


def restore_error(c, key, payload):
    try:
        c.cmd("RESTORE", key, 0, payload)
        return "accepted"
    except ReplyError as e:
        return "bad data" if "bad data format" in str(e).lower() else str(e)


def dirty(c):
    return int(c.cmd("INFO", "persistence").decode().split("rdb_changes_since_last_save:")[1].split()[0])


def main():
    s = Suite("16 import and restore")
    c = Client()
    c.cmd("FLUSHALL")
    saved_save = c.cmd("CONFIG", "GET", "save")[1]
    saved_comp = c.cmd("CONFIG", "GET", "rdbcompression")[1]
    c.cmd("CONFIG", "SET", "save", "")
    c.cmd("CONFIG", "SET", "rdbcompression", "no")

    s.section("R64 blobs with empty sub-bitmaps")
    vals = [2, 3, (1 << 32) + 7, (1 << 63) + 1]
    c.cmd("R64.SETINTARRAY", "ref", *vals)
    canonical = c.cmd("R64.EXPORT", "ref")
    sub = {}
    for h in (0, 1, 1 << 31):
        lo = [v & 0xFFFFFFFF for v in vals if v >> 32 == h]
        sub[h] = BitMap(lo).serialize()
    with_empty = r64_blob([(0, sub[0]), (1, sub[1]), (5, EMPTY32), (1 << 31, sub[1 << 31]),
                           ((1 << 32) - 1, EMPTY32)])
    s.check("IMPORT reply is the real cardinality", 4, c.cmd("R64.IMPORT", "imp", with_empty))
    for mode in ("EQ", "ALL"):
        s.check(f"imported == reference ({mode})", 1, c.cmd("R64.CONTAINS", "imp", "ref", mode))
        s.check(f"reference == imported ({mode})", 1, c.cmd("R64.CONTAINS", "ref", "imp", mode))
    s.check("not a strict superset", 0, c.cmd("R64.CONTAINS", "imp", "ref", "ALL_STRICT"))
    s.check("JACCARD 1.0", 1.0, float(c.cmd("R64.JACCARD", "imp", "ref")))
    s.check("EXPORT drops the empty entries", canonical, c.cmd("R64.EXPORT", "imp"))
    d0 = dirty(c)
    s.check("re-import of the same blob replies the cardinality", 4, c.cmd("R64.IMPORT", "imp", with_empty))
    s.check("an import that adds nothing is not a write", 0, dirty(c) - d0)
    only_empty = r64_blob([(3, EMPTY32), (9, EMPTY32)])
    s.check("blob of only empty sub-bitmaps creates an empty key", 0, c.cmd("R64.IMPORT", "oe", only_empty))
    s.check("that key exists", 1, c.cmd("EXISTS", "oe"))
    s.check("and exports the empty canonical blob", BitMap64().serialize(), c.cmd("R64.EXPORT", "oe"))
    c.cmd("R64.SETBIT", "e2", 1, 1)
    c.cmd("R64.SETBIT", "e2", 1, 0)
    s.check("empty-by-import EQ empty-by-writes", 1, c.cmd("R64.CONTAINS", "oe", "e2", "EQ"))
    s.check("BITOP over the imported key", 4, c.cmd("R64.BITOP", "OR", "o", "imp", "oe"))
    s.check("BITOP result EQ reference", 1, c.cmd("R64.CONTAINS", "o", "ref", "EQ"))

    s.section("RDB load path: RESTORE of an older-style payload")
    c.cmd("R64.SETINTARRAY", "d", 2)
    plain = c.cmd("R64.EXPORT", "d")          # no ties: equals the RDB bytes
    payload = c.cmd("DUMP", "d")
    crafted = splice_dump(payload, plain, r64_blob([(0, plain[12:]), (9, EMPTY32)]))
    s.check("crafted payload accepted", "OK", c.cmd("RESTORE", "rd", 0, crafted))
    s.check("restored EQ original", 1, c.cmd("R64.CONTAINS", "rd", "d", "EQ"))
    s.check("original EQ restored", 1, c.cmd("R64.CONTAINS", "d", "rd", "EQ"))
    s.check("restored exports canonically", plain, c.cmd("R64.EXPORT", "rd"))
    tampered = bytearray(crafted)
    tampered[-1] ^= 0xFF
    try:
        c.cmd("RESTORE", "bad", 0, bytes(tampered))
        s.check("bad checksum rejected", True, False)
    except ReplyError as e:
        s.check_true("bad checksum rejected", "checksum" in str(e).lower() or "payload" in str(e).lower(), str(e))
    for P in ("R", "R64"):
        c.cmd(f"{P}.SETINTARRAY", "rt", 1, 70_000, (1 << 32) - 1)
        c.cmd(f"{P}.SETRANGE", "rt", 100, 9_000)
        p = c.cmd("DUMP", "rt")
        c.cmd("RESTORE", "rt2", 0, p, "REPLACE")
        s.check(f"{P} DUMP/RESTORE round trip", c.cmd(f"{P}.EXPORT", "rt"), c.cmd(f"{P}.EXPORT", "rt2"))
        c.cmd("DEL", "rt", "rt2")

    s.section("validation: one exact blob, strictly increasing high words")
    lo_a, lo_b = BitMap([1, 2]).serialize(), BitMap([7]).serialize()
    bad64 = {
        "a repeated high word": r64_blob([(0, lo_a), (0, lo_b)]),
        "decreasing high words": r64_blob([(3, lo_a), (0, lo_b)]),
        "a repeat after an empty entry": r64_blob([(0, lo_a), (2, EMPTY32), (2, lo_b)]),
    }
    for name, blob in bad64.items():
        try:
            BitMap64.deserialize(blob)
            croaring = "accepted"
        except ValueError:
            croaring = "rejected"
        s.check(f"CRoaring rejects {name}", "rejected", croaring)
        c.cmd("DEL", "v")
        s.check(f"R64.IMPORT rejects {name}", "bad binary", import_error(c, "R64.IMPORT", "v", blob))
        s.check(f"  ... and stores nothing ({name})", 0, c.cmd("EXISTS", "v"))
    ok64 = r64_blob([(0, lo_a), (3, lo_b)])
    s.check("CRoaring accepts increasing high words", [1, 2, (3 << 32) + 7],
            list(BitMap64.deserialize(ok64)))
    s.check("R64.IMPORT accepts them", 3, c.cmd("R64.IMPORT", "v", ok64))
    for P, blob in (("R", BitMap([1, 2, 70_000]).serialize()), ("R64", ok64)):
        for extra in (b"\x00", b"junk"):
            c.cmd("DEL", "t")
            s.check(f"{P}.IMPORT rejects {len(extra)} trailing byte(s)", "bad binary",
                    import_error(c, f"{P}.IMPORT", "t", blob + extra))
            s.check(f"  ... and stores nothing ({P}, {len(extra)})", 0, c.cmd("EXISTS", "t"))
        c.cmd(f"{P}.SETINTARRAY", "t", 5)
        before = c.cmd(f"{P}.EXPORT", "t")
        import_error(c, f"{P}.IMPORT", "t", blob + b"\x00")
        s.check(f"{P}: a refused import leaves the existing key alone", before, c.cmd(f"{P}.EXPORT", "t"))
    # The same rules on the RDB load path: RESTORE of crafted payloads.
    for P, key in (("R", "d32"), ("R64", "d64")):
        c.cmd(f"{P}.SETINTARRAY", key, 2)
        plain = c.cmd(f"{P}.EXPORT", key)
        payload = c.cmd("DUMP", key)
        s.check(f"{P}: RESTORE refuses a value with trailing bytes", "bad data",
                restore_error(c, f"{key}r", splice_dump(payload, plain, plain + b"\x00")))
        s.check(f"  ... and stores nothing ({P})", 0, c.cmd("EXISTS", f"{key}r"))
    plain = c.cmd("R64.EXPORT", "d64")
    payload = c.cmd("DUMP", "d64")
    repeated = r64_blob([(0, plain[12:]), (0, plain[12:])])
    s.check("R64: RESTORE refuses a repeated high word", "bad data",
            restore_error(c, "d64r", splice_dump(payload, plain, repeated)))
    s.check("server alive after refused RESTOREs", "PONG", c.cmd("PING"))

    s.section("hostile blobs: rejected cleanly or accepted consistently")
    rng = random.Random(0xB10B)
    seeds = {
        "R": [BitMap([1, 2, 3]).serialize(), BitMap(range(0, 200_000, 3)).serialize(),
              (lambda b: (b.run_optimize(), b.serialize())[1])(BitMap(list(range(10, 9000)) + [70_000]))],
        "R64": [BitMap64([1, (1 << 40) + 5]).serialize(),
                (lambda b: (b.run_optimize(), b.serialize())[1])(BitMap64(list(range(5, 6000)) + [(1 << 63) + 9]))],
    }
    for P, cls in (("R", BitMap), ("R64", BitMap64)):
        c.cmd("DEL", "guard")
        c.cmd(f"{P}.SETINTARRAY", "guard", 42, 4242)
        guard = c.cmd(f"{P}.EXPORT", "guard")
        blobs = []
        for b in seeds[P]:
            blobs += [b[:cut] for cut in range(0, len(b), max(1, len(b) // 60))]
            for _ in range(150):
                m = bytearray(b)
                for _ in range(rng.randrange(1, 4)):
                    m[rng.randrange(len(m))] ^= 1 << rng.randrange(8)
                blobs.append(bytes(m))
        blobs += [bytes(rng.randrange(256) for _ in range(rng.randrange(0, 64))) for _ in range(300)]
        blobs += [b"\x3a\x30\x00\x00" + bytes(rng.randrange(256) for _ in range(40)) for _ in range(100)]
        blobs += [b"\x3b\x30" + bytes(rng.randrange(256) for _ in range(40)) for _ in range(100)]
        rejected = accepted = inconsistent = untouched = 0
        for b in blobs:
            try:
                c.cmd(f"{P}.IMPORT", "guard", b)
                accepted += 1
                got = [int(x) for x in c.cmd(f"{P}.GETINTARRAY", "guard")]
                decoded = list(cls.deserialize(c.cmd(f"{P}.EXPORT", "guard")))
                if got != decoded or not {42, 4242} <= set(got):
                    inconsistent += 1
                c.cmd(f"{P}.SETINTARRAY", "guard", 42, 4242)
            except ReplyError as e:
                rejected += 1
                if "bad binary" in str(e) and c.cmd(f"{P}.EXPORT", "guard") == guard:
                    untouched += 1
            c.cmd("DEL", "fresh")
        s.check(f"{P}: every rejection is the bad-binary error and leaves the key as it was",
                rejected, untouched)
        s.check(f"{P}: every accepted blob merges into a consistent set ({accepted} accepted)", 0, inconsistent)
        try:
            c.cmd(f"{P}.IMPORT", "fresh", b"\x00\x01")
        except ReplyError:
            pass
        s.check(f"{P}: a rejected import does not create its key", 0, c.cmd("EXISTS", "fresh"))
        s.check(f"{P}: server alive", "PONG", c.cmd("PING"))

    c.cmd("CONFIG", "SET", "save", saved_save)
    c.cmd("CONFIG", "SET", "rdbcompression", saved_comp)
    c.cmd("FLUSHALL")
    s.finish()


main()
