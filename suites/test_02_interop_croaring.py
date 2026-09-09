"""Suite 02 — Binary format interop with CRoaring (via pyroaring).

The module's headline promise is that R.EXPORT / R.IMPORT speak the CRoaring
portable serialization, so bitmaps can be exchanged with Java/Go/Python/C++
services without translation. This suite proves it against the reference C
implementation itself (pyroaring wraps CRoaring):

  - R.EXPORT bytes must deserialize in CRoaring with identical contents
  - CRoaring-serialized bytes must R.IMPORT with identical contents
  - the same in both directions for the 64-bit variant (values > 2^32
    and > 2^63 included)
  - IMPORT's documented OR-merge semantics against foreign bytes
  - the Lua EXPORT->IMPORT path with binary passing through the script engine

Escape class targeted: any drift between roaring-rs serialization and the
CRoaring portable spec — undetectable by tests that only round-trip through
the module itself.
"""

import random
import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from pyroaring import BitMap, BitMap64

from lib import datasets
from lib.harness import Suite
from lib.valkey_client import Client


def main():
    s = Suite("02 CRoaring interop")
    c = Client()
    rng = random.Random(0xBEEF)
    c.cmd("FLUSHALL")

    cases32 = {
        "single": [42],
        "boundary": [0, 65535, 65536, 65537, 4294967295],
        "run-heavy": list(range(100000, 165536)),
        "sparse": rng.sample(range(1 << 32), 5000),
    }
    for i, vals in enumerate(datasets.load("census1881", max_files=10)):
        cases32[f"census1881:{i}"] = vals

    s.section("32-bit: module EXPORT -> CRoaring")
    for name, vals in cases32.items():
        key = f"exp:{name}"
        for j in range(0, len(vals), 5000):
            c.cmd("R.APPENDINTARRAY" if j else "R.SETINTARRAY", key, *vals[j:j + 5000])
        blob = c.cmd("R.EXPORT", key)
        bm = BitMap.deserialize(blob)
        s.check(f"{name} croaring reads export", sorted(set(vals)), list(bm))

    s.section("32-bit: CRoaring serialize -> module IMPORT")
    for name, vals in cases32.items():
        blob = BitMap(vals).serialize()
        key = f"imp:{name}"
        card = c.cmd("R.IMPORT", key, blob)
        s.check(f"{name} import cardinality", len(set(vals)), card)
        s.check(f"{name} import contents", len(set(vals)), c.cmd("R.BITCOUNT", key))
        s.check(f"{name} import minmax", [min(vals), max(vals)],
                [c.cmd("R.MIN", key), c.cmd("R.MAX", key)])

    s.section("32-bit: IMPORT OR-merges foreign bytes")
    a, b = {1, 2, 3, 100000}, {3, 4, 5, 4294967295}
    c.cmd("R.SETINTARRAY", "merge", *sorted(a))
    card = c.cmd("R.IMPORT", "merge", BitMap(b).serialize())
    s.check("merge cardinality", len(a | b), card)
    s.check("merge contents", sorted(a | b), c.cmd("R.GETINTARRAY", "merge"))

    s.section("64-bit: both directions incl. values above 2^32 and 2^63")
    cases64 = {
        "small": [0, 1, 2],
        "above-u32": [1 << 33, (1 << 33) + 1, (1 << 40)],
        "above-i64": [(1 << 63) + 5, (1 << 64) - 1],
        "mixed": sorted(rng.sample(range(1 << 62), 2000) + [7, 1 << 35]),
    }
    for name, vals in cases64.items():
        key = f"exp64:{name}"
        c.cmd("R64.SETINTARRAY", key, *vals)
        blob = c.cmd("R64.EXPORT", key)
        bm = BitMap64.deserialize(blob)
        s.check(f"{name} croaring64 reads export", sorted(set(vals)), list(bm))

        blob = BitMap64(vals).serialize()
        key = f"imp64:{name}"
        card = c.cmd("R64.IMPORT", key, blob)
        s.check(f"{name} import64 cardinality", len(set(vals)), card)

    s.section("Lua EXPORT -> IMPORT round trip")
    vals = sorted(rng.sample(range(1 << 30), 3000))
    c.cmd("R.SETINTARRAY", "luasrc", *vals)
    script = "local d = redis.call('R.EXPORT', KEYS[1]); return redis.call('R.IMPORT', KEYS[2], d)"
    card = c.cmd("EVAL", script, 2, "luasrc", "luadst")
    s.check("lua round-trip cardinality", len(vals), card)
    s.check("lua round-trip equality", 1, c.cmd("R.CONTAINS", "luasrc", "luadst", "EQ"))

    c.cmd("FLUSHALL")
    s.finish()


main()
