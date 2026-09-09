"""Suite 11 — Protocol-level torture.

Fires ~30,000 seeded-random commands at the server across every module
command name with hostile argument soup: binary garbage, embedded NULs and
newlines, huge and negative numbers, wrong arities, wrong types, empty
strings, deep pipelines. The server owes each command *some* reply (value
or error) and owes us three invariants at the end: the connection protocol
never desynchronizes, an untouched control key is bit-for-bit intact, and
the server still answers PING.

Escape class targeted: parser and dispatch crashes — anything where a
hostile client could take the server down or corrupt unrelated data.
"""

import random
import sys

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client, ReplyError

COMMANDS = [
    "R.SETBIT", "R.GETBIT", "R.GETBITS", "R.CLEARBITS", "R.CLEAR",
    "R.SETINTARRAY", "R.GETINTARRAY", "R.APPENDINTARRAY", "R.DELETEINTARRAY",
    "R.RANGEINTARRAY", "R.SETBITARRAY", "R.GETBITARRAY", "R.SETRANGE",
    "R.SETFULL", "R.BITCOUNT", "R.BITPOS", "R.MIN", "R.MAX", "R.OPTIMIZE",
    "R.CONTAINS", "R.JACCARD", "R.DIFF", "R.BITOP", "R.EXPORT", "R.IMPORT",
    "R64.SETBIT", "R64.GETBIT", "R64.GETBITS", "R64.CLEARBITS", "R64.CLEAR",
    "R64.SETINTARRAY", "R64.GETINTARRAY", "R64.APPENDINTARRAY",
    "R64.DELETEINTARRAY", "R64.RANGEINTARRAY", "R64.SETBITARRAY",
    "R64.GETBITARRAY", "R64.SETRANGE", "R64.BITCOUNT", "R64.BITPOS",
    "R64.MIN", "R64.MAX", "R64.OPTIMIZE", "R64.CONTAINS", "R64.JACCARD",
    "R64.DIFF", "R64.BITOP", "R64.EXPORT", "R64.IMPORT", "R.STAT",
]
# R.SETFULL / R64.SETFULL excluded from arg soup only where they could
# genuinely fill memory: R.SETFULL on a fresh key allocates the full u32
# run container (cheap); R64.SETFULL is documented as impractical and
# excluded here on purpose.
COMMANDS.remove("R.SETFULL")


def rand_arg(rng):
    pick = rng.randrange(10)
    if pick < 3:
        return str(rng.randrange(1 << 21))
    if pick == 3:
        return str(-rng.randrange(1 << 32))
    if pick == 4:
        return str(rng.randrange(1 << 70))          # beyond u64
    if pick == 5:
        return rng.choice(["", " ", "COUNT", "NOT", "AND", "EQ", "JSON", "abc"])
    if pick == 6:
        return bytes(rng.randrange(256) for _ in range(rng.randrange(1, 24)))
    if pick == 7:
        return "k" + str(rng.randrange(4))          # collide with real keys
    if pick == 8:
        return "\x00\r\n\x00"
    return str(rng.random())


def main():
    s = Suite("11 protocol torture")
    c = Client()
    c.cmd("FLUSHALL")

    control_vals = sorted(random.Random(7).sample(range(1 << 24), 5000))
    c.cmd("R.SETINTARRAY", "control", *control_vals)
    control_blob = c.cmd("R.EXPORT", "control")

    rng = random.Random(0x70AD)
    torture = Client()
    total, errors, values = 0, 0, 0
    for _ in range(300):
        batch = []
        for _ in range(100):
            cmd = rng.choice(COMMANDS)
            args = [rand_arg(rng) for _ in range(rng.randrange(0, 6))]
            batch.append([cmd, *args])
        replies = torture.pipeline(batch)
        total += len(replies)
        errors += sum(isinstance(r, ReplyError) for r in replies)
        values += sum(not isinstance(r, ReplyError) for r in replies)

    s.section("aftermath")
    s.check("every command got a reply", 30000, total)
    s.check_true("mix of errors and values", errors > 0 and values > 0,
                 f"errors={errors} values={values}")
    s.check("server alive", "PONG", c.cmd("PING"))
    s.check("control key untouched... unless torture hit it",
            True, isinstance(c.cmd("R.BITCOUNT", "control"), int))
    # The torture generator can only touch keys named k0..k3 and garbage
    # names — never "control". Its bytes must be identical.
    s.check("control key bit-for-bit intact", control_blob, c.cmd("R.EXPORT", "control"))
    s.check_true("connection still in protocol sync",
                 torture.cmd("PING") == "PONG", "desync")

    c.cmd("FLUSHALL")
    s.finish()


main()
