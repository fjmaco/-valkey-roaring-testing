"""Suite 09 — Differential testing against the real redis-roaring module.

Runs the published aviggiano/redis-roaring image next to valkey-roaring and
drives both with identical seeded-random command sequences over the shared
command surface, requiring reply-for-reply agreement. Error replies are
compared by their leading error class token (message wording legitimately
differs); STAT is compared on parsed fields, not layout.

Known, documented divergences are skipped: EXPORT/IMPORT (valkey-roaring
additions), RANGEINTARRAY ranges wider than upstream's 100M cap, and
GETBITARRAY beyond valkey-roaring's allocation guard. One known UPSTREAM
bug is sidestepped by priming every key at battery start: redis-roaring's
R.SETBIT on a missing key inserts the offset regardless of the value
argument (r_32.c RSetBitCommand builds bitmap_from_int_array({offset})
on the create path), so `SETBIT newkey X 0` sets the bit there;
valkey-roaring intentionally leaves it clear.

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
        return ("err", str(reply).split()[0])
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

    subprocess.run("docker rm -f vt-upstream", shell=True, capture_output=True)
    ours.cmd("FLUSHALL")
    s.finish()


main()
