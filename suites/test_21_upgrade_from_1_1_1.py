"""Suite 21 — Upgrading from valkey-roaring 1.1.1: RDB, DUMP and AOF data.

Gated: runs only with VT_COMPAT=1 (pulls fjmaco/valkey-roaring:1.1.1).

The current build reads blobs strictly: IMPORT and the RDB loader refuse
trailing bytes and 64-bit blobs whose high words do not strictly increase,
and drop empty 32-bit sub-bitmaps. Data written by 1.1.1 has to survive the
upgrade. This suite starts the published 1.1.1 image as a helper, writes
keys of every shape there (sparse, dense, runs, R.SETFULL, values above
2^63, a full 2^32 sub-bitmap, sub-bitmaps on both sides of borders, empty
keys of both widths, a TTL, and an R64 key holding an empty sub-bitmap that
1.1.1's lenient R64.IMPORT stored), then requires the same sets and TTLs:

  1. DUMP on 1.1.1 -> RESTORE here;
  2. 1.1.1's dump.rdb loaded at startup by a fresh server of this build;
  3. 1.1.1's AOF directory replayed at startup by this build;
  4. this build as a replica of a 1.1.1 primary;
  5. a random stream of every 1.1.1-only input form (plain commands,
     MULTI/EXEC blocks and EVAL scripts, which replicate as effects) on a
     1.1.1 primary with AOF on: this build as its replica and this build
     replaying its AOF must both hold exactly the primary's sets.

1.1.1 accepted input the current build refuses from clients: IMPORT blobs
with trailing bytes or repeated/decreasing 64-bit high words, "+5"/"005"
values, "+1"/"01" bits and lowercase BITOP operations. Its AOF and its
replication stream carry those commands verbatim. Regression guard: they
once failed on replay ("ERR bad binary data", logged as CRITICAL only) and
the keys vanished after an upgrade. Commands replayed from the AOF or
received from the primary are now read with 1.1.1's rules, so sections 3
and 4 require exactly the sets the 1.1.1 primary held.

Escape class targeted: upgrade-path data loss from tightened decoding.
"""

import random
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from pyroaring import BitMap, BitMap64

from lib.compose import sh
from lib.harness import Suite, env_flag
from lib.valkey_client import Client, ReplyError

OLD_IMAGE = "fjmaco/valkey-roaring:1.1.1"
OLD_PORT, NEW_PORT = 6402, 6403
LOAD = "valkey-server --loadmodule /usr/lib/valkey/modules/libvalkey_roaring.so"
EMPTY32 = (12346).to_bytes(4, "little") + (0).to_bytes(4, "little")
KEYS = ["r_small", "r_big", "r_full", "r_runs", "r64_hi", "r64_emptysub", "r64_fullsub", "r64_multi",
        "r_empty", "r64_empty", "r_ttl"]


def r64blob(parts):
    return len(parts).to_bytes(8, "little") + b"".join(h.to_bytes(4, "little") + b for h, b in parts)


def critical_lines(container):
    """Lines the server logged as CRITICAL: a command that fails while the
    AOF is loaded, or that a replica rejects from its primary, lands there."""
    out = subprocess.run(["docker", "logs", container], capture_output=True, text=True)
    return sum("CRITICAL" in line for line in (out.stdout + out.stderr).splitlines())


def state_of(c, k):
    """A key's type and members, compared as 1.1.1 held them."""
    t = c.cmd("TYPE", k)
    if t == "none":
        return None
    wide = t in ("vroarng64", "roaring64", "reroaring64")
    return (wide, c.cmd("R64.GETINTARRAY" if wide else "R.GETINTARRAY", k))


def legacy_writes(c, prefix, b1, b7):
    """Writes 1.1.1 accepts and this build refuses from clients; returns
    the state 1.1.1 left for each key."""
    cmds = {
        "trailing": ["R.IMPORT", None, b1 + b"JUNK"],
        "dup_high": ["R64.IMPORT", None, r64blob([(0, b1), (0, b7)])],
        "dec_high": ["R64.IMPORT", None, r64blob([(5, b1), (2, b7)])],
        "dup_high_into": ["R64.IMPORT", None, r64blob([(3, b1), (3, b7), (1, b7)])],
        "plain": ["R.IMPORT", None, b1],
        "plus_value": ["R.SETBIT", None, "+5", "1"],
        "zero_pad": ["R.APPENDINTARRAY", None, "007", "+9", "10"],
        "bit_01": ["R64.SETBIT", None, "12", "01"],
        "range_pad": ["R.SETRANGE", None, "0010", "+15"],
        "lower_op": ["R.BITOP", "or", None, "{p}plain", "{p}zero_pad"],
        "lower_not": ["R.BITOP", "not", None, "{p}plain", "+6"],
        "mixed_op": ["R64.BITOP", "AnD", None, "{p}dup_high", "{p}dec_high"],
    }
    state = {}
    c.cmd("R64.SETINTARRAY", f"{prefix}dup_high_into", 1, (3 << 32) | 9)
    for name, cmd in cmds.items():
        k = prefix + name
        args = [k if a is None else (a.replace("{p}", prefix) if isinstance(a, str) else a) for a in cmd]
        c.cmd(*args)
        state[k] = state_of(c, k)
    return state


def legacy_stream(c, rng, n):
    """Write commands in every form 1.1.1 accepts and the strict grammar
    refuses, on small keys. Returns how many 1.1.1 accepted."""
    def num(v):
        return rng.choice([str(v), "+" + str(v), "0" * rng.randrange(1, 4) + str(v), "+00" + str(v)])

    def bit(b):
        return rng.choice([str(b), "+" + str(b), "0" + str(b), "+0" + str(b)])

    def op():
        o = rng.choice(["AND", "OR", "XOR", "ANDOR", "DIFF", "DIFF1", "ONE"])
        return rng.choice([o, o.lower(), o.capitalize()])

    def one():
        wide = rng.random() < 0.5
        P, K = ("R64", STREAM64) if wide else ("R", STREAM32)
        k = rng.choice(K)
        v = lambda: rng.randrange(1 << 18) + (rng.choice([0, 1 << 40, 1 << 63]) if wide else 0)
        p = rng.randrange(11)
        if p < 3:
            return [f"{P}.SETBIT", k, num(v()), bit(rng.randrange(2))]
        if p < 4:
            return [f"{P}.APPENDINTARRAY", k, *[num(v()) for _ in range(rng.randrange(1, 8))]]
        if p < 5:
            return [f"{P}.DELETEINTARRAY", k, *[num(v()) for _ in range(rng.randrange(1, 4))]]
        if p < 6:
            a = v()
            return [f"{P}.SETRANGE", k, num(a), num(a + rng.randrange(1, 300))]
        if p < 7:
            return [f"{P}.CLEARBITS", k, *[num(v()) for _ in range(rng.randrange(1, 4))]] + \
                (["COUNT"] if rng.random() < 0.5 else [])
        if p < 8:
            return [f"{P}.BITOP", op(), rng.choice(K), *[rng.choice(K) for _ in range(rng.randrange(2, 4))]]
        if p < 9:
            return [f"{P}.BITOP", rng.choice(["NOT", "not", "Not"]), f"{P}_nd", f"{P}_ns"] + \
                ([num(rng.randrange(1 << 17))] if rng.random() < 0.6 else [])
        if p < 10:
            vals = sorted({rng.randrange(1 << 18) for _ in range(rng.randrange(1, 20))})
            if wide:
                parts = [(rng.choice([0, 7, 9]), BitMap(vals).serialize())]
                if rng.random() < 0.5:
                    parts.append((parts[0][0], BitMap([x + 1 for x in vals]).serialize()))
                if rng.random() < 0.5:
                    parts.insert(0, (12, BitMap(vals).serialize()))
                blob = r64blob(parts)
            else:
                blob = BitMap(vals).serialize()
            if rng.random() < 0.5:
                blob += b"JUNK"[:rng.randrange(1, 5)]
            return [f"{P}.IMPORT", k, blob]
        return [f"{P}.SETINTARRAY", k, *[num(v()) for _ in range(rng.randrange(1, 10))]]

    c.cmd("R.SETINTARRAY", "R_ns", 3, 9, 100)
    c.cmd("R64.SETINTARRAY", "R64_ns", 3, 9, 100)
    accepted = 0
    for _ in range(n):
        form = rng.random()
        try:
            if form < 0.8:
                c.cmd(*one())
            elif form < 0.9:
                c.cmd("MULTI")
                for cmd in [one() for _ in range(rng.randrange(2, 5))]:
                    c.cmd(*cmd)
                c.cmd("EXEC")
            else:
                cmd = one()
                if any(isinstance(x, bytes) for x in cmd):
                    continue
                args = ",".join("ARGV[%d]" % (j + 1) for j in range(len(cmd)))
                c.cmd("EVAL", f"return redis.call({args})", 0, *cmd)
            accepted += 1
        except ReplyError:
            pass
    return accepted


STREAM32 = [f"s32_{i}" for i in range(5)]
STREAM64 = [f"s64_{i}" for i in range(5)]


def stream_digest(c):
    out = {}
    for k in STREAM32 + STREAM64 + ["R_nd", "R64_nd", "R_ns", "R64_ns"]:
        t = c.cmd("TYPE", k)
        out[k] = None if t == "none" else (t, c.cmd("R64.GETINTARRAY" if t == "vroarng64" else "R.GETINTARRAY", k))
    return out


def wait(port, tries=120):
    for _ in range(tries):
        try:
            c = Client(port=port, timeout=120)
            if c.cmd("PING") == "PONG" and b"loading:0" in c.cmd("INFO", "persistence"):
                return c
        except (OSError, ReplyError):
            pass
        time.sleep(0.5)
    return None


def populate(c, rng):
    c.cmd("FLUSHALL")
    c.cmd("R.SETINTARRAY", "r_small", 1, 2, 3, 4294967295)
    big = rng.sample(range(1 << 32), 300_000)
    for i in range(0, len(big), 50_000):
        c.cmd("R.SETINTARRAY" if i == 0 else "R.APPENDINTARRAY", "r_big", *big[i:i + 50_000])
    c.cmd("R.SETFULL", "r_full")
    for i in range(50):
        c.cmd("R.SETRANGE", "r_runs", i * 100_000, i * 100_000 + 3 + i)
    c.cmd("R64.SETINTARRAY", "r64_hi", 0, 2**63, 2**64 - 1, 2**40 + 5)
    c.cmd("R64.IMPORT", "r64_emptysub", r64blob([(0, BitMap([2, 3]).serialize()), (7, EMPTY32),
                                                 (9, BitMap([1]).serialize())]))
    c.cmd("R64.SETRANGE", "r64_fullsub", 0, 2**32)
    c.cmd("R64.SETRANGE", "r64_multi", 2**32 - 5, 2**34 + 5)
    c.cmd("R.SETBIT", "r_empty", 1, 1)
    c.cmd("R.SETBIT", "r_empty", 1, 0)
    c.cmd("R64.SETBIT", "r64_empty", 1, 1)
    c.cmd("R64.SETBIT", "r64_empty", 1, 0)
    c.cmd("R.SETINTARRAY", "r_ttl", 9)
    c.cmd("EXPIRE", "r_ttl", 100_000)


def digest(c):
    out = {}
    for k in KEYS:
        t = c.cmd("TYPE", k)
        if t == "none":
            out[k] = None
            continue
        wide = t in ("vroarng64", "roaring64", "reroaring64")
        bm = (BitMap64 if wide else BitMap).deserialize(c.cmd("R64.EXPORT" if wide else "R.EXPORT", k))
        out[k] = (wide, len(bm), bm.min() if len(bm) else None, bm.max() if len(bm) else None,
                  hash(tuple(bm)) if len(bm) < 2_000_000 else None, c.cmd("TTL", k) > 0)
    return out


def main():
    s = Suite("21 upgrade from 1.1.1")
    if not env_flag("VT_COMPAT"):
        print("skipped: set VT_COMPAT=1 to run the upgrade-compatibility suite")
        s.finish()
    cid = sh("docker compose ps -q valkey").stdout.strip()
    img = sh(f"docker inspect -f '{{{{.Config.Image}}}}' {cid}").stdout.strip()
    work = tempfile.mkdtemp(prefix="vt21-")
    rng = random.Random(0x111)

    sh("docker rm -f vt-old vt-new")
    sh(f"docker run -d --name vt-old -p {OLD_PORT}:6379 {OLD_IMAGE} {LOAD} --appendonly yes")
    old = wait(OLD_PORT)
    s.check_true("1.1.1 helper started", old is not None)
    populate(old, rng)
    want = digest(old)

    s.section("1. DUMP on 1.1.1, RESTORE here")
    new = Client()
    for k in KEYS:
        new.cmd("DEL", k)
    errors = []
    for k in KEYS:
        try:
            new.cmd("RESTORE", k, 0, old.cmd("DUMP", k))
        except ReplyError as e:
            errors.append((k, str(e)))
    new.cmd("EXPIRE", "r_ttl", 100_000)
    s.check("every 1.1.1 payload restores", [], errors)
    got = digest(new)
    s.check("restored sets equal 1.1.1's", [], [k for k in KEYS if got[k] != want[k]])
    new.cmd("R64.SETINTARRAY", "ref", 2, 3, (9 << 32) | 1)
    s.check("an empty sub-bitmap stored by 1.1.1 is dropped (EQ holds)", 1,
            new.cmd("R64.CONTAINS", "r64_emptysub", "ref", "EQ"))
    for k in KEYS + ["ref"]:
        new.cmd("DEL", k)

    s.section("2. 1.1.1's dump.rdb loaded at startup")
    old.cmd("SAVE")
    sh(f"docker cp vt-old:/data/dump.rdb {work}/dump.rdb")
    sh(f"docker create --name vt-new -p {NEW_PORT}:6379 {img} {LOAD}")
    sh(f"docker cp {work}/dump.rdb vt-new:/data/dump.rdb")
    sh("docker start vt-new")
    loaded = wait(NEW_PORT)
    s.check_true("the server starts on 1.1.1's RDB", loaded is not None)
    if loaded:
        got = digest(loaded)
        s.check("every key loaded with the same set", [], [k for k in KEYS if got[k] != want[k]])
    sh("docker rm -f vt-new")

    s.section("3. 1.1.1's AOF replayed at startup")
    b1, b7 = BitMap([1, 2]).serialize(), BitMap([7]).serialize()
    old_state = legacy_writes(old, "aof_", b1, b7)
    time.sleep(1.5)                                        # appendfsync everysec
    sh(f"rm -rf {work}/appendonlydir && docker cp vt-old:/data/appendonlydir {work}/")
    sh(f"docker create --name vt-new -p {NEW_PORT}:6379 {img} {LOAD} --appendonly yes")
    sh(f"docker cp {work}/appendonlydir vt-new:/data/")
    sh("docker start vt-new")
    replayed = wait(NEW_PORT)
    s.check_true("the server starts on 1.1.1's AOF", replayed is not None)
    if replayed:
        got = digest(replayed)
        s.check("keys from the AOF's RDB preamble survive", [], [k for k in KEYS if got[k] != want[k]])
        s.check("a strict-valid IMPORT replays", old_state["aof_plain"], state_of(replayed, "aof_plain"))
        diff = [k for k, v in old_state.items() if state_of(replayed, k) != v]
        s.check("regression guard: keys written with 1.1.1-only input replay to 1.1.1's sets", [], diff)
        s.check("no command failed during the replay (no CRITICAL in the log)", 0, critical_lines("vt-new"))
    sh("docker rm -f vt-new")

    s.section("4. this build replicating from a 1.1.1 primary")
    sh(f"docker run -d --name vt-new -p {NEW_PORT}:6379 {img} {LOAD}")
    replica = wait(NEW_PORT)
    s.check_true("replica started", replica is not None)
    if replica:
        old_ip = sh("docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' vt-old").stdout.strip()
        replica.cmd("REPLICAOF", old_ip, 6379)
        synced = False
        for _ in range(120):
            if b"master_link_status:up" in replica.cmd("INFO", "replication"):
                synced = True
                break
            time.sleep(0.5)
        s.check_true("replica linked to the 1.1.1 primary", synced)
        repl_state = legacy_writes(old, "repl_", b1, b7)
        old.cmd("SET", "repl_marker", "1")
        for _ in range(120):
            if replica.cmd("EXISTS", "repl_marker") == 1:
                break
            time.sleep(0.25)
        diff = [k for k, v in repl_state.items() if state_of(replica, k) != v]
        s.check("regression guard: 1.1.1-only input replicates to 1.1.1's sets", [], diff)
        s.check("the replica rejected nothing from its primary (no CRITICAL in the log)", 0,
                critical_lines("vt-new"))
        replica.cmd("REPLICAOF", "NO", "ONE")
    sh("docker rm -f vt-new vt-old")

    s.section("5. random 1.1.1-only input stream: replica and AOF replay")
    sh("docker rm -f vt-old5 vt-rep5 vt-aof5")
    sh(f"docker run -d --name vt-old5 -p 6404:6379 {OLD_IMAGE} {LOAD} --appendonly yes --save ''")
    old5 = wait(6404)
    ip5 = sh("docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' vt-old5").stdout.strip()
    sh(f"docker run -d --name vt-rep5 -p 6405:6379 {img} {LOAD} --replicaof {ip5} 6379")
    rep5 = wait(6405)
    for _ in range(120):
        if b"master_link_status:up" in rep5.cmd("INFO", "replication"):
            break
        time.sleep(0.5)
    accepted = legacy_stream(old5, random.Random(0x5EED), 2500)
    old5.cmd("SET", "stream_marker", "1")
    for _ in range(120):
        if rep5.cmd("EXISTS", "stream_marker") == 1:
            break
        time.sleep(0.25)
    want = stream_digest(old5)
    s.check_true(f"1.1.1 accepted the stream ({accepted} of 2500)", accepted > 2400, accepted)
    s.check("regression guard: replica of a 1.1.1 primary holds its sets",
            [], [k for k in want if stream_digest(rep5)[k] != want[k]])
    s.check("the replica rejected nothing", 0, critical_lines("vt-rep5"))
    time.sleep(1.5)                                        # appendfsync everysec
    sh(f"rm -rf {work}/appendonlydir && docker cp vt-old5:/data/appendonlydir {work}/")
    sh(f"docker create --name vt-aof5 -p 6406:6379 {img} {LOAD} --appendonly yes --save ''")
    sh(f"docker cp {work}/appendonlydir vt-aof5:/data/")
    sh("docker start vt-aof5")
    aof5 = wait(6406)
    s.check_true("this build starts on the stream's AOF", aof5 is not None)
    if aof5:
        s.check("regression guard: AOF replay holds the 1.1.1 primary's sets",
                [], [k for k in want if stream_digest(aof5)[k] != want[k]])
        s.check("the replay rejected nothing", 0, critical_lines("vt-aof5"))
    sh("docker rm -f vt-old5 vt-rep5 vt-aof5")
    subprocess.run(["rm", "-rf", work])
    s.finish()


main()
