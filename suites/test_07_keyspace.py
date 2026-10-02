"""Suite 07 — Generic keyspace machinery over module keys.

Module types must interoperate with Valkey's generic key commands. This
suite exercises TYPE, EXISTS, DEL/UNLINK, RENAME, COPY, EXPIRE/PERSIST/TTL,
SCAN with TYPE filter, RANDOMKEY, MEMORY USAGE, OBJECT ENCODING,
keyspace notifications for module writes, and the key-modified signal that
WATCH and client-side caching depend on: reads and writes that change
nothing must not fire it, real changes must.

Escape class targeted: integration seams between the module's type
registration (free/copy/mem_usage callbacks) and server machinery the
repo suite never touches.
"""

import random
import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client, ReplyError

def main():
    s = Suite("07 keyspace")
    c = Client()
    c.cmd("FLUSHALL")

    c.cmd("R.SETINTARRAY", "k", 1, 2, 3, 100000)
    c.cmd("R64.SETINTARRAY", "k64", 1, 1 << 40)

    s.section("TYPE / EXISTS / SCAN / OBJECT")
    s.check("type 32", "vrroaring", c.cmd("TYPE", "k"))
    s.check("type 64", "vroarng64", c.cmd("TYPE", "k64"))
    s.check("exists", 1, c.cmd("EXISTS", "k"))
    cursor, keys = c.cmd("SCAN", 0, "TYPE", "vrroaring")
    s.check("scan by module type", [b"k"], keys)
    s.check("object encoding is raw module", b"raw", c.cmd("OBJECT", "ENCODING", "k"))

    s.section("RENAME / COPY / DEL / UNLINK")
    c.cmd("RENAME", "k", "k2")
    s.check("renamed key readable", 4, c.cmd("R.BITCOUNT", "k2"))
    s.check("old name gone", 0, c.cmd("EXISTS", "k"))
    copied = c.cmd("COPY", "k2", "k3")
    s.check("copy returns 1", 1, copied)
    s.check("copy equal", 1, c.cmd("R.CONTAINS", "k2", "k3", "EQ"))
    c.cmd("R.SETBIT", "k3", 999, 1)
    s.check("copy is independent", 0, c.cmd("R.GETBIT", "k2", 999))
    s.check("del", 1, c.cmd("DEL", "k3"))
    c.cmd("COPY", "k2", "k4")
    s.check("unlink (lazy free path)", 1, c.cmd("UNLINK", "k4"))

    s.section("EXPIRE / TTL / PERSIST")
    c.cmd("EXPIRE", "k2", 100)
    s.check_true("ttl set", 0 < c.cmd("TTL", "k2") <= 100, c.cmd("TTL", "k2"))
    c.cmd("PERSIST", "k2")
    s.check("persist clears ttl", -1, c.cmd("TTL", "k2"))
    c.cmd("R.SETINTARRAY", "gone", 7)
    c.cmd("PEXPIRE", "gone", 50)
    time.sleep(0.3)
    s.check("expired module key vanishes", 0, c.cmd("EXISTS", "gone"))
    s.check("read after expiry acts empty", 0, c.cmd("R.BITCOUNT", "gone"))

    s.section("MEMORY USAGE reflects contents")
    small = c.cmd("MEMORY", "USAGE", "k2")
    c.cmd("R.SETRANGE", "bigmem", 0, 2000000)
    big = c.cmd("MEMORY", "USAGE", "bigmem")
    s.check_true("memory usage positive", small > 0, small)
    s.check_true("bigger key reports more", big > small, f"{big} vs {small}")

    # MEMORY USAGE estimates the value's heap footprint, so it should track
    # what the allocator reports for one freshly built key (sparse ids:
    # ~250 array containers). used_memory_dataset leaves out replication
    # backlog and AOF buffers, which the 1.8 MB command itself can grow once
    # an earlier suite attached a replica; the build runs on a separate
    # connection, closed before measuring, so its query buffer is not counted.
    lazy = c.cmd("CONFIG", "GET", "lazyfree-lazy-user-del")[1]
    c.cmd("CONFIG", "SET", "lazyfree-lazy-user-del", "no")
    rng = random.Random(0x7E57)
    vals = rng.sample(range(1 << 24), 200_000)

    def used_memory():
        info = c.cmd("INFO", "memory").decode()
        return int(info.split("used_memory_dataset:")[1].split()[0])

    for prefix in ("R", "R64"):
        before = used_memory()
        b = Client()
        b.cmd(f"{prefix}.SETINTARRAY", "acc", *vals)
        b.close()
        time.sleep(0.2)
        grown = used_memory() - before
        mem = c.cmd("MEMORY", "USAGE", "acc", "SAMPLES", "0")
        s.check_true(f"{prefix} MEMORY USAGE within 25% of allocated bytes",
                     0.75 <= mem / grown <= 1.25, f"usage={mem} allocated={grown}")
        c.cmd("DEL", "acc")
    c.cmd("CONFIG", "SET", "lazyfree-lazy-user-del", lazy)

    s.section("keyspace notifications")
    # Documented contract: module WRITE commands emit no keyspace events
    # (matching redis-roaring, which also never calls NotifyKeyspaceEvent).
    # Server-GENERATED events for module keys (expiry) must still fire.
    c.cmd("CONFIG", "SET", "notify-keyspace-events", "KEA")
    sub = Client()
    sub.cmd("PSUBSCRIBE", "__keyevent@0__:*")
    c.cmd("R.SETINTARRAY", "notified", 9)
    c.cmd("PEXPIRE", "notified", 100)
    deadline = time.time() + 5
    events = []
    while time.time() < deadline:
        try:
            sub.sock.settimeout(max(0.1, deadline - time.time()))
            msg = sub._read_reply()
        except OSError:
            break
        if msg and msg[0] == b"pmessage" and msg[3] == b"notified":
            events.append(msg[2])
            if b"expired" in msg[2]:
                break
    s.check_true("no write event, but expiry event fires",
                 any(b"expired" in e for e in events)
                 and not any(b"setbit" in e.lower() for e in events),
                 f"events={events}")
    c.cmd("CONFIG", "SET", "notify-keyspace-events", "")

    s.section("WATCH and client-side caching see real changes only")
    c.cmd("R.SETINTARRAY", "trk:a", 1, 2, 3)
    inv = Client()
    inv_id = inv.cmd("CLIENT", "ID")
    inv.cmd("SUBSCRIBE", "__redis__:invalidate")
    tracker = Client()
    tracker.cmd("CLIENT", "TRACKING", "on", "REDIRECT", inv_id, "BCAST", "PREFIX", "trk:")

    def invalidated(wait):
        keys = []
        deadline = time.time() + wait
        while time.time() < deadline:
            try:
                inv.sock.settimeout(max(0.05, deadline - time.time()))
                msg = inv._read_reply()
            except OSError:
                break
            if msg and msg[0] == b"message" and msg[1] == b"__redis__:invalidate":
                keys.extend(msg[2] or [])
        return keys

    # Each of these leaves trk:a = {1, 2, 3} exactly as it was.
    unchanged = [
        ["R.EXPORT", "trk:a"],
        ["R.GETINTARRAY", "trk:a"],
        ["R.SETBIT", "trk:a", 1, 1],
        ["R.SETBIT", "trk:a", 9, 0],
        ["R.APPENDINTARRAY", "trk:a", 2, 3],
        ["R.DELETEINTARRAY", "trk:a", 99],
        ["R.CLEARBITS", "trk:a", 99],
        ["R.SETRANGE", "trk:a", 1, 4],
        ["EVAL", "return redis.call('R.IMPORT', KEYS[1], redis.call('R.EXPORT', KEYS[1]))",
         1, "trk:a"],
    ]
    replies = c.pipeline(unchanged)
    s.check_true("no-op commands succeed",
                 not any(isinstance(r, ReplyError) for r in replies), f"replies={replies}")
    s.check("reads and no-op writes send no invalidation", [], invalidated(0.5))
    c.cmd("R.SETBIT", "trk:a", 50, 1)
    s.check("a real write invalidates the key", [b"trk:a"], invalidated(1))

    w = Client()
    w.cmd("WATCH", "trk:a")
    c.pipeline(unchanged)
    w.cmd("MULTI")
    w.cmd("R.BITCOUNT", "trk:a")
    s.check("WATCH survives reads and no-op writes", [4], w.cmd("EXEC"))
    w.cmd("WATCH", "trk:a")
    c.cmd("R.SETBIT", "trk:a", 51, 1)
    w.cmd("MULTI")
    w.cmd("R.BITCOUNT", "trk:a")
    s.check("WATCH aborts after a real write", None, w.cmd("EXEC"))
    tracker.cmd("CLIENT", "TRACKING", "off")
    for conn in (inv, tracker, w):
        conn.close()

    c.cmd("FLUSHALL")
    s.finish()


main()
