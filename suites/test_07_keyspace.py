"""Suite 07 — Generic keyspace machinery over module keys.

Module types must interoperate with Valkey's generic key commands. This
suite exercises TYPE, EXISTS, DEL/UNLINK, RENAME, COPY, EXPIRE/PERSIST/TTL,
SCAN with TYPE filter, RANDOMKEY, MEMORY USAGE, OBJECT ENCODING, and
keyspace notifications for module writes.

Escape class targeted: integration seams between the module's type
registration (free/copy/mem_usage callbacks) and server machinery the
repo suite never touches.
"""

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

    c.cmd("FLUSHALL")
    s.finish()


main()
