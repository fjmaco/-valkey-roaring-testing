"""Suite 24 — Configurable limits, the maxmemory check and Base64 blobs.

Contracts:

  - Limits are module configs. `valkey-roaring.max-reply-elements`
    (default 100,000,000, settable from 1 to 2^32) caps the elements
    GETINTARRAY, RANGEINTARRAY and GETBITARRAY may list;
    `valkey-roaring.max-write-values` (default 2^38, settable from 1 to
    2^38) bounds the contiguous values one SETRANGE, SETFULL or BITOP NOT
    may build. With the defaults the refusals are byte-identical to the
    fixed limits they replace; a changed value is enforced at its exact
    boundary on both widths and named in the refusal; values outside the
    bounds are refused by CONFIG SET and fail a server start; values given
    at startup apply from the first command. A replica or an AOF replay
    with a lower write limit than the primary still applies every write the
    primary accepted.
  - The maxmemory check. With maxmemory set and noeviction, a range write
    whose estimated result would push used memory past maxmemory is refused
    with the server's own OOM reply, before anything is allocated or
    created; smaller writes still go through; under an eviction policy only
    a write larger than maxmemory itself is refused; a replica (which
    ignores maxmemory) and an AOF replay apply such writes regardless;
    inside MULTI/EXEC and Lua the refusal is an ordinary per-command error.
  - Base64 blobs. `EXPORT key BASE64` replies the standard Base64 (with
    padding) of the raw EXPORT blob for real datasets on both widths, so it
    is canonical too; `IMPORT key text BASE64` accepts it, CRoaring blobs
    encoded by Python's base64, and text pasted on a valkey-cli command
    line; strict decoding refuses every malformed text with the bad-binary
    reply and stores nothing; the token is exact; a BASE64 IMPORT that adds
    nothing is a no-op (no dirty count, no WATCH abort); it replicates and
    replays from the AOF; the reply is a bulk string under RESP2 and RESP3.

Escape class targeted: limits that drift from their documented defaults or
ignore their configuration, a memory refusal that leaves partial state or
fires where it must not (replicas, replay), and a text encoding that is not
the exact inverse of the binary one.
"""

import base64
import subprocess
import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from pyroaring import BitMap, BitMap64

from lib import datasets
from lib.compose import sh
from lib.harness import Suite
from lib.raw_resp import RawClient, encode
from lib.valkey_client import Client, ReplyError

MRE = "valkey-roaring.max-reply-elements"
MWV = "valkey-roaring.max-write-values"
LOAD = "valkey-server --loadmodule /usr/lib/valkey/modules/libvalkey_roaring.so"
REPLICA_PORT, AOF_PORT, STARTUP_PORT = 6410, 6411, 6412

OOM = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
BAD = b"-ERR bad binary data for roaring\r\n"
SYNTAX = b"-ERR syntax error\r\n"
MB = 1 << 20
SUB_BYTES = 3 * MB  # one full 32-bit sub-bitmap of run containers


def too_large(limit):
    return b"-Roaring: range too large: maximum %d elements\r\n" % limit


def wait_ping(port, tries=60):
    for _ in range(tries):
        try:
            c = Client(port=port, timeout=5)
            if c.cmd("PING") == "PONG":
                return c
        except OSError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"server on :{port} did not come up")


def info_field(c, section, field):
    text = c.cmd("INFO", section).decode()
    return text.split(f"\r\n{field}:")[1].split("\r\n")[0]


def flush(c):
    """FLUSHALL, then wait out lazy freeing: the memory checks below size
    maxmemory from used_memory, which counts values still being freed."""
    c.cmd("FLUSHALL", "SYNC")
    for _ in range(100):
        if info_field(c, "memory", "lazyfree_pending_objects") == "0":
            return
        time.sleep(0.05)


def load_key(c, prefix, key, vals):
    for i in range(0, len(vals), 5000):
        c.cmd(f"{prefix}.APPENDINTARRAY" if i else f"{prefix}.SETINTARRAY", key, *vals[i:i + 5000])


def configs(c):
    flat = c.cmd("CONFIG", "GET", "valkey-roaring.*")
    return {flat[i].decode(): int(flat[i + 1]) for i in range(0, len(flat), 2)}


def server_image():
    cid = sh("docker compose ps -q valkey").stdout.strip()
    net = sh(f"docker inspect -f '{{{{range $k, $v := .NetworkSettings.Networks}}}}{{{{$k}}}}{{{{end}}}}' {cid}").stdout.strip()
    img = sh(f"docker inspect -f '{{{{.Config.Image}}}}' {cid}").stdout.strip()
    return net, img


def defaults_and_texts(s, c, raw):
    s.section("defaults: reply texts unchanged")
    s.check("both limits registered with their defaults",
            {MRE: 100_000_000, MWV: 1 << 38}, configs(c))
    c.cmd("R.SETINTARRAY", "lim:k", 1, 2, 3)
    c.cmd("R.SETFULL", "lim:full")
    rows = [
        (["R.RANGEINTARRAY", "lim:k", 0, 100_000_000], too_large(100_000_000)),
        (["R.RANGEINTARRAY", "lim:k", 0, 99_999_999], b"*3\r\n:1\r\n:2\r\n:3\r\n"),
        (["R.GETINTARRAY", "lim:full"], too_large(100_000_000)),
        (["R.GETBITARRAY", "lim:full"], too_large(100_000_000)),
        (["R64.SETFULL", "lim:f64"], too_large(1 << 38)),
        (["R64.SETRANGE", "lim:r64", 0, (1 << 38) + 1], too_large(1 << 38)),
        (["R64.BITOP", "NOT", "lim:n64", "lim:none", 1 << 38], too_large(1 << 38)),
    ]
    for resp in (2, 3):
        r = raw if resp == 2 else RawClient(resp=3)
        for args, expected in rows:
            s.check(f"RESP{resp} {' '.join(map(str, args))}", expected, r.reply(*args))
    s.check("nothing stored by the refusals", 0, c.cmd("EXISTS", "lim:f64", "lim:r64", "lim:n64"))
    c.cmd("DEL", "lim:k", "lim:full")


def configured_limits(s, c, raw):
    s.section("max-reply-elements at its boundaries")
    c.cmd("R.SETINTARRAY", "lim:six", 0, 10, 20, 30, 40, 50)
    c.cmd("R64.SETINTARRAY", "lim:six64", 0, 10, 20, 30, 40, 50)
    listing = b"*6\r\n" + b"".join(b":%d\r\n" % v for v in range(0, 60, 10))
    bit_string = "".join("1" if i % 10 == 0 else "0" for i in range(51)).encode()
    try:
        for cap in (1, 5, 6, 50, 51):
            c.cmd("CONFIG", "SET", MRE, cap)
            for P in ("R", "R64"):
                k = f"lim:six{'64' if P == 'R64' else ''}"
                s.check(f"cap {cap}: {P}.GETINTARRAY of 6 values",
                        too_large(cap) if cap < 6 else listing, raw.reply(f"{P}.GETINTARRAY", k))
                s.check(f"cap {cap}: {P}.RANGEINTARRAY window of cap positions",
                        min(cap, 6), len(c.cmd(f"{P}.RANGEINTARRAY", k, 0, cap - 1)))
                s.check(f"cap {cap}: {P}.RANGEINTARRAY window of cap+1 positions",
                        too_large(cap), raw.reply(f"{P}.RANGEINTARRAY", k, 0, cap))
                s.check(f"cap {cap}: {P}.GETBITARRAY with max 50",
                        too_large(cap) if cap <= 50 else b"$51\r\n" + bit_string + b"\r\n",
                        raw.reply(f"{P}.GETBITARRAY", k))
    finally:
        c.cmd("CONFIG", "SET", MRE, 100_000_000)
    for bad in (0, -1, (1 << 32) + 1):
        err = c.cmd_err("CONFIG", "SET", MRE, bad)
        s.check_true(f"max-reply-elements {bad} refused", "between 1 and 4294967296" in err, err)
    s.check("max-reply-elements accepts 2^32", "OK", c.cmd("CONFIG", "SET", MRE, 1 << 32))
    c.cmd("CONFIG", "SET", MRE, 100_000_000)

    s.section("max-write-values at its boundaries")
    try:
        for lim in (1, 1000, 65_536, 1 << 32, (1 << 32) + 1):
            c.cmd("CONFIG", "SET", MWV, lim)
            c.cmd("DEL", "lim:w", "lim:n")
            s.check(f"limit {lim}: R64.SETRANGE of exactly the limit", "OK",
                    c.cmd("R64.SETRANGE", "lim:w", 7, 7 + lim))
            s.check(f"limit {lim}: R64.SETRANGE one past it", too_large(lim),
                    raw.reply("R64.SETRANGE", "lim:w2", 7, 8 + lim))
            s.check(f"limit {lim}: R64.BITOP NOT universe of exactly the limit", lim,
                    c.cmd("R64.BITOP", "NOT", "lim:n", "lim:none", lim - 1))
            s.check(f"limit {lim}: R64.BITOP NOT one past it", too_large(lim),
                    raw.reply("R64.BITOP", "NOT", "lim:n2", "lim:none", lim))
            if lim + 1 <= (1 << 32) - 1:  # expressible as a 32-bit range
                s.check(f"limit {lim}: R.SETRANGE one past it", too_large(lim),
                        raw.reply("R.SETRANGE", "lim:w2", 0, lim + 1))
            c.cmd("DEL", "lim:f")
            full = raw.reply("R.SETFULL", "lim:f")
            s.check(f"limit {lim}: R.SETFULL", b"+OK\r\n" if lim >= 1 << 32 else too_large(lim), full)
            s.check(f"limit {lim}: refusals store nothing", 0, c.cmd("EXISTS", "lim:w2", "lim:n2"))
    finally:
        c.cmd("CONFIG", "SET", MWV, 1 << 38)
    for bad in (0, -5, (1 << 38) + 1):
        err = c.cmd_err("CONFIG", "SET", MWV, bad)
        s.check_true(f"max-write-values {bad} refused", "between 1 and 274877906944" in err, err)
    s.check("defaults restored", {MRE: 100_000_000, MWV: 1 << 38}, configs(c))
    c.cmd("DEL", "lim:six", "lim:six64", "lim:w", "lim:n", "lim:f")


def startup_configs(s):
    s.section("limits given at startup")
    net, img = server_image()
    sh("docker rm -f vt-lim-start")
    sh(f"docker run -d --name vt-lim-start -p {STARTUP_PORT}:6379 {img} {LOAD} "
       f"--{MRE} 10 --{MWV} 65536")
    try:
        c = wait_ping(STARTUP_PORT)
        r = RawClient(port=STARTUP_PORT)
        s.check("startup values in CONFIG GET", {MRE: 10, MWV: 65536}, configs(c))
        c.cmd("R.SETRANGE", "k", 0, 11)
        s.check("startup reply cap applies", too_large(10), r.reply("R.GETINTARRAY", "k"))
        s.check("startup write limit applies", too_large(65536), r.reply("R.SETRANGE", "k", 0, 65537))
    finally:
        sh("docker rm -f vt-lim-start")
    for name, bad in ((MWV, (1 << 38) + 1), (MRE, 0)):
        out = subprocess.run(
            f"timeout 30 docker run --rm --name vt-lim-bad {img} {LOAD} --{name} {bad}",
            shell=True, capture_output=True, text=True)
        sh("docker rm -f vt-lim-bad")
        log = out.stdout + out.stderr
        s.check_true(f"startup with {name} {bad} fails", out.returncode not in (0, 124), f"rc={out.returncode}")
        s.check_true(f"  ... naming the config", name in log and "between 1 and" in log, log[-300:])


def replica_and_aof(s, c):
    s.section("replica and AOF with a lower write limit and a small maxmemory")
    net, img = server_image()
    c.cmd("FLUSHALL")
    sh("docker rm -f vt-lim-replica vt-lim-aof")
    sh(f"docker run -d --name vt-lim-replica --network {net} -p {REPLICA_PORT}:6379 {img} {LOAD} "
       f"--replicaof valkey 6379 --{MWV} 1000 --maxmemory 8mb")
    sh(f"docker run -d --name vt-lim-aof --network {net} -p {AOF_PORT}:6379 {img} {LOAD} "
       f"--appendonly yes --save '' --{MWV} 1000 --maxmemory 8mb")
    try:
        replica = wait_ping(REPLICA_PORT)
        aof = wait_ping(AOF_PORT)
        for _ in range(60):
            if "master_link_status:up" in replica.cmd("INFO", "replication").decode():
                break
            time.sleep(0.5)
        # Accepted by servers with the default limits and no maxmemory.
        aof.cmd("CONFIG", "SET", MWV, 1 << 38)
        aof.cmd("CONFIG", "SET", "maxmemory", 0)
        blob = base64.b64encode(BitMap([3, 70_000, 4_000_000_000]).serialize())
        writes = [
            ["R.SETRANGE", "w:range", 0, 100_000],
            ["R64.SETRANGE", "w:24mb", 0, 1 << 35],
            ["R64.BITOP", "NOT", "w:not", "w:none", 70_000],
            ["R.IMPORT", "w:b64", blob, "BASE64"],
            ["R.SETFULL", "w:full"],
        ]
        for w in writes:
            c.cmd(*w)
            aof.cmd(*w)
        keys = [("R", "w:range"), ("R64", "w:24mb"), ("R64", "w:not"), ("R", "w:b64"), ("R", "w:full")]
        expected = {k: c.cmd(f"{P}.EXPORT", k) for P, k in keys}
        m_off = int(info_field(c, "replication", "master_repl_offset"))
        for _ in range(60):
            if int(info_field(replica, "replication", "slave_repl_offset")) >= m_off:
                break
            time.sleep(0.5)
        got = {k: replica.cmd(f"{P}.EXPORT", k) for P, k in keys}
        s.check("replica (write limit 1000, maxmemory 8mb) applied every write", expected, got)
        s.check("  ... while its own limit stays", 1000, configs(replica)[MWV])

        time.sleep(1.5)  # appendfsync everysec
        sh("docker restart vt-lim-aof")
        aof = wait_ping(AOF_PORT)
        s.check("AOF replay under write limit 1000 and maxmemory 8mb restores every key",
                expected, {k: aof.cmd(f"{P}.EXPORT", k) for P, k in keys})
        aof.cmd("CONFIG", "SET", "maxmemory", 0)  # over 8mb now: the server would refuse writes
        s.check("  ... the startup limit applies to clients again", too_large(1000),
                RawClient(port=AOF_PORT).reply("R.SETRANGE", "w:new", 0, 1001))
    finally:
        sh("docker rm -f vt-lim-replica vt-lim-aof")
    c.cmd("FLUSHALL")


def maxmemory_check(s, c, raw):
    s.section("maxmemory check")
    flush(c)
    policy = c.cmd("CONFIG", "GET", "maxmemory-policy")[1]
    used = lambda: int(info_field(c, "memory", "used_memory"))  # noqa: E731
    try:
        c.cmd("CONFIG", "SET", "maxmemory-policy", "noeviction")
        c.cmd("R.SETINTARRAY", "mm:dest", 1, 2, 3)
        c.cmd("R64.SETINTARRAY", "mm:dest64", 1, 2, 3)
        dest_blob = c.cmd("R64.EXPORT", "mm:dest64")
        c.cmd("CONFIG", "SET", "maxmemory", used() + 40 * MB)
        before = used()
        s.check("R64.SETRANGE of 2^38 values (~200 MB) refused", OOM,
                raw.reply("R64.SETRANGE", "mm:big", 0, 1 << 38))
        s.check("R64.BITOP NOT over 2^38 values refused", OOM,
                raw.reply("R64.BITOP", "NOT", "mm:dest64", "mm:none", (1 << 38) - 1))
        s.check_true("refusals allocated nothing", used() - before < MB, f"{used() - before} bytes")
        s.check("  ... created nothing", 0, c.cmd("EXISTS", "mm:big"))
        s.check("  ... left the destination as it was", dest_blob, c.cmd("R64.EXPORT", "mm:dest64"))
        s.check("R.BITOP NOT over the 32-bit space (3 MB) fits", 4294967293,
                c.cmd("R.BITOP", "NOT", "mm:not32", "mm:dest", 4294967295))
        c.cmd("DEL", "mm:not32")
        s.check("R.SETFULL (3 MB) fits", "OK", c.cmd("R.SETFULL", "mm:full"))
        s.check("24 MB with ~37 MB of headroom fits", "OK", c.cmd("R64.SETRANGE", "mm:24", 0, 1 << 35))
        s.check("another 24 MB with ~13 MB left is refused", OOM, raw.reply("R64.SETRANGE", "mm:24b", 0, 1 << 35))
        s.check("a small write still works", "OK", c.cmd("R64.SETRANGE", "mm:small", 0, 1000))

        s.section("maxmemory check inside MULTI/EXEC and Lua")
        # Raw frames read straight off the socket: RawClient's ECHO marker
        # would be queued inside MULTI.
        multi = RawClient()
        block = [["MULTI"], ["R.SETBIT", "mm:x", 1, 1], ["R64.SETRANGE", "mm:big", 0, 1 << 38],
                 ["R.SETBIT", "mm:x", 2, 1], ["EXEC"]]
        multi.sock.sendall(b"".join(encode(cmd) for cmd in block))
        s.check("EXEC: the big write fails alone",
                [b"+OK\r\n"] + [b"+QUEUED\r\n"] * 3 + [b"*3\r\n:0\r\n" + OOM + b":0\r\n"],
                [multi._frame() for _ in block])
        s.check("Lua pcall gets the OOM error", OOM,
                raw.reply("EVAL", "return redis.pcall('R64.SETRANGE', KEYS[1], 0, ARGV[1])", 1, "mm:big", 1 << 38))
        s.check("nothing created by either", 0, c.cmd("EXISTS", "mm:big"))

        s.section("maxmemory check under an eviction policy")
        c.cmd("CONFIG", "SET", "maxmemory", 0)
        flush(c)
        for i in range(8):
            c.cmd("R.SETFULL", f"mm:fill{i}")
        c.cmd("CONFIG", "SET", "maxmemory", used() + 5 * MB)
        s.check("noeviction: 24 MB with 5 MB of headroom refused", OOM,
                raw.reply("R64.SETRANGE", "mm:24", 0, 1 << 35))
        evicted = int(info_field(c, "stats", "evicted_keys"))
        c.cmd("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
        s.check("allkeys-lru: the same write is accepted", "OK", c.cmd("R64.SETRANGE", "mm:24", 0, 1 << 35))
        for _ in range(40):
            c.cmd("PING")  # every command starts by evicting down to maxmemory
            if int(info_field(c, "stats", "evicted_keys")) > evicted:
                break
            time.sleep(0.05)
        s.check_true("  ... and the server evicts to make room",
                     int(info_field(c, "stats", "evicted_keys")) > evicted, "no evictions")
        s.check("allkeys-lru: a write larger than maxmemory itself is refused", OOM,
                raw.reply("R64.SETRANGE", "mm:big", 0, 1 << 38))
    finally:
        c.cmd("CONFIG", "SET", "maxmemory", 0)
        c.cmd("CONFIG", "SET", "maxmemory-policy", policy)
    flush(c)
    s.check("without maxmemory there is no check", "OK", c.cmd("R64.SETRANGE", "mm:24", 0, 1 << 35))
    c.cmd("FLUSHALL")


def base64_blobs(s, c, raw):
    s.section("BASE64 on real datasets, both widths, vs CRoaring")
    c.cmd("FLUSHALL")
    mismatches = []
    for ds in ("census1881", "wikileaks-noquotes"):
        for i, vals in enumerate(datasets.load(ds, max_files=12)):
            for P, shift, cls in (("R", 0, BitMap), ("R64", 1 << 33, BitMap64)):
                key = f"b64:{ds}:{i}:{P}"
                shifted = [v + shift for v in vals]
                load_key(c, P, key, shifted)
                text = c.cmd(f"{P}.EXPORT", key, "BASE64")
                if text != base64.b64encode(c.cmd(f"{P}.EXPORT", key)):
                    mismatches.append(f"{key}: not the raw blob's Base64")
                if cls.deserialize(base64.b64decode(text, validate=True)) != cls(shifted):
                    mismatches.append(f"{key}: CRoaring reads other contents")
                if c.cmd(f"{P}.IMPORT", key + ":copy", text, "BASE64") != len(set(vals)):
                    mismatches.append(f"{key}: IMPORT cardinality")
                if c.cmd(f"{P}.CONTAINS", key, key + ":copy", "EQ") != 1:
                    mismatches.append(f"{key}: IMPORT contents")
                theirs = base64.b64encode(cls(shifted).serialize())
                if c.cmd(f"{P}.IMPORT", key + ":croaring", theirs, "BASE64") != len(set(vals)):
                    mismatches.append(f"{key}: CRoaring blob via BASE64")
                if c.cmd(f"{P}.CONTAINS", key, key + ":croaring", "EQ") != 1:
                    mismatches.append(f"{key}: CRoaring blob contents")
    s.check("every dataset key round-trips through BASE64", [], mismatches)

    s.section("BASE64 is canonical")
    for P in ("R", "R64"):
        c.cmd(f"{P}.SETRANGE", f"b64:run{P}", 5, 8)
        c.cmd(f"{P}.SETINTARRAY", f"b64:arr{P}", 5, 6, 7)
        s.check(f"{P}: run- and array-built {{5,6,7}} give one text",
                c.cmd(f"{P}.EXPORT", f"b64:arr{P}", "BASE64"), c.cmd(f"{P}.EXPORT", f"b64:run{P}", "BASE64"))
    for resp in (2, 3):
        r = raw if resp == 2 else RawClient(resp=3)
        s.check(f"RESP{resp}: EXPORT BASE64 is a bulk string", b"$", r.reply("R.EXPORT", "b64:runR", "BASE64")[:1])

    s.section("strict decoding")
    good = base64.b64encode(BitMap([1, 2, 3, 300_000]).serialize())  # 32 bytes: one '='
    good64 = base64.b64encode(BitMap64([1, 1 << 40]).serialize())    # 52 bytes: '=='
    s.check_true("test texts end in padding", good.endswith(b"=") and good64.endswith(b"=="),
                 f"{good!r} {good64!r}")
    long_text = base64.b64encode(BitMap(range(0, 200_000, 7)).serialize())
    alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

    def flip_unused_bit(text):
        """Set a bit the padding leaves unused in the last data character."""
        pad = text.index(b"=")
        last = alphabet.index(text[pad - 1])
        return text[:pad - 1] + bytes([alphabet[last | 1]]) + text[pad:]

    malformed = {
        "padding removed": good.rstrip(b"="),
        "one padding character too many": good + b"=",
        "truncated by one": good[:-1],
        "trailing newline": good + b"\n",
        "leading space": b" " + good,
        "MIME line breaks": b"\r\n".join(long_text[i:i + 76] for i in range(0, len(long_text), 76)),
        "URL-safe '-'": b"-" + good[1:],
        "URL-safe '_'": b"_" + good[1:],
        "padding inside": good[:4] + b"==" + good[6:],
        "NUL byte": good[:-4] + b"\0" + good[-3:],
        "nonzero bits under '='": flip_unused_bit(good),
        "nonzero bits under '=='": flip_unused_bit(good64),
        "empty text": b"",
        "Base64 of a non-blob": base64.b64encode(b"definitely not a roaring bitmap"),
    }
    for name, text in malformed.items():
        s.check(f"R.IMPORT refuses: {name}", BAD, raw.reply("R.IMPORT", "b64:bad", text, "BASE64"))
        s.check(f"R64.IMPORT refuses: {name}", BAD, raw.reply("R64.IMPORT", "b64:bad", text, "BASE64"))
    s.check("nothing stored", 0, c.cmd("EXISTS", "b64:bad"))
    s.check("the well-formed text imports", 4, c.cmd("R.IMPORT", "b64:good", good, "BASE64"))
    s.check("the well-formed 64-bit text imports", 2, c.cmd("R64.IMPORT", "b64:good64", good64, "BASE64"))

    s.section("token, arity and keys")
    for token in ("base64", "Base64", "BASE64\0", "BASE64 ", "BASE", "RAW", ""):
        s.check(f"EXPORT token {token!r}", SYNTAX, raw.reply("R.EXPORT", "b64:good", token))
        s.check(f"IMPORT token {token!r}", SYNTAX, raw.reply("R64.IMPORT", "b64:bad", good64, token))
    s.check("nothing stored", 0, c.cmd("EXISTS", "b64:bad"))
    s.check("EXPORT checks the key first", b"-Roaring: key does not exist\r\n",
            raw.reply("R.EXPORT", "b64:missing", "nope"))
    c.cmd("SET", "b64:str", "x")
    s.check_true("EXPORT BASE64 of a string key is WRONGTYPE",
                 raw.reply("R64.EXPORT", "b64:str", "BASE64").startswith(b"-WRONGTYPE"))
    for args in (["R.EXPORT", "b64:good", "BASE64", "x"], ["R64.IMPORT", "b64:good64", good64, "BASE64", "x"]):
        s.check_true(f"{args[0]} with one argument too many",
                     b"wrong number of arguments" in raw.reply(*args))
    s.check("COMMAND GETKEYS of IMPORT ... BASE64", [b"b64:k"],
            c.cmd("COMMAND", "GETKEYS", "R.IMPORT", "b64:k", good, "BASE64"))

    s.section("pasted on a valkey-cli command line")
    out = sh("docker compose exec -T valkey sh -c "
             "'valkey-cli R.IMPORT b64:pasted \"$(valkey-cli R.EXPORT b64:good BASE64)\" BASE64'")
    s.check("valkey-cli EXPORT BASE64 | IMPORT BASE64", "4", out.stdout.strip())
    s.check("  ... same set", 1, c.cmd("R.CONTAINS", "b64:pasted", "b64:good", "EQ"))

    s.section("write signals")
    saved = c.cmd("CONFIG", "GET", "save")[1]
    c.cmd("CONFIG", "SET", "save", "")
    try:
        dirty = lambda: int(info_field(c, "persistence", "rdb_changes_since_last_save"))  # noqa: E731
        sub = base64.b64encode(BitMap([1, 300_000]).serialize())
        w = Client()
        w.cmd("WATCH", "b64:good")
        d0 = dirty()
        s.check("IMPORT BASE64 of a subset replies the cardinality", 4,
                c.cmd("R.IMPORT", "b64:good", sub, "BASE64"))
        s.check("  ... and counts no change", 0, dirty() - d0)
        w.cmd("MULTI")
        w.cmd("PING")
        s.check_true("  ... nor aborts a WATCH", w.cmd("EXEC") is not None)
        new = base64.b64encode(BitMap([9]).serialize())
        s.check("IMPORT BASE64 that adds a value", 5, c.cmd("R.IMPORT", "b64:good", new, "BASE64"))
        s.check("  ... counts one change", 1, dirty() - d0)
    finally:
        c.cmd("CONFIG", "SET", "save", saved)
    c.cmd("FLUSHALL")


def main():
    s = Suite("24 limits and encoding")
    c = Client(timeout=120)
    raw = RawClient(timeout=120)
    c.cmd("FLUSHALL")
    try:
        defaults_and_texts(s, c, raw)
        configured_limits(s, c, raw)
        startup_configs(s)
        maxmemory_check(s, c, raw)
        base64_blobs(s, c, raw)
        replica_and_aof(s, c)
    except ReplyError as e:
        s.check_true("unexpected error reply", False, str(e))
    finally:
        c.cmd("CONFIG", "SET", MRE, 100_000_000)
        c.cmd("CONFIG", "SET", MWV, 1 << 38)
        c.cmd("CONFIG", "SET", "maxmemory", 0)
        c.cmd("FLUSHALL")
    s.finish()


main()
