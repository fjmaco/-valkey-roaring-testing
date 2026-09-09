"""Suite 03 — Persistence at scale: RDB, DUMP/RESTORE, AOF.

Loads a real dataset, snapshots a digest of every key (cardinality, min,
max, EXPORT blob), then verifies the digests survive:

  - BGSAVE followed by a hard container restart (RDB reload path)
  - DUMP / RESTORE of module keys (same-server key cloning), including a
    RESTORE under a new name and a REPLACE
  - an AOF-enabled server across restart and across BGREWRITEAOF

Escape class targeted: serialization bugs that only appear at real data
shapes (container-type mixes), and the module's rdb_load/rdb_save pairing
with big values — the repo suite only checks a two-value bitmap.
"""

import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib import datasets
from lib.compose import sh
from lib.harness import Suite
from lib.valkey_client import Client


def wait_ping(port, tries=60):
    for _ in range(tries):
        try:
            c = Client(port=port, timeout=3)
            if c.cmd("PING") == "PONG":
                return c
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError(f"server on :{port} did not come up")


def digest(c, keys):
    out = {}
    for prefix, key in keys:
        out[key] = (
            c.cmd(f"{prefix}.BITCOUNT", key),
            c.cmd(f"{prefix}.MIN", key),
            c.cmd(f"{prefix}.MAX", key),
            c.cmd(f"{prefix}.EXPORT", key),
        )
    return out


def main():
    s = Suite("03 persistence")
    c = Client()
    c.cmd("FLUSHALL")

    s.section("load census1881 into both widths")
    keys = []
    for i, vals in enumerate(datasets.load("census1881", max_files=30)):
        for j in range(0, len(vals), 5000):
            c.cmd("R.APPENDINTARRAY" if j else "R.SETINTARRAY", f"p:{i}", *vals[j:j + 5000])
        vals64 = [v + (1 << 34) for v in vals]
        for j in range(0, len(vals64), 5000):
            c.cmd("R64.APPENDINTARRAY" if j else "R64.SETINTARRAY", f"p64:{i}", *vals64[j:j + 5000])
        keys += [("R", f"p:{i}"), ("R64", f"p64:{i}")]
    before = digest(c, keys)

    s.section("BGSAVE + hard restart")
    c.cmd("BGSAVE")
    time.sleep(2)
    sh("docker compose restart valkey")
    c = wait_ping(6379)
    after = digest(c, keys)
    s.check("all digests survive RDB restart", before, after)

    s.section("DUMP / RESTORE")
    blob = c.cmd("DUMP", "p:0")
    s.check_true("dump produces payload", blob and len(blob) > 10, f"len={len(blob or b'')}")
    c.cmd("RESTORE", "p:0:copy", 0, blob)
    s.check("restored copy equal", 1, c.cmd("R.CONTAINS", "p:0", "p:0:copy", "EQ"))
    err = c.cmd_err("RESTORE", "p:0:copy", 0, blob)
    s.check_true("restore without REPLACE errors", "BUSYKEY" in err, err)
    s.check("restore with REPLACE", "OK", c.cmd("RESTORE", "p:0:copy", 0, blob, "REPLACE"))
    blob64 = c.cmd("DUMP", "p64:0")
    c.cmd("RESTORE", "p64:0:copy", 0, blob64)
    s.check("restored r64 copy equal", 1, c.cmd("R64.CONTAINS", "p64:0", "p64:0:copy", "EQ"))

    s.section("AOF server: replay + rewrite")
    cid = sh("docker compose ps -q valkey").stdout.strip()
    net = sh(f"docker inspect -f '{{{{range $k, $v := .NetworkSettings.Networks}}}}{{{{$k}}}}{{{{end}}}}' {cid}").stdout.strip()
    img = sh(f"docker inspect -f '{{{{.Config.Image}}}}' {cid}").stdout.strip()
    sh("docker rm -f vt-aof")
    sh(f"docker run -d --name vt-aof --network {net} -p 6390:6379 {img} "
       f"valkey-server --loadmodule /usr/lib/valkey/modules/libvalkey_roaring.so --appendonly yes")
    a = wait_ping(6390)
    vals = datasets.load("census1881", max_files=1)[0]
    for j in range(0, len(vals), 5000):
        a.cmd("R.APPENDINTARRAY" if j else "R.SETINTARRAY", "aof:key", *vals[j:j + 5000])
    a.cmd("R64.SETBIT", "aof:big", (1 << 63) + 3, 1)
    expected = (len(set(vals)), min(vals), max(vals))
    time.sleep(1)

    sh("docker restart vt-aof")
    a = wait_ping(6390)
    got = (a.cmd("R.BITCOUNT", "aof:key"), a.cmd("R.MIN", "aof:key"), a.cmd("R.MAX", "aof:key"))
    s.check("AOF replay restores dataset key", expected, got)
    s.check("AOF replay restores >2^63 bit", 1,
            a.cmd("R64.GETBIT", "aof:big", (1 << 63) + 3))

    a.cmd("BGREWRITEAOF")
    time.sleep(2)
    sh("docker restart vt-aof")
    a = wait_ping(6390)
    got = (a.cmd("R.BITCOUNT", "aof:key"), a.cmd("R.MIN", "aof:key"), a.cmd("R.MAX", "aof:key"))
    s.check("AOF rewrite preserves dataset key", expected, got)
    sh("docker rm -f vt-aof")

    c.cmd("FLUSHALL")
    s.finish()


main()
