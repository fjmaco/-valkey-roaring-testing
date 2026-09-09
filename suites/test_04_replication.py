"""Suite 04 — Replication consistency under live write load.

Attaches a real replica, then streams thousands of mixed write commands at
the primary while the replica is syncing. At the end, waits for offset
convergence and requires byte-identical EXPORT blobs for every key on both
sides, plus replica-side read correctness.

Escape class targeted: verbatim-propagation gaps (a real bug class in this
module's history: writes silently not replicating), ordering effects, and
commands whose replication payload differs from their effect.
"""

import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib import datasets
from lib.compose import sh
from lib.harness import Suite
from lib.valkey_client import Client


def main():
    s = Suite("04 replication")
    c = Client()
    c.cmd("FLUSHALL")

    cid = sh("docker compose ps -q valkey").stdout.strip()
    net = sh(f"docker inspect -f '{{{{range $k, $v := .NetworkSettings.Networks}}}}{{{{$k}}}}{{{{end}}}}' {cid}").stdout.strip()
    img = sh(f"docker inspect -f '{{{{.Config.Image}}}}' {cid}").stdout.strip()
    sh("docker rm -f vt-replica")
    sh(f"docker run -d --name vt-replica --network {net} -p 6391:6379 {img} "
       f"valkey-server --loadmodule /usr/lib/valkey/modules/libvalkey_roaring.so "
       f"--replicaof valkey 6379")

    # Stream mixed writes WHILE the replica is connecting/syncing
    s.section("write load during sync")
    data = datasets.load("wikileaks-noquotes", max_files=20)
    cmds = []
    for i, vals in enumerate(data):
        head, tail = vals[: len(vals) // 2], vals[len(vals) // 2:]
        cmds.append(["R.SETINTARRAY", f"r:{i}", *head[:4000]])
        cmds.append(["R.APPENDINTARRAY", f"r:{i}", *tail[:4000]])
        cmds.append(["R.SETBIT", f"r:{i}", 4294967295, 1])
        cmds.append(["R.CLEARBITS", f"r:{i}", *vals[:50]])
        cmds.append(["R64.SETINTARRAY", f"r64:{i}", *[v + (1 << 35) for v in head[:2000]]])
        cmds.append(["R64.SETBIT", f"r64:{i}", (1 << 63) + i, 1])
        cmds.append(["R.BITOP", "XOR", f"rx:{i}", f"r:{i}", f"r:{max(0, i - 1)}"])
        cmds.append(["R.BITOP", "NOT", f"rn:{i}", f"r:{i}", 100000])
        cmds.append(["R.SETRANGE", f"rr:{i}", i * 100000, i * 100000 + 70000])
        cmds.append(["R.OPTIMIZE", f"rr:{i}"])
    for j in range(0, len(cmds), 50):
        c.pipeline(cmds[j:j + 50])

    # Wait for replica catch-up
    replica = None
    for _ in range(60):
        try:
            replica = Client(port=6391, timeout=3)
            info = replica.cmd("INFO", "replication").decode()
            if "master_link_status:up" in info:
                m = c.cmd("INFO", "replication").decode()
                m_off = int(m.split("master_repl_offset:")[1].split()[0])
                r_off = int(info.split("slave_repl_offset:")[1].split()[0])
                if r_off >= m_off:
                    break
        except (OSError, IndexError):
            pass
        time.sleep(1)

    s.section("digest equality for every key")
    keys = sorted(k.decode() for k in c.cmd("KEYS", "*"))
    s.check_true("keys exist", len(keys) > 70, f"n={len(keys)}")
    s.check("replica has same keyset", keys,
            sorted(k.decode() for k in replica.cmd("KEYS", "*")))
    mismatches = 0
    for k in keys:
        prefix = "R64" if k.startswith("r64:") else "R"
        if c.cmd(f"{prefix}.EXPORT", k) != replica.cmd(f"{prefix}.EXPORT", k):
            mismatches += 1
            print(f"  MISMATCH: {k}")
    s.check("EXPORT blobs identical on replica", 0, mismatches)

    s.section("replica-side reads")
    s.check("replica GETBIT", 1, replica.cmd("R.GETBIT", "r:3", 4294967295))
    s.check("replica R64 >2^63 bit", 1, replica.cmd("R64.GETBIT", "r64:5", (1 << 63) + 5))

    sh("docker rm -f vt-replica")
    c.cmd("FLUSHALL")
    s.finish()


main()
