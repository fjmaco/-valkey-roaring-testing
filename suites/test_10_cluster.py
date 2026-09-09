"""Suite 10 — Cluster mode: slot validation and dynamic key reporting.

Starts a cluster-enabled node (all 16384 slots assigned) with the module
loaded. In cluster mode the server derives every command's key set — for
R.BITOP that comes from the module's dynamic getkeys callback — and rejects
cross-slot key combinations. This validates the promise that BITOP's key
positions are reported correctly: hash-tagged same-slot keys must work, a
trailing NOT `last` argument must NOT be treated as a key, and cross-slot
sources must be rejected with CROSSSLOT.

Escape class targeted: getkeys regressions — the exact bug class upstream
fixed in v1.7.3 — which standalone-mode tests can never observe.
"""

import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.compose import sh
from lib.harness import Suite
from lib.valkey_client import Client, ReplyError



def main():
    s = Suite("10 cluster")
    cid = sh("docker compose ps -q valkey").stdout.strip()
    img = sh(f"docker inspect -f '{{{{.Config.Image}}}}' {cid}").stdout.strip()
    sh("docker rm -f vt-cluster")
    sh(f"docker run -d --name vt-cluster -p 6393:6379 {img} "
       f"valkey-server --loadmodule /usr/lib/valkey/modules/libvalkey_roaring.so "
       f"--cluster-enabled yes --cluster-allow-reads-when-down yes")
    c = None
    for _ in range(30):
        try:
            c = Client(port=6393, timeout=3)
            if c.cmd("PING") == "PONG":
                break
        except OSError:
            time.sleep(1)
    c.cmd("CLUSTER", "ADDSLOTSRANGE", 0, 16383)
    for _ in range(30):
        if b"cluster_state:ok" in c.cmd("CLUSTER", "INFO"):
            break
        time.sleep(1)

    s.section("same-slot operations work (hash tags)")
    c.cmd("R.SETINTARRAY", "{t}a", 1, 2, 3)
    c.cmd("R.SETINTARRAY", "{t}b", 2, 3, 4)
    s.check("BITOP AND same slot", 2, c.cmd("R.BITOP", "AND", "{t}dest", "{t}a", "{t}b"))
    s.check("BITOP NOT with last arg (must not be a key)", 97,
            c.cmd("R.BITOP", "NOT", "{t}not", "{t}a", 99))
    s.check("DIFF same slot", "OK", c.cmd("R.DIFF", "{t}diff", "{t}a", "{t}b"))
    s.check("CONTAINS same slot", 1, c.cmd("R.CONTAINS", "{t}a", "{t}b"))
    c.cmd("R64.SETINTARRAY", "{t}a64", 1, 1 << 40)
    c.cmd("R64.SETINTARRAY", "{t}b64", 2, 1 << 40)
    s.check("R64 BITOP same slot", 3,
            c.cmd("R64.BITOP", "OR", "{t}d64", "{t}a64", "{t}b64"))

    s.section("cross-slot combinations rejected")
    c.cmd("R.SETINTARRAY", "{u}c", 9)
    err = c.cmd_err("R.BITOP", "AND", "{t}dest", "{t}a", "{u}c")
    s.check_true("BITOP cross-slot sources", "CROSSSLOT" in err, err)
    err = c.cmd_err("R.DIFF", "{t}diff", "{t}a", "{u}c")
    s.check_true("DIFF cross-slot", "CROSSSLOT" in err, err)
    err = c.cmd_err("R.CONTAINS", "{t}a", "{u}c")
    s.check_true("CONTAINS cross-slot", "CROSSSLOT" in err, err)
    err = c.cmd_err("R.JACCARD", "{t}a", "{u}c")
    s.check_true("JACCARD cross-slot", "CROSSSLOT" in err, err)

    s.section("single-key commands unaffected")
    s.check("SETBIT in cluster", 0, c.cmd("R.SETBIT", "solo", 7, 1))
    s.check("BITCOUNT in cluster", 1, c.cmd("R.BITCOUNT", "solo"))

    sh("docker rm -f vt-cluster")
    s.finish()


main()
