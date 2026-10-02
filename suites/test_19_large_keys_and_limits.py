"""Suite 19 — Very large keys, lazy free, and the output-buffer safety net.

Gated: runs only with VT_HEAVY=1 (several GB of traffic, a few minutes).

  1. Ten-million-value keys of both widths: bulk build, full GETINTARRAY
     (about 100 MB of reply), deep RANGEINTARRAY pages, BITOP between two
     such keys, EXPORT -> IMPORT round trip, all checked against the model
     and with the connection staying in sync.
  2. Lazy free: UNLINK of a key holding three million containers must
     return quickly and leave the main thread responsive (the type reports
     a free effort, so the value is freed on a background thread); DEL is
     timed alongside for reference.
  3. GETINTARRAY of an R.SETFULL key (2^32 values) is refused before any
     reply is written; 1.1.1 streamed it until the kernel killed the
     server. A key exactly at the 100M-value cap still streams about 1 GB:
     in a helper container capped at 2 GB, with a normal-client output
     buffer limit configured and a client that never reads, the server
     must survive, the client being dropped once its buffer passes the
     limit, and memory must stay bounded. The time the main thread spends
     streaming into the dropped client is printed.

Escape class targeted: behaviour that only appears at scale — reply
streaming at 10M elements, main-thread stalls on free, and memory blow-ups
from replies no client can consume.
"""

import random
import socket
import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.compose import sh
from lib.harness import Suite, env_flag
from lib.valkey_client import Client


def load(c, P, key, vals, chunk=50_000):
    c.cmd("DEL", key)
    for i in range(0, len(vals), chunk):
        c.cmd(f"{P}.SETINTARRAY" if i == 0 else f"{P}.APPENDINTARRAY", key, *vals[i:i + chunk])


def as_int(x):
    return int(x) if isinstance(x, bytes) else x


def main():
    s = Suite("19 large keys and limits")
    if not env_flag("VT_HEAVY"):
        print("skipped: set VT_HEAVY=1 to run the large-key suite")
        s.finish()
    c = Client(timeout=600)
    c.cmd("FLUSHALL")
    rng = random.Random(0x1A26E)

    s.section("1. ten-million-value keys")
    n = 10_000_000
    a = sorted(rng.sample(range(1 << 31), n))
    b = sorted(rng.sample(range(1 << 31), n))
    for P, shift in (("R", 0), ("R64", (1 << 32) - (1 << 30))):
        av = [v + shift for v in a]
        t0 = time.monotonic()
        load(c, P, "A", av)
        load(c, P, "B", [v + shift for v in b])
        print(f"    {P}: built 2 x 10M in {time.monotonic() - t0:.1f}s")
        s.check(f"{P} cardinality", n, c.cmd(f"{P}.BITCOUNT", "A"))
        t0 = time.monotonic()
        got = c.cmd(f"{P}.GETINTARRAY", "A")
        print(f"    {P}: GETINTARRAY 10M in {time.monotonic() - t0:.1f}s")
        s.check(f"{P} GETINTARRAY of 10M values exact", True, len(got) == n and [as_int(x) for x in got] == av)
        del got
        s.check(f"{P} in sync after 10M-element reply", "PONG", c.cmd("PING"))
        for st in (0, n // 3, n - 1_000_000, n - 1):
            page = c.cmd(f"{P}.RANGEINTARRAY", "A", st, st + 999_999)
            s.check(f"{P} 1M page at {st}", av[st:st + 1_000_000], [as_int(x) for x in page])
        t0 = time.monotonic()
        card = c.cmd(f"{P}.BITOP", "AND", "C", "A", "B")
        s.check(f"{P} AND cardinality", len(set(a) & set(b)), card)
        card = c.cmd(f"{P}.BITOP", "XOR", "C", "A", "B")
        s.check(f"{P} XOR cardinality", len(set(a) ^ set(b)), card)
        print(f"    {P}: two BITOPs over 10M keys in {time.monotonic() - t0:.2f}s")
        blob = c.cmd(f"{P}.EXPORT", "A")
        c.cmd("DEL", "D")
        s.check(f"{P} IMPORT of the 10M blob", n, c.cmd(f"{P}.IMPORT", "D", blob))
        s.check(f"{P} round trip EQ", 1, c.cmd(f"{P}.CONTAINS", "D", "A", "EQ"))
        c.cmd("DEL", "A", "B", "C", "D")

    s.section("2. lazy free keeps the main thread responsive")
    lazy = c.cmd("CONFIG", "GET", "lazyfree-lazy-user-del")[1]
    c.cmd("CONFIG", "SET", "lazyfree-lazy-user-del", "no")
    vals = [i << 16 for i in range(3_000_000)]          # one container per value
    load(c, "R64", "huge", vals)
    c.cmd("COPY", "huge", "huge2")
    lat, stop = [], [False]

    def pinger():
        p = Client(timeout=60)
        while not stop[0]:
            t0 = time.perf_counter()
            p.cmd("PING")
            lat.append(time.perf_counter() - t0)
            time.sleep(0.001)

    th = threading.Thread(target=pinger)
    th.start()
    time.sleep(0.3)
    res = {}
    for cmd, key in (("DEL", "huge"), ("UNLINK", "huge2")):
        lat.clear()
        t0 = time.perf_counter()
        c.cmd(cmd, key)
        dt = time.perf_counter() - t0
        time.sleep(0.5)
        res[cmd] = (dt, max(lat or [0]))
    stop[0] = True
    th.join()
    print(f"    DEL: {1000 * res['DEL'][0]:.1f} ms (max PING {1000 * res['DEL'][1]:.1f} ms); "
          f"UNLINK: {1000 * res['UNLINK'][0]:.1f} ms (max PING {1000 * res['UNLINK'][1]:.1f} ms)")
    s.check_true("UNLINK of 3M containers returns within 15 ms", res["UNLINK"][0] < 0.015,
                 f"{1000 * res['UNLINK'][0]:.1f} ms")
    s.check_true("UNLINK is much cheaper than DEL on the main thread", res["UNLINK"][0] * 3 < res["DEL"][0],
                 f"UNLINK {res['UNLINK'][0]:.4f}s vs DEL {res['DEL'][0]:.4f}s")
    c.cmd("CONFIG", "SET", "lazyfree-lazy-user-del", lazy)

    s.section("3. GETINTARRAY limits and the output-buffer limit")
    cid = sh("docker compose ps -q valkey").stdout.strip()
    img = sh(f"docker inspect -f '{{{{.Config.Image}}}}' {cid}").stdout.strip()
    sh("docker rm -f vt-capped")
    sh(f"docker run -d --name vt-capped --memory=2g --memory-swap=2g -p 6396:6379 {img} "
       f"valkey-server --loadmodule /usr/lib/valkey/modules/libvalkey_roaring.so "
       f"--client-output-buffer-limit 'normal 256mb 64mb 60' --save ''")
    cap = None
    for _ in range(30):
        try:
            cap = Client(port=6396, timeout=5)
            cap.cmd("PING")
            break
        except OSError:
            time.sleep(0.5)
    cap.cmd("R.SETFULL", "full")
    t_full = time.monotonic()
    refused = cap.cmd_err("R.GETINTARRAY", "full")
    s.check_true("GETINTARRAY of 2^32 values refused up front", "range too large" in refused, refused)
    s.check_true("  ... at once", time.monotonic() - t_full < 1.0)
    cap.cmd("R.SETRANGE", "capped", 0, 100_000_000)
    victim = socket.create_connection(("127.0.0.1", 6396))
    victim.sendall(Client._encode(["R.GETINTARRAY", "capped"]))   # never read
    t0 = time.monotonic()
    time.sleep(1)                        # let the server start streaming
    back = None
    while time.monotonic() - t0 < 300:
        state = sh("docker inspect -f '{{.State.Status}} {{.State.OOMKilled}}' vt-capped").stdout.split()
        if state and state[0] != "running":
            break
        try:
            probe = Client(port=6396, timeout=2)
            if probe.cmd("PING") == "PONG":
                back = time.monotonic() - t0
                break
        except OSError:
            pass
        time.sleep(1)
    state = sh("docker inspect -f '{{.State.Status}} {{.State.OOMKilled}}' vt-capped").stdout.split()
    s.check("server survives (not OOM-killed)", ["running", "false"], state)
    if back is not None:
        info = Client(port=6396, timeout=5).cmd("INFO", "memory").decode()
        peak = int(info.split("used_memory_peak:")[1].split()[0])
        print(f"    responsive again after {back:.1f}s; used_memory_peak={peak:,}")
        s.check_true("peak memory bounded by the limit (< 600 MB)", peak < 600 * 1024 * 1024, f"peak={peak}")
    victim.close()
    sh("docker rm -f vt-capped")

    c.cmd("FLUSHALL")
    s.finish()


main()
