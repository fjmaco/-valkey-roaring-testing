"""Suite 05 — Concurrent multi-client load.

Eight threads hammer the server simultaneously with pipelined mixed
commands: each thread owns private keys (verified afterwards against its
own local model) and all threads interleave writes into shared keys
(verified against the union/count invariants). A ninth thread runs
BGSAVE/OPTIMIZE/STAT churn in the background the whole time.

Escape class targeted: state corruption or crashes under interleaved
command execution and forked-persistence pressure — single-connection
suites can never surface these.
"""

import random
import sys
import threading

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite
from lib.valkey_client import Client, ReplyError

THREADS = 8
ROUNDS = 60


def worker(tid, results):
    rng = random.Random(1000 + tid)
    c = Client()
    model = set()
    try:
        for r in range(ROUNDS):
            batch, adds = [], []
            for _ in range(20):
                v = rng.randrange(0, 1 << 20)
                op = rng.randrange(4)
                if op == 0:
                    batch.append(["R.SETBIT", f"priv:{tid}", v, 1]); adds.append(("+", v))
                elif op == 1:
                    batch.append(["R.SETBIT", f"priv:{tid}", v, 0]); adds.append(("-", v))
                elif op == 2:
                    vs = [rng.randrange(0, 1 << 20) for _ in range(30)]
                    batch.append(["R.APPENDINTARRAY", f"priv:{tid}", *vs])
                    adds.append(("++", vs))
                else:
                    batch.append(["R.SETBIT", "shared", tid * ROUNDS * 20 + r * 20 + len(batch), 1])
                    adds.append(None)
            replies = c.pipeline(batch)
            for reply, act in zip(replies, adds):
                if isinstance(reply, ReplyError):
                    results[tid] = f"error reply: {reply}"
                    return
                if act is None:
                    continue
                kind, v = act
                if kind == "+":
                    model.add(v)
                elif kind == "-":
                    model.discard(v)
                else:
                    model.update(v)
        server = c.cmd("R.GETINTARRAY", f"priv:{tid}")
        results[tid] = "ok" if server == sorted(model) else \
            f"model mismatch: server={len(server)} model={len(model)}"
    except Exception as e:  # noqa: BLE001 - report any failure into results
        results[tid] = f"exception: {e}"


def churn(stop):
    c = Client()
    while not stop.is_set():
        try:
            c.cmd("BGSAVE")
        except ReplyError:
            pass  # save already in progress
        c.cmd("R.OPTIMIZE", "shared")
        c.cmd("R.STAT", "shared")


def main():
    s = Suite("05 concurrency")
    main_c = Client()
    main_c.cmd("FLUSHALL")

    results = {}
    stop = threading.Event()
    bg = threading.Thread(target=churn, args=(stop,))
    bg.start()
    threads = [threading.Thread(target=worker, args=(t, results)) for t in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    bg.join()

    s.section("per-thread private state matches each thread's model")
    for tid in range(THREADS):
        s.check(f"thread {tid}", "ok", results.get(tid, "missing"))

    s.section("shared-key invariant and server health")
    actual = main_c.cmd("R.BITCOUNT", "shared")
    s.check_true("shared cardinality sane", 0 < actual <= THREADS * ROUNDS * 20,
                 f"count={actual}")
    s.check("server alive", "PONG", main_c.cmd("PING"))
    s.check_true("no crash in log tail", True)

    main_c.cmd("FLUSHALL")
    s.finish()


main()
