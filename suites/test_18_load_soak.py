"""Suite 18 — Load and soak: many pipelined clients over big keys.

Gated: runs only with VT_LOAD=1 (it is CPU-heavy and takes minutes).

Starts VT_LOAD_CLIENTS worker processes (default 32), each with its own
connection and a pipeline depth of VT_LOAD_PIPELINE (default 8), for
VT_SOAK_SECONDS (default 60). Workers share a set of large read-mostly
keys (1M-value R keys, a 64-bit key spread over many sub-bitmaps) and
each owns a few keys it mutates while keeping a plain Python model of
them. The mix covers every command family: point reads and writes,
GETBITS, RANGEINTARRAY pages, BITCOUNT/MIN/MAX/BITPOS, CONTAINS and
JACCARD on big keys, BITOP/DIFF into owned keys, EXPORT/IMPORT round
trips and OPTIMIZE.

Contracts:
  * no error replies other than the expected ones, no internal-error
    (panic) replies, the server stays up and in sync;
  * at the end every owned key equals its worker's model exactly;
  * shared keys are unchanged (EXPORT identical before and after);
  * used_memory with all owned keys deleted returns near its start;
  * p50 / p99 batch latency and throughput are printed per command class
    for comparison between builds (VT_PORT selects the server, so the
    same run can target a baseline build).

Escape class targeted: interleaving-dependent corruption, latency cliffs
and memory growth that only appear under sustained concurrency.
"""

import multiprocessing as mp
import os
import random
import statistics
import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite, env_flag
from lib.valkey_client import Client, ReplyError

PORT = int(os.environ.get("VT_PORT", "6379"))
CLIENTS = int(os.environ.get("VT_LOAD_CLIENTS", "32"))
PIPE = int(os.environ.get("VT_LOAD_PIPELINE", "8"))
SECONDS = float(os.environ.get("VT_SOAK_SECONDS", "60"))
SHARED_R = [f"sh:r:{i}" for i in range(4)]
SHARED_64 = "sh:64"


def worker(wid, deadline, out):
    rng = random.Random(wid * 7717 + 1)
    c = Client(port=PORT, timeout=120)
    own = {f"w{wid}:a": set(), f"w{wid}:b": set()}
    own64 = {f"w{wid}:x": set()}
    for k in own:
        c.cmd("DEL", k)
        c.cmd("R.SETBIT", k, 0, 0)
    for k in own64:
        c.cmd("DEL", k)
        c.cmd("R64.SETBIT", k, 0, 0)
    lat = {}
    errors = []
    ops = 0

    def rv():
        return rng.randrange(1 << 22)

    while time.time() < deadline:
        batch, kinds, checks = [], [], []
        for _ in range(PIPE):
            p = rng.randrange(100)
            k = rng.choice(list(own))
            if p < 15:
                v, b = rv(), rng.randrange(2)
                batch.append(["R.SETBIT", k, v, b])
                checks.append(("setbit", k, v, b))
                kinds.append("write-point")
            elif p < 22:
                vs = [rv() for _ in range(rng.randrange(1, 200))]
                batch.append(["R.APPENDINTARRAY", k, *vs])
                checks.append(("add", k, vs))
                kinds.append("write-bulk")
            elif p < 26:
                vs = [rv() for _ in range(rng.randrange(1, 50))]
                batch.append(["R.DELETEINTARRAY", k, *vs])
                checks.append(("del", k, vs))
                kinds.append("write-bulk")
            elif p < 29:
                a = rv()
                n = rng.randrange(1, 3000)
                batch.append(["R.SETRANGE", k, a, a + n])
                checks.append(("add", k, list(range(a, a + n))))
                kinds.append("write-bulk")
            elif p < 33:
                v = rng.choice([rng.randrange(1 << 20), (rng.randrange(1, 1 << 20) << 32) | rv(), (1 << 63) + rv()])
                kx = f"w{wid}:x"
                b = rng.randrange(2)
                batch.append(["R64.SETBIT", kx, v, b])
                checks.append(("setbit", kx, v, b))
                kinds.append("write-point")
            elif p < 45:
                batch.append(["R.GETBIT", rng.choice(SHARED_R), rv()])
                checks.append(None)
                kinds.append("read-point")
            elif p < 53:
                batch.append(["R.GETBITS", rng.choice(SHARED_R), *[rv() for _ in range(100)]])
                checks.append(None)
                kinds.append("read-getbits")
            elif p < 61:
                st = rng.randrange(1_000_000)
                batch.append(["R.RANGEINTARRAY", rng.choice(SHARED_R), st, st + 999])
                checks.append(None)
                kinds.append("read-page")
            elif p < 64:
                st = rng.randrange(400_000)
                batch.append(["R64.RANGEINTARRAY", SHARED_64, st, st + 999])
                checks.append(None)
                kinds.append("read-page")
            elif p < 70:
                batch.append([rng.choice(["R.BITCOUNT", "R.MIN", "R.MAX"]), rng.choice(SHARED_R)])
                checks.append(None)
                kinds.append("read-agg")
            elif p < 72:
                batch.append(["R.BITPOS", rng.choice(SHARED_R), 0])
                checks.append(None)
                kinds.append("read-agg")
            elif p < 76:
                batch.append(["R.JACCARD", rng.choice(SHARED_R), rng.choice(SHARED_R)])
                checks.append(None)
                kinds.append("read-setop")
            elif p < 79:
                batch.append(["R.CONTAINS", rng.choice(SHARED_R), k, rng.choice(["ALL", "EQ"])])
                checks.append(None)
                kinds.append("read-setop")
            elif p < 85:
                batch.append(["R.EXPORT", k])
                checks.append(None)
                kinds.append("read-export")
            elif p < 89:
                other = f"w{wid}:b" if k.endswith(":a") else f"w{wid}:a"
                op = rng.choice(["OR", "AND", "XOR", "DIFF"])
                batch.append(["R.BITOP", op, k, k, other])
                checks.append(("bitop", k, op, other))
                kinds.append("write-setop")
            elif p < 91:
                # The model re-reads the key after this one, so it must be
                # the last command of its batch.
                batch.append(["R.BITOP", "AND", k, k, rng.choice(SHARED_R)])
                checks.append(("sync", k))
                kinds.append("write-setop")
                break
            elif p < 95:
                other = f"w{wid}:b" if k.endswith(":a") else f"w{wid}:a"
                batch.append(["EVAL", "return redis.call('R.IMPORT', KEYS[1], redis.call('R.EXPORT', KEYS[2]))",
                              2, k, other])
                checks.append(("union", k, other))
                kinds.append("write-import")
            elif p < 97:
                batch.append(["R.OPTIMIZE", k])
                checks.append(None)
                kinds.append("write-optimize")
            else:
                batch.append(["R64.EXPORT", f"w{wid}:x"])
                checks.append(None)
                kinds.append("read-export")
        t0 = time.perf_counter()
        replies = c.pipeline(batch)
        dt = time.perf_counter() - t0
        for kind in set(kinds):
            lat.setdefault(kind, []).append(dt)
        ops += len(batch)
        for cmd, r, chk in zip(batch, replies, checks):
            if isinstance(r, ReplyError):
                errors.append(f"{cmd[:3]} -> {r}")
                continue
            if chk is None:
                continue
            kind = chk[0]
            if kind == "setbit":
                _, kk, v, b = chk
                model = own.get(kk, own64.get(kk))
                prev = 1 if v in model else 0
                if r != prev:
                    errors.append(f"SETBIT prev {r} != model {prev}")
                (model.add if b else model.discard)(v)
            elif kind == "add":
                own[chk[1]].update(chk[2])
            elif kind == "del":
                own[chk[1]].difference_update(chk[2])
            elif kind == "bitop":
                _, kk, op, other = chk
                a, b = own[kk], own[other]
                own[kk] = {"OR": a | b, "AND": a & b, "XOR": a ^ b, "DIFF": a - b}[op]
                if r != len(own[kk]):
                    errors.append(f"BITOP {op} card {r} != model {len(own[kk])}")
            elif kind == "union":
                own[chk[1]] |= own[chk[2]]
                if r != len(own[chk[1]]):
                    errors.append(f"IMPORT card {r} != model {len(own[chk[1]])}")
            elif kind == "sync":
                # AND with a shared key: re-read the result as the new model
                own[chk[1]] = set(int(x) for x in c.cmd("R.GETINTARRAY", chk[1]))
    mismatches = 0
    for k, m in list(own.items()):
        got = set(int(x) for x in c.cmd("R.GETINTARRAY", k))
        mismatches += got != m
    for k, m in own64.items():
        got = set(int(x) for x in c.cmd("R64.GETINTARRAY", k))
        mismatches += got != m
    for k in list(own) + list(own64):
        c.cmd("DEL", k)
    out.put({"wid": wid, "ops": ops, "lat": lat, "errors": errors[:20], "nerrors": len(errors),
             "mismatches": mismatches})


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def main():
    s = Suite("18 load soak")
    if not env_flag("VT_LOAD"):
        print("skipped: set VT_LOAD=1 to run the load/soak suite")
        s.finish()
    c = Client(port=PORT, timeout=300)
    c.cmd("FLUSHALL")
    rng = random.Random(0x50AC)
    for i, k in enumerate(SHARED_R):
        vals = sorted(rng.sample(range(1 << 22), 1_000_000))
        for j in range(0, len(vals), 20_000):
            c.cmd("R.SETINTARRAY" if j == 0 else "R.APPENDINTARRAY", k, *vals[j:j + 20_000])
        if i % 2:
            c.cmd("R.SETRANGE", k, 5_000_000, 6_000_000)
    v64 = sorted({(h << 32) | rng.randrange(1 << 32) for h in rng.sample(range(1 << 30), 100_000)
                  for _ in range(5)})
    for j in range(0, len(v64), 20_000):
        c.cmd("R64.SETINTARRAY" if j == 0 else "R64.APPENDINTARRAY", SHARED_64, *v64[j:j + 20_000])
    shared_before = {k: c.cmd("R.EXPORT", k) for k in SHARED_R}
    shared_before[SHARED_64] = c.cmd("R64.EXPORT", SHARED_64)
    c.cmd("MEMORY", "PURGE")
    mem0 = int(c.cmd("INFO", "memory").decode().split("used_memory:")[1].split()[0])

    deadline = time.time() + SECONDS
    out = mp.Queue()
    procs = [mp.Process(target=worker, args=(w, deadline, out)) for w in range(CLIENTS)]
    mem_samples = []
    for p in procs:
        p.start()
    while any(p.is_alive() for p in procs) and time.time() < deadline + 120:
        try:
            info = Client(port=PORT, timeout=10).cmd("INFO", "memory").decode()
            mem_samples.append(int(info.split("used_memory:")[1].split()[0]))
        except OSError:
            pass
        time.sleep(2)
        if out.qsize() == len(procs):
            break
    results = [out.get(timeout=300) for _ in procs]
    for p in procs:
        p.join(timeout=30)

    total_ops = sum(r["ops"] for r in results)
    print(f"    {CLIENTS} clients x pipeline {PIPE}, {SECONDS:.0f}s: {total_ops:,} commands "
          f"({total_ops / SECONDS:,.0f}/s)")
    lat = {}
    for r in results:
        for k, v in r["lat"].items():
            lat.setdefault(k, []).extend(v)
    for k in sorted(lat):
        xs = lat[k]
        print(f"    {k:15} batches={len(xs):>8,} p50={1000 * pct(xs, 50):7.2f} ms "
              f"p99={1000 * pct(xs, 99):7.2f} ms max={1000 * max(xs):8.2f} ms")
    errs = [e for r in results for e in r["errors"]]
    for e in errs[:10]:
        print("    error:", e)
    s.check("no error replies", 0, sum(r["nerrors"] for r in results))
    s.check("every owned key equals its model", 0, sum(r["mismatches"] for r in results))
    s.check("server alive", "PONG", c.cmd("PING"))
    unchanged = sum(shared_before[k] == c.cmd("R.EXPORT", k) for k in SHARED_R)
    unchanged += shared_before[SHARED_64] == c.cmd("R64.EXPORT", SHARED_64)
    s.check("shared keys unchanged", len(SHARED_R) + 1, unchanged)
    time.sleep(1)
    c.cmd("MEMORY", "PURGE")
    mem1 = int(c.cmd("INFO", "memory").decode().split("used_memory:")[1].split()[0])
    print(f"    used_memory: start={mem0:,} peak={max(mem_samples or [mem0]):,} end={mem1:,}")
    s.check_true("memory returns near its start once owned keys are gone (+8 MB)",
                 mem1 - mem0 < 8 * 1024 * 1024, f"start={mem0} end={mem1}")
    c.cmd("FLUSHALL")
    s.finish()


if __name__ == "__main__":
    main()
