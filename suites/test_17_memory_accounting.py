"""Suite 17 — Memory accounting and leak-freedom across key lifecycles.

MEMORY USAGE on a module key is whatever the module's mem_usage callback
estimates. It now models the value's heap footprint (container records,
array/bitset/run payloads, growth slack, allocator rounding, the R64
BTreeMap nodes), so it should track what the allocator actually handed
out. This suite builds keys of very different shapes — sparse arrays,
dense bitsets, long runs, R64 values scattered over thousands of
sub-bitmaps, keys grown by many small appends, small results of large
set operations, keys shrunk by deletes — and compares MEMORY USAGE with
the growth of used_memory_dataset while the key is built on a separate,
closed connection. Ratios are printed for every shape.

It then runs repeated create / refresh / merge / export / delete cycles
and requires used memory to come back to where it started: no leaked
containers, no growth from replaced values or abandoned import buffers.
VT_LOAD=1 runs ten times more cycles.

Escape class targeted: estimates that drift from reality (misleading
capacity planning, eviction-order surprises) and leaks on value
replacement paths that only show over many cycles.
"""

import random
import sys
import time

sys.path.insert(0, __file__.rsplit("/suites/", 1)[0])
from lib.harness import Suite, env_flag
from lib.valkey_client import Client

# Accepted MEMORY USAGE / allocated ratio. 1.1.1 reported 0.09-0.29 for
# several shapes below (it returned the serialized size). The heap model now
# estimates vector capacities and allocator size classes, and reads
# 0.93-1.21 on every shape here (highest: runs after OPTIMIZE, whose exact
# run vectors it takes for grown ones).
LO, HI = 0.8, 1.3


def dataset(c):
    info = c.cmd("INFO", "memory").decode()
    return int(info.split("used_memory_dataset:")[1].split()[0])


def used(c):
    info = c.cmd("INFO", "memory").decode()
    return int(info.split("used_memory:")[1].split()[0])


def build(cmds):
    b = Client(timeout=120)
    for i in range(0, len(cmds), 20):
        b.pipeline(cmds[i:i + 20])
    b.close()


def chunks(P, key, vals, n=20_000, first="SETINTARRAY"):
    out = []
    for i in range(0, len(vals), n):
        out.append([f"{P}.{first if i == 0 else 'APPENDINTARRAY'}", key, *vals[i:i + n]])
    return out


def main():
    s = Suite("17 memory accounting")
    c = Client(timeout=120)
    c.cmd("FLUSHALL")
    lazy = c.cmd("CONFIG", "GET", "lazyfree-lazy-user-del")[1]
    c.cmd("CONFIG", "SET", "lazyfree-lazy-user-del", "no")
    rng = random.Random(0x3E3)

    sparse = sorted(rng.sample(range(1 << 26), 200_000))
    dense = sorted(rng.sample(range(1 << 21), 1_000_000))
    shapes = {
        "R sparse arrays": chunks("R", "m", sparse),
        "R dense bitsets": chunks("R", "m", dense),
        "R long runs": [["R.SETRANGE", "m", i * 100_000, i * 100_000 + 60_000] for i in range(1000)],
        "R grown by 200 small appends": [["R.SETBIT", "m", 0, 1]] + [
            ["R.APPENDINTARRAY", "m", *sparse[i:i + 1000]] for i in range(0, 200_000, 1000)],
        "R small AND of large keys": chunks("R", "a1", dense) + chunks("R", "a2", [v + 1 for v in dense])
        + [["R.BITOP", "AND", "m", "a1", "a2"], ["DEL", "a1", "a2"]],
        "R shrunk by DELETEINTARRAY": chunks("R", "m", dense) + [
            ["R.DELETEINTARRAY", "m", *dense[i:i + 20_000]] for i in range(0, 900_000, 20_000)],
        "R after OPTIMIZE": chunks("R", "m", sorted(set(range(0, 3_000_000)) - set(sparse))) + [["R.OPTIMIZE", "m"]],
        "R SETFULL": [["R.SETFULL", "m"]],
        "R64 sparse over 50k sub-bitmaps": chunks("R64", "m", sorted((h << 32) | rng.randrange(1 << 32)
                                                                   for h in rng.sample(range(1 << 24), 50_000))),
        "R64 dense in one sub-bitmap": chunks("R64", "m", [(7 << 32) | v for v in dense]),
        "R64 runs across borders": [["R64.SETRANGE", "m", (h << 32) - 30_000, (h << 32) + 30_000] for h in range(1, 300)],
        "R64 imported blob": [["R64.SETINTARRAY", "src", *sparse[:50_000]],
                              ["EVAL", "return redis.call('R64.IMPORT', 'm', redis.call('R64.EXPORT', 'src'))", 0],
                              ["DEL", "src"]],
    }
    s.section(f"MEMORY USAGE vs allocated bytes (accepted ratio {LO}..{HI})")
    # The server allocates a latency histogram (~24 KB) for each command the
    # first time it runs; with tracking on, a shape that uses a new command
    # would count that as key memory.
    tracking = c.cmd("CONFIG", "GET", "latency-tracking")[1]
    c.cmd("CONFIG", "SET", "latency-tracking", "no")
    for name, cmds in shapes.items():
        c.cmd("FLUSHALL")
        time.sleep(0.2)
        before = dataset(c)
        build(cmds)
        time.sleep(0.2)
        grown = dataset(c) - before
        mem = c.cmd("MEMORY", "USAGE", "m", "SAMPLES", "0")
        ratio = mem / grown if grown > 0 else float("inf")
        print(f"    {name:34} usage={mem:>11,} allocated={grown:>11,} ratio={ratio:.2f}")
        s.check_true(f"{name}: MEMORY USAGE tracks allocation", LO <= ratio <= HI,
                     f"usage={mem} allocated={grown} ratio={ratio:.2f}")
    c.cmd("CONFIG", "SET", "latency-tracking", tracking)

    s.section("create / refresh / delete cycles return memory")
    cycles = 300 if env_flag("VT_LOAD") else 30
    c.cmd("FLUSHALL")
    c.cmd("CONFIG", "SET", "lazyfree-lazy-user-del", lazy)
    vals = sorted(rng.sample(range(1 << 24), 60_000))

    def cycle(i):
        cmds = []
        for P, shift in (("R", 0), ("R64", (1 << 40))):
            k = f"cy:{P}:{i % 7}"
            v = [x + shift + i for x in vals]
            cmds += chunks(P, k, v)
            cmds.append([f"{P}.SETRANGE", k, shift + 5, shift + 200_000])
            cmds.append([f"{P}.BITOP", "XOR", f"{k}:x", k, k])
            cmds.append([f"{P}.BITOP", "OR", f"{k}:o", k, f"{k}:x"])
            cmds.append(["EVAL", f"return redis.call('{P}.IMPORT', KEYS[1], redis.call('{P}.EXPORT', KEYS[2]))",
                         2, f"{k}:o", k])
            cmds.append([f"{P}.OPTIMIZE", f"{k}:o"])
            cmds.append([f"{P}.DELETEINTARRAY", k, *v[:5000]])
            cmds.append(["COPY", k, f"{k}:c", "REPLACE"])
            cmds.append(["UNLINK" if i % 2 else "DEL", k, f"{k}:x", f"{k}:o", f"{k}:c"])
        return cmds

    for i in range(3):                       # warm the allocator
        build(cycle(i))
    time.sleep(0.5)
    c.cmd("MEMORY", "PURGE")
    start = used(c)
    samples = []
    t0 = time.monotonic()
    for i in range(cycles):
        build(cycle(i))
        if i % max(1, cycles // 10) == 0:
            samples.append(used(c))
    time.sleep(1.0)                          # let lazy frees finish
    c.cmd("MEMORY", "PURGE")
    end = used(c)
    print(f"    {cycles} cycles in {time.monotonic() - t0:.1f}s: used_memory start={start:,} end={end:,} "
          f"peak sample={max(samples):,}")
    s.check("no keys left behind", 0, c.cmd("DBSIZE"))
    s.check_true("used memory returns to its starting level (+2 MB)", end - start < 2 * 1024 * 1024,
                 f"start={start} end={end}")

    c.cmd("FLUSHALL")
    s.finish()


main()
