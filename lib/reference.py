"""Pure-Python reference model of every valkey-roaring command.

The model is deliberately naive — plain Python sets and O(n) scans — so it
cannot share bugs with the roaring implementation. Suites drive the server
and the model with the same operations and require identical observable
state. Values are ints; the model is width-agnostic (the caller enforces
u32/u64 bounds, as the server's parser does).
"""


class Model:
    def __init__(self):
        self.keys = {}          # name -> set of ints

    # -- write commands ---------------------------------------------------
    def setbit(self, key, offset, value):
        s = self.keys.setdefault(key, set())
        prev = 1 if offset in s else 0
        (s.add if value else s.discard)(offset)
        return prev

    def clearbits(self, key, offsets):
        if key not in self.keys:
            return 0
        s = self.keys[key]
        n = 0
        for o in offsets:
            if o in s:
                s.discard(o)
                n += 1
        return n

    def clear(self, key):
        if key not in self.keys:
            return None
        n = len(self.keys[key])
        self.keys[key] = set()
        return n

    def setintarray(self, key, vals):
        self.keys[key] = set(vals)

    def appendintarray(self, key, vals):
        self.keys.setdefault(key, set()).update(vals)

    def deleteintarray(self, key, vals):
        self.keys.setdefault(key, set()).difference_update(vals)

    def setrange(self, key, start, end):
        self.keys.setdefault(key, set()).update(range(start, end + 1))

    def setbitarray(self, key, bits):
        self.keys[key] = {i for i, b in enumerate(bits) if b == "1"}

    def diff(self, dest, k1, k2):
        self.keys[dest] = self.keys[k1] - self.keys[k2]

    def bitop(self, op, dest, srcs, last=None):
        sets = [self.keys.get(s, set()) for s in srcs]
        if op == "NOT":
            src = sets[0]
            top = max(src) if src else None
            if last is not None:
                top = last if top is None else max(top, last)
            result = set() if top is None else set(range(top + 1)) - src
        elif op == "AND":
            result = set(sets[0])
            for s in sets[1:]:
                result &= s
        elif op == "OR":
            result = set()
            for s in sets:
                result |= s
        elif op == "XOR":
            result = set()
            for s in sets:
                result ^= s
        elif op == "ANDOR":
            rest = set()
            for s in sets[1:]:
                rest |= s
            result = sets[0] & rest
        elif op == "DIFF":
            result = set(sets[0])
            for s in sets[1:]:
                result -= s
        elif op == "DIFF1":
            rest = set()
            for s in sets[1:]:
                rest |= s
            result = rest - sets[0]
        elif op == "ONE":
            counts = {}
            for s in sets:
                for v in s:
                    counts[v] = counts.get(v, 0) + 1
            result = {v for v, c in counts.items() if c == 1}
        else:
            raise ValueError(op)
        self.keys[dest] = result
        return len(result)

    # -- read commands ----------------------------------------------------
    def getbit(self, key, offset):
        return 1 if offset in self.keys.get(key, set()) else 0

    def getbits(self, key, offsets):
        s = self.keys.get(key, set())
        return [1 if o in s else 0 for o in offsets]

    def bitcount(self, key):
        return len(self.keys.get(key, set()))

    def bitpos(self, key, bit):
        s = self.keys.get(key, set())
        if bit == 1:
            return min(s) if s else -1
        i = 0
        while i in s:
            i += 1
        return i

    def minimum(self, key):
        s = self.keys.get(key, set())
        return min(s) if s else -1

    def maximum(self, key):
        s = self.keys.get(key, set())
        return max(s) if s else -1

    def getintarray(self, key):
        return sorted(self.keys.get(key, set()))

    def rangeintarray(self, key, start, end):
        return sorted(v for v in self.keys.get(key, set()) if start <= v <= end)

    def getbitarray(self, key):
        s = self.keys.get(key, set())
        if not s:
            return ""
        top = max(s)
        return "".join("1" if i in s else "0" for i in range(top + 1))

    def contains(self, k1, k2, mode):
        a, b = self.keys[k1], self.keys[k2]
        if mode == "NONE":
            return int(bool(a & b))
        if mode == "ALL":
            return int(b <= a)
        if mode == "ALL_STRICT":
            return int(b <= a and a != b)
        if mode == "EQ":
            return int(a == b)
        raise ValueError(mode)

    def jaccard(self, k1, k2):
        a, b = self.keys[k1], self.keys[k2]
        u = len(a | b)
        return 0.0 if u == 0 else len(a & b) / u
