"""Tiny assertion harness shared by all suites: counters + readable failures."""

import os
import sys
import time


class Suite:
    def __init__(self, name):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.failures = []
        self.t0 = time.monotonic()

    def check(self, label, expected, actual):
        if expected == actual:
            self.passed += 1
        else:
            self.failed += 1
            self.failures.append((label, expected, actual))
            print(f"  FAIL: {label}\n    expected: {expected!r}\n    actual:   {actual!r}")

    def check_true(self, label, cond, detail=""):
        if cond:
            self.passed += 1
        else:
            self.failed += 1
            self.failures.append((label, "truthy", detail))
            print(f"  FAIL: {label}  {detail}")

    def section(self, title):
        print(f"--- {title} ---")

    def finish(self):
        dt = time.monotonic() - self.t0
        status = "PASSED" if self.failed == 0 else "FAILED"
        print(f"\n[{self.name}] {status}: {self.passed} ok, {self.failed} failed ({dt:.1f}s)")
        sys.exit(1 if self.failed else 0)


def env_flag(name):
    return os.environ.get(name, "") not in ("", "0", "false")
