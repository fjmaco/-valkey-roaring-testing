"""Driving the module server's docker compose project.

The suites that restart the server, attach a replica or start a cluster
need to reach the compose project this repository owns
(`docker-compose.yml` at the root). They shell out rather than using a
Docker SDK, so they exercise the same path an operator would.

The image itself is built from a checkout of the module's source, which
lives in a different repository; `run_all.sh` resolves that checkout and
exports VR_SOURCE for compose to use as its build context.
"""

import os
import subprocess

# Repository root — where docker-compose.yml lives.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def sh(cmd):
    """Run `cmd` in the compose project directory, capturing its output."""
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=ROOT)
