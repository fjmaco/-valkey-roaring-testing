"""Open-source dataset loader for the validation suite.

All datasets come from RoaringBitmap/real-roaring-datasets — the reference
corpus used to benchmark roaring implementations across languages. Each
dataset is a directory of .txt files; each file is a comma-separated sorted
list of u32 values and represents one bitmap. The datasets were chosen for
their different container-shape characteristics:

  census1881          clustered, moderate density   (mixed containers)
  census-income       dense runs                    (run containers)
  wikileaks-noquotes  sparse, scattered             (array containers)
  uscensus2000        tiny, extremely sparse        (degenerate cases)
  weather_sept_85     largest, mixed                (FULL=1 runs only)

Files are downloaded once into datasets/ at the repository root and cached.
"""

import io
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.request
import zipfile

BASE = "https://github.com/RoaringBitmap/real-roaring-datasets/raw/master"
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.path.join(HERE, "datasets")

DOWNLOAD_ATTEMPTS = 4

DATASETS = [
    "census1881",
    "census-income",
    "wikileaks-noquotes",
    "uscensus2000",
    "weather_sept_85",
]


def fetch(name):
    """Ensure dataset `name` is downloaded; return its directory path."""
    assert name in DATASETS, name
    dest = os.path.join(CACHE, name)
    if os.path.isdir(dest) and os.listdir(dest):
        return dest

    os.makedirs(CACHE, exist_ok=True)
    url = f"{BASE}/{name}.zip"
    print(f"  downloading {url} ...")
    # The corpus is served by GitHub's raw backend, which sheds load with a
    # transient 503 often enough to fail an otherwise good run. Retry a few
    # times before giving up, and extract via a temporary directory so an
    # interrupted run cannot leave a half-populated cache entry that later
    # runs would treat as complete.
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            data = urllib.request.urlopen(url, timeout=120).read()
            break
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == DOWNLOAD_ATTEMPTS:
                raise RuntimeError(
                    f"could not download {url} after {DOWNLOAD_ATTEMPTS} attempts: {exc}"
                ) from exc
            delay = 2 ** attempt
            print(f"    attempt {attempt} failed ({exc}); retrying in {delay}s")
            time.sleep(delay)

    staging = tempfile.mkdtemp(dir=CACHE, prefix=f".{name}.")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            z.extractall(staging)
        os.rename(staging, dest)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return dest


def load(name, max_files=None):
    """Load dataset as a list of sorted u32 value-lists (one per bitmap)."""
    dest = fetch(name)
    files = sorted(f for f in os.listdir(dest) if f.endswith(".txt"))
    if max_files:
        files = files[:max_files]
    out = []
    for f in files:
        with open(os.path.join(dest, f)) as fh:
            text = fh.read()
        vals = [int(t) for t in text.replace("\n", ",").split(",") if t.strip()]
        out.append(vals)
    return out
