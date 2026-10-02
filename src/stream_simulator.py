"""Simulates a stream by 'delivering' sample files one after another."""
import os
import shutil
import time
from pathlib import Path


def simulate_arrivals(source_dir: Path, target_dir: Path) -> int:
    """Copy batch_*.json into target_dir with strictly increasing mtimes.

    Spark's file source orders new files by modification time, so combined with
    maxFilesPerTrigger=1 each file becomes exactly one micro-batch, in order.
    """
    source_dir, target_dir = Path(source_dir), Path(target_dir)
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True)

    files = sorted(source_dir.glob("batch_*.json"))
    start = time.time() - 10 * len(files)
    for i, src in enumerate(files):
        dst = target_dir / src.name
        shutil.copyfile(src, dst)
        stamp = start + 10 * i
        os.utime(dst, (stamp, stamp))
    return len(files)
