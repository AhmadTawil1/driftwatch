"""Resolves the current commit SHA for provenance columns (N7).

Works unmodified both on the host and inside the Airflow containers:
git discovers .git by walking up from the given cwd, and the container
mounts .git two directory levels above this file — the same relative
position it sits in on the host (repo_root/.git vs
repo_root/src/watchdog/git_info.py).
"""

import subprocess
from pathlib import Path


def git_sha() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, cwd=Path(__file__).resolve().parent
    ).strip()
