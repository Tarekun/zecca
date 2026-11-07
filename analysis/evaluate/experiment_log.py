"""Append-only JSONL log of every evaluation run through the CLI.

Its main purpose is multiple-testing accounting: the more variants of a
strategy are tried, the more likely the best one looks good by luck. A
variant is a distinct (strategy code, parameters) pair -- editing the
strategy's file or changing any parameter makes a new one, and every
candidate evaluated inside a walk-forward counts too."""

import json
from pathlib import Path
from typing import Any


def append(path: Path, entry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def read(path: Path, strategy: str | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with open(path) as f:
        entries = [json.loads(line) for line in f if line.strip()]
    if strategy is not None:
        entries = [e for e in entries if e["strategy"] == strategy]
    return entries


def variants(entries: list[dict[str, Any]], strategy: str) -> int:
    """Number of distinct (code_hash, params) evaluated for `strategy`."""
    seen = set()
    for entry in entries:
        if entry["strategy"] != strategy:
            continue
        for params in entry.get("candidates") or [entry.get("params") or {}]:
            seen.add((entry["code_hash"], json.dumps(params, sort_keys=True)))
    return len(seen)
