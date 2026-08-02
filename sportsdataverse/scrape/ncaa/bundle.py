"""Raw-bundle read/write helpers for captured NCAA game pages.

Each captured contest is stored as one gzip-compressed JSON file at
``root/{league}/raw/{season}/{contest_id}.json.gz`` containing the raw
``play_by_play`` / ``box_score`` / ``individual_stats`` payloads plus their
source URLs and capture timestamp.
"""

from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
from typing import Any

__all__ = ["bundle_path", "write_bundle", "read_bundle", "is_captured"]


def bundle_path(root: str | Path, league: str, season: str, contest_id: str) -> Path:
    """Return the on-disk path for a captured contest bundle."""
    return Path(root) / league / "raw" / season / f"{contest_id}.json.gz"


def write_bundle(
    root: str | Path,
    league: str,
    season: str,
    contest_id: str,
    pages: dict[str, Any],
    urls: dict[str, Any],
    captured_at: str,
) -> Path:
    """Gzip-write the contract bundle for a contest, atomically.

    Args:
        root: Root directory of the raw data tree.
        league: League slug (e.g. ``"mbb"``).
        season: Season label, used verbatim in the path (e.g. ``"2025-26"``).
        contest_id: Contest identifier as a string (never cast to int).
        pages: Dict with keys ``play_by_play``, ``box_score``, ``individual_stats``.
        urls: Dict of source URLs matching ``pages`` keys.
        captured_at: ISO-8601 capture timestamp (caller-supplied).

    Returns:
        The path the bundle was written to.
    """
    path = bundle_path(root, league, season, contest_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    bundle = {
        "contest_id": contest_id,
        "league": league,
        "season": season,
        "captured_at": captured_at,
        "urls": urls,
        "pages": pages,
    }
    payload = json.dumps(bundle).encode("utf-8")

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp_path, "wb") as f:
        f.write(payload)
    os.replace(tmp_path, path)

    return path


def read_bundle(path: str | Path) -> dict[str, Any]:
    """Gunzip and parse a contest bundle written by :func:`write_bundle`."""
    with gzip.open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


def is_captured(root: str | Path, league: str, season: str, contest_id: str) -> bool:
    """Return True if a bundle already exists for this contest."""
    return bundle_path(root, league, season, contest_id).exists()
