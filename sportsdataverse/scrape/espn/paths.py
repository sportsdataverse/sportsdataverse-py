"""Raw-tree path builders and the public raw.githubusercontent URL form."""

from __future__ import annotations

from pathlib import Path

GITHUB_RAW = "https://raw.githubusercontent.com/sportsdataverse"
BRANCH = "main"


def raw_github_url(repo: str, *parts: str) -> str:
    """Public URL for a file committed in an SDV ``-raw`` repo.

    Args:
        repo: Repository name, e.g. ``"wehoop-wbb-raw"``.
        *parts: Path segments under the repo root.

    Returns:
        The ``raw.githubusercontent.com`` URL for that file.

    Example:
        ::

            raw_github_url("wehoop-wbb-raw", "wbb", "json", "final", "401811123.json")
    """
    return f"{GITHUB_RAW}/{repo}/{BRANCH}/" + "/".join(parts)


def game_json_path(root: Path | str, league: str, game_id: int | str, kind: str = "final") -> Path:
    """On-disk path for a per-game summary payload (``kind``: raw | final)."""
    return Path(root) / league / "json" / kind / f"{game_id}.json"


def family_json_path(root: Path | str, league: str, family: str, game_id: int | str) -> Path:
    """On-disk path for a per-game sibling family (game_rosters, officials)."""
    return Path(root) / league / family / "json" / f"{game_id}.json"
