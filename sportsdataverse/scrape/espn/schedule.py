"""Capture-state and URL columns for the per-season schedule files.

The per-season schedule is the ORIGIN of every flag. The master does not
compute flags -- it inherits them by union, so a flag added here appears in the
master and the coverage index with no further wiring.

These archives carry ``has_*`` (capture) flags only. ``in_*`` (build) flags are
a ``-data`` repo fact; stamping them here would make the archive read the data
repo, a dependency in the wrong direction that goes stale whenever either side
rebuilds alone.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl

from sportsdataverse.scrape.espn.ids import with_int64_ids
from sportsdataverse.scrape.espn.league_config import LeagueConfig, by_key
from sportsdataverse.scrape.espn.paths import raw_github_url

ID_COLUMNS = ("game_id", "home_id", "away_id", "venue_id")


def resolve_league(league: str | LeagueConfig) -> LeagueConfig:
    """Accept either a league slug or an already-resolved config.

    Shell drivers pass a slug; callers that already hold a config should not be
    forced to round-trip it through a string.
    """
    return league if isinstance(league, LeagueConfig) else by_key(league)


def add_capture_columns(df: pl.DataFrame, *, root: Path | str, league: str | LeagueConfig) -> pl.DataFrame:
    """Add per-family URL columns and ``has_*`` capture flags to a schedule.

    Args:
        df: A season schedule frame containing ``game_id``.
        root: Repo root the ``<league>/`` tree hangs off.
        league: League slug (``"nba"``/``"mbb"``/``"wnba"``/``"wbb"``) or a
            :class:`LeagueConfig`. Required and keyword-only: a defaulted league
            is how a well-formed capture ends up written under the wrong
            league's tree, which fails silently.

    Returns:
        The frame with ids canonicalized to Int64, one ``<stem>_url`` column
        per family this league publishes, and a ``has_<stem>`` boolean for each
        flagged family.

    URLs are emitted for every row whether or not the file exists -- the
    ``has_*`` flag is the truth, the URL is the address. Filenames are built
    from the integer id, so a float-origin id can never address ``123.0.json``.
    """
    config = resolve_league(league)
    root = Path(root)
    out = with_int64_ids(df, *ID_COLUMNS)
    game_ids = out["game_id"].to_list()

    columns: list[pl.Series] = []
    for stem, segments in config.families:
        columns.append(
            pl.Series(
                f"{stem}_url",
                [
                    None if gid is None else raw_github_url(config.repo, config.key, *segments, f"{gid}.json")
                    for gid in game_ids
                ],
                dtype=pl.Utf8,
            )
        )
        if stem in config.flagged:
            captured = _captured_ids(root / config.key / Path(*segments))
            columns.append(
                pl.Series(
                    f"has_{stem}",
                    [gid is not None and gid in captured for gid in game_ids],
                    dtype=pl.Boolean,
                )
            )
    return out.with_columns(columns)


def _captured_ids(directory: Path) -> set[int]:
    """Every game id present in a family directory, as one listing.

    A per-game ``Path.exists()`` would be ~400k syscalls across the full
    archive (133k games x 3 families) and made the daily step crawl on Windows.
    One scandir per family is O(files) and answers every membership test.
    """
    if not directory.is_dir():
        return set()
    ids: set[int] = set()
    with os.scandir(directory) as entries:
        for entry in entries:
            name, _, ext = entry.name.rpartition(".")
            if ext == "json" and name.isdigit():
                ids.add(int(name))
    return ids
