"""End-to-end native PBP build: Shield game payload -> nflverse-shape play frame.

Chains the module pipeline (parse -> description -> features -> labels) into a
single frame carrying the Track 6 ``REQUIRED_COLUMNS`` (and more), reconstructed
entirely from the committed ``nfl/raw`` Shield feed (plus game-level roof /
spread_line / total_line that come from a schedules join).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl

from sportsdataverse.nfl.shield_pbp.description import add_description_features, add_pass_rush, add_qb_dropback
from sportsdataverse.nfl.shield_pbp.drives import add_drive_detail
from sportsdataverse.nfl.shield_pbp.features import add_game_state
from sportsdataverse.nfl.shield_pbp.labels import add_labels
from sportsdataverse.nfl.shield_pbp.parse import parse_game
from sportsdataverse.nfl.shield_pbp.repairs import apply_game_repairs, fix_scrambles
from sportsdataverse.nfl.shield_pbp.series import add_series_data


def build_pbp(
    game: Dict[str, Any],
    *,
    roof: Optional[str] = None,
    spread_line: Optional[float] = None,
    total_line: Optional[float] = None,
    game_id: Optional[str] = None,
) -> pl.DataFrame:
    """Reconstruct one game's nflverse-shape play-by-play from a Shield payload.

    Args:
        game: A single Shield game object.
        roof: Game roof from a schedules join (the Shield feed omits it).
        spread_line: Closing spread (home-relative) from a schedules join.
        total_line: Game over/under from a schedules join (the Shield feed omits it).
        game_id: Override the nflverse game_id (computed from the payload if None).

    Returns:
        A polars DataFrame, one row per play, with the base nflverse columns
        Track 6 consumes. Empty payloads return a zero-row frame.
    """
    df = parse_game(game, game_id=game_id)
    if df.height == 0:
        return df
    df = apply_game_repairs(df)
    df = add_description_features(df)
    # fix_scrambles runs right after qb_scramble is string-detected from desc
    # (add_description_features) and before play_type/qb_dropback would be
    # re-derived from it downstream — matching nflfastR's ordering (reference
    # §2): the 1999-2005 charting-data backfill only flips the qb_scramble
    # flag, it must not retroactively change anything computed from the
    # un-fixed value. add_pass_rush then derives the nflverse pass/rush 0/1
    # classification from the BACKFILLED qb_scramble (in nflfastR, clean_pbp's
    # pass derivation likewise runs long after fix_scrambles) and applies the
    # fix_weird_pass_plays override (reference §3) internally, between the
    # pass and rush derivations — nflfastR's exact position.
    df = fix_scrambles(df)
    df = add_pass_rush(df)
    df = add_qb_dropback(df)
    df = add_game_state(df, roof=roof, spread_line=spread_line, total_line=total_line)
    df = add_labels(df, game)
    # add_drive_detail (fixed_drive/fixed_drive_result/drive_*) must run after
    # add_labels (field_goal_result is a labels-stage column); add_series_data
    # must run after add_drive_detail (its "new drive" branch reads fixed_drive)
    # — matching nflfastR's own pipeline order: add_drive_results() |> add_series_data()
    # (reference §7/§8), both AFTER the EP/WP/CP model application step this
    # lighter-weight reconstruction pipeline doesn't perform.
    df = add_drive_detail(df)
    df = add_series_data(df)
    # Drop TIMEOUT rows (timeouts + two-minute warnings) to match nflverse's row
    # set — done AFTER add_game_state so the timeout cumsum already counted them.
    # fill_null keeps rows whose shield_play_type is null (null != "TIMEOUT" is null
    # in polars and would otherwise be filtered out).
    df = df.filter(pl.col("shield_play_type").fill_null("") != "TIMEOUT")
    return df.drop("_points_home", "_points_away")


def build_pbp_from_file(
    path: str | Path,
    *,
    roof: Optional[str] = None,
    spread_line: Optional[float] = None,
    total_line: Optional[float] = None,
) -> pl.DataFrame:
    """Load a ``nfl/raw/{season}/{game_id}.json`` file and build its PBP frame."""
    path = Path(path)
    game = json.loads(path.read_text(encoding="utf-8"))
    return build_pbp(game, roof=roof, spread_line=spread_line, total_line=total_line, game_id=path.stem)


def build_season(
    season: int,
    raw_dir: str | Path = "nfl/raw",
    *,
    schedule_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    game_ids: Optional[List[str]] = None,
) -> pl.DataFrame:
    """Build every game in a season into one concatenated PBP frame.

    Preseason games are skipped: nflverse play-by-play covers REG and POST only.

    Args:
        season: NFL season year.
        raw_dir: Root of the committed per-game library.
        schedule_lookup: Optional ``{game_id: {"roof": ..., "spread_line": ...,
            "total_line": ...}}`` map (from a schedules join) supplying the
            game-level fields the Shield feed omits. Missing games fall back to
            ``roof=None``, ``spread_line=None``, ``total_line=None``.
        game_ids: Optional subset of game_ids to build (default: all in the season).

    Returns:
        Concatenated polars DataFrame across all games (diagonal_relaxed union).
    """
    season_dir = Path(raw_dir) / str(season)
    wanted = set(game_ids) if game_ids is not None else None
    frames: List[pl.DataFrame] = []
    for path in sorted(season_dir.glob(f"{season}_*.json")):
        if wanted is not None and path.stem not in wanted:
            continue
        game = json.loads(path.read_text(encoding="utf-8"))
        if game.get("seasonType") == "PRE":
            continue
        meta = (schedule_lookup or {}).get(path.stem, {})
        df = build_pbp(
            game,
            roof=meta.get("roof"),
            spread_line=meta.get("spread_line"),
            total_line=meta.get("total_line"),
            game_id=path.stem,
        )
        if df.height:
            frames.append(df)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")
