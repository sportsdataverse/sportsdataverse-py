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
from sportsdataverse.nfl.shield_pbp.live import add_live_columns, current_situation_row, resolve_context
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


def shield_nfl_pbp(
    game_detail: Optional[Dict[str, Any]] = None,
    shield_game_id: Optional[str] = None,
    *,
    enrich: bool = True,
    context: Optional[Dict[str, Any]] = None,
    game_id: Optional[str] = None,
) -> pl.DataFrame:
    """Build one NFL game's nflverse-shape play-by-play from Shield, at ANY game phase.

    The live entry point: the same parser :func:`build_pbp` runs on the archived
    ``nfl/raw`` finals, plus the four things a game still being played needs — the
    in-progress drive's possession, game-outcome columns held null until the feed says
    FINAL, a next-snap row from ``summary``, and provisional rows flagged (see
    :mod:`sportsdataverse.nfl.shield_pbp.live`). Safe to poll: pass the payload you
    already have via *game_detail* (no network), or a *shield_game_id* to fetch it.

    Args:
        game_detail: A Shield ``experience/v2/gamedetails`` payload (the raw body, or
            a ``{"data": ...}`` envelope). Takes precedence over *shield_game_id*, so
            tests and pollers that already hold a payload never touch the network.
        shield_game_id: Shield game uuid, fetched via
            :func:`sportsdataverse.nfl.nfl_game_details_v2` with
            ``include_drive_chart=True, return_parsed=False`` when *game_detail* is None.
        enrich: Run :func:`sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` on the result
            (default True) for the ``nfl_model_pbp`` EP/EPA/WP/WPA/CP/CPOE columns.
            Pass False for the base frame only (no model loads).
        context: Game context ``{"roof": ..., "spread_line": ..., "total_line": ...}``
            the Shield feed omits. Unset fields fall back to the nflverse schedule row
            for this game, then to ``live.DEFAULT_CONTEXT`` (``outdoors`` / 2.5 / 55.5,
            the same default the ESPN processor uses).
        game_id: Override the nflverse game_id (computed from the payload when None).

    Returns:
        A polars DataFrame, one row per play (plus, while ``summary.phase`` is
        ``INGAME``, one current-situation row), carrying the ``nfl_model_pbp`` columns — the
        :func:`build_pbp` base frame, the EP/WP enrichment when *enrich* is True, and:

        | col_name | type | description |
        |----------|------|-------------|
        | `live_phase` | `str` | The payload's `summary.phase`: `PREGAME`, `INGAME`, `HALFTIME`, `FINAL` or `FINAL_OVERTIME`. |
        | `is_play` | `int` | `1` for a real play; `0` for the feed's `GAME_START` / `END_QUARTER` / `END_GAME` markers and the current-situation row. |
        | `provisional` | `int` | `1` when the feed has not closed the play (`playEndTime` null) and it is in the trailing run of such plays of a non-final game — its text, yardage and stats may still change. Always `0` on a final game. |

        ``home_score`` / ``away_score`` / ``result`` are null until the game is final.

        The current-situation row is not inert once *enrich* is True: it is the next
        state, so it also completes the **previous** play's lead-diff columns (``epa``,
        ``qb_epa``, ``wpa``, ``vegas_wpa``, the ``total_*`` running sums). That play is
        usually still ``provisional``, so those values can move on the next poll.

        A payload Shield has not populated a drive chart for (every scheduled game
        before kickoff) returns a zero-row frame carrying only the three live columns —
        check ``df.is_empty()`` before selecting anything else.

    Raises:
        ValueError: Neither *game_detail* nor *shield_game_id* was given.

    Example:
        Poll a live game::

            df = shield_nfl_pbp(shield_game_id="a9a8944e-4feb-11f1-abca-2c54536568a9")
            df.filter(pl.col("is_play") == 0).select("posteam", "down", "ydstogo", "wp")
    """
    if game_detail is None:
        if not shield_game_id:
            raise ValueError("shield_nfl_pbp needs either game_detail= or shield_game_id=")
        from sportsdataverse.nfl.nfl_api import nfl_game_details_v2

        game_detail = nfl_game_details_v2(shield_game_id, include_drive_chart=True, return_parsed=False)
    # ``or {}``: the fetch returns a ``{"data": null}`` envelope for a uuid Shield does
    # not know, and an unguarded ``None`` here is an AttributeError three calls deeper
    # instead of the documented empty frame.
    game = (game_detail.get("data") if "driveChart" not in game_detail and "data" in game_detail else game_detail) or {}

    resolved_id = game_id or _game_id_of(game)
    roof, spread_line, total_line = resolve_context(game, context, game_id=resolved_id)
    df = build_pbp(game, roof=roof, spread_line=spread_line, total_line=total_line, game_id=game_id)
    if df.height == 0:
        # A scheduled game Shield has not populated a drive chart for yet -- the state
        # every not-yet-played game is in, so a poller started before kickoff hits it
        # first. ``build_pbp`` returns a schema-less frame there, which would make
        # ``df.filter(pl.col("is_play") == 1)`` a ColumnNotFoundError rather than an
        # empty result, so the live columns are declared even with nothing to describe.
        # Declared via the schema, not ``with_columns``/``select``: a literal added to a
        # 0x0 frame yields a ONE-row frame in polars, which would invent a play.
        return pl.DataFrame(schema={**df.schema, "live_phase": pl.Utf8, "is_play": pl.Int64, "provisional": pl.Int64})
    df = add_live_columns(df, game)
    df = current_situation_row(df, game)
    if enrich:
        from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

        df = enrich_nfl_pbp(df)
    return df


def _game_id_of(game: Dict[str, Any]) -> Optional[str]:
    """nflverse game_id for the schedule-context lookup (None when unresolvable)."""
    from sportsdataverse.nfl.shield_pbp.game_id import nflverse_game_id

    if not game.get("homeTeam"):
        return None
    season = game.get("season")
    reg_weeks = 17 if (season is not None and int(season) <= 2020) else 18
    try:
        return nflverse_game_id(game, reg_weeks=reg_weeks)
    except Exception:  # noqa: BLE001 — a malformed payload must not block a live build
        return None


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
