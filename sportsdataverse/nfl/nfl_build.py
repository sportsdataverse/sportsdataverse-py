"""Season-compile helper for ESPN NFL play-by-play.

Provides :func:`build_nfl_season` which processes a list of game IDs through
``NFLPlayProcess`` and concatenates the resulting plays frames into a single
polars ``DataFrame``. A per-game parquet cache (keyed by game id +
``PIPELINE_VERSION``) avoids re-processing on repeated calls.

Cache behavior mirrors the existing :mod:`sportsdataverse.nfl.cache` layer:

- ``memory`` — per-process dict; survives within a Python session until
  :func:`sportsdataverse.nfl.clear_cache` is called or the process exits.
- ``filesystem`` — parquet file under ``NflConfig.cache_dir``.
- ``off`` — no cache; every game is always re-processed.

Bump ``PIPELINE_VERSION`` whenever the processing pipeline changes in a way
that would produce different output columns or values, so stale cached frames
are automatically invalidated.
"""

from __future__ import annotations

import hashlib
import json
import logging
import warnings
from typing import TYPE_CHECKING, Any, Literal, overload

import polars as pl

from sportsdataverse.nfl.cache import cache_get, cache_put

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

# ---------------------------------------------------------------------------
# Pipeline version — bump to invalidate all per-game caches.
# ---------------------------------------------------------------------------
PIPELINE_VERSION: int = 1  # 1 -> initial release

logger = logging.getLogger("sdv.nfl_build")
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Internal per-game cache helpers (separate key namespace from cached_loader)
# ---------------------------------------------------------------------------


def _game_cache_key(game_id: int, pipeline_version: int) -> str:
    """Stable sha256 key for a single processed game frame."""
    payload = {"game_id": game_id, "pipeline_version": pipeline_version}
    return "nfl_build__" + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _game_cache_read(key: str) -> pl.DataFrame | None:
    """Return cached frame for *key* or ``None`` on miss/expiry."""
    return cache_get(key)


def _game_cache_write(key: str, frame: pl.DataFrame) -> None:
    """Persist *frame* to the active cache backend."""
    cache_put(key, frame)


# ---------------------------------------------------------------------------
# Per-game builder (ESPN source)
# ---------------------------------------------------------------------------


def _build_game_espn(game_id: int) -> pl.DataFrame:
    """Process one ESPN game and return its plays as a polars ``DataFrame``."""
    # Import lazily to keep module-level imports light and to allow tests to
    # monkeypatch the function before the first call.
    from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess  # noqa: PLC0415

    proc = NFLPlayProcess(gameId=game_id)
    proc.espn_nfl_pbp()
    result: dict = proc.run_processing_pipeline()
    plays = result.get("plays", [])
    return pl.DataFrame(plays, infer_schema_length=None)


# ---------------------------------------------------------------------------
# Shield source: game-level fields the Shield feed omits (mirrors nfl-data's
# native_pbp/cli.py ``_build_schedule_lookup``)
# ---------------------------------------------------------------------------


def _shield_schedule_lookup(season: int) -> dict[str, dict[str, Any]]:
    """``{game_id: {roof, spread_line, total_line}}`` from the nflverse schedule.

    Degrades to an empty map (the three columns stay null) with a
    ``RuntimeWarning`` if the schedule cannot be loaded -- a betting-line
    lookup must never fail a build.
    """
    try:
        from sportsdataverse.nfl.nfl_loaders import load_nfl_schedule  # noqa: PLC0415

        sched = load_nfl_schedule([season])
        if not isinstance(sched, pl.DataFrame):
            sched = pl.from_pandas(sched)
        keep = [c for c in ("game_id", "roof", "spread_line", "total_line") if c in sched.columns]
        if "game_id" not in keep:
            return {}
        return {
            row["game_id"]: {
                "roof": row.get("roof"),
                "spread_line": row.get("spread_line"),
                "total_line": row.get("total_line"),
            }
            for row in sched.select(keep).iter_rows(named=True)
        }
    except Exception as exc:  # network / schema / loader failure -> degrade, never fail the build
        warnings.warn(
            f"build_nfl_season(source='shield'): schedule lookup failed for {season} "
            f"({type(exc).__name__}: {exc}); roof/spread_line/total_line will be null",
            RuntimeWarning,
            stacklevel=3,
        )
        return {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@overload
def build_nfl_season(
    game_ids: list[int] | None = ...,
    *,
    seasons: list[int] | None = ...,
    source: str = ...,
    return_as_pandas: Literal[False] = ...,
    raw_dir: "str | Path | None" = ...,
    schedule_lookup: "dict[str, dict[str, Any]] | None" = ...,
) -> pl.DataFrame: ...
@overload
def build_nfl_season(
    game_ids: list[int] | None = ...,
    *,
    seasons: list[int] | None = ...,
    source: str = ...,
    return_as_pandas: Literal[True],
    raw_dir: "str | Path | None" = ...,
    schedule_lookup: "dict[str, dict[str, Any]] | None" = ...,
) -> "pd.DataFrame": ...
def build_nfl_season(
    game_ids: list[int] | None = None,
    *,
    seasons: list[int] | None = None,
    source: str = "espn",
    return_as_pandas: bool = False,
    raw_dir: "str | Path | None" = None,
    schedule_lookup: "dict[str, dict[str, Any]] | None" = None,
) -> "pl.DataFrame | pd.DataFrame":
    """Compile play-by-play for multiple NFL games into one tidy frame.

    The ``source`` parameter determines which input parameter is required:

    - ``source="espn"`` — requires *game_ids*; *seasons* must be ``None``.
    - ``source="nflverse"`` — requires *seasons*; *game_ids* must be ``None``.
    - ``source="shield"`` — requires *seasons* and *raw_dir*; *game_ids* must be ``None``.

    For ESPN games the function either loads a previously cached plays frame or
    processes the game fresh via ``NFLPlayProcess``.  Individual game failures
    are logged and skipped so a single bad game does not abort the whole season
    build.  The per-game frames are concatenated with ``how="diagonal_relaxed"``
    (schema union, missing columns filled with ``null``) so games with slightly
    different column sets merge cleanly.

    Args:
        game_ids: ESPN event IDs to compile (e.g. ``[401671801, 401671802]``).
            Required when ``source="espn"``; must be ``None`` for other sources.
        seasons: Season years to compile (e.g. ``[2023, 2024]``).
            Required when ``source="nflverse"``; must be ``None`` for other sources.
        source: Data source.

            - ``"espn"`` *(default)*: each game is processed via
              ``NFLPlayProcess(gameId=gid).espn_nfl_pbp()`` +
              ``run_processing_pipeline()``.  Pass *game_ids*.
            - ``"nflverse"``: delegates to :func:`sportsdataverse.nfl.load_nfl_pbp`
              for the requested seasons.  Pass *seasons*.  Returns the full
              pre-enriched season frame as-is.
            - ``"shield"``: reconstructs nflverse-shape play-by-play from a
              committed library of Shield (api.nfl.com) per-game JSON files via
              :func:`sportsdataverse.nfl.shield_pbp.build_season` (the nflfastR
              parser port graduated from nfl-data's ``native_pbp``). Pass
              *seasons* and *raw_dir*. Preseason games are skipped and TIMEOUT
              rows dropped, matching nflverse's row set. The frame is NOT
              EP/WP-enriched; feed it to
              :func:`sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` for the
              ``nfl_model_pbp`` columns.

        return_as_pandas: If ``True``, return a ``pandas.DataFrame`` instead
            of polars.
        raw_dir: ``source="shield"`` only. Root of the per-game Shield JSON
            library laid out as ``{raw_dir}/{season}/{game_id}.json`` (the
            ``nfl-raw`` repo's ``nfl/raw``). Required for the shield source;
            must be ``None`` otherwise.
        schedule_lookup: ``source="shield"`` only. ``{game_id: {"roof": ...,
            "spread_line": ..., "total_line": ...}}`` supplying the game-level
            fields the Shield feed omits. ``None`` *(default)* builds it from
            :func:`sportsdataverse.nfl.load_nfl_schedule` for each season,
            degrading to nulls with a ``RuntimeWarning`` if the schedule cannot
            be loaded. Pass ``{}`` to skip the lookup (hermetic; the three
            columns stay null).

    Returns:
        polars.DataFrame: All plays from the requested games/seasons,
        concatenated with schema-union semantics (missing columns are ``null``).
        Returns a zero-row frame if every game failed (ESPN source only).
        When *return_as_pandas* is ``True``, returns a ``pandas.DataFrame``
        instead. For ``source="shield"`` the frame carries the nflverse base
        columns (233; a superset of the EP/WP/CP training contract) with the
        same names, types and meanings as :func:`sportsdataverse.nfl.load_nfl_model_pbp`
        minus the EP/WP/CP enrichment columns: identifiers (``game_id``,
        ``play_id``, ``posteam``, ``defteam``), game state (``down``, ``ydstogo``,
        ``yardline_100``, ``qtr``, ``half_seconds_remaining``,
        ``game_seconds_remaining``, ``score_differential``,
        ``posteam_timeouts_remaining``), play classification (``play_type``,
        ``pass``, ``rush``, ``desc``, ``yards_gained``, ``touchdown``,
        ``field_goal_result``), drive/series (``fixed_drive``,
        ``fixed_drive_result``, ``series``, ``series_result``), schedule fields
        (``roof``, ``spread_line``, ``total_line``) and game outcome
        (``home_score``, ``away_score``, ``result``).

    Raises:
        ValueError: If *source* is not one of ``"espn"``, ``"nflverse"``,
            ``"shield"``; or if the wrong input parameter is supplied for the
            chosen source (e.g. passing *seasons* to ``source="espn"``,
            *game_ids* to ``source="nflverse"``, or *raw_dir* to a non-shield
            source); or if the required parameter is missing or empty.

    Example:
        ESPN season compile (pass ESPN event IDs)::

            from sportsdataverse.nfl import build_nfl_season
            df = build_nfl_season(game_ids=[401671801, 401671802])
            print(df.shape)

        nflverse season compile (pass season years)::

            from sportsdataverse.nfl import build_nfl_season
            df = build_nfl_season(seasons=[2023], source="nflverse")
            print(df.shape)

        Shield season compile from a committed raw library (nfl-raw checkout)::

            from sportsdataverse.nfl import build_nfl_season
            df = build_nfl_season(seasons=[2024], source="shield", raw_dir="nfl-raw/nfl/raw")
            print(df.shape)

        With filesystem cache enabled (ESPN)::

            from sportsdataverse.nfl import build_nfl_season, update_config
            update_config(cache_mode="filesystem")
            df = build_nfl_season(game_ids=[401671801, 401671802])  # processes + caches
            df2 = build_nfl_season(game_ids=[401671801, 401671802]) # served from cache

        Pandas output::

            from sportsdataverse.nfl import build_nfl_season
            df_pd = build_nfl_season(game_ids=[401671801], return_as_pandas=True)
            print(df_pd.shape)

        See Also:
            * `nflverse`_ -- full NFL data ecosystem
            * `nflfastR`_ -- R sister package for NFL PBP

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflfastR: https://www.nflfastr.com
    """
    _VALID_SOURCES = {"espn", "nflverse", "shield"}

    if source not in _VALID_SOURCES:
        raise ValueError(f"Unknown source {source!r}. Expected one of: {sorted(_VALID_SOURCES)!r}")

    if source != "shield" and (raw_dir is not None or schedule_lookup is not None):
        raise ValueError("raw_dir and schedule_lookup apply to source='shield' only.")

    if source == "shield":
        if game_ids is not None:
            raise ValueError("source='shield' takes seasons, not game_ids. Pass seasons=[year, ...] instead.")
        if not seasons:
            raise ValueError(
                "source='shield' requires seasons to be a non-empty list of season years (e.g. seasons=[2024])."
            )
        if raw_dir is None:
            raise ValueError("source='shield' requires raw_dir (root of the {season}/{game_id}.json Shield library).")
        from sportsdataverse.nfl.shield_pbp.build import build_season  # noqa: PLC0415

        season_frames = [
            build_season(
                season,
                raw_dir=raw_dir,
                schedule_lookup=_shield_schedule_lookup(season) if schedule_lookup is None else schedule_lookup,
            )
            for season in seasons
        ]
        season_frames = [f for f in season_frames if f.height]
        frame = pl.concat(season_frames, how="diagonal_relaxed") if season_frames else pl.DataFrame()
        if return_as_pandas:
            return frame.to_pandas()
        return frame

    if source == "nflverse":
        if game_ids is not None:
            raise ValueError("source='nflverse' takes seasons, not game_ids. Pass seasons=[year, ...] instead.")
        if not seasons:
            raise ValueError(
                "source='nflverse' requires seasons to be a non-empty list of season years (e.g. seasons=[2023])."
            )
        from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp  # noqa: PLC0415

        frame = load_nfl_pbp(seasons=seasons, return_as_pandas=False)
        if return_as_pandas:
            return frame.to_pandas()
        return frame

    # ---- source == "espn" ------------------------------------------------
    if seasons is not None:
        raise ValueError("source='espn' takes game_ids, not seasons. Pass game_ids=[espn_event_id, ...] instead.")
    if not game_ids:
        raise ValueError(
            "source='espn' requires game_ids to be a non-empty list of ESPN event IDs (e.g. game_ids=[401671801])."
        )

    frames: list[pl.DataFrame] = []
    skipped: list[int] = []

    for gid in game_ids:
        key = _game_cache_key(gid, PIPELINE_VERSION)
        cached = _game_cache_read(key)
        if cached is not None:
            frames.append(cached)
            continue

        try:
            frame = _build_game_espn(gid)
        except Exception as exc:
            warnings.warn(
                f"build_nfl_season: skipping game_id={gid} after error: {exc}",
                UserWarning,
                stacklevel=2,
            )
            skipped.append(gid)
            continue

        _game_cache_write(key, frame)
        frames.append(frame)

    if skipped:
        logger.warning("build_nfl_season: skipped %d game(s): %s", len(skipped), skipped)

    if not frames:
        return pl.DataFrame()

    result = pl.concat(frames, how="diagonal_relaxed")

    if return_as_pandas:
        return result.to_pandas()
    return result
