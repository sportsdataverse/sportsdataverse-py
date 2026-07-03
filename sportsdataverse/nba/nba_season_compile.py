"""Resumable, cached, throttled full-season possession compiler.

Mirrors ``nfl/nfl_build.py``: per-game parquet cache keyed by
``(game_id, PIPELINE_VERSION)`` is the resume mechanism; a killed run skips
already-compiled games. Best-effort — a failing/empty game is logged and skipped.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
import polars as pl

_LOG = logging.getLogger(__name__)

#: Bump when the possession pipeline changes in a way that invalidates cached parquet.
PIPELINE_VERSION: int = 2

_LEAGUE_ID = "00"


def _default_cache_dir() -> Path:
    root = os.environ.get("SDV_PY_NBA_CACHE_DIR") or str(Path.home() / ".sdv_py_nba_cache")
    return Path(root) / "possessions"


def _game_cache_key(game_id: str) -> str:
    return f"{game_id}__v{PIPELINE_VERSION}.parquet"


def _game_ids_for_season(season: int, season_type: str) -> List[str]:
    """Return the (deduped) game ids for a season (monkeypatchable).

    ``season`` is the start year (2023 -> "2023-24"). The team-level game log
    returns two rows per game, so ids are deduplicated (order preserved).

    Args:
        season: Season start year (e.g. 2023 for 2023-24).
        season_type: NBA season type string (e.g. ``"Regular Season"``).

    Returns:
        Ordered, deduplicated list of game id strings.
    """
    from .nba_schedule import year_to_season
    from .nba_stats import nba_stats_leaguegamelog

    log = nba_stats_leaguegamelog(
        season=year_to_season(season),
        season_type_all_star=season_type,
        league_id=_LEAGUE_ID,
    )
    if log.is_empty() or "game_id" not in log.columns:
        return []
    return log["game_id"].cast(pl.Utf8).unique(maintain_order=True).to_list()


def _fetch_possessions(game_id: str, league_id: str, *, lineup_source: str = "auto") -> pl.DataFrame:
    """Fetch one game's possession+lineup frame (monkeypatchable).

    Args:
        game_id: ESPN/NBA game identifier string.
        league_id: NBA league id (``"00"`` for NBA, ``"20"`` for G-League).
        lineup_source: Which on-court lineup producer to use — ``"auto"``
            (default; tries rotation then falls back to pbp), ``"rotation"``
            (gamerotation endpoint only), or ``"pbp"`` (pbp-derived, no
            gamerotation fetch).

    Returns:
        Possession stint matrix as a polars DataFrame.
    """
    from .nba_possessions import nba_possessions

    return nba_possessions(game_id, league_id, lineup_source=lineup_source)


def compile_nba_season(
    season: int,
    season_type: str = "Regular Season",
    *,
    resume: bool = True,
    cache_dir: Optional[str] = None,
    delay_s: float = 0.6,
    lineup_source: str = "auto",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Compile a full season's possession stint matrix (cached + resumable + throttled).

    Discovers game ids, dedupes, then per game loads the cached parquet if present
    (``resume``), else fetches via :func:`_fetch_possessions`, caches it, and sleeps
    ``delay_s`` (throttle; only on live fetches). A game that errors or returns no
    possessions is logged and skipped (best-effort, never raises). The assembled
    frame is tagged with a ``season`` column.

    Args:
        season: Season start year (e.g. 2023 for 2023-24).
        season_type: ``"Regular Season"`` (default) or ``"Playoffs"``.
        resume: Reuse per-game cached parquet when present.
        cache_dir: Cache root; defaults to ``SDV_PY_NBA_CACHE_DIR`` or
            ``~/.sdv_py_nba_cache/possessions``.
        delay_s: Seconds to sleep after each live fetch (rate-limit throttle).
        lineup_source: Which on-court lineup producer to use — ``"auto"``
            (default; tries rotation then falls back to pbp), ``"rotation"``
            (gamerotation endpoint only), or ``"pbp"`` (pbp-derived, no
            gamerotation fetch — useful when the gamerotation endpoint is
            throttled or unavailable).
        return_as_pandas: Return pandas instead of polars.

    Returns:
        The season possession frame (+ ``season`` col). Empty typed frame if no games.

    Example:
        Compile the 2023-24 regular season (requires live stats.nba.com access)::

            from sportsdataverse.nba.nba_season_compile import compile_nba_season

            poss = compile_nba_season(2023)
            print(poss.shape)          # (n_possessions, n_cols)
            print(poss["season"][0])   # 2023

        Resume a partially completed run and return as pandas::

            poss_pd = compile_nba_season(2023, resume=True, return_as_pandas=True)
            print(type(poss_pd))       # <class 'pandas.core.frame.DataFrame'>

        Compile Playoffs with a custom cache directory::

            poss = compile_nba_season(
                2023,
                season_type="Playoffs",
                cache_dir="/tmp/nba_cache",
            )
    """
    cdir = Path(cache_dir) if cache_dir else _default_cache_dir()
    cdir.mkdir(parents=True, exist_ok=True)

    # dedupe preserving order
    game_ids = list(dict.fromkeys(_game_ids_for_season(season, season_type)))

    frames: List[pl.DataFrame] = []
    total = len(game_ids)

    for i, gid in enumerate(game_ids, 1):
        cache_path = cdir / _game_cache_key(gid)

        # --- cache hit ---
        if resume and cache_path.exists():
            try:
                frames.append(pl.read_parquet(cache_path))
                continue
            except Exception as exc:  # corrupt cache -> fall through to re-fetch
                _LOG.warning("re-fetch %s: bad cache (%s)", gid, exc)

        # --- live fetch (throttle only on successful fetches, not failures) ---
        try:
            poss = _fetch_possessions(gid, _LEAGUE_ID, lineup_source=lineup_source)
        except Exception as exc:
            _LOG.warning("skip game %s (%d/%d): fetch failed: %s", gid, i, total, exc)
            continue
        if delay_s:
            time.sleep(delay_s)

        if poss.is_empty():
            _LOG.info("skip game %s (%d/%d): no possessions", gid, i, total)
            continue

        poss.write_parquet(cache_path)
        frames.append(poss)
        _LOG.info("compiled %s (%d/%d)", gid, i, total)

    if frames:
        out = pl.concat(frames, how="diagonal_relaxed").with_columns(pl.lit(season).alias("season"))
    else:
        out = pl.DataFrame(schema={"game_id": pl.Utf8, "season": pl.Int64})

    return out.to_pandas() if return_as_pandas else out
