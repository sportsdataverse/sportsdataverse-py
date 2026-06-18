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
from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse.nfl.cache import cache_get, cache_put

if TYPE_CHECKING:
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
# Public API
# ---------------------------------------------------------------------------


@overload
def build_nfl_season(
    game_ids: list[int],
    *,
    source: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def build_nfl_season(
    game_ids: list[int],
    *,
    source: str = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def build_nfl_season(
    game_ids: list[int],
    *,
    source: str = "espn",
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Compile play-by-play for multiple NFL games into one tidy frame.

    For each *game_id* the function either loads a previously cached plays
    frame or processes the game fresh via ``NFLPlayProcess``.  Individual
    game failures are logged and skipped so a single bad game does not abort
    the whole season build.  The per-game frames are concatenated with
    ``how="diagonal_relaxed"`` (schema union, missing columns filled with
    ``null``) so games with slightly different column sets merge cleanly.

    Args:
        game_ids: ESPN event IDs to compile (e.g. ``[401671801, 401671802]``).
        source: Data source.

            - ``"espn"`` *(default)*: each game is processed via
              ``NFLPlayProcess(gameId=gid).espn_nfl_pbp()`` +
              ``run_processing_pipeline()``.
            - ``"nflverse"``: delegates to :func:`sportsdataverse.nfl.load_nfl_pbp`
              for the seasons implied by *game_ids*.  **Note:** the nflverse
              loader is season-oriented (not game-id-oriented); use it when
              you want a full pre-enriched season frame rather than ESPN-
              processed individual games.
            - ``"shield"``: raises :class:`NotImplementedError` — Shield
              (api.nfl.com) play-by-play lives in the native-pipeline
              (`nfl-data`) repository, not sdv-py.

        return_as_pandas: If ``True``, return a ``pandas.DataFrame`` instead
            of polars.

    Returns:
        polars.DataFrame: All plays from the requested games, concatenated
        with schema-union semantics (missing columns are ``null``).  Returns
        a zero-row frame if every game failed.  When *return_as_pandas* is
        ``True``, returns a ``pandas.DataFrame`` instead.

    Raises:
        ValueError: If *source* is not one of ``"espn"``, ``"nflverse"``,
            ``"shield"``.
        NotImplementedError: If ``source="shield"``.

    Example:
        Basic ESPN season compile::

            from sportsdataverse.nfl import build_nfl_season
            df = build_nfl_season([401671801, 401671802])
            print(df.shape)

        With filesystem cache enabled::

            from sportsdataverse.nfl import build_nfl_season, update_config
            update_config(cache_mode="filesystem")
            df = build_nfl_season([401671801, 401671802])  # processes + caches
            df2 = build_nfl_season([401671801, 401671802]) # served from cache

        Pandas output::

            df_pd = build_nfl_season([401671801], return_as_pandas=True)

        See Also:
            * `nflverse`_ -- full NFL data ecosystem
            * `nflfastR`_ -- R sister package for NFL PBP

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflfastR: https://www.nflfastr.com
    """
    _VALID_SOURCES = {"espn", "nflverse", "shield"}

    if source not in _VALID_SOURCES:
        raise ValueError(f"Unknown source {source!r}. Expected one of: {sorted(_VALID_SOURCES)!r}")

    if source == "shield":
        raise NotImplementedError(
            "source='shield' is not supported in sdv-py. "
            "Shield (api.nfl.com) play-by-play lives in the native-pipeline "
            "(nfl-data) repository."
        )

    if source == "nflverse":
        from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp  # noqa: PLC0415

        # nflverse loader is season-oriented; derive unique seasons from ids.
        # game_ids here are treated as season years for nflverse parity
        # (the caller should pass seasons, not ESPN game IDs, for this source).
        warnings.warn(
            "source='nflverse' treats game_ids as season years "
            "(e.g. [2024]) rather than ESPN event IDs.  "
            "Use source='espn' for per-game processing.",
            UserWarning,
            stacklevel=2,
        )
        frame = load_nfl_pbp(seasons=game_ids, return_as_pandas=False)
        if return_as_pandas:
            return frame.to_pandas()
        return frame

    # ---- source == "espn" ------------------------------------------------
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
