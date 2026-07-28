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
from typing import TYPE_CHECKING, Callable, List, Optional, Union

if TYPE_CHECKING:
    from .nba_possessions import RawStoreDir

import pandas as pd
import polars as pl

_LOG = logging.getLogger(__name__)

#: Bump when the possession pipeline changes in a way that invalidates cached parquet.
PIPELINE_VERSION: int = 3

_LEAGUE_ID = "00"


def _default_cache_dir() -> Path:
    root = os.environ.get("SDV_PY_NBA_CACHE_DIR") or str(Path.home() / ".sdv_py_nba_cache")
    return Path(root) / "possessions"


def _game_cache_key(game_id: str) -> str:
    return f"{game_id}__v{PIPELINE_VERSION}.parquet"


def _index_select(frame: pl.DataFrame) -> pl.DataFrame:
    """``(game_id, game_date)`` index from a parsed leaguegamelog frame.

    One row per game id (the team-level log has two rows per game; first kept);
    ``game_date`` parsed from the first 10 chars, tolerating bare-date and
    ISO-datetime string forms. Empty typed frame when the columns are absent.
    """
    if frame.is_empty() or "game_id" not in frame.columns or "game_date" not in frame.columns:
        return pl.DataFrame(schema={"game_id": pl.Utf8, "game_date": pl.Date})
    return frame.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d"),
    ).unique(subset=["game_id"], keep="first", maintain_order=True)


def _season_index_from_store(season: int, season_type: str, raw_store_dir: "RawStoreDir") -> Optional[pl.DataFrame]:
    """``(game_id, game_date)`` index from a COMMITTED leaguegamelog in the raw
    store (local dir or ``http(s)://`` base), or ``None`` when the store is unset
    or the capture is absent.

    Lets discovery run clone-free / offline (CI) off the same committed tree the
    per-game compile reads — closing the one stats.nba.com call the compile would
    otherwise always make. Season-level captures live at
    ``leaguegamelog/{season}/{variant}.json`` (variant = season-type slug), the
    layout the ``-raw`` scraper writes; the raw resultSets payload is parsed the
    same way the live wrapper parses it.
    """
    from .nba_possessions import nba_raw_store_season_frame

    parsed = nba_raw_store_season_frame(
        "leaguegamelog",
        season,
        season_type.lower().replace(" ", "-"),
        raw_store_dir=raw_store_dir,
    )
    if parsed is None or "game_id" not in parsed.columns:
        return None
    idx = _index_select(parsed)
    return idx if not idx.is_empty() else None


def _season_game_index(
    season: int,
    season_type: str,
    *,
    proxy_url: Optional[str] = None,
    raw_store_dir: "RawStoreDir" = None,
) -> pl.DataFrame:
    """``(game_id, game_date)`` index for a season from leaguegamelog (monkeypatchable).

    A committed leaguegamelog in ``raw_store_dir`` (local dir or URL) is used when
    present — making discovery clone-free/offline in CI — else the live
    stats.nba.com wrapper is called.

    Args:
        season: Season END year (e.g. 2024 for 2023-24).
        season_type: NBA season type string (e.g. ``"Regular Season"``).
        proxy_url: Optional proxy URL forwarded to the live transport. Discovery
            is a stats.nba.com call like any other — on a datacenter host an
            unproxied call returns no rows, silently compiling the season to zero
            games (irrelevant once ``raw_store_dir`` serves it offline).
        raw_store_dir: Raw JSON store root (dir or ``http(s)://`` base) or
            per-endpoint mapping; ``None`` -> env vars. When it yields a committed
            leaguegamelog the live call is skipped entirely.

    Returns:
        Polars DataFrame with ``game_id: Utf8`` and ``game_date: Date``.
    """
    from_store = _season_index_from_store(season, season_type, raw_store_dir)
    if from_store is not None:
        return from_store

    from .nba_schedule import year_to_season
    from .nba_stats import nba_stats_leaguegamelog

    log = nba_stats_leaguegamelog(
        season=year_to_season(season - 1),
        season_type_all_star=season_type,
        league_id=_LEAGUE_ID,
        proxy_url=proxy_url,
    )
    return _index_select(log)


def _game_ids_for_season(
    season: int,
    season_type: str,
    *,
    proxy_url: Optional[str] = None,
    raw_store_dir: "RawStoreDir" = None,
) -> List[str]:
    """Return the (deduped) game ids for a season (delegates to :func:`_season_game_index`)."""
    return _season_game_index(season, season_type, proxy_url=proxy_url, raw_store_dir=raw_store_dir)[
        "game_id"
    ].to_list()


def _fetch_possessions(
    game_id: str,
    league_id: str,
    *,
    lineup_source: str = "auto",
    proxy_url: Optional[str] = None,
    raw_store_dir: RawStoreDir = None,
    raw_store_readonly: Optional[bool] = None,
) -> pl.DataFrame:
    """Fetch one game's possession+lineup frame (monkeypatchable).

    Args:
        game_id: ESPN/NBA game identifier string.
        league_id: NBA league id (``"00"`` for NBA, ``"20"`` for G-League).
        lineup_source: Which on-court lineup producer to use — ``"auto"``
            (default; tries rotation then falls back to pbp), ``"rotation"``
            (gamerotation endpoint only), or ``"pbp"`` (pbp-derived, no
            gamerotation fetch).
        proxy_url: Optional proxy URL forwarded to every underlying
            ``stats.nba.com`` call for this game.
        raw_store_dir: Explicit raw JSON store root or per-endpoint mapping
            (``None`` -> env vars), forwarded to
            :func:`~sportsdataverse.nba.nba_possessions.nba_possessions`.
        raw_store_readonly: Explicit store read-only flag (``None`` -> env var).

    Returns:
        Possession stint matrix as a polars DataFrame.
    """
    from .nba_possessions import nba_possessions

    return nba_possessions(
        game_id,
        league_id,
        lineup_source=lineup_source,
        proxy_url=proxy_url,
        raw_store_dir=raw_store_dir,
        raw_store_readonly=raw_store_readonly,
    )


def compile_nba_season(
    season: int,
    season_type: str = "Regular Season",
    *,
    resume: bool = True,
    cache_dir: Optional[str] = None,
    delay_s: float = 0.6,
    lineup_source: str = "auto",
    proxy_provider: Optional[Callable[[], Optional[str]]] = None,
    raw_store_dir: RawStoreDir = None,
    raw_store_readonly: Optional[bool] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Compile a full season's possession stint matrix (cached + resumable + throttled).

    Discovers game ids, dedupes, then per game loads the cached parquet if present
    (``resume``), else fetches via :func:`_fetch_possessions`, caches it, and sleeps
    ``delay_s`` (throttle; only on live fetches). A game that errors or returns no
    possessions is logged and skipped (best-effort — a per-game failure never
    raises; see ``Raises`` for the game_date integrity error). The assembled
    frame is tagged with a ``season`` column.

    Args:
        season: Season END year (e.g. 2024 for 2023-24).
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
        proxy_provider: Optional zero-arg callable returning a proxy URL (or
            ``None``). **Called once for game discovery, then once per game**
            (``N + 1`` calls for an ``N``-game season), so a rotating pool
            spreads a season's fetches across many exit IPs rather than
            hammering ``stats.nba.com`` from one address. ``stats.nba.com``
            rejects or hangs on datacenter/cloud IPs, so an unattended host
            (CI, a droplet) MUST supply one — a proxied request is judged on
            the proxy's exit IP, which is what makes such a host viable at all.
            Note discovery is proxied too: an unproxied index call returns no
            rows there, compiling the season to zero games without an error.
            Any ``() -> str | None`` works; a round-robin pool's ``.next``
            matches the signature directly::

                compile_nba_season(2024, proxy_provider=round_robin.next)

        raw_store_dir: Explicit raw JSON store root forwarded to every
            per-game fetch — a single path, or a per-endpoint mapping
            (``"*"`` default key) so payload families can live in
            independent trees. ``None`` -> env vars (per-endpoint
            ``SDV_PY_NBA_RAW_JSON_DIR_{ENDPOINT}``, then the generic
            ``SDV_PY_NBA_RAW_JSON_DIR``); ``""`` force-disables. Same
            spirit as ``cache_dir``'s arg-over-env precedence.
        raw_store_readonly: If ``True``, per-game fetches read the store but
            never persist misses (pure-consumer mode); ``None`` defers to
            ``SDV_PY_NBA_RAW_JSON_READONLY``.
        return_as_pandas: Return pandas instead of polars.

    Returns:
        The season possession frame (+ ``season`` and ``game_date`` cols). Empty
        typed frame if no games.

    Raises:
        ValueError: If any compiled possession's ``game_id`` is missing (or has a
            null) ``game_date`` in the season index — surfaced as an explicit
            error rather than silently emitting null dates.

    Example:
        Compile the 2023-24 regular season (requires live stats.nba.com access)::

            from sportsdataverse.nba.nba_season_compile import compile_nba_season

            poss = compile_nba_season(2024)
            print(poss.shape)          # (n_possessions, n_cols)
            print(poss["season"][0])   # 2024

        Resume a partially completed run and return as pandas::

            poss_pd = compile_nba_season(2024, resume=True, return_as_pandas=True)
            print(type(poss_pd))       # <class 'pandas.core.frame.DataFrame'>

        Compile Playoffs with a custom cache directory::

            poss = compile_nba_season(
                2024,
                season_type="Playoffs",
                cache_dir="/tmp/nba_cache",
            )
    """
    cdir = Path(cache_dir) if cache_dir else _default_cache_dir()
    cdir.mkdir(parents=True, exist_ok=True)

    # Discovery is proxied too: unproxied it returns no rows on a datacenter host,
    # which compiles the whole season to zero games and still exits clean.
    index = _season_game_index(
        season,
        season_type,
        proxy_url=proxy_provider() if proxy_provider is not None else None,
        raw_store_dir=raw_store_dir,
    )
    # dedupe preserving order
    game_ids = list(dict.fromkeys(index["game_id"].to_list()))

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
            # Called PER GAME so a pool (e.g. RoundRobin.next) rotates the exit IP
            # across the season instead of hammering stats.nba.com from one address.
            poss = _fetch_possessions(
                gid,
                _LEAGUE_ID,
                lineup_source=lineup_source,
                proxy_url=proxy_provider() if proxy_provider is not None else None,
                raw_store_dir=raw_store_dir,
                raw_store_readonly=raw_store_readonly,
            )
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
        out = pl.concat(frames, how="diagonal_relaxed").join(index, on="game_id", how="left")
        n_missing = int(out["game_date"].null_count())
        if n_missing:
            raise ValueError(f"game_date join failed for {n_missing} possessions — season game index incomplete")
        out = out.with_columns(pl.lit(season).alias("season"))
    else:
        out = pl.DataFrame(schema={"game_id": pl.Utf8, "game_date": pl.Date, "season": pl.Int64})

    return out.to_pandas() if return_as_pandas else out
