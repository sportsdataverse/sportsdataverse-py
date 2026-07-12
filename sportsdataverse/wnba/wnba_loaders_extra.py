"""Hand-written WNBA Stats loaders preserved alongside the generated
``sportsdataverse.wnba.wnba_loaders``: deprecation shims for 4 legacy
per-measure-type release tags (``wnba_stats_{lineups,player_season_stats,
team_season_stats,standings}``) now superseded by the consolidated
``wnba_stats_leaguedash`` parameter-cube tag.

The old tags stacked a handful of measure types into one release-per-season
asset; the cube ships each measure type as its own asset instead (plus wide
``*_master`` mega tables). Reconstructing the old stacked-by-``measure_type``
shape from several per-measure assets is beyond what the per-season
release-pull codegen template can express, so these 4 functions live here as
hand-written residuals (see ``tools/codegen/generate.py``'s
``_GENERATED_LOADER_LEAGUES`` comment for the ``{league}_loaders_extra.py``
convention used elsewhere for cfb/nhl/pwhl).
"""

from __future__ import annotations

from typing import Callable, Dict, Optional

import polars as pl

from sportsdataverse._codegen_runtime import _as_season_list, _read_release_parquet, cli_warn
from sportsdataverse._deprecation import warn_deprecated

__all__ = [
    "load_wnba_stats_player_season_stats",
    "load_wnba_stats_lineups",
    "load_wnba_stats_team_season_stats",
    "load_wnba_stats_standings",
]

_BASE_URL = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_leaguedash"

# measure-type slug (asset filename) -> human label (old "measure_type" column value).
_PLAYER_MEASURES = {
    "base": "Base",
    "advanced": "Advanced",
    "misc": "Misc",
    "scoring": "Scoring",
    "usage": "Usage",
    "defense": "Defense",
}
_TEAM_MEASURES = {
    "base": "Base",
    "advanced": "Advanced",
    "misc": "Misc",
    "scoring": "Scoring",
    "defense": "Defense",
    "opponent": "Opponent",
}
# The old wnba_stats_lineups tag only ever scraped 5-man lineups; the cube
# covers 2/3/4/5-man, so the row_filter below narrows back to 5-man for the
# old contract.
_LINEUP_MEASURES = {"base": "Base", "advanced": "Advanced"}


def _stack_measures(
    fn_name: str,
    table_prefix: str,
    measures: Dict[str, str],
    seasons,
    *,
    row_filter: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
) -> pl.DataFrame:
    """Fetch each measure-type asset per season from the cube, tag + stack.

    Args:
        fn_name: caller's name, for the missing-season warning.
        table_prefix: cube asset family, e.g. ``"player_stats"``.
        measures: ``{asset_slug: measure_type_label}``.
        seasons: an int or iterable of seasons.
        row_filter: optional per-asset row filter (used by lineups to narrow
            to ``group_quantity == 5``, matching the old tag's contract).

    Returns:
        A polars DataFrame with a ``measure_type`` column distinguishing the
        stacked measure types; empty frame if nothing was found.
    """
    frames = []
    any_missing = False
    for season in _as_season_list(seasons):
        season_frames = []
        for slug, label in measures.items():
            df = _read_release_parquet(f"{_BASE_URL}/{table_prefix}_{slug}_{season}.parquet")
            if df is None:
                continue
            if row_filter is not None:
                df = row_filter(df)
            season_frames.append(df.with_columns(pl.lit(label).alias("measure_type")))
        if not season_frames:
            any_missing = True
            continue
        frames.append(pl.concat(season_frames, how="diagonal_relaxed"))
    if any_missing:
        cli_warn(f"{fn_name}: no data for one or more requested seasons (skipped)")
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def load_wnba_stats_player_season_stats(seasons, return_as_pandas: bool = False) -> pl.DataFrame:
    """Load season-level WNBA player statistics (deprecated).

    Args:
        seasons: an int or iterable of seasons.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per player-season-measure_type,
        stacked from the ``wnba_stats_leaguedash`` cube's ``player_stats_*``
        assets (``Base``/``Advanced``/``Misc``/``Scoring``/``Usage``/``Defense``
        — matches the old ``wnba_stats_player_season_stats`` tag's coverage;
        player-level ``Opponent``/``Four Factors`` are empty upstream and were
        never populated by either version).

    Example:
        Quick start::

            from sportsdataverse.wnba import load_wnba_stats_player_season_stats
            df = load_wnba_stats_player_season_stats(seasons=2026)
            print(df.shape)

        Pipeline next step (Advanced-only rows)::

            import polars as pl
            adv = df.filter(pl.col("measure_type") == "Advanced")

        See Also:
            * `nba_api <https://github.com/swar/nba_api>`_ -- underlying stats.wnba.com surface
    """
    warn_deprecated(
        "load_wnba_stats_player_season_stats",
        replacement="the wnba_stats_leaguedash release's player_stats_* / player_master assets",
        removed_in="0.1.0",
    )
    out = _stack_measures("load_wnba_stats_player_season_stats", "player_stats", _PLAYER_MEASURES, seasons)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def load_wnba_stats_lineups(seasons, return_as_pandas: bool = False) -> pl.DataFrame:
    """Load season-level WNBA 5-man lineup statistics (deprecated).

    Args:
        seasons: an int or iterable of seasons.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per lineup-season-measure_type,
        stacked from the ``wnba_stats_leaguedash`` cube's ``lineups_{base,
        advanced}`` assets filtered to ``group_quantity == 5`` — matching the
        old ``wnba_stats_lineups`` tag's 5-man-only, Base+Advanced-only
        coverage. Call the cube's ``lineups_*`` assets directly (unfiltered)
        for 2/3/4-man lineups or the other 4 measure types.

    Example:
        Quick start::

            from sportsdataverse.wnba import load_wnba_stats_lineups
            df = load_wnba_stats_lineups(seasons=2026)
            print(df.shape)

        See Also:
            * `nba_api <https://github.com/swar/nba_api>`_ -- underlying stats.wnba.com surface
    """
    warn_deprecated(
        "load_wnba_stats_lineups",
        replacement="the wnba_stats_leaguedash release's lineups_* / lineups_master assets",
        removed_in="0.1.0",
    )
    out = _stack_measures(
        "load_wnba_stats_lineups",
        "lineups",
        _LINEUP_MEASURES,
        seasons,
        row_filter=lambda df: df.filter(pl.col("group_quantity") == 5),
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def load_wnba_stats_team_season_stats(seasons, return_as_pandas: bool = False) -> pl.DataFrame:
    """Load season-level WNBA team statistics (deprecated).

    Args:
        seasons: an int or iterable of seasons.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per team-season-measure_type,
        stacked from the ``wnba_stats_leaguedash`` cube's ``team_stats_*``
        assets (``Base``/``Advanced``/``Misc``/``Scoring``/``Defense``/
        ``Opponent`` — matches the old ``wnba_stats_team_season_stats`` tag's
        coverage; team-level ``Usage``/``Four Factors`` are empty upstream).

    Example:
        Quick start::

            from sportsdataverse.wnba import load_wnba_stats_team_season_stats
            df = load_wnba_stats_team_season_stats(seasons=2026)
            print(df.shape)

        See Also:
            * `nba_api <https://github.com/swar/nba_api>`_ -- underlying stats.wnba.com surface
    """
    warn_deprecated(
        "load_wnba_stats_team_season_stats",
        replacement="the wnba_stats_leaguedash release's team_stats_* / team_master assets",
        removed_in="0.1.0",
    )
    out = _stack_measures("load_wnba_stats_team_season_stats", "team_stats", _TEAM_MEASURES, seasons)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def load_wnba_stats_standings(seasons, return_as_pandas: bool = False) -> pl.DataFrame:
    """Load season-level WNBA standings (deprecated).

    Args:
        seasons: an int or iterable of seasons.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per team-season, read from the
        ``wnba_stats_leaguedash`` cube's ``standings`` asset -- the same
        underlying ``leaguestandingsv3`` endpoint/params as the old
        ``wnba_stats_standings`` tag, so this is close to a pure passthrough.

    Example:
        Quick start::

            from sportsdataverse.wnba import load_wnba_stats_standings
            df = load_wnba_stats_standings(seasons=2026)
            print(df.shape)

        See Also:
            * `nba_api <https://github.com/swar/nba_api>`_ -- underlying stats.wnba.com surface
    """
    warn_deprecated(
        "load_wnba_stats_standings",
        replacement="the wnba_stats_leaguedash release's standings asset",
        removed_in="0.1.0",
    )
    frames, missing = [], []
    for season in _as_season_list(seasons):
        df = _read_release_parquet(f"{_BASE_URL}/standings_{season}.parquet")
        if df is None:
            missing.append(season)
            continue
        frames.append(df)
    if missing:
        cli_warn(f"load_wnba_stats_standings: no data for season(s) {missing} (skipped)")
    out = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
