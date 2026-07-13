"""NCAA basketball team-id crosswalk (bigballR ``teamids`` port).

stats.ncaa.org assigns every college a NEW numeric team id each season, so
name-based lookups need a ``(team, season) -> id`` crosswalk. bigballR ships
this as ``data/teamids.RData`` (men) and wbigballR as its own women's copy;
sdv-py bundles both as CSVs:

- ``sportsdataverse/mbb/data/ncaa_teamids_mbb.csv`` (2009-10 -> 2025-26)
- ``sportsdataverse/wbb/data/ncaa_teamids_wbb.csv`` (2009-10 -> 2024-25)

Deliberate divergence from wbigballR: its ``get_team_schedule`` /
``get_team_roster`` resolve names against ``bigballR::teamids`` -- the MEN'S
table -- so women's ids never resolve (spec_wbigballr_divergence.md section 3).
Here every lookup is per-league via :func:`resolve_ncaa_team_id`'s ``league``
argument.

:func:`refresh_ncaa_team_ids` ports wbigballR's ``update_team_ids.R``
maintainer recipe (a 100%-commented-out scratch script -- the only
in-ecosystem documentation of the annual refresh).
"""

from __future__ import annotations

import io
import re
import urllib.parse
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Sequence, Union

import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

_LEAGUE_PKG: Dict[str, str] = {"mbb": "sportsdataverse.mbb", "wbb": "sportsdataverse.wbb"}
_CACHE: Dict[str, pl.DataFrame] = {}

#: update_team_ids.R team-text cleanup: leading spaces + " (W-L)" record tag.
_LEADING_WS_RE = re.compile(r"^ +")
_RECORD_RE = re.compile(r" \([0-9]-[0-9]\)")


def _teamids_csv_bytes(league: str) -> bytes:
    """Bundled-CSV bytes for *league*, importlib.resources first, Path fallback.

    The plain-``Path`` fallback keeps tests green before the CSVs are wired
    into ``[tool.setuptools.package-data]``.
    """
    if league not in _LEAGUE_PKG:
        raise ValueError(f"league must be one of {sorted(_LEAGUE_PKG)}, got {league!r}")
    fname = f"ncaa_teamids_{league}.csv"
    try:
        return resources.files(_LEAGUE_PKG[league]).joinpath("data", fname).read_bytes()
    except (FileNotFoundError, ModuleNotFoundError, NotADirectoryError, OSError):
        return (Path(__file__).resolve().parents[1] / league / "data" / fname).read_bytes()


def _ncaa_bb_team_ids(league: str) -> pl.DataFrame:
    """The *league* crosswalk: ``team``/``conference``/``id`` (Int64)/``season``."""
    if league not in _CACHE:
        df = pl.read_csv(
            io.BytesIO(_teamids_csv_bytes(league)),
            schema_overrides={"ID": pl.Int64},
        ).rename({"Team": "team", "Conference": "conference", "ID": "id", "Season": "season"})
        _CACHE[league] = df.select("team", "conference", "id", "season")
    return _CACHE[league]


def ncaa_mbb_team_ids(*, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Men's-basketball ``(team, season) -> stats.ncaa.org id`` crosswalk.

    Port of bigballR's bundled ``teamids`` data asset (one row per team per
    season, 2009-10 through 2025-26).

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with columns ``team`` (str), ``conference`` (str),
        ``id`` (Int64 -- the season-specific stats.ncaa.org team id) and
        ``season`` (str, ``"YYYY-YY"``).

    Example:
        Quick start::

            from sportsdataverse.mbb import ncaa_mbb_team_ids
            df = ncaa_mbb_team_ids()
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("season") == "2025-26").head()

    See Also:
        * `hoopR`_ -- men's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    df = _ncaa_bb_team_ids("mbb")
    return df.to_pandas() if return_as_pandas else df


def resolve_ncaa_team_id(team: str, season: str, league: str = "mbb") -> Optional[int]:
    """Resolve a school name + season to its stats.ncaa.org team id.

    Exact ``(team, season)`` match first (bigballR semantics), then a
    case-insensitive fallback.

    Args:
        team: School name as it appears in the crosswalk (``"Illinois"``,
            not ``"Illinois Fighting Illini"``).
        season: Season string, e.g. ``"2025-26"``.
        league: ``"mbb"`` or ``"wbb"`` -- which league's crosswalk to search.
            (Deliberate fix of wbigballR, which always searched the men's
            table.)

    Returns:
        The team id as ``int``, or ``None`` when no row matches.

    Example:
        Quick start::

            from sportsdataverse.mbb import resolve_ncaa_team_id
            resolve_ncaa_team_id("Illinois", "2025-26")

        Women's league lookup::

            resolve_ncaa_team_id("South Carolina", "2024-25", league="wbb")
    """
    df = _ncaa_bb_team_ids(league)
    hit = df.filter((pl.col("team") == team) & (pl.col("season") == season))
    if hit.height == 0:
        hit = df.filter((pl.col("team").str.to_lowercase() == team.lower()) & (pl.col("season") == season))
    if hit.height == 0:
        return None
    return int(hit.get_column("id")[0])


def refresh_ncaa_team_ids(
    season: str,
    season_division_id: int,
    dates: Sequence[str],
    *,
    league: str = "mbb",
    fetcher: Optional["NcaaFetcher"] = None,
    prior_season: Optional[str] = None,
    conference_overrides: Optional[Dict[str, str]] = None,
    extra_teams: Optional[Iterable[Dict[str, object]]] = None,
) -> pl.DataFrame:
    """Extend the bundled crosswalk with a new season (update_team_ids recipe).

    Port of wbigballR's ``R/update_team_ids.R`` maintainer scratch script:
    scrape early-season scoreboard pages, harvest the ``/teams/{id}`` anchor
    links, keep teams known from the prior season, carry each team's prior
    conference forward, hand-patch realignments, and append the stamped rows
    to the historical table.

    Network path -- hits stats.ncaa.org via :class:`~sportsdataverse.mbb.
    mbb_ncaa_fetch.NcaaFetcher`. The result is returned (NOT written); a
    maintainer overwrites ``sportsdataverse/<league>/data/ncaa_teamids_
    <league>.csv`` with it after review.

    Args:
        season: Season being added, e.g. ``"2026-27"``.
        season_division_id: The ``season_divisions/{id}/scoreboards`` id for
            that season + division (league-specific; see get_date_games'
            season table).
        dates: Scoreboard dates to sweep, ``"MM/DD/YYYY"`` -- the recipe uses
            ~9 early-November dates so every D-I team appears at least once.
        league: ``"mbb"`` or ``"wbb"``.
        fetcher: Injectable :class:`NcaaFetcher`; defaults to a fresh one
            (which requires proxy/browser configuration at fetch time).
        prior_season: Season whose team list + conferences seed the join;
            defaults to the max season already in the bundled table.
        conference_overrides: ``{team: new_conference}`` hand-patches for
            realignments (the recipe's ``Conference[Team == p] <- ...`` block).
        extra_teams: Rows for brand-new programs the prior-season filter
            drops, e.g. ``[{"team": "St. Thomas (MN)", "conference":
            "Summit League", "id": 529315}]`` (``season`` is stamped).

    Returns:
        The full refreshed crosswalk (historical rows + the new season),
        deduplicated and sorted by season/team.

    Example:
        Refresh the men's table for a new season::

            from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
            from sportsdataverse.mbb.mbb_ncaa_team_ids import refresh_ncaa_team_ids
            dates = [f"11/{d:02d}/2026" for d in range(9, 18)]
            df = refresh_ncaa_team_ids("2026-27", 18823, dates,
                                       fetcher=NcaaFetcher.with_browser())
            df.write_csv("sportsdataverse/mbb/data/ncaa_teamids_mbb.csv")
    """
    from sportsdataverse.mbb.mbb_ncaa_html import parse_html

    if fetcher is None:
        from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

        fetcher = NcaaFetcher()

    base = _ncaa_bb_team_ids(league)
    prior = prior_season if prior_season is not None else str(base.get_column("season").max())
    prior_rows = base.filter(pl.col("season") == prior)
    prior_teams = set(prior_rows.get_column("team").to_list())

    # 1-2. sweep the scoreboard dates, harvest <a href="/teams/{id}">Team</a>.
    pairs: "list[tuple[str, str]]" = []
    for date in dates:
        path = (
            f"season_divisions/{season_division_id}/scoreboards?utf8=%E2%9C%93"
            f"&season_division_id=&game_date={urllib.parse.quote(date, safe='')}"
            f"&conference_id=0&tournament_id=&commit=Submit"
        )
        soup = parse_html(fetcher.fetch_html(path))
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).replace("/teams/", "", 1)
            if not href or "/" in href or not href.isdigit():
                continue
            team = _RECORD_RE.sub("", _LEADING_WS_RE.sub("", a.get_text()))
            pairs.append((team, href))

    harvested = pl.DataFrame(
        {"team": [p[0] for p in pairs], "id": [p[1] for p in pairs]},
        schema={"team": pl.Utf8, "id": pl.Utf8},
    ).unique(maintain_order=True)

    # 3. keep known programs, carry the prior conference forward, stamp season.
    new_rows = (
        harvested.filter(pl.col("team").is_in(sorted(prior_teams)))
        .sort("team")
        .with_columns(pl.col("id").cast(pl.Int64))
        .join(prior_rows.select("team", "conference").unique(), on="team", how="left")
        .with_columns(pl.lit(season).alias("season"))
        .select("team", "conference", "id", "season")
    )

    # 4. hand-patches: realignment overrides + brand-new programs.
    for team, conference in (conference_overrides or {}).items():
        new_rows = new_rows.with_columns(
            pl.when(pl.col("team") == team).then(pl.lit(conference)).otherwise(pl.col("conference")).alias("conference")
        )
    if extra_teams is not None:
        extra = pl.DataFrame([dict(row) for row in extra_teams]).with_columns(
            pl.col("id").cast(pl.Int64), pl.lit(season).alias("season")
        )
        new_rows = pl.concat([new_rows, extra.select("team", "conference", "id", "season")])

    return pl.concat([base, new_rows]).unique(maintain_order=True).sort("season", "team")


__all__ = [
    "ncaa_mbb_team_ids",
    "resolve_ncaa_team_id",
    "refresh_ncaa_team_ids",
]
