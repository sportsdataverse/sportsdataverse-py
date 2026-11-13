"""stats.ncaa.org <-> ESPN college-basketball team-id crosswalk.

stats.ncaa.org mints a NEW numeric team id every season and writes school
names in AP style (``Central Conn. St.``, ``Ark.-Pine Bluff``); ESPN keeps one
stable id per school and spells the names out. Joining the two therefore needs
a **season-keyed** crosswalk.

It is built and published by sdv-reference-data (``sdv_reference/crosswalks/``)
to the ``{mbb,wbb}_crosswalk`` sportsdataverse-data releases, keyed on the NCAA
school (org id), with each season's conference from the ``{mbb,wbb}_groups``
tables. The loader reads that release, so a new season needs no sdv-py release,
and falls back to the copy sdv-py bundles when the release cannot be read:

- ``sportsdataverse/mbb/data/ncaa_espn_team_crosswalk_mbb.csv``
- ``sportsdataverse/wbb/data/ncaa_espn_team_crosswalk_wbb.csv``

The build is deterministic: there is no fuzzy matching at runtime OR at build
time, and a school that cannot be resolved is emitted with a null
``espn_team_id`` rather than dropped.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa

from sportsdataverse._codegen_runtime import _pandas_ids, _read_release_parquet, cli_warn
from sportsdataverse.config import SDVRELEASES
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.mbb.mbb_ncaa_team_ids import _LEAGUE_PKG, _league_data_bytes

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

_CROSSWALK_CACHE: dict[str, pl.DataFrame] = {}

#: ``espn_team_id`` is Int32, the dtype of the ESPN team ids in the ``load_{mbb,wbb}_*_crosswalk`` and
#: ``load_ncaa_{mbb,wbb}_*`` releases, so it joins those schema-equal (it joins the Int64 parser ids as it is).
#: NCAA ids are Int64 to match ``ncaa_{mbb,wbb}_team_ids().id``. ``conference_id`` is the SDV group id
#: (``"mbb:big-ten"``), text.
_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Utf8,
    "ncaa_team_id": pl.Int64,
    "ncaa_team": pl.Utf8,
    "ncaa_conference": pl.Utf8,
    "conference_id": pl.Utf8,
    "espn_team_id": pl.Int32,
    "espn_display_name": pl.Utf8,
    "espn_location": pl.Utf8,
    "espn_mascot": pl.Utf8,
    "espn_abbreviation": pl.Utf8,
    "espn_conference_name": pl.Utf8,
    "espn_conference_id": pl.Int64,
    "match_method": pl.Utf8,
}

_RELEASE_URL = SDVRELEASES + "{league}_crosswalk/ncaa_espn_team_crosswalk.parquet"


def _packaged_crosswalk(league: str) -> pl.DataFrame:
    """The crosswalk bundled with sdv-py (the fallback)."""
    raw = _league_data_bytes(league, f"ncaa_espn_team_crosswalk_{league}.csv")
    return pl.read_csv(io.BytesIO(raw), schema_overrides=_SCHEMA).select(list(_SCHEMA))


def _release_crosswalk(league: str) -> pl.DataFrame | None:
    """The ``{league}_crosswalk`` release asset in the loader's schema; ``None`` (with a warning) when it is absent
    or cannot be read or conformed, so the caller falls back to the bundled copy."""
    url = _RELEASE_URL.format(league=league)
    try:
        df = _read_release_parquet(url)
        if df is not None:
            return df.select([pl.col(c).cast(t) for c, t in _SCHEMA.items()])
        reason = "no published asset"
    # A failed fetch (offline, rate-limited), a corrupt or truncated asset (pyarrow cannot read it) or a drifted
    # one (a column gone or of another type) is a stale fallback, not an empty one: the bundled copy still
    # answers, with a warning (MD-4). Anything else is a bug and raises.
    except (AssetFetchError, NoDataError, pa.ArrowException, pl.exceptions.PolarsError) as e:
        reason = f"{type(e).__name__}: {e}"
    cli_warn(f"ncaa_espn_team_crosswalk: {url} unavailable ({reason}); using the bundled copy")
    return None


def ncaa_espn_team_crosswalk(league: str = "mbb", *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Season-keyed stats.ncaa.org -> ESPN team-id crosswalk.

    One row per ``(season, ncaa_team_id)``. Reads the ``{league}_crosswalk``
    sportsdataverse-data release, which gains a season as soon as it is
    published; when that cannot be read it warns and returns the copy bundled
    with sdv-py. Teams that could not be resolved to an ESPN team are kept with
    a null ``espn_team_id`` and ``match_method="unmatched"`` -- never dropped.

    Args:
        league: ``"mbb"`` (men's, 2009-10 onward) or ``"wbb"`` (women's).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with columns ``season`` (str, ``"YYYY-YY"``),
        ``ncaa_team_id`` (Int64 -- the season-specific stats.ncaa.org id),
        ``ncaa_team`` / ``ncaa_conference`` (str), ``conference_id`` (str,
        nullable -- the SDV group id, e.g. ``"mbb:big-ten"``), ``espn_team_id``
        (Int32, nullable -- the dtype of the ESPN team ids in the crosswalk and NCAA releases),
        ``espn_display_name`` / ``espn_location`` / ``espn_mascot`` /
        ``espn_abbreviation`` / ``espn_conference_name`` (str, nullable),
        ``espn_conference_id`` (Int64, nullable), and ``match_method`` (str --
        ``"exact"``, ``"dict"``, ``"alias"``, ``"org"`` (the school's ESPN id
        from its other seasons) or ``"unmatched"``). The three
        conference columns and ``ncaa_conference`` are per season: Maryland is
        ACC in 2013-14 and Big Ten in 2014-15.

    Raises:
        ValueError: If *league* is not ``"mbb"`` or ``"wbb"``.

    Example:
        Quick start::

            from sportsdataverse.mbb import ncaa_espn_team_crosswalk
            df = ncaa_espn_team_crosswalk()
            print(df.shape)

        Women's crosswalk as pandas::

            wdf = ncaa_espn_team_crosswalk(league="wbb", return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("season") == "2025-26").select("ncaa_team_id", "espn_team_id")

    See Also:
        * `hoopR`_ -- men's college basketball in R, source of the name dictionary
        * `wehoop`_ -- women's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if league not in _LEAGUE_PKG:
        raise ValueError(f"league must be one of {sorted(_LEAGUE_PKG)}, got {league!r}")
    df = _CROSSWALK_CACHE.get(league)
    if df is None:
        release = _release_crosswalk(league)
        if release is not None:
            _CROSSWALK_CACHE[league] = df = release
        else:
            # Not cached: a later call retries the release (a transient failure must not pin the
            # bundled copy for the life of the process).
            df = _packaged_crosswalk(league)
    return _pandas_ids(df) if return_as_pandas else df


#: The two school tables sdv-reference-data publishes beside the crosswalk. ``ncaa_org_id`` is the stable
#: stats.ncaa.org school (org) id, ``ncaa_team_id`` the per-season team id; ``espn_team_id`` is Int32, the dtype
#: of the ESPN team ids in the ``load_{mbb,wbb}_*_crosswalk`` and ``load_ncaa_{mbb,wbb}_*`` releases.
_SCHOOLS_SCHEMA: dict[str, pl.DataType] = {
    "ncaa_org_id": pl.Int64,
    "ncaa_team": pl.Utf8,
    "espn_team_id": pl.Int32,
    "espn_display_name": pl.Utf8,
}
_SCHOOL_SEASONS_SCHEMA: dict[str, pl.DataType] = {
    "ncaa_org_id": pl.Int64,
    "sport": pl.Utf8,
    "season": pl.Utf8,
    "ncaa_team_id": pl.Int64,
    "espn_team_id": pl.Int32,
}
_SCHOOLS_URL = SDVRELEASES + "{league}_crosswalk/{name}.parquet"


def _release_table(name: str, league: str, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """One static ``{league}_crosswalk`` table in ``schema``.

    sdv-py bundles no copy of these, so there is nothing to fall back to: an absent asset (``NoDataError``) is
    the documented columns with zero rows and a warning, while a failed fetch (``AssetFetchError``) raises --
    zero rows there would null every id joined through the table.
    """
    if league not in _LEAGUE_PKG:
        raise ValueError(f"league must be one of {sorted(_LEAGUE_PKG)}, got {league!r}")
    url = _SCHOOLS_URL.format(league=league, name=name)
    df = _read_release_parquet(url)
    if df is None:
        cli_warn(f"{name}: no published asset at {url} (returning an empty frame)")
        return pl.DataFrame(schema=schema)
    return df.select([pl.col(c).cast(t) for c, t in schema.items()])


def ncaa_espn_schools(league: str = "mbb", *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """stats.ncaa.org schools (org ids) and the ESPN team each one maps to.

    One row per NCAA school, read from the static ``ncaa_espn_schools`` asset of
    the ``{league}_crosswalk`` sportsdataverse-data release that sdv-reference-data
    builds. A school keeps one ``ncaa_org_id`` across seasons while its
    stats.ncaa.org team id changes every season; :func:`ncaa_espn_school_seasons`
    holds that per-season mapping, and :func:`ncaa_espn_team_crosswalk` the full
    season-keyed crosswalk built from both.

    Args:
        league: ``"mbb"`` (men's) or ``"wbb"`` (women's): which release tag to read.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with ``ncaa_org_id`` (Int64 -- the stats.ncaa.org school id),
        ``ncaa_team`` (str, the stats.ncaa.org school name), ``espn_team_id``
        (Int32, nullable -- null for a school with no ESPN team) and
        ``espn_display_name`` (str, nullable). An absent asset returns these
        columns with zero rows and a warning.

    Raises:
        ValueError: If *league* is not ``"mbb"`` or ``"wbb"``.
        AssetFetchError: If the release asset exists but cannot be fetched.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.mbb import ncaa_espn_schools
            df = ncaa_espn_schools()
            print(df.shape)

        Women's tag as pandas::

            wdf = ncaa_espn_schools(league="wbb", return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("espn_team_id").is_null()).select("ncaa_org_id", "ncaa_team")

    See Also:
        * `hoopR`_ -- men's college basketball in R
        * `wehoop`_ -- women's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    df = _release_table("ncaa_espn_schools", league, _SCHOOLS_SCHEMA)
    return _pandas_ids(df) if return_as_pandas else df


def ncaa_espn_school_seasons(league: str = "mbb", *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per-season stats.ncaa.org team id of each NCAA school, with its ESPN team.

    One row per ``(ncaa_org_id, season)`` for *league*, read from the static
    ``ncaa_espn_school_seasons`` asset of the ``{league}_crosswalk``
    sportsdataverse-data release. The asset carries both sports in a ``sport``
    column; only *league*'s rows are returned. Join it to
    :func:`ncaa_espn_schools` on ``ncaa_org_id``, or to a
    ``load_ncaa_{mbb,wbb}_*`` frame on that season's ``ncaa_team_id``.

    Args:
        league: ``"mbb"`` (men's) or ``"wbb"`` (women's).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with ``ncaa_org_id`` (Int64), ``sport`` (str, equal to
        *league*), ``season`` (str, ``"YYYY-YY"``), ``ncaa_team_id`` (Int64 --
        that season's stats.ncaa.org team id) and ``espn_team_id`` (Int32,
        nullable). An absent asset returns these columns with zero rows and a
        warning.

    Raises:
        ValueError: If *league* is not ``"mbb"`` or ``"wbb"``.
        AssetFetchError: If the release asset exists but cannot be fetched.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.mbb import ncaa_espn_school_seasons
            df = ncaa_espn_school_seasons()
            print(df.shape)

        Women's seasons as pandas::

            wdf = ncaa_espn_school_seasons(league="wbb", return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("season") == "2024-25").select("ncaa_team_id", "espn_team_id")

    See Also:
        * `hoopR`_ -- men's college basketball in R
        * `wehoop`_ -- women's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    df = _release_table("ncaa_espn_school_seasons", league, _SCHOOL_SEASONS_SCHEMA)
    df = df.filter(pl.col("sport") == league)
    return _pandas_ids(df) if return_as_pandas else df


#: One dtype per id: NCAA team ids Int64 (as in the team crosswalk), NCAA player ids Utf8 (as in
#: every ``load_ncaa_{mbb,wbb}_*`` release), ESPN ids Int64 (as everywhere in sdv-py).
_PLAYER_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int32,
    "ncaa_team_id": pl.Int64,
    "ncaa_player_id": pl.Utf8,
    "ncaa_player_name": pl.Utf8,
    "ncaa_jersey": pl.Utf8,
    "espn_team_id": pl.Int64,
    "espn_athlete_id": pl.Int64,
    "espn_athlete_name": pl.Utf8,
    "espn_jersey": pl.Utf8,
    "match_method": pl.Utf8,
    "match_confidence": pl.Float64,
    "unmatched_reason": pl.Utf8,
}


def _ncaa_rosters(league: str, season: int) -> pl.DataFrame:
    """The ``ncaa_{league}_team_rosters`` release for one season (the NCAA side)."""
    if league == "mbb":
        from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_team_rosters as load
    else:
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_team_rosters as load
    return load(seasons=season)


def _join(left: pl.DataFrame, right: pl.DataFrame, on: list[str]) -> pl.DataFrame:
    """Left join that refuses mismatched key dtypes instead of matching nothing."""
    for key in on:
        assert left.schema[key] == right.schema[key], f"join key {key}: {left.schema[key]} != {right.schema[key]}"
    return left.join(right, on=on, how="left", maintain_order="left")


def ncaa_espn_player_crosswalk(
    season: int, league: str = "mbb", *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Season crosswalk from the stats.ncaa.org player id to the ESPN athlete id.

    One row per NCAA roster player per team for one season, built from published
    releases only: the ``ncaa_{league}_team_rosters`` roster, resolved to an ESPN
    team through :func:`ncaa_espn_team_crosswalk`, matched against the ESPN
    athletes of that team in ``load_{league}_player_boxscore`` (each athlete's
    most-worn jersey). Players are compared only inside one ESPN team, in four
    tiers -- normalized name + jersey, name, last name + jersey, then a
    Jaro-Winkler score of at least 0.92 -- and a tier only takes a pair that is
    unique on both sides, so a tie is left unmatched rather than guessed
    (:mod:`sportsdataverse.scrape.ncaa.espn_player_xwalk`). Unmatched players
    are kept, with the reason.

    stats.ncaa.org issues new team and player ids every season, so a player has
    a different ``ncaa_player_id`` on each season's row. The multi-season history
    of the same pairs is published as ``load_{mbb,wbb}_player_aliases`` (rows with
    ``source == "ncaa"``), with the same tier names and confidences.

    Args:
        season: Ending year of the season (``2024`` = 2023-24).
        league: ``"mbb"`` (men's) or ``"wbb"`` (women's).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with ``season`` (Int32), ``ncaa_team_id`` (Int64),
        ``ncaa_player_id`` / ``ncaa_player_name`` / ``ncaa_jersey`` (str),
        ``espn_team_id`` / ``espn_athlete_id`` (Int64, nullable),
        ``espn_athlete_name`` / ``espn_jersey`` (str, nullable),
        ``match_method`` (``"name_jersey"``, ``"name"``, ``"last_jersey"``,
        ``"jaro_winkler"``, or null when unmatched), ``match_confidence``
        (Float64: 1.0 / 0.95 / 0.9 / 0.85 by tier, null when unmatched) and
        ``unmatched_reason`` (``"no_espn_team"``, ``"no_espn_players"``,
        ``"ambiguous"``, ``"no_candidate"``, or null when matched). A season
        with no published roster (2010 onward) returns these columns with zero rows.

    Raises:
        ValueError: If *league* is not ``"mbb"`` or ``"wbb"``.
        SeasonNotFoundError: If *season* is before 2010 (the first NCAA roster release).

    Example:
        Quick start::

            from sportsdataverse.mbb import ncaa_espn_player_crosswalk
            df = ncaa_espn_player_crosswalk(2024)
            print(df["match_method"].value_counts())

        Women's crosswalk as pandas::

            wdf = ncaa_espn_player_crosswalk(2024, league="wbb", return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("match_confidence") >= 0.95).select("ncaa_player_id", "espn_athlete_id")

    See Also:
        * `hoopR`_ -- men's college basketball in R
        * `wehoop`_ -- women's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if league not in _LEAGUE_PKG:
        raise ValueError(f"league must be one of {sorted(_LEAGUE_PKG)}, got {league!r}")
    out = _build_player_crosswalk(league, int(season))
    return _pandas_ids(out) if return_as_pandas else out


def _build_player_crosswalk(league: str, season: int) -> pl.DataFrame:
    """The :func:`ncaa_espn_player_crosswalk` frame (polars) for one validated league-season."""
    from sportsdataverse.scrape.ncaa import espn_player_xwalk as px

    roster = _ncaa_rosters(league, season)
    if roster.height == 0:
        return pl.DataFrame(schema=_PLAYER_SCHEMA)
    label = f"{season - 1}-{season % 100:02d}"
    teams = (
        ncaa_espn_team_crosswalk(league)
        .filter(pl.col("season") == label)
        .select("ncaa_team_id", "espn_team_id")
        .unique("ncaa_team_id", keep="first")
    )
    ncaa = (
        roster.select(
            pl.col("team_id").cast(pl.Int64).alias("ncaa_team_id"),
            px._id(pl.col("player_id")).alias("id"),
            pl.col("clean_name").cast(pl.Utf8).alias("name"),
            px._jersey(pl.col("jersey")).alias("jersey"),
        )
        .drop_nulls("id")
        .unique(["ncaa_team_id", "id"], keep="first", maintain_order=True)
    )
    ncaa = _join(ncaa, teams, ["ncaa_team_id"])
    espn = px.espn_athlete_side(league, season)
    pairs = px.match_players(ncaa.with_columns(pl.col("ncaa_team_id", "espn_team_id").cast(pl.Utf8)), espn)
    names_ncaa = ncaa.select(
        "ncaa_team_id",
        pl.col("id").alias("ncaa_player_id"),
        pl.col("name").alias("ncaa_player_name"),
        pl.col("jersey").alias("ncaa_jersey"),
    )
    names_espn = espn.select(
        pl.col("espn_team_id").cast(pl.Int64),
        pl.col("id").cast(pl.Int64).alias("espn_athlete_id"),
        pl.col("name").alias("espn_athlete_name"),
        pl.col("jersey").alias("espn_jersey"),
    )
    out = pairs.with_columns(pl.col("ncaa_team_id", "espn_team_id", "espn_athlete_id").cast(pl.Int64))
    out = _join(out, names_ncaa, ["ncaa_team_id", "ncaa_player_id"])
    out = _join(out, names_espn, ["espn_team_id", "espn_athlete_id"])
    return out.with_columns(
        pl.lit(season, dtype=pl.Int32).alias("season"),
        pl.col("match_method")
        .replace_strict(px.MATCH_CONFIDENCE, default=None, return_dtype=pl.Float64)
        .alias("match_confidence"),
    ).select([pl.col(c).cast(t) for c, t in _PLAYER_SCHEMA.items()])


__all__ = ["ncaa_espn_player_crosswalk", "ncaa_espn_school_seasons", "ncaa_espn_schools", "ncaa_espn_team_crosswalk"]
