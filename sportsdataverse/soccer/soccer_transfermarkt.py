"""Transfermarkt clubs, players, market values, transfers and competitions, plus an ESPN team-id bridge.

The tables are the CC0 1.0 Transfermarkt dataset, a snapshot current to 2026-07-06 that is not being
refreshed, so 2026-27 squads, transfers and values are absent. Each loader reads the table's parquet from the
``soccer_transfermarkt`` sportsdataverse-data release (a mirror, with sha256s and a README naming source and
licence; the hashes are for audit, the loader does not check them); when that cannot be read it warns and reads
the original gzip CSV instead, typing the ids at this boundary. Either way ``player_id``, ``club_id`` and every ``*_club_id`` are the Int64 Transfermarkt ids;
competition ids such as ``"GB1"`` stay text.

:func:`soccer_espn_transfermarkt_team_crosswalk` maps ESPN soccer team ids to Transfermarkt ``club_id``
from the bundled ESPN <-> reep crosswalk (whose reep rows carry the Transfermarkt id for 166 of its 168
ESPN teams) plus a reviewed id for a club whose reep row has none. No request is made.
"""

from __future__ import annotations

import functools
import gzip
import io
import warnings
import zlib
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._codegen_runtime import _check_response, _fetch_release_parquet, _transport_errors, cli_warn
from sportsdataverse.config import SDVRELEASES
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError, SchemaDriftWarning
from sportsdataverse.soccer.soccer_reep_crosswalk import soccer_espn_reep_team_crosswalk

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "load_soccer_transfermarkt_clubs",
    "load_soccer_transfermarkt_competitions",
    "load_soccer_transfermarkt_player_valuations",
    "load_soccer_transfermarkt_players",
    "load_soccer_transfermarkt_transfers",
    "soccer_espn_transfermarkt_player_crosswalk",
    "soccer_espn_transfermarkt_team_crosswalk",
]

TRANSFERMARKT_DATA_URL = "https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/{table}.csv.gz"
#: The mirror (``tools/publish_soccer_transfermarkt.py``): the same tables, already typed, as parquet.
TRANSFERMARKT_RELEASE_URL = SDVRELEASES + "soccer_transfermarkt/{table}.parquet"

_I, _F, _S, _D = pl.Int64, pl.Float64, pl.Utf8, pl.Date

#: Column -> dtype per table, in the published column order. Dates arrive as ``YYYY-MM-DD`` or
#: ``YYYY-MM-DD 00:00:00`` and become Date. ``clubs.filename`` (the producer's raw-file path) and
#: ``clubs.total_market_value`` (empty in every row of the snapshot) are not served.
_TABLES: dict[str, dict[str, pl.DataType]] = {
    "clubs": {
        "club_id": _I, "club_code": _S, "name": _S, "domestic_competition_id": _S, "squad_size": _I,
        "average_age": _F, "foreigners_number": _I, "foreigners_percentage": _F, "national_team_players": _I,
        "stadium_name": _S, "stadium_seats": _I, "net_transfer_record": _S, "coach_name": _S,
        "last_season": _I, "url": _S,
    },
    "players": {
        "player_id": _I, "first_name": _S, "last_name": _S, "name": _S, "last_season": _I,
        "current_club_id": _I, "player_code": _S, "country_of_birth": _S, "city_of_birth": _S,
        "country_of_citizenship": _S, "date_of_birth": _D, "sub_position": _S, "position": _S, "foot": _S,
        "height_in_cm": _I, "contract_expiration_date": _D, "agent_name": _S, "image_url": _S,
        "international_caps": _I, "international_goals": _I, "current_national_team_id": _I, "url": _S,
        "current_club_domestic_competition_id": _S, "current_club_name": _S, "market_value_in_eur": _I,
        "highest_market_value_in_eur": _I,
    },
    "player_valuations": {
        "player_id": _I, "date": _D, "market_value_in_eur": _I, "current_club_name": _S,
        "current_club_id": _I, "player_club_domestic_competition_id": _S,
    },
    "transfers": {
        "player_id": _I, "transfer_date": _D, "transfer_season": _S, "from_club_id": _I, "to_club_id": _I,
        "from_club_name": _S, "to_club_name": _S, "transfer_fee": _F, "market_value_in_eur": _F,
        "player_name": _S,
    },
    "competitions": {
        "competition_id": _S, "competition_code": _S, "name": _S, "sub_type": _S, "type": _S,
        "country_id": _I, "country_name": _S, "domestic_league_code": _S, "confederation": _S,
        "total_clubs": _I, "url": _S,
    },
}  # fmt: skip

#: ESPN team id -> Transfermarkt club_id for ESPN teams whose reep row has no Transfermarkt id, each reviewed
#: (2026-11-26) against the Transfermarkt clubs table: the same club name, league and stadium.
_TM_REVIEWED: dict[str, int] = {
    "83": 131,  # Barcelona -> FC Barcelona (ES1, Spotify Camp Nou); reep's FC Barcelona row has no provider ids
}


def _fetch(table: str) -> pl.DataFrame:
    """One published table, every column read as text (no inference: ids never pass through a float)."""
    url = TRANSFERMARKT_DATA_URL.format(table=table)
    with _transport_errors(url):
        resp = download(url)
    _check_response(resp, url)
    body = resp.content or b""
    if body[:2] != b"\x1f\x8b":
        raise AssetFetchError(f"{url} answered HTTP {resp.status_code} with something other than a gzip CSV")
    try:
        out = pl.read_csv(io.BytesIO(gzip.decompress(body)), infer_schema=False)
    # a truncated or corrupt gzip, an empty file, a ragged CSV: the fetch failed, it did not answer "nothing here"
    except (EOFError, OSError, zlib.error, pl.exceptions.PolarsError) as exc:
        raise AssetFetchError(f"{url}: the body is not a readable gzip CSV ({type(exc).__name__}: {exc})") from exc
    if out.is_empty():  # every published table has rows; a header-only file is a broken publish, not an answer
        raise AssetFetchError(f"{url}: the gzip CSV has a header and no rows")
    return out


def _typed(raw: pl.DataFrame, table: str) -> pl.DataFrame:
    """``raw`` in the table's dtypes. A column a value would not survive stays text, with a warning."""
    schema = _TABLES[table]
    missing = [c for c in schema if c not in raw.columns]
    if missing:
        raise AssetFetchError(f"transfermarkt {table}: the published file has no {missing}")
    out = raw.select(list(schema))
    refused = []
    for c, t in schema.items():
        if t == pl.Utf8:
            continue
        s = out[c].str.strip_chars().replace("", None)
        if t == pl.Int64 and s.str.contains(r"[^0-9-]").any():
            refused.append(c)  # an integer column never rounds or drops a decimal value
            continue
        try:
            # only the two published date forms; a time other than midnight or any other suffix is refused
            cast = (
                s.str.strip_suffix(" 00:00:00").str.to_date("%Y-%m-%d", strict=True, exact=True)
                if t == pl.Date
                else s.cast(t, strict=True)
            )
        except pl.exceptions.PolarsError:
            refused.append(c)
            continue
        out = out.with_columns(cast.alias(c))
    if refused:
        warnings.warn(
            f"load_soccer_transfermarkt_{table}: columns {refused} hold values of another type; kept as text",
            SchemaDriftWarning,
            stacklevel=4,
        )
    return out


def _from_release(table: str) -> pl.DataFrame | None:
    """The mirrored table, or ``None`` (with a warning naming why) when it cannot be read or has drifted."""
    import pyarrow as pa

    url = TRANSFERMARKT_RELEASE_URL.format(table=table)
    try:
        df = _fetch_release_parquet(url)
        if df.schema != pl.Schema(_TABLES[table]):  # names, dtypes and order
            reason = f"columns {dict(df.schema)} are not the loader's {_TABLES[table]}"
        elif df.is_empty():
            reason = "the asset has no rows"
        else:
            return df
    # absent (404), failed (429/5xx/dropped), or unreadable (pyarrow reports a corrupt footer as a bare OSError;
    # every transport OSError is already an AssetFetchError here): the original source still answers, with a warning
    except (NoDataError, AssetFetchError, pa.ArrowException, pl.exceptions.PolarsError, OSError) as e:
        reason = f"{type(e).__name__}: {' '.join(str(e).split())}"  # one line, whatever the parser wrote
    cli_warn(f"load_soccer_transfermarkt_{table}: {url} unavailable ({reason}); reading the original source")
    return None


def _load(table: str, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    out = _from_release(table)
    if out is None:
        out = _typed(_fetch(table), table)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def load_soccer_transfermarkt_players(*, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Load the CC0 Transfermarkt players table: one row per player, with current club and market value.

    About 50,000 players: everyone who appeared for a club of the first-tier leagues the dataset covers
    (Premier League, LaLiga, Bundesliga, Serie A, Ligue 1, MLS, Liga MX and about 20 more). A snapshot
    current to 2026-07-06 (not refreshed), read on every call from the release mirror. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``player_id`` (Int64). ``current_club_id`` (Int64)
        joins ``club_id`` of ``load_soccer_transfermarkt_clubs`` and of
        ``soccer_espn_transfermarkt_team_crosswalk``. A column holding a value of another type comes back as
        text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The original source answered 400 / 422.
        NoDataError: The original source answered 404 (the file moved).
        AssetFetchError: The original source, read when the release mirror cannot be (with a warning), failed
            (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or
            lacks a column this loader types.

    Example:
        Quick start::

            from sportsdataverse.soccer import load_soccer_transfermarkt_players
            players = load_soccer_transfermarkt_players()
            print(players.shape)

        Most valuable players::

            players.sort("market_value_in_eur", descending=True, nulls_last=True).head(10)

    See Also:
        * `transfermarkt-datasets`_ -- the published dataset and its table documentation

    .. _transfermarkt-datasets: https://github.com/dcaribou/transfermarkt-datasets
    """
    return _load("players", return_as_pandas)


def load_soccer_transfermarkt_clubs(*, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Load the CC0 Transfermarkt clubs table: one row per club of the first-tier leagues the dataset covers.

    About 800 clubs, each with its league, squad profile and stadium as of its last season in the dataset.
    A snapshot current to 2026-07-06 (not refreshed), read on every call from the release mirror.
    Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``club_id`` (Int64), the id
        ``soccer_espn_transfermarkt_team_crosswalk`` maps ESPN teams to. A column holding a value of another
        type comes back as text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The original source answered 400 / 422.
        NoDataError: The original source answered 404 (the file moved).
        AssetFetchError: The original source, read when the release mirror cannot be (with a warning), failed
            (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or
            lacks a column this loader types.

    Example:
        Quick start::

            from sportsdataverse.soccer import load_soccer_transfermarkt_clubs
            clubs = load_soccer_transfermarkt_clubs()

        ESPN Premier League teams with their Transfermarkt club rows::

            from sportsdataverse.soccer import soccer_espn_transfermarkt_team_crosswalk
            soccer_espn_transfermarkt_team_crosswalk(league="eng.1").join(clubs, on="club_id", how="left")

    See Also:
        * `transfermarkt-datasets`_ -- the published dataset and its table documentation

    .. _transfermarkt-datasets: https://github.com/dcaribou/transfermarkt-datasets
    """
    return _load("clubs", return_as_pandas)


def load_soccer_transfermarkt_player_valuations(*, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Load the CC0 Transfermarkt market-value history: one row per player and valuation date.

    About 656,000 valuations, from 2000 to 2026-06-12 (the snapshot is not refreshed), read on every call from
    the release mirror. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per (``player_id``, ``date``); ``player_id`` and
        ``current_club_id`` are Int64 and join the players and clubs tables. A column holding a value of
        another type comes back as text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The original source answered 400 / 422.
        NoDataError: The original source answered 404 (the file moved).
        AssetFetchError: The original source, read when the release mirror cannot be (with a warning), failed
            (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or
            lacks a column this loader types.

    Example:
        Quick start::

            from sportsdataverse.soccer import load_soccer_transfermarkt_player_valuations
            values = load_soccer_transfermarkt_player_valuations()

        One player's value curve::

            import polars as pl
            values.filter(pl.col("player_id") == 418560).sort("date")

    See Also:
        * `transfermarkt-datasets`_ -- the published dataset and its table documentation

    .. _transfermarkt-datasets: https://github.com/dcaribou/transfermarkt-datasets
    """
    return _load("player_valuations", return_as_pandas)


def load_soccer_transfermarkt_transfers(*, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Load the CC0 Transfermarkt transfers table: one row per player move between clubs, with its fee.

    About 175,000 transfers involving the clubs the dataset covers, to the 2025-26 season (the snapshot is
    current to 2026-07-06 and not refreshed), read on every call from the release mirror. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per transfer. ``player_id``, ``from_club_id`` and
        ``to_club_id`` are Int64 Transfermarkt ids. A column holding a value of another type comes back as
        text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The original source answered 400 / 422.
        NoDataError: The original source answered 404 (the file moved).
        AssetFetchError: The original source, read when the release mirror cannot be (with a warning), failed
            (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or
            lacks a column this loader types.

    Example:
        Quick start::

            from sportsdataverse.soccer import load_soccer_transfermarkt_transfers
            moves = load_soccer_transfermarkt_transfers()

        The largest fees of one season::

            import polars as pl
            moves.filter(pl.col("transfer_season") == "25/26").sort("transfer_fee", descending=True, nulls_last=True)

    See Also:
        * `transfermarkt-datasets`_ -- the published dataset and its table documentation

    .. _transfermarkt-datasets: https://github.com/dcaribou/transfermarkt-datasets
    """
    return _load("transfers", return_as_pandas)


def load_soccer_transfermarkt_competitions(*, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Load the CC0 Transfermarkt competitions table: the leagues and cups the dataset covers.

    65 competitions: first-tier domestic leagues, domestic cups and international club competitions, read
    on every call from the release mirror. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``competition_id`` (text, e.g. ``"GB1"``), the id
        the clubs and players tables carry as ``domestic_competition_id`` /
        ``current_club_domestic_competition_id``.

    Raises:
        ValueError: The original source answered 400 / 422.
        NoDataError: The original source answered 404 (the file moved).
        AssetFetchError: The original source, read when the release mirror cannot be (with a warning), failed
            (429, 5xx, a dropped connection), returned something other than a readable gzip CSV with rows, or
            lacks a column this loader types.

    Example:
        Quick start::

            from sportsdataverse.soccer import load_soccer_transfermarkt_competitions
            comps = load_soccer_transfermarkt_competitions()

        First-tier leagues only::

            import polars as pl
            comps.filter(pl.col("sub_type") == "first_tier")

    See Also:
        * `transfermarkt-datasets`_ -- the published dataset and its table documentation

    .. _transfermarkt-datasets: https://github.com/dcaribou/transfermarkt-datasets
    """
    return _load("competitions", return_as_pandas)


def soccer_espn_transfermarkt_team_crosswalk(
    league: str | None = None, *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """ESPN soccer team id -> Transfermarkt ``club_id``, for the leagues of the ESPN <-> reep crosswalk.

    Read from the bundled ESPN <-> reep table (``soccer_espn_reep_team_crosswalk``), whose reep rows carry
    the Transfermarkt id, plus a reviewed id for a club whose reep row has none. No request is made. A
    newly promoted club can carry a ``club_id`` the Transfermarkt clubs table does not list: that snapshot
    ends in July 2026 and covers first tiers only.

    Args:
        league: An ESPN league slug of the reep crosswalk (``eng.1``, ``eng.2``, ``esp.1``, ``ger.1``,
            ``ita.1``, ``fra.1``, ``usa.1``, ``mex.1``); ``None`` (default) returns every league.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``espn_team_id`` (Utf8, unique; joins the ESPN
        soccer frames' team ids). ``club_id`` is Int64, null when no Transfermarkt id is known, and never on
        two ESPN teams.

    Raises:
        ValueError: If *league* is not one of the covered league slugs.

    Example:
        Quick start::

            from sportsdataverse.soccer import soccer_espn_transfermarkt_team_crosswalk
            tm = soccer_espn_transfermarkt_team_crosswalk(league="eng.1")

        Current Transfermarkt squads of the ESPN Premier League teams::

            from sportsdataverse.soccer import load_soccer_transfermarkt_players
            tm.join(load_soccer_transfermarkt_players(), left_on="club_id", right_on="current_club_id")

    See Also:
        * `soccer_espn_reep_team_crosswalk` -- the reep crosswalk this table is read from
        * `transfermarkt-datasets`_ -- the Transfermarkt dataset ``club_id`` belongs to

    .. _transfermarkt-datasets: https://github.com/dcaribou/transfermarkt-datasets
    """
    xw = soccer_espn_reep_team_crosswalk(league)
    assert isinstance(xw, pl.DataFrame)
    reviewed = pl.DataFrame(
        {"espn_team_id": list(_TM_REVIEWED), "_reviewed": list(_TM_REVIEWED.values())},
        schema={"espn_team_id": pl.Utf8, "_reviewed": pl.Int64},
    )
    assert xw.schema["espn_team_id"] == reviewed.schema["espn_team_id"]
    out = (
        xw.join(reviewed, on="espn_team_id", how="left")
        .with_columns(pl.col("key_transfermarkt").cast(pl.Int64, strict=True).alias("_reep"))
        .select(
            "espn_team_id",
            "espn_league",
            "espn_display_name",
            pl.coalesce("_reep", "_reviewed").alias("club_id"),
            pl.when(pl.col("_reep").is_not_null())
            .then(pl.lit("reep"))
            .when(pl.col("_reviewed").is_not_null())
            .then(pl.lit("review"))
            .alias("match_method"),
        )
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


_PLAYER_FILE = "espn_transfermarkt_player_crosswalk.csv"
#: ``espn_athlete_id`` / ``espn_team_id`` are Utf8, the dtype of the ESPN soccer roster and team frames' ids;
#: ``player_id`` is Int64, the dtype of ``load_soccer_transfermarkt_players().player_id``.
_PLAYER_SCHEMA: dict[str, pl.DataType] = {
    "espn_athlete_id": pl.Utf8,
    "espn_team_id": pl.Utf8,
    "espn_league": pl.Utf8,
    "espn_date_of_birth": pl.Date,
    "player_id": pl.Int64,
    "tm_name": pl.Utf8,
    "match_method": pl.Utf8,
}


@functools.lru_cache(maxsize=1)
def _bundled_players() -> pl.DataFrame:
    """The bundled table, read once per process (importlib.resources, then the source tree)."""
    try:
        raw = resources.files("sportsdataverse.soccer").joinpath("data", _PLAYER_FILE).read_bytes()
    except (ModuleNotFoundError, OSError):  # a source tree run before the CSV is wired into package-data
        raw = (Path(__file__).resolve().parent / "data" / _PLAYER_FILE).read_bytes()
    return pl.read_csv(io.BytesIO(raw), schema=_PLAYER_SCHEMA)


def soccer_espn_transfermarkt_player_crosswalk(
    league: str | None = None, *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """ESPN soccer athlete id -> Transfermarkt ``player_id``, for the first-tier leagues' current ESPN rosters.

    One row per athlete on the ESPN rosters (fetched 2026-11-27) of the eng.1, esp.1, ger.1, ita.1, fra.1, usa.1 and
    mex.1 teams whose Transfermarkt club is in the dataset: 4,346 athletes, 3,661 resolved (84%). The table ships
    with sdv-py; no request is made. An athlete is resolved by birth date and name against every Transfermarkt
    player: exactly one player with the same birth date and a shared name token, or, among several, the one with the
    athlete's citizenship. A 1 January birth date is too common a stand-in to count alone, so such an athlete is
    resolved only when exactly one same-date, same-name player also plays for the team's Transfermarkt club (or
    moved to it from 2025-07-01) or shares the citizenship. No player is matched to two athletes, and where the reep
    register carries both ids they agree on 99% of 2,478 pairs. Known limits: an ESPN birth date that belongs to a
    same-surname player resolves to that player (about a dozen athletes found at review), first names are not
    compared, and ESPN and Transfermarkt spell some countries differently ("USA", "Ivory Coast"), so the
    citizenship checks never fire for those athletes. The Transfermarkt data ends in July 2026, so a player who
    joined later can resolve with his previous club.

    Args:
        league: One of the seven ESPN league slugs above, to keep that league's rows; ``None`` (default) returns all.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``espn_athlete_id`` (Utf8, unique). ``player_id`` (Int64)
        joins ``load_soccer_transfermarkt_players().player_id`` and ``load_soccer_transfermarkt_player_valuations()``;
        it is null, with ``match_method == "unmatched"``, when no rule resolved the athlete.

    Raises:
        ValueError: If *league* is not one of the covered league slugs.

    Example:
        Quick start::

            from sportsdataverse.soccer import soccer_espn_transfermarkt_player_crosswalk
            xw = soccer_espn_transfermarkt_player_crosswalk()
            print(xw.shape)

        One league, as pandas::

            epl = soccer_espn_transfermarkt_player_crosswalk(league="eng.1", return_as_pandas=True)

        Market values for ESPN athletes::

            from sportsdataverse.soccer import load_soccer_transfermarkt_players
            xw = soccer_espn_transfermarkt_player_crosswalk()
            xw.join(load_soccer_transfermarkt_players(), on="player_id").select("espn_athlete_id", "market_value_in_eur")

    See Also:
        * `soccer_espn_transfermarkt_team_crosswalk` -- the team-level bridge the club check uses
        * `transfermarkt-datasets`_ -- the Transfermarkt dataset ``player_id`` belongs to

    .. _transfermarkt-datasets: https://github.com/dcaribou/transfermarkt-datasets
    """
    df = _bundled_players()
    if league is not None:
        known = sorted(set(df["espn_league"].to_list()))
        if league not in known:
            raise ValueError(f"league must be one of {known}, got {league!r}")
        df = df.filter(pl.col("espn_league") == league)
    return df.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else df.clone()
