"""Transfermarkt clubs, players, market values, transfers and competitions, plus an ESPN team-id bridge.

The tables are the CC0 1.0 Transfermarkt dataset, one gzip CSV per table. It is a snapshot current to
2026-07-06 and is not being refreshed, so 2026-27 squads, transfers and values are absent. Each loader
reads its published file on every call and types the ids at this boundary: ``player_id``, ``club_id`` and
every ``*_club_id`` are the Int64 Transfermarkt ids; competition ids such as ``"GB1"`` stay text.

:func:`soccer_espn_transfermarkt_team_crosswalk` maps ESPN soccer team ids to Transfermarkt ``club_id``
from the bundled ESPN <-> reep crosswalk (whose reep rows carry the Transfermarkt id for 166 of its 168
ESPN teams) plus a reviewed id for a club whose reep row has none. No request is made.
"""

from __future__ import annotations

import gzip
import io
import warnings
import zlib
from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._codegen_runtime import _check_response, _transport_errors
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, SchemaDriftWarning
from sportsdataverse.soccer.soccer_reep_crosswalk import soccer_espn_reep_team_crosswalk

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "load_soccer_transfermarkt_clubs",
    "load_soccer_transfermarkt_competitions",
    "load_soccer_transfermarkt_player_valuations",
    "load_soccer_transfermarkt_players",
    "load_soccer_transfermarkt_transfers",
    "soccer_espn_transfermarkt_team_crosswalk",
]

TRANSFERMARKT_DATA_URL = "https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/{table}.csv.gz"

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
        return pl.read_csv(io.BytesIO(gzip.decompress(body)), infer_schema=False)
    # a truncated or corrupt gzip, an empty file, a ragged CSV: the fetch failed, it did not answer "nothing here"
    except (EOFError, OSError, zlib.error, pl.exceptions.PolarsError) as exc:
        raise AssetFetchError(f"{url}: the body is not a readable gzip CSV ({type(exc).__name__}: {exc})") from exc


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


def _load(table: str, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    out = _typed(_fetch(table), table)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def load_soccer_transfermarkt_players(*, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Load the CC0 Transfermarkt players table: one row per player, with current club and market value.

    About 50,000 players: everyone who appeared for a club of the first-tier leagues the dataset covers
    (Premier League, LaLiga, Bundesliga, Serie A, Ligue 1, MLS, Liga MX and about 20 more). A snapshot
    current to 2026-07-06 (not refreshed), read from its published file on every call. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``player_id`` (Int64). ``current_club_id`` (Int64)
        joins ``club_id`` of ``load_soccer_transfermarkt_clubs`` and of
        ``soccer_espn_transfermarkt_team_crosswalk``. A column holding a value of another type comes back as
        text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The host answered 400 / 422.
        NoDataError: The host answered 404 (the file moved).
        AssetFetchError: The fetch failed (429, 5xx, a dropped connection), returned something other than a
            readable gzip CSV, or the file lacks a column this loader types.

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
    A snapshot current to 2026-07-06 (not refreshed), read from its published file on every call.
    Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``club_id`` (Int64), the id
        ``soccer_espn_transfermarkt_team_crosswalk`` maps ESPN teams to. A column holding a value of another
        type comes back as text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The host answered 400 / 422.
        NoDataError: The host answered 404 (the file moved).
        AssetFetchError: The fetch failed (429, 5xx, a dropped connection), returned something other than a
            readable gzip CSV, or the file lacks a column this loader types.

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

    About 656,000 valuations, from 2000 to 2026-06-12 (the snapshot is not refreshed), read from the
    published file on every call. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per (``player_id``, ``date``); ``player_id`` and
        ``current_club_id`` are Int64 and join the players and clubs tables. A column holding a value of
        another type comes back as text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The host answered 400 / 422.
        NoDataError: The host answered 404 (the file moved).
        AssetFetchError: The fetch failed (429, 5xx, a dropped connection), returned something other than a
            readable gzip CSV, or the file lacks a column this loader types.

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
    current to 2026-07-06 and not refreshed), read from the published file on every call. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per transfer. ``player_id``, ``from_club_id`` and
        ``to_club_id`` are Int64 Transfermarkt ids. A column holding a value of another type comes back as
        text with a ``SchemaDriftWarning``.

    Raises:
        ValueError: The host answered 400 / 422.
        NoDataError: The host answered 404 (the file moved).
        AssetFetchError: The fetch failed (429, 5xx, a dropped connection), returned something other than a
            readable gzip CSV, or the file lacks a column this loader types.

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
    from the published file on every call. Licence: CC0 1.0.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``competition_id`` (text, e.g. ``"GB1"``), the id
        the clubs and players tables carry as ``domestic_competition_id`` /
        ``current_club_domestic_competition_id``.

    Raises:
        ValueError: The host answered 400 / 422.
        NoDataError: The host answered 404 (the file moved).
        AssetFetchError: The fetch failed (429, 5xx, a dropped connection), returned something other than a
            readable gzip CSV, or the file lacks a column this loader types.

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
