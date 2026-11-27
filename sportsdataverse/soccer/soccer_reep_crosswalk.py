"""ESPN soccer team id -> reep team id crosswalk, with reep's ids at about 20 other providers.

reep is a CC0 football id register: one row per club with its Wikidata, Transfermarkt, FBref, Opta,
FotMob (and more) ids. It carries ESPN ids for only a couple of dozen clubs, so this table
bridges the rest by name, matched only inside the league's country, and settles every ambiguous or
unmatched name by hand (``tools/soccer_espn_reep_overrides.yaml``). The table is built by
``tools/build_soccer_espn_reep_crosswalk.py`` and bundled with sdv-py:

- ``sportsdataverse/soccer/data/espn_reep_team_crosswalk.csv``

The build fails unless at least 92% of the ESPN teams match overall and 75% in every league, no ESPN
id appears twice and no reep row is claimed by two ESPN teams.
"""

from __future__ import annotations

import functools
import io
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = ["soccer_espn_reep_team_crosswalk"]

_FILE = "espn_reep_team_crosswalk.csv"

#: Every column is text. ``espn_team_id`` is Utf8 because every ESPN soccer frame in sdv-py carries its
#: team ids as Utf8 (``espn_soccer_teams_site().team_id``, the scoreboard and summary team ids), so it
#: joins them schema-equal. reep's ids are opaque text at several providers (Opta, FBref, Understat).
_SCHEMA: dict[str, pl.DataType] = dict.fromkeys(
    (
        "espn_team_id",
        "espn_league",
        "espn_display_name",
        "espn_abbreviation",
        "reep_id",
        "reep_name",
        "reep_country",
        "key_wikidata",
        "key_transfermarkt",
        "key_fbref",
        "key_soccerway",
        "key_opta",
        "key_opta_numeric",
        "key_kicker",
        "key_flashscore",
        "key_sofascore",
        "key_soccerbase",
        "key_uefa",
        "key_footballdatabase_eu",
        "key_worldfootball",
        "key_playmakerstats",
        "key_clubelo",
        "key_sportmonks",
        "key_api_football",
        "key_sofifa",
        "key_fotmob",
        "key_thesportsdb",
        "key_understat",
        "key_capology",
        "match_method",
    ),
    pl.Utf8,
)


@functools.lru_cache(maxsize=1)
def _bundled() -> pl.DataFrame:
    """The bundled table, read once per process (importlib.resources, then the source tree)."""
    try:
        raw = resources.files("sportsdataverse.soccer").joinpath("data", _FILE).read_bytes()
    except (ModuleNotFoundError, OSError):  # a source tree run before the CSV is wired into package-data
        raw = (Path(__file__).resolve().parent / "data" / _FILE).read_bytes()
    return pl.read_csv(io.BytesIO(raw), schema=_SCHEMA)


def soccer_espn_reep_team_crosswalk(
    league: str | None = None, *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """ESPN soccer team id -> reep team id, with reep's ids for the same club at about 20 providers.

    One row per ESPN team in the leagues the table covers: ``eng.1``, ``eng.2``, ``esp.1``, ``ger.1``,
    ``ita.1``, ``fra.1``, ``usa.1`` and ``mex.1`` (the clubs ESPN listed when the table was built,
    2026-11-26). The table ships with sdv-py; no request is made. reep data is CC0 1.0 (release
    2026.25). NWSL is not covered: reep has no row for 5 of its 16 clubs, below the 75% per-league floor.

    Args:
        league: An ESPN league slug from the list above, to keep that league's rows; ``None`` (default)
            returns every league.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ``espn_team_id`` (unique). Every column is text
        (Utf8): ``espn_team_id`` joins ``espn_soccer_teams_site().team_id`` and the ESPN soccer
        scoreboard and summary team ids as it is. The reep columns are null when no reep row is that
        club (``match_method == "unmatched"``).

    Raises:
        ValueError: If *league* is not one of the covered league slugs.

    Example:
        Quick start::

            from sportsdataverse.soccer import soccer_espn_reep_team_crosswalk
            xw = soccer_espn_reep_team_crosswalk()
            print(xw.shape)

        One league, as pandas::

            epl = soccer_espn_reep_team_crosswalk(league="eng.1", return_as_pandas=True)

        Pipeline next step (one line)::

            xw.select("espn_team_id", "key_transfermarkt", "key_fbref").drop_nulls("key_transfermarkt")

    See Also:
        * `reep`_ -- the CC0 football id register the reep columns come from
        * `espn_soccer_teams_site` -- the ESPN teams frame whose ``team_id`` this table keys on

    .. _reep: https://github.com/withqwerty/reep
    """
    df = _bundled()
    if league is not None:
        known = sorted(set(df["espn_league"].to_list()))
        if league not in known:
            raise ValueError(f"league must be one of {known}, got {league!r}")
        df = df.filter(pl.col("espn_league") == league)
    return df.to_pandas() if return_as_pandas else df.clone()  # never hand out the cached frame
