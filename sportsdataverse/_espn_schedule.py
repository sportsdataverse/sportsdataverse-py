"""Frame assembly shared by the hand-written ``espn_<lg>_schedule`` functions (task 2.9).

Each league module keeps its own request and its own ``scoreboard_event_parsing``; this module
owns what happens to the events afterwards, so the contract is stated once:

* an empty slate returns a zero-row frame carrying the documented columns (B-H2, B-M12);
* one malformed event is dropped and the rest of the day kept (B-M24);
* the column set is the documented one on every day, never the set the day happened to fill
  (B-M16 shape half); a column ESPN adds later is appended after the documented ones;
* ``Postponed`` and ``Canceled`` games carry null linescores (B-GB-21);
* the caller's payload is never mutated, so it can be parsed twice (B-L2);
* every ESPN id column is ``Int64``, cast from the raw integer (task 5.2, B-M15).

The column lists mirror ``tools/codegen/schemas/autodoc/<lg>/espn_<lg>_schedule.yaml``; the
offline schedule tests assert the two agree.
"""

from __future__ import annotations

import copy
import warnings
from collections.abc import Callable
from typing import Any

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64
from sportsdataverse.dl_utils import underscore

_LINESCORES = pl.List(pl.Struct({"value": pl.Float64, "displayValue": pl.String, "period": pl.Int64}))
_RECORDS = pl.List(pl.Struct({"name": pl.String, "abbreviation": pl.String, "type": pl.String, "summary": pl.String}))
# Columns whose documented type is a fold of a nested list (the autodoc mapper); their real dtype.
_NESTED = {
    "home_linescores": _LINESCORES,
    "away_linescores": _LINESCORES,
    "home_records": _RECORDS,
    "away_records": _RECORDS,
    "highlights": pl.List(pl.Null),
    "status_featured_athletes": pl.List(pl.Null),
}
_INT32 = {"season", "season_type", "week"}  # cast explicitly by the parser
_DOC_DTYPES = {"character": pl.String, "logical": pl.Boolean, "double": pl.Float64, "integer": pl.Int64}

_C, _L, _D, _I = "character", "logical", "double", "integer"

_TEAM = [
    ("id", _I),
    ("uid", _C),
    ("location", _C),
    ("name", _C),
    ("abbreviation", _C),
    ("display_name", _C),
    ("short_display_name", _C),
    ("color", _C),
    ("alternate_color", _C),
    ("is_active", _L),
    ("venue_id", _I),
    ("logo", _C),
    ("score", _C),
    ("linescores", "list"),
    ("records", _C),
]

#: The 67 columns all seven ESPN-scoreboard schedules document, in documented order.
_BASE: list[tuple] = [
    ("id", _I),
    ("uid", _C),
    ("date", _C),
    ("attendance", _I),
    ("time_valid", _L),
    ("neutral_site", _L),
    ("play_by_play_available", _L),
    ("recent", _L),
    ("start_date", _C),
    ("broadcast", _C),
    ("notes_type", _C),
    ("notes_headline", _C),
    ("broadcast_market", _C),
    ("broadcast_name", _C),
    ("type_id", _I),
    ("type_abbreviation", _C),
    ("venue_id", _I),
    ("venue_full_name", _C),
    ("venue_address_city", _C),
    ("venue_address_state", _C),
    ("venue_indoor", _L),
    ("status_clock", _D),
    ("status_display_clock", _C),
    ("status_period", _I),
    ("status_type_id", _I),
    ("status_type_name", _C),
    ("status_type_state", _C),
    ("status_type_completed", _L),
    ("status_type_description", _C),
    ("status_type_detail", _C),
    ("status_type_short_detail", _C),
    ("format_regulation_periods", _I),
    *[(f"home_{n}", t) for n, t in _TEAM],
    *[(f"away_{n}", t) for n, t in _TEAM],
    ("game_id", _I),
    ("season", _I),
    ("season_type", _I),
    ("home_logo_dark", _C),
    ("away_logo_dark", _C),
]

_WINNER = [("home_winner", _L), ("away_winner", _L)]
_RANK = [("home_current_rank", _I), ("away_current_rank", _I)]
_COLLEGE_BB = [
    ("conference_competition", _L),
    ("tournament_id", _I),
    ("highlights", _C),
    ("home_conference_id", _I),
    *_WINNER,
    ("away_conference_id", _I),
    *_RANK,
]
_EXTRA: dict[str, list[tuple]] = {
    "mbb": _COLLEGE_BB,
    "wbb": _COLLEGE_BB,
    "nba": [("conference_competition", _L), ("highlights", _I), *_WINNER],
    "wnba": [("conference_competition", _L), ("highlights", _C), *_WINNER, ("status_type_alt_detail", _C)],
    "nfl": [
        ("conference_competition", _L),
        ("highlights", _C),
        ("venue_address_country", _C),
        ("status_is_tbd_flex", _L),
        *_RANK,
        ("week", _I),
        ("format_overtime_periods", _I),
        *_WINNER,
    ],
    "nhl": [
        ("highlights", _I),
        ("venue_address_country", _C),
        ("status_featured_athletes", _I),
        *_WINNER,
        ("status_type_alt_detail", _C),
    ],
    "cfb": [
        ("date_valid", _L),
        ("conference_competition", _L),
        ("highlights", _I),
        ("venue_address_country", _C),
        ("groups_id", _I),
        ("groups_name", _C),
        ("groups_short_name", _C),
        ("groups_is_conference", _L),
        ("home_conference_id", _I),
        *_RANK,
        ("away_conference_id", _I),
        ("week", _I),
        *_WINNER,
        ("status_type_alt_detail", _C),
    ],
}

#: ``espn_mlb_schedule`` builds its own fixed 23-key row; only its empty frame comes from here.
_MLB: list[tuple] = [
    ("game_id", _I),
    ("date", _C),
    ("season_year", _I),
    ("season_type", _I),
    ("status_type_state", _C),
    ("status_type_completed", _L),
    ("status_type_description", _C),
    ("venue_id", _I),
    ("venue_full_name", _C),
    ("venue_city", _C),
    ("venue_state", _C),
    *[
        (f"{side}_{n}", t)
        for side in ("home", "away")
        for n, t in (
            ("id", _I),
            ("name", _C),
            ("abbreviation", _C),
            ("display_name", _C),
            ("score", _C),
            ("winner", _L),
        )
    ],
]


def _dtype(name: str, doc_type: str, league: str) -> pl.DataType:
    if name in _NESTED:
        return _NESTED[name]
    if name in _INT32 and league != "mlb":
        return pl.Int32()
    return _DOC_DTYPES[doc_type]()


def _schedule_schema(league: str) -> dict[str, pl.DataType]:
    """The documented ``espn_<league>_schedule`` columns and the dtype an empty frame gives each."""
    cols = _MLB if league == "mlb" else _BASE + _EXTRA[league]
    return {name: _dtype(name, doc_type, league) for name, doc_type in cols}


def _empty_schedule(league: str, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """A zero-row schedule frame carrying the documented columns."""
    df = pl.DataFrame(schema=_schedule_schema(league))
    return df.to_pandas() if return_as_pandas else df


def _schedule_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Every ``id`` / ``*_id`` column as ``Int64``, from the raw integer (task 5.2, B-M15)."""
    return _cast_ids_int64(df, [c for c in df.columns if c == "id" or c.endswith("_id")])


def _competition(event: Any, parse_event: Callable[[dict], dict], league: str, week: bool) -> dict[str, Any] | None:
    """One event's flattened ``competitions[0]`` row, or ``None`` when the event has no game.

    ESPN ships placeholder events (``{}``, null or one-sided ``competitors``; the 2010/2014 CFB
    scoreboards) that carry no game, so they are skipped quietly. A game the league parser cannot
    read is skipped with a warning naming it: the rest of the day survives either way.
    """
    if not isinstance(event, dict):
        return None
    comps = event.get("competitions") or []
    if not comps or not isinstance(comps[0], dict) or len(comps[0].get("competitors") or []) < 2:
        return None
    event = copy.deepcopy(event)  # the league parsers pop keys; never touch the caller's payload
    comp = event["competitions"][0]
    for key in ("notes", "broadcasts"):
        if not isinstance(comp.get(key), list):
            comp[key] = []
    try:
        comp = parse_event(event)["competitions"][0]
    except (AttributeError, IndexError, KeyError, TypeError) as exc:
        warnings.warn(
            f"espn_{league}_schedule: skipped event {event.get('id')!r} the parser could not read ({exc!r})",
            UserWarning,
            stacklevel=4,
        )
        return None
    season = event.get("season") or {}
    comp["season"] = season.get("year")
    comp["season_type"] = season.get("type")
    if week:
        comp["week"] = (event.get("week") or {}).get("number")
    return comp


def _schedule_frame(
    events: list[Any] | None,
    parse_event: Callable[[dict], dict],
    league: str,
    *,
    week: bool = False,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Turn a scoreboard ``events`` list into the documented ``espn_<league>_schedule`` frame.

    Args:
        events: The scoreboard payload's ``events`` (``None`` or empty -> the empty frame).
        parse_event: The league module's ``scoreboard_event_parsing``.
        league: The league prefix (``mbb``, ``wbb``, ``nba``, ``wnba``, ``nfl``, ``nhl``, ``cfb``).
        week: Whether the league carries ``week`` (football).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The documented columns in documented order (absent ones as typed nulls), then any
        column ESPN added since; zero rows when no event carries a game.
    """
    rows = [r for r in (_competition(e, parse_event, league, week) for e in events or []) if r is not None]
    if not rows:
        return _empty_schedule(league, return_as_pandas)
    df = pl.from_pandas(pd.json_normalize(rows, sep="_"))
    df = df.rename({c: underscore(c) for c in df.columns})
    masked = pl.col("status_type_description").is_in(["Postponed", "Canceled"])
    df = _schedule_ids(df).with_columns(
        game_id=pl.col("id"),
        **{c: pl.col(c).cast(pl.Int32) for c in ("season", "season_type", "week") if c in df.columns},
        **{
            c: pl.when(masked).then(None).otherwise(pl.col(c))
            for c in ("home_linescores", "away_linescores")
            if c in df.columns
        },
    )
    schema = _schedule_schema(league)
    fixed = []
    for name, dtype in schema.items():
        if name not in df.columns or df[name].null_count() == df.height:  # all-null: documented dtype
            fixed.append(pl.lit(None, dtype=dtype).alias(name))
        elif dtype == pl.Int64 and df.schema[name].is_float():
            # a documented integer that a missing value turned Float64 on the pandas hop
            fixed.append(pl.col(name).cast(pl.Int64))
        else:
            fixed.append(pl.col(name))
    df = df.select(*fixed, *[c for c in df.columns if c not in schema])
    return df.to_pandas() if return_as_pandas else df
