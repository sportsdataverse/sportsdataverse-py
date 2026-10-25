"""Team and athlete steps shared by the hand-written ``espn_<lg>_game_rosters`` chains (task 2.10).

The seven Core v2 roster chains (mbb, wbb, nba, wnba, nfl, nhl, cfb) each keep their own request,
``helper_<lg>_game_items`` and ``helper_<lg>_roster_items``; their ``helper_<lg>_team_items`` and
``helper_<lg>_athlete_items`` were AST-identical apart from the team ``pop_cols``, so the fixed
bodies live here once (merging the rest is task 6.1):

* the athlete's ``position`` object keeps its fields, only its ``$ref`` is dropped, so the frame
  carries ``position_id`` / ``position_name`` / ``position_abbreviation`` as hoopR and wehoop do
  (B-M26);
* one athlete ``$ref`` that 404s drops that athlete with a warning, not the whole game (B-L5);
* one team ``pop_cols`` list for every league (B-M30 data half, B-GB-32);
* a team with one logo keeps it, and a logo shape the flattening cannot read warns instead of
  silently writing ``""`` (B-GB-19);
* every id column of the finished frame is ``Int64``, cast from the raw integer (task 5.2, B-M15).
"""

from __future__ import annotations

import warnings
from typing import Any

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64, _download_json
from sportsdataverse.dl_utils import download, normalize_team_roster_columns, underscore
from sportsdataverse.errors import NoDataError

# Every league's per-team Core v2 keys that are links or nested collections, never team fields.
# normalize_team_roster_columns keeps only the canonical team columns anyway; popping these first
# keeps json_normalize from flattening them at all. A key a league does not ship is a no-op.
TEAM_POP_COLS = (
    "$ref",
    "record",
    "athletes",
    "venue",
    "groups",
    "ranks",
    "statistics",
    "leaders",
    "links",
    "notes",
    "againstTheSpreadRecords",
    "oddsRecords",
    "franchise",
    "events",
    "college",
    "depthCharts",
    "transactions",
    "awards",
    "injuries",
    "coaches",
    "recruiting",
    "recruitings",
    "attendance",
)

# Athlete keys dropped before flattening. ``position`` is NOT here: only its ``$ref`` goes.
ATHLETE_POP_COLS = (
    "links",
    "injuries",
    "teams",
    "team",
    "college",
    "proAthlete",
    "statistics",
    "notes",
    "eventLog",
    "$ref",
)


def _logo_columns(teams_df: pl.DataFrame) -> pl.DataFrame:
    """Flatten ``logos`` into ``logo_href`` (first logo) and ``logo_dark_href`` (second), ``""`` if absent."""
    if "logos" not in teams_df.columns:
        return teams_df.with_columns(logo_href=pl.lit(""), logo_dark_href=pl.lit(""))
    try:
        return teams_df.with_columns(
            logo_href=pl.col("logos").list.get(0, null_on_oob=True).struct.field("href").fill_null(""),
            logo_dark_href=pl.col("logos").list.get(1, null_on_oob=True).struct.field("href").fill_null(""),
        ).drop("logos")
    except pl.exceptions.PolarsError as err:
        warnings.warn(
            f"ESPN team logos have a shape the roster flattening cannot read ({err!r}); "
            "logo_href and logo_dark_href are blank",
            UserWarning,
            stacklevel=3,
        )
        return teams_df.with_columns(logo_href=pl.lit(""), logo_dark_href=pl.lit("")).drop("logos")


def roster_ids(rosters: pl.DataFrame) -> pl.DataFrame:
    """Every ``id`` / ``*_id`` column of a finished game-rosters frame as ``Int64`` (task 5.2, B-M15).

    The chain read ``team_id`` / ``game_id`` as Int32, ``athlete_id`` / ``player_id`` as Int64 and the
    athlete's ``status_id`` as String; one dtype per id lets the frame join every other ESPN surface.
    """
    return _cast_ids_int64(rosters, [c for c in rosters.columns if c == "id" or c.endswith("_id")])


def team_items(items: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
    """Fetch each competitor's Core v2 team and flatten it to one ``team_*`` row per team."""
    teams_df = pl.DataFrame()
    for href in items["team_href"]:
        team = _download_json(download, href, **kwargs)
        for k in TEAM_POP_COLS:
            team.pop(k, None)
        teams_df = pl.concat([teams_df, pl.from_pandas(pd.json_normalize(team, sep="_"))], how="diagonal")
    teams_df = _logo_columns(normalize_team_roster_columns(teams_df))
    return teams_df.with_columns(team_id=pl.col("team_id").cast(pl.Int32))


def athlete_items(teams_rosters: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
    """Fetch each rostered athlete's Core v2 resource and flatten it to one row per athlete.

    Raises:
        NoDataError: every athlete ``$ref`` answered 404.
    """
    game_athletes = pl.DataFrame()
    for href in teams_rosters["athlete_href"]:
        try:
            athlete = _download_json(download, href, **kwargs)
        except NoDataError:  # a FAILED fetch (AssetFetchError) still raises
            warnings.warn(f"ESPN has no athlete resource at {href} (404); that athlete is left out", stacklevel=3)
            continue
        for k in ATHLETE_POP_COLS:
            athlete.pop(k, None)
        position = athlete.pop("position", None)
        if isinstance(position, dict):
            position = {k: v for k, v in position.items() if k != "$ref"}
        if position:
            athlete["position"] = position
        frame = pl.from_pandas(pd.json_normalize(athlete, sep="_"))
        names = [underscore(c.replace("$ref", "href")) for c in frame.columns]
        # MLB ships ``debutYear`` beside ``debut.year``: both snake-case to ``debut_year``, so the first (the
        # top-level key) is kept rather than the frame failing on a duplicate column
        keep = [i for i, n in enumerate(names) if n not in names[:i]]
        frame = frame.select([frame.columns[i] for i in keep])
        frame.columns = [names[i] for i in keep]
        game_athletes = pl.concat([game_athletes, frame], how="diagonal")
    if game_athletes.is_empty():
        raise NoDataError("NoDataError: no athlete resource found for any rostered player")
    game_athletes = game_athletes.rename(
        {
            "id": "athlete_id",
            "uid": "athlete_uid",
            "guid": "athlete_guid",
            "type": "athlete_type",
            "display_name": "athlete_display_name",
        },
    )
    game_athletes = game_athletes.with_columns(athlete_id=pl.col("athlete_id").cast(pl.Int64))
    if "position_id" in game_athletes.columns:  # hoopR / wehoop: integer
        game_athletes = game_athletes.with_columns(position_id=pl.col("position_id").cast(pl.Int32))
    return game_athletes
