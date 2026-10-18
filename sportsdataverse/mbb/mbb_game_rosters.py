from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse import _espn_game_rosters
from sportsdataverse._codegen_runtime import _download_json
from sportsdataverse.dl_utils import (  # noqa: F401 -- normalize_team_roster_columns leaks via `import *` (coverage_allowlist.yaml)
    download,
    normalize_team_roster_columns,
    underscore,
)


def espn_mbb_game_rosters(
    game_id: int, raw=False, return_as_pandas=False, **kwargs
) -> pl.DataFrame | pd.DataFrame | dict:
    """espn_mbb_game_rosters() - Pull the game by id.

    Args:
        game_id (int): Unique game_id, can be obtained from mbb_schedule().
        raw (bool): If True, returns the Core v2 competitors payload (dict) as ESPN sent it;
            its roster, team and athlete ``$ref`` links are not followed.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of game roster data with columns:
        'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type',
        'first_name', 'last_name', 'full_name', 'athlete_display_name',
        'short_name', 'weight', 'display_weight', 'height', 'display_height',
        'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active',
        'alternate_ids_sdr', 'birth_place_city', 'birth_place_state',
        'birth_place_country', 'headshot_href', 'headshot_alt',
        'experience_years', 'experience_display_value',
        'experience_abbreviation', 'status_id', 'status_name', 'status_type',
        'status_abbreviation', 'hand_type', 'hand_abbreviation',
        'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year',
        'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play',
        'display_name', 'ejected', 'athlete_href', 'position_href',
        'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug',
        'team_location', 'team_name', 'team_nickname', 'team_abbreviation',
        'team_display_name', 'team_short_display_name', 'team_color',
        'team_alternate_color', 'is_active', 'is_all_star',
        'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'
        plus the athlete's ``position_id``, ``position_name``, ``position_display_name``,
        ``position_abbreviation`` and ``position_leaf``. With ``raw=True``, the competitors
        payload (dict).

    Example:
        Quick start (2024 NCAA M championship game)::

            from sportsdataverse.mbb import espn_mbb_game_rosters
            roster = espn_mbb_game_rosters(game_id=401638637)
            print(roster.shape)

        Identify starters::

            import polars as pl
            starters = roster.filter(pl.col("starter") == True).select(
                ["full_name", "jersey", "team_display_name"]
            )

        Pandas round-trip::

            roster_pd = espn_mbb_game_rosters(game_id=401638637, return_as_pandas=True)
            roster_pd.head()

        See Also:
            * `hoopR`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    # summary endpoint for pickcenter array
    summary_url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/events/{x}/competitions/{x}/competitors".format(
        x=game_id,
    )
    summary = _download_json(download, summary_url, **kwargs)
    if raw:
        return summary
    items = helper_mbb_game_items(summary)
    team_rosters = helper_mbb_roster_items(items=items, summary_url=summary_url, **kwargs)
    team_rosters = team_rosters.join(items[["team_id", "order", "home_away", "winner"]], how="left", on="team_id")
    teams_df = helper_mbb_team_items(items=items, **kwargs)
    teams_rosters = team_rosters.join(teams_df, how="left", on="team_id")
    athletes = helper_mbb_athlete_items(teams_rosters=team_rosters, **kwargs)
    rosters = athletes.join(teams_rosters, how="left", left_on="athlete_id", right_on="player_id")
    rosters = rosters.with_columns(game_id=pl.lit(game_id).cast(pl.Int32))
    rosters.columns = [underscore(c) for c in rosters.columns]
    return rosters.to_pandas() if return_as_pandas else rosters


def helper_mbb_game_items(summary):
    items = pl.from_pandas(pd.json_normalize(summary, record_path="items", sep="_"))
    items.columns = [col.replace("$ref", "href") for col in items.columns]

    items.columns = [underscore(c) for c in items.columns]
    # Older games (e.g. pre-2021) sometimes omit the team-level ``statistics``
    # ``$ref`` in the competitors payload, so ``statistics_href`` is absent and a
    # strict rename raises ColumnNotFoundError (the renamed ``team_statistics_href``
    # is not used downstream). Rename only the keys actually present.
    rename_map = {"id": "team_id", "uid": "team_uid", "statistics_href": "team_statistics_href"}
    items = items.rename({k: v for k, v in rename_map.items() if k in items.columns})
    items = items.with_columns(team_id=pl.col("team_id").cast(pl.Int32))

    return items


def helper_mbb_team_items(items, **kwargs):
    return _espn_game_rosters.team_items(items, **kwargs)


def helper_mbb_roster_items(items, summary_url, **kwargs):
    from sportsdataverse.errors import NoDataError

    team_ids = list(items["team_id"])
    game_rosters = pl.DataFrame()
    for tm in team_ids:
        team_roster_url = "{x}/{t}/roster".format(x=summary_url, t=tm)
        try:
            entries = _download_json(download, team_roster_url, **kwargs).get("entries", [])
        except NoDataError:
            # ESPN has no roster resource for this team in this game (a 404 —
            # common for older games and non-D1 opponents). Skip it so the other
            # team's roster is still recovered instead of failing the whole game.
            continue
        if not entries:
            continue
        team_roster = pl.from_pandas(pd.json_normalize(entries, sep="_"))
        team_roster.columns = [col.replace("$ref", "href") for col in team_roster.columns]
        team_roster.columns = [underscore(c) for c in team_roster.columns]
        team_roster = team_roster.with_columns(team_id=pl.lit(tm).cast(pl.Int32))
        game_rosters = pl.concat([game_rosters, team_roster], how="diagonal")
    if game_rosters.is_empty():
        # No team in this game exposes a roster resource — genuinely no data.
        raise NoDataError(f"NoDataError: No roster data found for any team at {summary_url}")
    game_rosters = game_rosters.drop([c for c in ["period", "for_player_id", "active"] if c in game_rosters.columns])
    game_rosters = game_rosters.with_columns(
        player_id=pl.col("player_id").cast(pl.Int64),
        team_id=pl.col("team_id").cast(pl.Int32),
    )
    return game_rosters


def helper_mbb_athlete_items(teams_rosters, **kwargs):
    return _espn_game_rosters.athlete_items(teams_rosters, **kwargs)


def helper_mbb_game_rosters(payload: dict, *, season: int, game_id: int | str) -> pl.DataFrame:
    """Parse one game's rosters sidecar into the released game-rosters frame.

    Faithful polars port of the script-local parsers in
    ``hoopR-mbb-data/R/espn_mbb_09_game_rosters_creation.R`` -- CONFIG-only
    (numbering/tag/name) diffs vs the WBB parser, so this delegates to the
    shared implementation (NOT the live ``espn_mbb_game_rosters`` pipeline
    above; this one consumes the stored ``mbb/game_rosters/json/{game_id}.json``
    sidecar). The R-released ``espn_mens_college_basketball_game_rosters``
    parquet is the parity oracle.

    Args:
        payload: One game's ``mbb/game_rosters/json/{game_id}.json`` as a dict.
        season: Season year the sidecar belongs to.
        game_id: ESPN game id the sidecar belongs to (released dtype String).

    Returns:
        pl.DataFrame: One row per rostered athlete; empty (zero-column) frame
        for degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_game_rosters
            payload = json.load(open("401746082.json", encoding="utf-8"))
            df = helper_mbb_game_rosters(payload, season=2025, game_id=401746082)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse.wbb.wbb_game_rosters import helper_wbb_game_rosters

    return helper_wbb_game_rosters(payload, season=season, game_id=game_id)
