from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, normalize_team_roster_columns, underscore


def espn_wnba_game_rosters(game_id: int, raw=False, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_wnba_game_rosters() - Pull the game by id.

    Args:
        game_id (int): Unique game_id, can be obtained from espn_wnba_schedule().
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars Data frame of game roster data with columns:
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
        'team_location', 'team_name', 'team_abbreviation',
        'team_display_name', 'team_short_display_name', 'team_color',
        'team_alternate_color', 'is_active', 'is_all_star',
        'logo_href', 'logo_dark_href', 'game_id'
    Example:
        Pull both teams' rosters for a single game::

            from sportsdataverse.wnba import espn_wnba_game_rosters
            rosters = espn_wnba_game_rosters(game_id=401620238)  # 2024 WNBA Finals Game 1
            print(rosters.shape)
            rosters.select(["athlete_display_name", "jersey", "team_abbreviation", "starter"]).head(10)

        Just the starters::

            import polars as pl
            rosters.filter(pl.col("starter") == True).select(["athlete_display_name", "team_abbreviation"])

        Pandas round-trip::

            rosters_pd = espn_wnba_game_rosters(game_id=401620238, return_as_pandas=True)
            rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # summary endpoint for pickcenter array
    summary_url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/wnba/events/{x}/competitions/{x}/competitors".format(
        x=game_id,
    )
    summary_resp = download(summary_url, **kwargs)
    summary = summary_resp.json()
    items = helper_wnba_game_items(summary)
    team_rosters = helper_wnba_roster_items(items=items, summary_url=summary_url, **kwargs)
    team_rosters = team_rosters.join(items[["team_id", "order", "home_away", "winner"]], how="left", on="team_id")
    teams_df = helper_wnba_team_items(items=items, **kwargs)
    teams_rosters = team_rosters.join(teams_df, how="left", on="team_id")
    athletes = helper_wnba_athlete_items(teams_rosters=team_rosters, **kwargs)
    rosters = athletes.join(teams_rosters, how="left", left_on="athlete_id", right_on="player_id")
    rosters = rosters.with_columns(game_id=pl.lit(game_id).cast(pl.Int32))
    rosters.columns = [underscore(c) for c in rosters.columns]
    return rosters.to_pandas() if return_as_pandas else rosters


def helper_wnba_game_items(summary):
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


def helper_wnba_team_items(items, **kwargs):
    pop_cols = [
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
        "franchise",
        "events",
        "college",
        "depthCharts",
        "transactions",
        "awards",
        "injuries",
        "coaches",
    ]
    teams_df = pl.DataFrame()
    for x in items["team_href"]:
        team = download(x, **kwargs).json()
        for k in pop_cols:
            team.pop(k, None)
        team_row = pl.from_pandas(pd.json_normalize(team, sep="_"))
        teams_df = pl.concat([teams_df, team_row], how="diagonal")

    teams_df = normalize_team_roster_columns(teams_df)
    if "logos" in teams_df.columns:
        try:
            teams_df = teams_df.with_columns(
                logo_href=pl.col("logos").list.get(0).struct.field("href").fill_null(""),
                logo_dark_href=pl.col("logos").list.get(1).struct.field("href").fill_null(""),
            ).drop("logos")
        except Exception:
            teams_df = teams_df.with_columns(logo_href=pl.lit(""), logo_dark_href=pl.lit("")).drop(["logos"])
    else:
        teams_df = teams_df.with_columns(logo_href=pl.lit(""), logo_dark_href=pl.lit(""))
    teams_df = teams_df.with_columns(team_id=pl.col("team_id").cast(pl.Int32))
    return teams_df


def helper_wnba_roster_items(items, summary_url, **kwargs):
    from sportsdataverse.errors import NoDataError

    team_ids = list(items["team_id"])
    game_rosters = pl.DataFrame()
    for tm in team_ids:
        team_roster_url = "{x}/{t}/roster".format(x=summary_url, t=tm)
        try:
            team_roster_resp = download(team_roster_url, **kwargs)
        except NoDataError:
            # ESPN has no roster resource for this team in this game (a 404 —
            # common for older games). Skip it so the other team's roster is
            # still recovered instead of failing the whole game.
            continue
        entries = team_roster_resp.json().get("entries", [])
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


def helper_wnba_athlete_items(teams_rosters, **kwargs):
    athlete_hrefs = list(teams_rosters["athlete_href"])
    game_athletes = pl.DataFrame()
    pop_cols = [
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
        "position",
    ]
    for athlete_href in athlete_hrefs:
        athlete_res = download(athlete_href, **kwargs)
        athlete_resp = athlete_res.json()
        for k in pop_cols:
            athlete_resp.pop(k, None)
        athlete = pl.from_pandas(pd.json_normalize(athlete_resp, sep="_"))
        athlete.columns = [col.replace("$ref", "href") for col in athlete.columns]
        athlete.columns = [underscore(c) for c in athlete.columns]

        game_athletes = pl.concat([game_athletes, athlete], how="diagonal")

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
    return game_athletes


def helper_wnba_game_rosters(payload: dict, *, season: int, game_id: int | str) -> pl.DataFrame:
    """Parse one game's rosters sidecar into the released game-rosters frame.

    Faithful polars port of the script-local parsers in
    ``wehoop-wnba-data/R/espn_wnba_09_game_rosters_creation.R`` --
    byte-identical to the WBB parser after league normalization, so this
    delegates to the shared implementation (NOT the live
    ``espn_wnba_game_rosters`` pipeline above; this one consumes the stored
    ``wnba/game_rosters/json/{game_id}.json`` sidecar). The R-released
    ``espn_wnba_game_rosters`` parquet is the parity oracle.

    Args:
        payload: One game's ``wnba/game_rosters/json/{game_id}.json`` as a dict.
        season: Season year the sidecar belongs to.
        game_id: ESPN game id the sidecar belongs to (released dtype String).

    Returns:
        pl.DataFrame: One row per rostered athlete; empty (zero-column) frame
        for degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_game_rosters
            payload = json.load(open("401736126.json", encoding="utf-8"))
            df = helper_wnba_game_rosters(payload, season=2025, game_id=401736126)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    from sportsdataverse.wbb.wbb_game_rosters import helper_wbb_game_rosters

    return helper_wbb_game_rosters(payload, season=season, game_id=game_id)
