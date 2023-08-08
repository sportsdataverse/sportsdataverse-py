import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore


def espn_wbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_wbb_game_rosters() - Pull the game by id.

    Args:
        game_id (int): Unique game_id, can be obtained from wbb_schedule().
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
    Example:
        `wbb_df = sportsdataverse.wbb.espn_wbb_game_rosters(game_id=401266534)`
    """
    # summary endpoint for pickcenter array
    summary_url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/{x}/competitions/{x}/competitors".format(
        x=game_id
    )
    summary_resp = download(summary_url, **kwargs)
    summary = summary_resp.json()
    items = helper_wbb_game_items(summary)
    team_rosters = helper_wbb_roster_items(items=items, summary_url=summary_url, **kwargs)
    team_rosters = team_rosters.join(items[["team_id", "order", "home_away", "winner"]], how="left", on="team_id")
    teams_df = helper_wbb_team_items(items=items, **kwargs)
    teams_rosters = team_rosters.join(teams_df, how="left", on="team_id")
    athletes = helper_wbb_athlete_items(teams_rosters=team_rosters, **kwargs)
    rosters = athletes.join(teams_rosters, how="left", left_on="athlete_id", right_on="player_id")
    rosters = rosters.with_columns(game_id=pl.lit(game_id).cast(pl.Int32))
    rosters.columns = [underscore(c) for c in rosters.columns]
    return rosters.to_pandas() if return_as_pandas else rosters


def helper_wbb_game_items(summary):
    items = pl.from_pandas(pd.json_normalize(summary, record_path="items", sep="_"))
    items.columns = [col.replace("$ref", "href") for col in items.columns]

    items.columns = [underscore(c) for c in items.columns]
    items = items.rename({"id": "team_id", "uid": "team_uid", "statistics_href": "team_statistics_href"})
    items = items.with_columns(team_id=pl.col("team_id").cast(pl.Int32))

    return items


def helper_wbb_team_items(items, **kwargs):
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
    ]
    teams_df = pl.DataFrame()
    for x in items["team_href"]:
        team = download(x, **kwargs).json()
        for k in pop_cols:
            team.pop(k, None)
        team_row = pl.from_pandas(pd.json_normalize(team, sep="_"))
        teams_df = pl.concat([teams_df, team_row], how="vertical")

    teams_df.columns = [
        "team_id",
        "team_guid",
        "team_uid",
        "team_slug",
        "team_location",
        "team_name",
        "team_nickname",
        "team_abbreviation",
        "team_display_name",
        "team_short_display_name",
        "team_color",
        "team_alternate_color",
        "is_active",
        "is_all_star",
        "logos",
        "team_alternate_ids_sdr",
    ]
    teams_df = teams_df.with_columns(logo_href=pl.lit(""), logo_dark_href=pl.lit(""))

    for row in range(len(teams_df["logos"])):
        team = teams_df["logos"][row]
        teams_df[row, "logo_href"] = team[0]["href"]
        teams_df[row, "logo_dark_href"] = team[1]["href"]

    teams_df = teams_df.drop(["logos"])
    teams_df = teams_df.with_columns(team_id=pl.col("team_id").cast(pl.Int32))
    return teams_df


def helper_wbb_roster_items(items, summary_url, **kwargs):
    team_ids = list(items["team_id"])
    game_rosters = pl.DataFrame()
    for tm in team_ids:
        team_roster_url = "{x}/{t}/roster".format(x=summary_url, t=tm)
        team_roster_resp = download(team_roster_url, **kwargs)
        team_roster = pl.from_pandas(pd.json_normalize(team_roster_resp.json().get("entries", []), sep="_"))
        team_roster.columns = [col.replace("$ref", "href") for col in team_roster.columns]
        team_roster.columns = [underscore(c) for c in team_roster.columns]
        team_roster = team_roster.with_columns(team_id=pl.lit(tm).cast(pl.Int32))
        game_rosters = pl.concat([game_rosters, team_roster], how="vertical")
    game_rosters = game_rosters.drop(["period", "for_player_id", "active"])
    game_rosters = game_rosters.with_columns(
        player_id=pl.col("player_id").cast(pl.Int64), team_id=pl.col("team_id").cast(pl.Int32)
    )
    return game_rosters


def helper_wbb_athlete_items(teams_rosters, **kwargs):
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
        }
    )
    game_athletes = game_athletes.with_columns(athlete_id=pl.col("athlete_id").cast(pl.Int64))
    return game_athletes
