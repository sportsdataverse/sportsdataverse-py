import pandas as pd
import numpy as np
import os
import json
import re
from typing import List, Callable, Iterator, Union, Optional, Dict
from sportsdataverse.dl_utils import download, underscore

def espn_mbb_game_rosters(game_id: int, raw = False) -> pd.DataFrame:
    """espn_mbb_game_rosters() - Pull the game by id.

    Args:
        game_id (int): Unique game_id, can be obtained from mbb_schedule().

    Returns:
        pd.DataFrame: Data frame of game roster data with columns:
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
        `mbb_df = sportsdataverse.mbb.espn_mbb_game_rosters(game_id=401265031)`
    """
    # play by play
    pbp_txt = {}
    # summary endpoint for pickcenter array
    summary_url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/events/{x}/competitions/{x}/competitors".format(x=game_id)
    summary_resp = download(summary_url)
    summary = json.loads(summary_resp)
    items = helper_mbb_game_items(summary)
    team_rosters = helper_mbb_roster_items(items = items, summary_url = summary_url)
    team_rosters = team_rosters.merge(items[['team_id', 'order', 'home_away', 'winner']], how = 'left', on = 'team_id')
    teams_df = helper_mbb_team_items(items = items)
    teams_rosters = team_rosters.merge(teams_df, how = 'left',on = 'team_id')
    athletes = helper_mbb_athlete_items(teams_rosters = team_rosters)
    rosters = athletes.merge(teams_rosters, how = 'left', left_on = 'athlete_id', right_on = 'player_id')
    rosters['game_id'] = int(game_id)
    rosters.columns = [underscore(c) for c in rosters.columns.tolist()]
    return rosters

def helper_mbb_game_items(summary):
    items = pd.json_normalize(summary, record_path = "items", sep = '_')
    items.columns = [col.replace("$ref", "href") for col in items.columns]

    items.columns = [underscore(c) for c in items.columns.tolist()]
    items = items.rename(
        columns = {
            "id": "team_id",
            "uid": "team_uid",
            "statistics_href": "team_statistics_href"
        }
      )
    items['team_id'] = items['team_id'].astype(int)

    return items

def helper_mbb_team_items(items):
    pop_cols = [
        '$ref',
        'record',
        'athletes',
        'venue',
        'groups',
        'ranks',
        'statistics',
        'leaders',
        'links',
        'notes',
        'againstTheSpreadRecords',
        'franchise',
        'events',
        'college'
    ]
    teams_df = pd.DataFrame()
    for x in items['team_href']:
        team = json.loads(download(x))
        for k in pop_cols:
            team.pop(k, None)
        team_row = pd.json_normalize(team, sep = '_')
        teams_df = pd.concat([teams_df, team_row], axis = 0, ignore_index = True)

    teams_df.columns = [
        'team_id',
        'team_guid',
        'team_uid',
        'team_slug',
        'team_location',
        'team_name',
        'team_nickname',
        'team_abbreviation',
        'team_display_name',
        'team_short_display_name',
        'team_color',
        'team_alternate_color',
        'is_active',
        'is_all_star',
        'logos',
        'team_alternate_ids_sdr'
    ]

    teams_df['logos'][0]
    for row in range(len(teams_df['logos'])):
        team = teams_df['logos'][row]
        teams_df.loc[row, 'logo_href'] = team[0]['href']
        teams_df.loc[row, 'logo_dark_href'] = team[1]['href']
    teams_df = teams_df.drop(['logos'], axis = 1)
    teams_df['team_id'] = teams_df['team_id'].astype(int)
    return teams_df

def helper_mbb_roster_items(items, summary_url):
    team_ids = list(items['team_id'])
    game_rosters = pd.DataFrame()
    for tm in team_ids:
        team_roster_url = "{x}/{t}/roster".format(x = summary_url,t = tm)
        team_roster_resp = download(team_roster_url)
        team_roster = pd.json_normalize(json.loads(team_roster_resp).get('entries',[]), sep = '_')
        team_roster.columns = [col.replace("$ref", "href") for col in team_roster.columns]
        team_roster.columns = [underscore(c) for c in team_roster.columns.tolist()]
        team_roster['team_id'] = int(tm)
        game_rosters = pd.concat([game_rosters, team_roster], axis = 0, ignore_index = True)
    game_rosters = game_rosters.drop(["period", "for_player_id", "active"], axis = 1)
    game_rosters['player_id'] = game_rosters['player_id'].astype(int)
    game_rosters['team_id'] = game_rosters['team_id'].astype(int)
    return game_rosters


def helper_mbb_athlete_items(teams_rosters):
    athlete_hrefs = list(teams_rosters['athlete_href'])
    game_athletes = pd.DataFrame()
    pop_cols = [
        'links',
        'injuries',
        'teams',
        'team',
        'college',
        'proAthlete',
        'statistics',
        'notes',
        'eventLog',
        "$ref",
        "position"
    ]
    for athlete_href in athlete_hrefs:

        athlete_res = download(athlete_href)
        athlete_resp = json.loads(athlete_res)
        for k in pop_cols:
            athlete_resp.pop(k, None)
        athlete = pd.json_normalize(athlete_resp, sep='_')
        athlete.columns = [col.replace("$ref", "href") for col in athlete.columns]
        athlete.columns = [underscore(c) for c in athlete.columns.tolist()]

        game_athletes = pd.concat([game_athletes, athlete], axis = 0, ignore_index = True)


    game_athletes = game_athletes.rename(columns={
        "id": "athlete_id",
        "uid": "athlete_uid",
        "guid": "athlete_guid",
        "type": "athlete_type",
        "display_name": "athlete_display_name"
    })
    game_athletes['athlete_id'] = game_athletes['athlete_id'].astype(int)
    return game_athletes