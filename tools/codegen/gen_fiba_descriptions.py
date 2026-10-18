"""Column descriptions for the FIBA returns-schemas (imported by ``gen_fiba.py``).

``fiba-returns.md`` (sdv-internal-refs/fiba) documents every field of every body by JSON path. A parsed
column is described by the row of the path it came from: the section of its route is filtered to the table
the shaper explodes (``players.[]``, ``periods.[]``...), the container prefix is stripped, and the rest of the
path, snake-cased, is the column. Columns a shaper adds (ids from the parent, derived values, renamed box
stat codes) are described here. The first sentence of a description carries what the value is; caveats the
refs record (unverified meanings, key spaces, units) stay in it.
"""

from __future__ import annotations

import re
from typing import Callable, Dict, List, Optional, Tuple

from sportsdataverse.dl_utils import underscore

Rows = List[Tuple[str, str, str]]

# short -> (md section, include container to strip or None, exclude prefixes, column alias prefix -> md prefix)
GDAP_SECTIONS: Dict[str, Tuple[str, Optional[str], Tuple[str, ...], Dict[str, str]]] = {
    "schedule": ("getgdapgamesbycompetitionid", None, (), {}),
    "games_between": ("getgdapgamesbetweentwodates", None, (), {}),
    "game": ("getgdapgamebyid", None, ("nominations", "periods"), {}),
    "game_periods": ("getgdapgamebyid", "periods.[]", (), {}),
    "game_officials": ("getgdapgamebyid", "nominations.[]", (), {}),
    "competition": ("getGdapCompetitionById", None, (), {}),
    "editions": ("getgdapcompetitionsbyseasonsandcompetitionmarketingname", None, (), {}),
    "teams": ("getgdapcompetitionteamsbycompetitionid", None, (), {}),
    "rounds": ("getgdaproundsbycompetitionid", None, ("groupStandings",), {}),
    "round_standings": ("getgdaproundsbycompetitionid", "groupStandings.groups.[].teams.[]", (), {}),
    "final_standings": ("getgdapcompetitionfinalstandingbycompetitionid", "standings.[]", (), {}),
    "round_gamedays": ("getgdapcompetitionroundgamedaysbyid", "rounds.[]", (), {}),
    "awards": ("getgdapcompetitionawardsbyid", None, (), {}),
    "nominations": ("getgdapcompetitionnominationsbyid", None, (), {}),
    "team_staff": ("getgdapteamstaffbycompetitionid", "coaches.[]", (), {}),
    "roster": ("getgdapcompetitionteamrosterbyteamid", "players.[]", (), {}),
    "roster_coaches": ("getgdapcompetitionteamrosterbyteamid", "coaches.[]", (), {}),
    "roster_latest": ("getgdapcompetitionteamlatestrosterbyteamid", "players.[]", (), {}),
    "team_stats": (
        "getgdapcompetitionteamstatisticsbyteamid",
        None,
        ("playerInCompetitionTeamStatistics",),
        {"bench_": "bench_statistics_"},
    ),
    "team_player_stats": ("getgdapcompetitionteamstatisticsbyteamid", "playerInCompetitionTeamStatistics.[]", (), {}),
    "team_games": ("getgdapgameswithleaderdetailsbyteamid", None, (), {}),
    "player_info": ("getgdapplayerinfobycompetitionid", None, (), {}),
    "player_stats": ("getgdapplayercompetitionstatisticsbyplayerid", None, (), {}),
    "player_game_stats": ("getgdapplayergamestatisticsbyplayerid", "gameStatistics.[]", (), {}),
    "federation_games": ("getgdapfederationgamesbyfederationid", None, (), {}),
    "national_teams": ("getgdapnationalteams", None, (), {}),
    "medal_count": ("getgdapmedalcount", None, (), {}),
    "ranking_zones": ("getgdaprankingzones", None, (), {}),
    "federation_ranking": ("getgdapfederationsranking", "items.[]", (), {}),
    "gateway_federation_ranking": ("getcustomgateway:GetcustomgatewayFederationRanking", "items.[]", (), {}),
    "gateway_competition_ids": ("getcustomgateway:GetcustomgatewayCompetitionIdsBetweenTwoDates", None, (), {}),
}

#: descriptions of columns a shaper adds or renames, by short then column
GDAP_EXTRA: Dict[str, Dict[str, str]] = {
    "awards": {
        "competition_official_name": "Official name of the competition edition the award was given in.",
    },
    "national_teams": {
        "display_name": "English display name of the federation (the country), as the FIBA site shows it.",
    },
    "ranking_zones": {
        "official_name": "Official name of the ranking zone (World, FIBA Europe, FIBA Americas...).",
    },
    "game_periods": {
        "game_id": "FIBA game id (Int64) of the game the period belongs to.",
    },
    "game_officials": {
        "game_id": "FIBA game id (Int64) of the game the official was nominated to.",
    },
    "round_standings": {
        "round_id": "Round id (Int64) the group belongs to.",
        "round_name": "Round name (Preliminary Round, Final Round...).",
        "round_code": "Round code (a short code such as G or F).",
        "group_name": "Group name within the round (A, B...).",
        "group_status_code": "Group status code (COMPL when the group is complete).",
    },
    "final_standings": {
        "competition_id": "FIBA competition id (Int64) the final ranking belongs to; the comp argument.",
    },
    "round_gamedays": {
        "competition_id": "FIBA competition id (Int64) the rounds belong to; the comp argument.",
        "gamedays": "Game days of the round as a JSON string; empty (null) for the competitions sampled.",
    },
    "team_staff": {
        "team_id": "FIBA team id (Int64), one per team per competition; the team the coach is registered with.",
        "team_name": "Team name the coach is registered with.",
        "team_code": "Team code (GER, USA...) the coach is registered with.",
    },
    "team_player_stats": {
        "competition_id": "FIBA competition id (Int64) the averages belong to.",
    },
    "player_game_stats": {
        "player_id": "FIBA person id (Int64), global; the player_id argument.",
        "competition_id": "FIBA competition id (Int64) the game log belongs to; the comp argument.",
    },
    "federation_ranking": {
        "as_of_date": "Date of the ranking returned: the latest ranking published on or before the requested date.",
        "zone_ranking_code": "Ranking zone code (WORLD, AFRICA...).",
        "zone_ranking_official_name": "Ranking zone official name.",
    },
    "gateway_federation_ranking": {
        "as_of_date": "Date of the ranking returned: the latest ranking published on or before the requested date.",
        "zone_ranking_code": "Ranking zone code (WORLD, AFRICA...).",
        "zone_ranking_official_name": "Ranking zone official name.",
    },
    "gateway_competition_ids": {
        "competition_id": "FIBA competition id (Int64, GDAP key space) of a competition that runs in the window.",
    },
}


#: replaces a refs description shorter than 15 characters (or missing), by column name
#: Texts that replace the md row on every GDAP route (the md states the wire type, not the parsed one).
ALWAYS: Dict[str, str] = {
    "utc_offset": "Venue UTC offset at tip-off, in hours (Float64: a half-hour zone such as India +5.5 or Adelaide "
    "+9.5 is a fraction; whole hours in every sample).",
}

COMMON: Dict[str, str] = {
    "short_name": "Short team name: the country code for a national team, the club's short name for a club.",
    "team_short_name": "Short team name: the country code for a national team, the club's short name for a club.",
    "team_a_short_name": "Short name of team A: the country code for a national team, the club's short name for a club.",
    "team_b_short_name": "Short name of team B: the country code for a national team, the club's short name for a club.",
    "competition_category_name": "Name of the competition's category (FIBA Basketball World Cup, Olympic Basketball Tournament...).",
    "competition_competition_category_name": "Name of the competition's category (FIBA Basketball World Cup, Olympic Basketball Tournament...).",
    "category_name": "Name of the category (FIBA Basketball World Cup, Olympic Basketball Tournament...).",
    "gender": "Men or Women: the gender of the competition.",
    "competition_gender": "Men or Women: the gender of the competition.",
    "profile_gender": "Men or Women: the gender of the team's competition.",
    "zone_information_zone_official_name": "Official name of the competition's FIBA zone (FIBA Europe, FIBA Americas, World).",
    "competition_zone_information_zone_official_name": "Official name of the competition's FIBA zone (FIBA Europe, FIBA Americas, World).",
    "spectators": "Attendance at the game, as recorded by FIBA.",
    "team_a_official_name": "Official name of team A: the country for a national team, the club for a club team.",
    "team_b_official_name": "Official name of team B: the country for a national team, the club for a club team.",
    "venue_name": "Name of the arena the game is played in.",
    "height_in_cm": "Player height in centimetres.",
    "organisation_id": "Stable federation or club id (Int64), the same in every competition and season (64 = Germany).",
    "points_for": "Points the team scored in the group's games.",
    "profile_code": "Team code (FIBA country code or club abbreviation).",
    "profile_name": "Team name as registered for the competition.",
    "profile_website": "The team's website address.",
    "team_code": "Team code (FIBA country code or club abbreviation).",
    "team_name": "Team name (the country for a national team, the club for a club team).",
    "team_official_name": "Official team name (the country for a national team, the club for a club team).",
    "total_second_rank": "Number of silver medals the federation has won in the competition family.",
    "total_third_rank": "Number of bronze medals the federation has won in the competition family.",
    "total_first_rank": "Number of gold medals the federation has won in the competition family.",
}


def _normalise(path: str) -> str:
    return re.sub(r"^\[\]\.?", "", path)


def _rows_for(rows: Rows, strip: Optional[str], exclude: Tuple[str, ...]) -> Tuple[Rows, Rows]:
    """``(table rows, top-level rows)``: the rows under ``strip`` with the prefix removed, and the unprefixed scalars."""
    table: Rows = []
    heads: Rows = []
    for path, typ, desc in rows:
        p = _normalise(path)
        if any(p == e or p.startswith(e + ".") for e in exclude):
            continue
        if strip is None:
            table.append((p, typ, desc))
        elif p.startswith(strip + "."):
            table.append((p[len(strip) + 1 :], typ, desc))
        elif "." not in p:
            heads.append((p, typ, desc))
    return table, heads


def gdap_notes(
    sections: Dict[str, Rows], dtypes: Dict[str, Dict[str, str]], desc_maps: Callable[[Rows], Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    """``{short: {column: description}}``; a column with no source row is left empty (the caller refuses it)."""
    out: Dict[str, Dict[str, str]] = {}
    for short, (section, strip, exclude, alias) in GDAP_SECTIONS.items():
        table, heads = _rows_for(sections[section], strip, exclude)
        maps, head_maps = desc_maps(table), desc_maps(heads)
        listy = {underscore("_".join(p.split("."))) for p, t, _ in table if t == "array"}
        extra = GDAP_EXTRA.get(short, {})
        notes: Dict[str, str] = {}
        for col in dtypes[f"parse_fiba_gdap_{short}"]:
            key = col
            for mine, theirs in alias.items():
                if col.startswith(mine):
                    key = theirs + col[len(mine) :]
            if col in extra:
                text = extra[col]
            elif maps.get(key):
                text = maps[key]
                if col in listy and "JSON" not in text:
                    text = text.rstrip(".") + ". Stored as a JSON string."
            else:
                text = head_maps.get(col) or ""
            if len(text) < 15 or col.endswith("short_name"):
                text = COMMON.get(col, text)
            text = ALWAYS.get(col, text)
            notes[col] = " ".join(text.split())
        out[short] = notes
    return out


# --------------------------------------------------------------------------------------------------------------------
# game page
# --------------------------------------------------------------------------------------------------------------------
PAGE_HEAD: Dict[str, str] = {
    "game_id": "FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL.",
    "competition_id": "FIBA competition id (Int64) of the game's edition; the comp argument.",
    "statistic_system": "Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text.",
    "team_side": "A or B: whether the row is the schedule's team A or team B (null when the page does not say).",
    "organisation_id": "Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id.",
    "player_id": "FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id.",
    "period": "Period name: Q1..Q4, then OT1, OT2...",
    "period_number": "1-based position of the period in the game.",
    "first_name": "Given name(s) as registered with FIBA.",
    "last_name": "Last name as registered with FIBA.",
    "jersey": "Jersey number as a string.",
    "position": "Playing position code from the lineup card (PG, SG, SF, PF, C).",
    "is_captain": "True for the team captain.",
}

PBP: Dict[str, str] = {
    "action_id": "Action id (String): '<n>:<seq>' in OVR games (3:0015), a sequence number in FLS games; unique within the game.",
    "order": "Sort key within the period (OVR: 1, 2, ...; FLS: 1000, 2000, ...); null on some period events.",
    "clock": "Game clock: time REMAINING in the period, 'm:ss', counting down from 10:00 (5:00 in overtime).",
    "clock_seconds": "Seconds remaining in the period, derived from clock.",
    "wall_time_ms": "Wall-clock time of the action, Unix epoch milliseconds.",
    "score_a": "Team A score after the action.",
    "score_b": "Team B score after the action.",
    "action_code": "Action code: P2 two-point shot, P3 three-point shot, FT free throw, REB rebound, TREB team rebound, ASS assist, ST steal, BS block, TO turnover, TTO team turnover, FOUL foul, RFOUL foul drawn, SUBST substitution, TIMO timeout, plus period events (STARTG, STARTP, ENDP, ENDG).",
    "action_family": "Action family: shot, subst, timeout, periods, and 'unknwown' (FIBA's spelling) for every other action.",
    "description": "English action text ('3pt jump shot missed', 'defensive rebound'); null on most OVR actions.",
    "player2_id": "FIBA person id (Int64) of the second player on a substitution; null when the game does not record it (the refs note FLS only, but the Paris 2024 final, an OVR game, carries it and the FLS games sampled leave it null).",
    "sub_direction": "Substitution direction: IN or OUT; null on other actions.",
    "made": "True when the shot went in (two-pointers, three-pointers and free throws only; null otherwise).",
    "points_attempted": "Points the attempt was worth (2, 3 or 1), made or not; null on other actions.",
    "shot_type": "Shot type derived from action_code: 2PT, 3PT or FT; null on other actions.",
    "shot_x": "Shot x, integer pixels on a half-court image across the court width (observed 0-278, basket near x = 139). Present only on shots; free throws are 0. Which sideline is x = 0 and the scale are UNVERIFIED.",
    "shot_y": "Shot y, integer pixels from the baseline (y = 0) towards midcourt (observed 0-224, basket near y = 25); both teams' shots are drawn on one basket. Free throws are 0. Scale UNVERIFIED.",
    "has_location": "True when shot_x / shot_y locate the shot (two- and three-pointers); False for free throws (always 0, 0); null on other actions.",
}

BOX_STATS: Dict[str, str] = {
    "points": "Points scored in the game.",
    "fg_made": "Field goals made (2 + 3 points).",
    "fg_attempted": "Field goals attempted (2 + 3 points).",
    "fg_pct": "Field-goal percentage, 0-100.",
    "fg2_made": "Two-point field goals made.",
    "fg2_attempted": "Two-point field goals attempted.",
    "fg2_pct": "Two-point field-goal percentage, 0-100.",
    "fg3_made": "Three-point field goals made.",
    "fg3_attempted": "Three-point field goals attempted.",
    "fg3_pct": "Three-point field-goal percentage, 0-100.",
    "ft_made": "Free throws made.",
    "ft_attempted": "Free throws attempted.",
    "ft_pct": "Free-throw percentage, 0-100.",
    "reb_offensive": "Offensive rebounds.",
    "reb_defensive": "Defensive rebounds.",
    "reb_total": "Total rebounds (a team row includes the team rebounds team_reb_total).",
    "assists": "Assists credited in the game.",
    "steals": "Steals credited in the game.",
    "blocks": "Blocked shots (blocks made).",
    "blocks_received": "Own shots blocked by the opponent (blocks received).",
    "turnovers": "Turnovers (a team row includes the team turnovers team_turnovers).",
    "fouls": "Personal fouls (a team row includes the team fouls team_fouls).",
    "fouls_drawn": "Fouls drawn in the game.",
    "paint_fg_made": "Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game).",
    "paint_fg_attempted": "Field goals attempted in the paint.",
    "paint_fg_pct": "Field-goal percentage in the paint, 0-100.",
    "efficiency": "Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers).",
}

PLAYER_BOX: Dict[str, str] = {
    "is_starter": "True for the starting five.",
    "has_played": "True when the player got on the court.",
    "minutes": "Minutes played as an 'm:ss' string ('0:0' = did not play).",
    "seconds_played": "Seconds played, derived from minutes.",
    "plus_minus": "Plus/minus: the team's point differential while the player was on the court.",
    "cb": "Boolean flag, false on every sampled player; meaning unverified.",
}

TEAM_BOX: Dict[str, str] = {
    "score": "Team final score.",
    "biggest_lead": "Biggest lead, points.",
    "biggest_lead_score": "Score when the biggest lead was reached, 'A-B' string (spacing varies; empty when the team never led).",
    "biggest_run": "Biggest scoring run, points.",
    "biggest_run_score": "Score of the biggest scoring run, 'x-y' string; exact meaning unverified.",
    "fast_break_points": "Fast-break points.",
    "points_off_turnovers": "Points scored after turnovers.",
    "bench_points": "Points from the bench (the sum of the non-starters' points).",
    "points_in_paint": "Points in the paint (2 x paint_fg_made).",
    "second_chance_points": "Second-chance points.",
    "a_act": "Always 0 in the samples; meaning unverified (FIBA stat code A_ACT).",
    "a_foul": "Always 0 in the samples; meaning unverified (FIBA stat code A_FOUL).",
    "a_tod": "Always 0 in the samples; meaning unverified (FIBA stat code A_TOD).",
    "a_tot": "Always 0 in the samples; meaning unverified (FIBA stat code A_TOT).",
    "team_reb_offensive": "Team offensive rebounds, credited to no player.",
    "team_reb_defensive": "Team defensive rebounds, credited to no player.",
    "team_reb_total": "Team rebounds (total rebounds minus the players' sum).",
    "team_fouls": "Team (bench/coach) fouls (personal fouls minus the players' sum).",
    "team_turnovers": "Team turnovers (turnovers minus the players' sum).",
    "team_bench_points_flag": "Bench points in FLS games (equal to bench_points); 0 in OVR games (FIBA stat code T_PFB).",
    "time_led": "Time the team led, 'mm:ss' string.",
    "lead_changes": "Lead changes in the game (game-level; repeats on both team rows).",
    "times_tied": "Times the score was tied (game-level; repeats on both team rows).",
    "duration": "Game duration as an 'h:mm' string (game-level; repeats on both team rows).",
    "attendance": "Attendance (game-level; repeats on both team rows).",
}

PERIOD: Dict[str, str] = {
    "points": "Points the team scored in the period.",
    "points_first_five_min": "Points the team scored in the first five minutes of the period (matches the play-by-play except free throws on the 5:00 boundary); null when absent.",
}


def gamepage_notes(
    sections: Dict[str, Rows], dtypes: Dict[str, Dict[str, str]], desc_maps: Callable[[Rows], Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    tables = {
        "pbp": {**PAGE_HEAD, **PBP},
        "shots": {**PAGE_HEAD, **PBP},
        "box_players": {**PAGE_HEAD, **BOX_STATS, **PLAYER_BOX},
        "box_teams": {**PAGE_HEAD, **BOX_STATS, **TEAM_BOX},
        "period_scores": {**PAGE_HEAD, **PERIOD},
        "lineup": PAGE_HEAD,
    }
    return {
        short: {col: " ".join(tables[short].get(col, "").split()) for col in dtypes[f"parse_fiba_gamepage_{short}"]}
        for short in tables
    }
