"""Fixture helpers shared by the VTB tests: committed bodies only, never the sdv-internal-refs checkout."""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path
from typing import Any, Dict, Optional

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
ROOT = FIXTURES.parents[1]


def raw(family: str, rel: str) -> Any:
    """A fixture body exactly as committed (the host's JSON, scrubbed of personal data)."""
    path = FIXTURES / family / rel
    text = gzip.open(path, "rt", encoding="utf-8").read() if rel.endswith(".gz") else path.read_text(encoding="utf-8")
    return json.loads(text)


def body(family: str, rel: str, ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """A fixture as the runtime hands it to a parser: a dict (a list body wrapped as ``items``) with the request stamped."""
    data = copy.deepcopy(raw(family, rel))
    data = {"items": data} if isinstance(data, list) else data
    data["_request"] = dict(ctx or {})
    return data


# (family, short, [(fixture, request stamp)]): every fixture of a parser, full bodies first.
CASES = [
    ("vtb_league", "leagues", [("captures/v2__leagues.json", {})]),
    (
        "vtb_league",
        "seasons",
        [
            ("captures/v2__leagues__league__seasons__vtb.json", {"league_slug": "vtb"}),
            ("captures/v2__leagues__league__seasons__wbc.json", {"league_slug": "wbc"}),
        ],
    ),
    (
        "vtb_league",
        "matches",
        [
            ("full/matches_vtb_2026.json.gz", {"league_slug": "vtb", "season": 2026}),
            (
                "captures/v2__leagues__league__seasons__season__matches__vtb_2026.json",
                {"league_slug": "vtb", "season": 2026},
            ),
            (
                "captures/v2__leagues__league__seasons__season__matches__wbc_2026.json",
                {"league_slug": "wbc", "season": 2026},
            ),
        ],
    ),
    ("vtb_infobasket", "seasons", [("captures/Comp__GetSeasonsForTag__vtb.json", {"tag": "vtb"})]),
    (
        "vtb_infobasket",
        "calendar",
        [
            ("full/calendar_50714.json.gz", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
            ("captures/Comp__GetCalendar__2026.json", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
            ("captures/Comp__GetCalendar__2023.json", {"season": 2023, "season_comp_id": 39799, "tag": "vtb"}),
            ("captures/Comp__GetCalendar__2014.json", {"season": 2014, "season_comp_id": 9113, "tag": "vtb"}),
        ],
    ),
    (
        "vtb_infobasket",
        "team_stats",
        [
            ("full/team_stats_50714.json.gz", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
            ("captures/Comp__GetTeamStats__compId__2026.json", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
        ],
    ),
    (
        "vtb_infobasket",
        "team_stats_players",
        [
            ("full/team_stats_50714.json.gz", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
            ("captures/Comp__GetTeamStats__compId__2026.json", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
        ],
    ),
    (
        "vtb_infobasket",
        "team_stats_coaches",
        [
            ("full/team_stats_50714.json.gz", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
            ("captures/Comp__GetTeamStats__compId__2026.json", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
        ],
    ),
    (
        "vtb_infobasket",
        "best_players",
        [("captures/Comp__GetBestPlayers__compId__2026.json", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"})],
    ),
    (
        "vtb_infobasket",
        "best_teams",
        [("captures/Comp__GetBestTeams__compId__2026.json", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"})],
    ),
    (
        "vtb_infobasket",
        "team_preview",
        [("captures/Comp__GetTeamStatsForPreview__gameId__2026.json", {"game_id": 921391, "season_comp_id": 50714})],
    ),
    (
        "vtb_infobasket",
        "game",
        [
            ("full/online_921391.json.gz", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2026.json", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2023.json", {"game_id": 647209}),
            ("captures/Widget__GetOnline__gameId__2014.json", {"game_id": 684495}),
        ],
    ),
    (
        "vtb_infobasket",
        "box_score",
        [
            ("full/online_921391.json.gz", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2026.json", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2023.json", {"game_id": 647209}),
            ("captures/Widget__GetOnline__gameId__2014.json", {"game_id": 684495}),
        ],
    ),
    (
        "vtb_infobasket",
        "team_box_score",
        [
            ("full/online_921391.json.gz", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2026.json", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2023.json", {"game_id": 647209}),
            ("captures/Widget__GetOnline__gameId__2014.json", {"game_id": 684495}),
        ],
    ),
    (
        "vtb_infobasket",
        "pbp",
        [
            ("full/online_921391.json.gz", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2026.json", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2023.json", {"game_id": 647209}),
            ("captures/Widget__GetOnline__gameId__2014.json", {"game_id": 684495}),
        ],
    ),
    (
        "vtb_infobasket",
        "game_staff",
        [
            ("full/online_921391.json.gz", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2026.json", {"game_id": 921391}),
            ("captures/Widget__GetOnline__gameId__2023.json", {"game_id": 647209}),
            ("captures/Widget__GetOnline__gameId__2014.json", {"game_id": 684495}),
        ],
    ),
    (
        "vtb_infobasket",
        "game_page",
        [
            ("captures/Widget__GamePage__gameId__2026.json", {"game_id": 921391}),
            ("captures/Widget__GamePage__gameId__2023.json", {"game_id": 647209}),
            ("captures/Widget__GamePage__gameId__2014.json", {"game_id": 684495}),
        ],
    ),
    (
        "vtb_infobasket",
        "comp_issue",
        [("captures/Widget__CompIssue__compId__2026.json", {"season": 2026, "season_comp_id": 50714, "tag": "vtb"})],
    ),
    (
        "vtb_infobasket",
        "standings",
        [
            ("full/standings_50720.json", {"comp_id": 50720}),
            ("captures/Widget__CompTeamResults__compId__2026.json", {"comp_id": 50720}),
        ],
    ),
    ("vtb_infobasket", "cross_table", [("captures/Widget__CrossTable__compId__2026.json", {"comp_id": 50720})]),
    ("vtb_infobasket", "playoff", [("captures/Widget__PlayOff__compId__2026.json", {"comp_id": 54281})]),
    ("vtb_infobasket", "playoff_games", [("captures/Widget__PlayOff__compId__2026.json", {"comp_id": 54281})]),
    ("vtb_infobasket", "best_player_parameters", [("captures/Widget__GetBestPlayerParameters.json", {})]),
    ("vtb_infobasket", "best_team_parameters", [("captures/Widget__GetBestTeamParameters.json", {})]),
    ("vtb_infobasket", "player", [("captures/Widget__PlayerPage__personId.json", {"person_id": 423011, "tag": "vtb"})]),
    (
        "vtb_infobasket",
        "player_season_stats",
        [
            ("full/player_season_stats_423011.json", {"person_id": 423011, "tag": "vtb"}),
            ("captures/Widget__PlayerSeasonStats__personId.json", {"person_id": 423011, "tag": "vtb"}),
        ],
    ),
    (
        "vtb_infobasket",
        "player_career_stats",
        [
            ("full/player_season_stats_423011.json", {"person_id": 423011, "tag": "vtb"}),
            ("captures/Widget__PlayerSeasonStats__personId.json", {"person_id": 423011, "tag": "vtb"}),
        ],
    ),
    (
        "vtb_infobasket",
        "player_game_log",
        [
            ("full/player_game_log_423011_50714.json.gz", {"person_id": 423011, "tag": "vtb", "team_id": 2650}),
            ("captures/Widget__PlayerStats__personId__2026.json", {"person_id": 423011, "tag": "vtb", "team_id": 2650}),
        ],
    ),
    ("vtb_infobasket", "coach", [("captures/Widget__CoachPage__personId.json", {"person_id": 131047})]),
    ("vtb_infobasket", "team", [("captures/Widget__TeamPage__teamId.json", {"team_id": 2650, "tag": "vtb"})]),
    (
        "vtb_infobasket",
        "team_seasons",
        [("captures/Widget__GetTeamSeasons__teamId.json", {"team_id": 2650, "tag": "vtb"})],
    ),
    (
        "vtb_infobasket",
        "team_comps",
        [
            (
                "captures/Widget__GetTeamComps__teamId__2026.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            )
        ],
    ),
    (
        "vtb_infobasket",
        "roster",
        [
            ("full/roster_2650_50714.json", {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
            (
                "captures/Widget__TeamRoster__teamId__2026.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
        ],
    ),
    (
        "vtb_infobasket",
        "roster_coaches",
        [
            ("full/roster_2650_50714.json", {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"}),
            (
                "captures/Widget__TeamRoster__teamId__2026.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
        ],
    ),
    (
        "vtb_infobasket",
        "team_games",
        [
            (
                "captures/Widget__TeamGames__teamId__2026.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            )
        ],
    ),
    (
        "vtb_infobasket",
        "team_season_stats",
        [
            (
                "full/team_season_stats_2650_50714.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
            (
                "captures/Widget__TeamStats__teamId__2026.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
        ],
    ),
    (
        "vtb_infobasket",
        "team_player_stats",
        [
            (
                "full/team_season_stats_2650_50714.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
            (
                "captures/Widget__TeamStats__teamId__2026.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
        ],
    ),
    (
        "vtb_infobasket",
        "team_coach_stats",
        [
            (
                "full/team_season_stats_2650_50714.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
            (
                "captures/Widget__TeamStats__teamId__2026.json",
                {"team_id": 2650, "season": 2026, "season_comp_id": 50714, "tag": "vtb"},
            ),
        ],
    ),
]
