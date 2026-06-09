"""Capture HockeyTech fixtures. Run manually (hits the live API):
    python tests/fixtures/hockeytech/_capture.py
Writes <stem>.json (JSONP already stripped) next to this file.
"""

from __future__ import annotations

import json
import pathlib

from sportsdataverse.hockeytech._client import hockeytech_api

HERE = pathlib.Path(__file__).parent
CAPTURES = {
    "pwhl_seasons": ("pwhl", "modulekit", "seasons", {}),
    "pwhl_schedule_2025": (
        "pwhl",
        "modulekit",
        "scorebar",
        {"numberofdaysback": 400, "numberofdaysahead": 0, "limit": 200, "league_id": 1},
    ),
    "pwhl_pbp_42": ("pwhl", "statviewfeed", "gameCenterPlayByPlay", {"game_id": 42, "league_id": ""}),
    "pwhl_gameshifts_42": ("pwhl", "modulekit", "gameshifts", {"game_id": 42}),
    "pwhl_standings_5": (
        "pwhl",
        "statviewfeed",
        "teams",
        {
            "groupTeamsBy": "division",
            "context": "overall",
            "special": "false",
            "league_id": 1,
            "sort": "points",
            "season": 5,
        },
    ),
    "pwhl_teams_5": ("pwhl", "modulekit", "teamsbyseason", {"season": 5}),
    "pwhl_roster_1_5": ("pwhl", "modulekit", "roster", {"team_id": 1, "season_id": 5}),
    "pwhl_player_stats_27": ("pwhl", "modulekit", "player", {"player_id": 27, "category": "seasonstats"}),
    "pwhl_leaders_5": (
        "pwhl",
        "statviewfeed",
        "leadersExtended",
        # ``season`` returns empty results for a completed season; ``season_id``
        # is the correct key for historical season data.
        {"season_id": 5, "team_id": 0, "playerTypes": "skaters", "skaterStatTypes": "points,goals", "activeOnly": 0},
    ),
    "pwhl_game_summary_42": ("pwhl", "gc", "gamesummary", {"game_id": 42}),
}


def main() -> None:
    for stem, (lg, feed, view, params) in CAPTURES.items():
        data = hockeytech_api(lg, feed, view, params)
        (HERE / f"{stem}.json").write_text(json.dumps(data, indent=1), encoding="utf-8")
        n = len(data) if isinstance(data, (list, dict)) else 0
        print(f"wrote {stem} (top-level size {n})")


if __name__ == "__main__":
    main()
