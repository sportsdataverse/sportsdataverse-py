"""Descriptions for the PHF (Premier Hockey Federation) loader return tables.

PHF assets come from the fastRhockey-lineage scrape of the league's own site, so
these are provider fields rather than computed metrics. They are described at
face value; nothing asserts a derivation this repo does not perform.

Compositional families (slot/period/size indexes) are generated rather than
repeated by hand:
  * on-ice skater slots 1-6 (offensive/defensive/neutral naming all appear)
  * team logo URLs by rendition size
  * per-period and shootout scoring/shots
"""

from __future__ import annotations

import pathlib
import re
import sys

sys.path.insert(0, ".")

STATIC: dict[str, str] = {
    # --- pbp -------------------------------------------------------------
    "play_description": "Free-text description of the play as published by the league.",
    "on_ice_situation": "Strength situation on the ice for the play (e.g. even strength, power play).",
    "penalty_level": "Severity classification of the penalty (e.g. minor, major).",
    "start_power_play": "True on the play where a power play begins.",
    "end_power_play": "True on the play where a power play ends.",
    "power_play_seconds": "Elapsed seconds of the power play at this play.",
    "goalie_change": "True when the play records a goaltender change.",
    "goalie_involved": "Name of the goaltender involved in the play.",
    "home_goalie_jersey": "Jersey number of the home goaltender on the ice.",
    "away_goalie_jersey": "Jersey number of the away goaltender on the ice.",
    "home_nickname": "Nickname of the home team.",
    "away_nickname": "Nickname of the away team.",
    "home_score_total": "Home team's cumulative score after the play.",
    "away_score_total": "Away team's cumulative score after the play.",
    "scoring_team_abbrev": "Abbreviation of the team credited with the goal.",
    "scoring_team_on_ice": "Skaters the scoring team had on the ice for the goal.",
    "defending_team_abbrev": "Abbreviation of the team defending on the play.",
    "defending_team_on_ice": "Skaters the defending team had on the ice for the play.",
    "leader": "Team leading the game at this point in the play sequence.",
    # --- player boxscores -------------------------------------------------
    "player_jersey": "Player's jersey number.",
    "faceoffs_win_pct": "Share of the player's faceoffs won.",
    "faceoffs_won_lost": "Faceoffs won and lost, as the league's combined won-lost string.",
    "powerplay_goals": "Goals the player scored on the power play.",
    "save_percent": "Share of shots faced that the goaltender saved.",
    "shots_blocked": "Shots the player blocked.",
    "goalies_href": "Relative link to the league's goaltender table for this game.",
    "skaters_href": "Relative link to the league's skater table for this game.",
    # --- team boxscores ---------------------------------------------------
    "total_scoring": "Goals the team scored in the game.",
    "total_shots": "Shots the team took in the game.",
    "overtime_scoring": "Goals the team scored in overtime.",
    "overtime_shots": "Shots the team took in overtime.",
    "power_play_percent": "Share of the team's power plays that produced a goal.",
    "successful_power_play": "Number of power plays on which the team scored.",
    "blocked_opponent_shots": "Opponent shots the team blocked.",
    "shootout_made_scoring": "Shootout attempts the team converted.",
    "shootout_made_shots": "Shootout shots the team took that scored.",
    "shootout_missed_scoring": "Shootout attempts the team failed to convert.",
    "shootout_missed_shots": "Shootout shots the team took that did not score.",
    # --- schedules --------------------------------------------------------
    "allow_players": "League flag for whether player-level detail is published for the game.",
    "has_play_by_play": "True when a play-by-play feed exists for the game.",
    "created_at": "Timestamp at which the league created the game record.",
    "updated_at": "Timestamp at which the league last updated the game record.",
    "datetime": "Scheduled start of the game as a timestamp.",
    "datetime_tz": "Scheduled start of the game including its time-zone offset.",
    "date_group": "League grouping key for the game's date, used to bucket a slate.",
    "time_zone": "Time zone in which the game is played.",
    "time_zone_abbr": "Abbreviated form of the game's time zone.",
    "facility": "Name of the facility hosting the game.",
    "facility_address": "Street address of the hosting facility.",
    "facility_id": "League identifier for the hosting facility.",
    "rink": "Name of the rink within the facility.  NEVER POPULATED: the column is all-null in every published season, which is why it is typed Boolean -- that dtype is polars' inference for an entirely empty column, not a flag.",
    "rink_id": "League identifier for the rink.  NEVER POPULATED: the column is all-null in every published season, which is why it is typed Boolean -- that dtype is polars' inference for an entirely empty column, not a flag.",
    "external_url": "League-published external link for the game.  NEVER POPULATED: the column is all-null in every published season, which is why it is typed Boolean -- that dtype is polars' inference for an entirely empty column, not a flag.",
    "tickets_url": "Link to purchase tickets for the game.",
    "watch_live_url": "Link to the live broadcast of the game.",
    "highlight_color": "Display colour the league uses for the game in its schedule UI.  NEVER POPULATED: the column is all-null in every published season, which is why it is typed Boolean -- that dtype is polars' inference for an entirely empty column, not a flag.",
    "home_team_short": "Short display name of the home team.",
    "away_team_short": "Short display name of the away team.",
    "home_division_id": "League identifier for the home team's division.",
    "away_division_id": "League identifier for the away team's division.",
    "home_roster_count": "Number of players dressed for the home team.",
    "away_roster_count": "Number of players dressed for the away team.",
    "home_penalty_minutes": "Penalty minutes assessed to the home team.",
    "away_penalty_minutes": "Penalty minutes assessed to the away team.",
}


def describe(col: str) -> str | None:
    if col in STATIC:
        return STATIC[col]
    # on-ice skater slots: {offensive|defensive}_player_{name|jersey}_{1..6}
    m = re.fullmatch(r"(offensive|defensive)_player_(name|jersey)_([1-6])", col)
    if m:
        side, kind, n = m.groups()
        what = "Name" if kind == "name" else "Jersey number"
        role = "attacking" if side == "offensive" else "defending"
        return f"{what} of the {role} team's skater in on-ice slot {n} for the play."
    # neutral slots: player_{name|jersey}_{1..3}
    m = re.fullmatch(r"player_(name|jersey)_([1-3])", col)
    if m:
        kind, n = m.groups()
        what = "Name" if kind == "name" else "Jersey number"
        return f"{what} of the player in slot {n} of the play's participant list."
    # team logo renditions
    m = re.fullmatch(r"(home|away)_team_logo_url_(\d+|full|large|medium|small)", col)
    if m:
        side, size = m.groups()
        rend = f"{size}px" if size.isdigit() else size
        return f"URL of the {side} team's logo at the {rend} rendition."
    # per-period scoring / shots
    m = re.fullmatch(r"period_([123])_(scoring|shots)", col)
    if m:
        n, kind = m.groups()
        return f"{'Goals the team scored' if kind == 'scoring' else 'Shots the team took'} in period {n}."
    return None


def main() -> None:
    import yaml

    targets = (
        "load_phf_pbp",
        "load_phf_player_boxscores",
        "load_phf_schedules",
        "load_phf_team_boxscores",
    )
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    out, unparsed = {}, []
    for t in targets:
        got = {}
        for c in schemas.get(t, []):
            d = describe(c["name"])
            if d:
                got[c["name"]] = d
            else:
                # report, never silently omit -- a column this generator cannot
                # resolve must surface so it is authored rather than lost
                unparsed.append(f"{t}.{c['name']}")
        out[t] = got
    # report anything declared in STATIC but absent from every schema
    declared = {c["name"] for t in targets for c in schemas.get(t, [])}
    stale = sorted(k for k in STATIC if k not in declared)
    print("composed: " + ", ".join(f"{t.split('_', 2)[-1]}={len(v)}" for t, v in out.items()))
    print(f"unresolved (left blank, never invented): {len(unparsed)}")
    for u in unparsed[:40]:
        print(f"   {u}")
    if stale:
        print(f"WARNING described but not in any PHF schema: {stale}")
    with open("_phf_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            {k: dict(sorted(v.items())) for k, v in out.items() if v},
            fh,
            sort_keys=True,
            allow_unicode=True,
            width=120,
        )
    print("wrote _phf_descs.yaml")


if __name__ == "__main__":
    main()
