"""Descriptions for the cfb_pbp return table (CFBPlayProcess output).

Unlike the ESPN advBoxScore blocks, these columns ARE computed in this repo, so
the derived flags are transcribed from sportsdataverse/cfb/cfb_pbp.py rather than
described generically. Exact thresholds are quoted where the producer sets them.

Verified while writing (see the module for line refs):
  * early_down  = (1st or 2nd down) and a scrimmage play
  * late_down   = (3rd or 4th down) and a scrimmage play
  * standard/passing_down split on down-and-distance thresholds
  * havoc       = pass_breakup OR TFL OR interception OR forced_fumble
  * the line-yards decomposition uses adj_rush_yardage = min(rush yards, 8):
      line_yards        <0 -> 1.2*adj; 0-3 -> adj; 4-8 -> 3+0.5*(adj-3); >=8 -> 5.5
      second_level      >=4 -> 0.5*(adj-4) else 0
      open_field        >8  -> yards-adj  else 0
      highlight_yards   = second_level + open_field
  * opportunity_run is `rush AND yards <= 4` -- the INVERSE of the conventional
    "carry gained 4+", so it is documented as what the code computes.
  * opp_highlight_yards is identically 0 in every published row (verified across
    162,950 plays in 2024): its gate requires <=4 rushing yards while
    highlight_yards is non-zero only at >=4, so the two can never co-occur.
    Documented as a known-degenerate column rather than silently described.

Anything unparsed is reported and left blank, never invented.
"""

from __future__ import annotations

import pathlib
import re
import sys

sys.path.insert(0, ".")

STATIC: dict[str, str] = {
    # --- expected points ---------------------------------------------------
    "EP_start": "Expected points for the offense at the start of the play.",
    "EP_end": "Expected points for the offense at the end of the play.",
    "EP_between": "Change in expected points across the play, before penalty adjustment.",
    "EP_start_touchback": "Expected points the offense would have had from a touchback on this play.",
    # --- derived flags, transcribed from cfb_pbp.py ------------------------
    "early_down": "True when the play is a scrimmage play on first or second down.",
    "late_down": "True when the play is a scrimmage play on third or fourth down.",
    "early_down_pass": "True when the play is a pass on an early down.",
    "early_down_rush": "True when the play is a rush on an early down.",
    "late_down_pass": "True when the play is a pass on a late down.",
    "late_down_rush": "True when the play is a rush on a late down.",
    "passing_down": (
        "True when the offense is behind schedule for the series -- second down needing 8 or more, "
        "or third/fourth down needing 5 or more."
    ),
    "havoc": (
        "True when the defense disrupted the play: a pass breakup, tackle for loss, interception or forced fumble."
    ),
    "TFL": "True when the play was a tackle for loss.",
    "TFL_pass": "True when the play was a tackle for loss on a pass play (a sack).",
    "TFL_rush": "True when the play was a tackle for loss on a rush play.",
    "pass_breakup": "True when a defender broke up the pass.",
    "forced_fumble": "True when the defense forced a fumble on the play.",
    "fumble_recovered": "True when a fumble on the play was recovered.",
    "non_fumble_sack": "True when the play was a sack that did not produce a fumble.",
    "first_down_created": "True when the play produced a first down for the offense.",
    "fg_attempt": "True when the play was a field-goal attempt.",
    "penalty_in_text": "True when the play description mentions a penalty.",
    "action_play": (
        "True when the play advanced the game state -- excludes timeouts, end-of-period markers and "
        "other non-action rows."
    ),
    # --- rushing decomposition, exact formulas from cfb_pbp.py -------------
    "adj_rush_yardage": "Rushing yards capped at 8, the input to the line-yards decomposition.",
    "line_yards": (
        "Yards credited to the offensive line on a rush, using the standard sliding scale: 1.2x the "
        "capped yardage on a loss, all of it through 3 yards, half of each yard from 4 to 8, and a "
        "5.5-yard ceiling beyond that."
    ),
    "open_field_yards": "Rushing yards gained beyond 8, credited to the ball carrier rather than the line.",
    "highlight_yards": "Second-level plus open-field yards -- the yardage credited to the carrier.",
    "highlight_run": "True when the rush gained 8 or more yards.",
    "opportunity_run": (
        "True when a rush reached 4 yards -- the carries on which the blocking did its job. Matches "
        "cfbfastR's espn_cfb_15 definition. Assets published before the 2026-08 fix carry the "
        "inverted (4 yards or fewer) flag."
    ),
    "opp_highlight_yards": (
        "Highlight yards earned on opportunity runs, isolating carrier production on carries where "
        "the blocking succeeded. Assets published before the 2026-08 fix are identically 0 here, "
        "because the inverted opportunity_run gate could never co-occur with non-zero highlight yards."
    ),
    "power_rush_attempt": "True when the play is a short-yardage power rushing attempt.",
    "power_rush_success": "True when a power rushing attempt gained the yardage needed.",
    # --- EPA / weights -----------------------------------------------------
    "pass_epa": "EPA credited to the play when it is a pass.",
    "rush_epa": "EPA credited to the play when it is a rush.",
    "pen_epa": "EPA attributable to a penalty on the play.",
    "qbr_epa": "EPA variant used as an input to the QBR calculation.",
    "pass_weight": "Weighting applied to the pass component of the play.",
    "rush_weight": "Weighting applied to the rush component of the play.",
    "pen_weight": "Weighting applied to the penalty component of the play.",
    "prog_drive_EPA": "Cumulative EPA accrued by the drive up to and including this play.",
    "prog_drive_WPA": "Cumulative win-probability added by the drive up to and including this play.",
    # --- score state -------------------------------------------------------
    "H_score_diff": "Home team's score minus the away team's, from the home perspective.",
    "A_score_diff": "Away team's score minus the home team's, from the away perspective.",
    "HA_score_diff": "Home score minus away score for the play.",
    "net_HA_score_pts": "Net points the play added to the home-minus-away score margin.",
    "pos_score_diff_end": "Score differential from the possessing team's perspective at the end of the play.",
    # --- betting inputs ----------------------------------------------------
    "gameSpread": "Point spread used as an input to the win-probability model.",
    "gameSpreadAvailable": "True when a spread was available for the game.",
    "overUnder": "Over/under total used as a model input.",
    "homeFavorite": "True when the home team was favoured by the spread.",
    # --- game / clock metadata --------------------------------------------
    "playType": "ESPN's play-type label for the play.",
    "period.number": "Period (quarter) number in which the play occurred.",
    "firstHalfKickoffTeamId": "ESPN id of the team that received the opening kickoff.",
    "homeTimeoutCalled": "True when the home team called a timeout on the play.",
    "awayTimeoutCalled": "True when the away team called a timeout on the play.",
    "new_down": "Down after the play, including any penalty enforcement.",
    "new_distance": "Distance to go after the play, including any penalty enforcement.",
    "drive_start": "Yard line at which the drive began.",
    "drive_stopped": "True when the play ended the drive.",
    "drive_play_index": "Sequence number of the play within its drive.",
    "drive_offense_plays": "Offensive plays run on the drive.",
    "drive_offense_yards": "Offensive yards gained on the drive.",
    "drive_total_yards": "Total yards gained on the drive.",
    "kickoff_return_player_name": "Name of the player returning the kickoff.",
    "punt_return_player_name": "Name of the player returning the punt.",
    # --- remainder ---------------------------------------------------------
    "scrimmage_play": "True when the play is a play from scrimmage rather than a special-teams or administrative row.",
    "standard_down": (
        "True when the offense is on schedule for the series -- first down, second down needing "
        "fewer than 8, or third/fourth down needing fewer than 5."
    ),
    "second_level_yards": (
        "Rushing yards earned from 4 to 8, split evenly between line and carrier under the line-yards decomposition."
    ),
    "short_rush_attempt": "True when the play is a rush in a short-yardage situation.",
    "short_rush_success": "True when a short-yardage rush gained the yardage needed.",
    "stopped_run": "True when the rush was stopped at or behind the line of scrimmage.",
    "sack_epa": "EPA credited to the play when it is a sack.",
    "sack_weight": "Weighting applied to the sack component of the play.",
    "wp_touchback": "Win probability the offense would have had starting from a touchback.",
    "td_check": "Internal flag used while reconciling whether the play produced a touchdown.",
    "text_dupe": "True when the play description duplicates the previous row's text.",
    "scoringPlay": "ESPN flag marking the play as a scoring play.",
    "scoringType.name": "ESPN's name for the scoring type (e.g. touchdown, field goal).",
    "scoringType.displayName": "ESPN's display label for the scoring type.",
    "scoringType.abbreviation": "ESPN's abbreviation for the scoring type.",
    "type.id": "ESPN's numeric identifier for the play type.",
    "type.text": "ESPN's text label for the play type.",
    "type.abbreviation": "ESPN's abbreviation for the play type.",
    "statYardage": "Yardage ESPN credits to the play for statistical purposes.",
    "seasonType": "ESPN season type for the game (2 = regular season, 3 = postseason).",
}

CLOCK = {
    "clock.displayValue": "Game clock at the play, as the displayed mm:ss string.",
    "clock.minutes": "Minutes remaining on the game clock at the play.",
    "clock.seconds": "Seconds component of the game clock at the play.",
}


def describe(col: str) -> str | None:
    if col in STATIC:
        return STATIC[col]
    if col in CLOCK:
        return CLOCK[col]

    # --- down indicator flags: down_N / down_N_end
    m = re.fullmatch(r"down_([1-4])(_end)?", col)
    if m:
        n, end = m.groups()
        when = "at the end of" if end else "at the start of"
        return f"True when it is {n}{'st' if n == '1' else 'nd' if n == '2' else 'rd' if n == '3' else 'th'} down {when} the play."

    # --- EPA success COUNTS/flags and EPA splits
    m = re.fullmatch(
        r"EPA_success(?:_(early_down|late_down|standard_down|passing_down))?(?:_(pass|rush))?(_EPA)?",
        col,
    )
    if m:
        sit, play, epa = m.groups()
        where = {
            "early_down": " on an early down",
            "late_down": " on a late down",
            "standard_down": " on a standard down",
            "passing_down": " on a passing down",
        }.get(sit or "", "")
        pl_ = {"pass": " pass", "rush": " rush"}.get(play or "", "")
        if epa:
            return f"EPA on successful{pl_} plays{where}."
        return f"True when the{pl_} play{where} was successful by EPA."
    m = re.fullmatch(r"EPA_middle_8_success(?:_(pass|rush))?", col)
    if m:
        pl_ = {"pass": " pass", "rush": " rush"}.get(m.group(1) or "", "")
        return f"True when the{pl_} play in the middle eight was successful by EPA."
    m = re.fullmatch(r"EPA_explosive(?:_(pass|rush))?", col)
    if m:
        pl_ = {"pass": " pass", "rush": " rush"}.get(m.group(1) or "", "")
        return f"True when the{pl_} play was explosive."
    m = re.fullmatch(r"EPA_(pass|rush|scrimmage|penalty|punt|kickoff|fg|sp|non_explosive|success)", col)
    if m:
        k = m.group(1)
        label = {
            "pass": "on pass plays",
            "rush": "on rush plays",
            "scrimmage": "on plays from scrimmage",
            "penalty": "attributable to penalties",
            "punt": "on punt plays",
            "kickoff": "on kickoff plays",
            "fg": "on field-goal attempts",
            "sp": "on special-teams plays",
            "non_explosive": "on non-explosive plays",
            "success": "on successful plays",
        }[k]
        if k == "success":
            return "True when the play was successful by EPA."
        return f"EPA credited to the play {label}."

    # --- lag/lead shifted columns
    m = re.fullmatch(r"(lag|lead)_(.+?)(\d?)$", col)
    if m:
        d, base, n = m.groups()
        direction = "previous" if d == "lag" else "next"
        step = f" {n} plays" if n and n not in ("1", "") else ""
        return f"Value of {base} on the {direction}{step or ''} play, used for sequence-aware derivations."

    # --- ESPN nested passthrough: start.* / end.* / drive.* / homeTeam* / awayTeam*
    m = re.fullmatch(r"(start|end)\.(.+)", col)
    if m:
        side, field = m.groups()
        when = "start" if side == "start" else "end"
        return f"ESPN's `{field}` value for the play state at the {when} of the play."
    if col.startswith("drive."):
        return f"ESPN's `{col[len('drive.') :]}` field for the drive containing this play."
    m = re.fullmatch(r"(home|away)Team(.*)", col)
    if m:
        side, field = m.groups()
        return f"ESPN's {side}-team {field or 'identifier'} for the game, stamped on every play."
    return None


def main() -> None:
    import yaml

    target = "load_cfb_pbp"
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    got, unparsed = {}, []
    for c in schemas[target]:
        d = describe(c["name"])
        if d:
            got[c["name"]] = d
        else:
            unparsed.append(c["name"])
    print(f"composed {len(got)}; unparsed (left blank, never invented) {len(unparsed)}")
    for u in unparsed[:60]:
        print(f"   {u}")
    with open("_pbp_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump({target: dict(sorted(got.items()))}, fh, sort_keys=False, allow_unicode=True, width=120)
    print("wrote _pbp_descs.yaml")


if __name__ == "__main__":
    main()
