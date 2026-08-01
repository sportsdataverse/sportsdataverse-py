"""Descriptions for the remaining CFB loader return tables.

Covers the tail after team_summaries / adv_* / cfb_pbp: play_participants,
player_box, passing/rushing/receiving, percentiles, adv_passing, model_pbp,
ratings, crosswalks and the small leftovers.

play_participants is a pure grid -- ``{role}_player_{id|ids|name|names}`` -- and
the scalar/plural contract is the one this repo documents: the SCALAR column is
the first/primary participant of that role, the PLURAL column lists every
participant, so multi-entry roles (split sacks, gang tackles) are not silently
collapsed.

Percentile columns are the summaries metrics expressed as a percentile, so they
reuse that vocabulary rather than redefining it.

Anything unmatched is reported and left blank, never invented.
"""

from __future__ import annotations

import pathlib
import re
import sys

sys.path.insert(0, ".")

ROLE = {
    "passer": "the passer",
    "rusher": "the ball carrier on a rush",
    "receiver": "the targeted receiver",
    "sacked_by": "a defender credited with the sack",
    "tackler": "a defender credited with the tackle",
    "assisted_by": "a defender credited with an assisted tackle",
    "forced_by": "the defender who forced the fumble",
    "recoverer": "the player who recovered the fumble",
    "pass_defender": "the defender credited with defending the pass",
    "penalized": "the penalized player",
    "returner": "the player returning the kick or punt",
    "kicker": "the kicker",
    "punter": "the punter",
    "scorer": "the player credited with the score",
    "pat_passer": "the passer on the point-after attempt",
    "pat_scorer": "the player credited with the point-after score",
}

# summaries/percentile shared vocabulary (see gen_cfb_summary_descriptions.py)
METRIC = {
    "EPAplay": "EPA generated per play",
    "EPAgame": "EPA generated per game",
    "EPAdrive": "EPA generated per drive",
    "EPAdropback": "EPA generated per dropback",
    "EPArush": "EPA generated per rushing attempt",
    "TEPA": "total EPA summed over every play",
    "early_down_EPA": "EPA per early-down play",
    "nonExplosiveEpaPerPlay": "EPA per play excluding explosive plays",
    "success": "success rate across the team plays",
    "pass_success": "success rate on pass plays",
    "rush_success": "success rate on rush plays",
    "early_down_success": "success rate on early downs",
    "late_down_success": "success rate on late downs",
    "third_down_success": "success rate on third down",
    "red_zone_success": "success rate in the red zone",
    "third_down_distance": "average yards to go on third down",
    "explosive": "explosive-play rate",
    "pass_explosive": "explosive-play rate on pass plays",
    "rush_explosive": "explosive-play rate on rush plays",
    "havoc": "havoc rate",
    "play_stuffed": "stuffed-play rate",
    "lineyards": "line yards per rush",
    "opportunity_run": "opportunity-run rate",
    "yardsplay": "yards per play",
    "yardsgame": "yards per game",
    "yardsrush": "yards per rush",
    "yardsdropback": "yards per dropback",
    "dropbacks": "dropbacks taken by the passer",
    "rushes": "rushing attempts",
    "playsgame": "plays per game",
    "GEI": "game excitement index",
}

STATIC: dict[str, str] = {
    # --- passing / rushing / receiving season tables ---------------------
    "att": "Pass attempts thrown.",
    "comp": "Completed passes.",
    "comppct": "Completion percentage.",
    "pass_int": "Interceptions thrown.",
    "sacked": "Times the passer was sacked.",
    "sack_yds": "Yards lost to sacks.",
    "sack_adj_yards": "Passing yards adjusted for sack yardage lost.",
    "team_games": "Games the team played, used as the per-game denominator.",
    "fbs_class": (
        "Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, "
        "derived from conference membership. Null for teams outside FBS."
    ),
    "detmer": (
        "Detmer rating -- the composite passing-efficiency measure this pipeline publishes, named "
        "for the college passing-efficiency tradition."
    ),
    "detmergame": "Detmer rating expressed per game.",
    "pctile": "Percentile bucket the row reports, from 0 to 100.",
    # --- adv_passing (ESPN block + model outputs) ------------------------
    "Att": "Pass attempts recorded in the advanced box score.",
    "Comp": "Completed passes recorded in the advanced box score.",
    "CompPct": "Completion percentage from the advanced box score.",
    "Yds": "Passing yards from the advanced box score.",
    "YPA": "Yards per pass attempt.",
    "Int": "Interceptions thrown.",
    "Pass_TD": "Passing touchdowns.",
    "Sck": "Times the passer was sacked.",
    "SR": "Success rate on the passer's plays.",
    "EPA_per_Play": "EPA per play on the passer's plays.",
    "CPOE": "Completion percentage over expected -- actual minus modelled completion rate.",
    "xComp": "Expected completions, summed from the per-play completion model.",
    "xCompPct": "Expected completion percentage from the per-play completion model.",
    "exp_qbr": "Expected QBR for the passer.",
    "era0": "Rule-era indicator for the earliest modelled era.",
    "era1": "Rule-era indicator for the second modelled era.",
    "era2": "Rule-era indicator for the third modelled era.",
    "era3": "Rule-era indicator for the most recent modelled era.",
    "pass_epa": "EPA credited to the player's pass plays.",
    "rush_epa": "EPA credited to the player's rush plays.",
    "sack_epa": "EPA credited to the player's sacks taken.",
    "pen_epa": "EPA attributable to penalties on the player's plays.",
    "qbr_epa": "EPA variant used as an input to the QBR calculation.",
    "pos_team_id": "ESPN team id of the team on offense. Present for every season 2004+.",
    # --- model_pbp -------------------------------------------------------
    "completion_prob": "Modelled probability the pass is completed.",
    "cp_model_version": "Version of the completion-probability model that scored the play.",
    "ep_model_version": "Version of the expected-points model that scored the play.",
    "wp_model_version": "Version of the win-probability model that scored the play.",
    "model_pbp_version": "Version of the model-scored play-by-play build.",
    "scored_date": "Date on which the play was scored by the models.",
    # --- player_box (ESPN camelCase stat names) --------------------------
    "adjQBR": "Adjusted Total QBR for the quarterback.",
    "completions/passingAttempts": "Completions and pass attempts, as ESPN's combined string.",
    "fieldGoalsMade/fieldGoalAttempts": "Field goals made and attempted, as ESPN's combined string.",
    "extraPointsMade/extraPointAttempts": "Extra points made and attempted, as ESPN's combined string.",
    "fieldGoalPct": "Field-goal percentage.",
    "longFieldGoalMade": "Longest field goal made, in yards.",
    "totalKickingPoints": "Total points scored by kicking.",
    "passingYards": "Net passing yards gained.",
    "passingTouchdowns": "Passing touchdowns.",
    "yardsPerPassAttempt": "Yards gained per pass attempt.",
    "rushingYards": "Net rushing yards gained.",
    "rushingAttempts": "Rushing attempts.",
    "rushingTouchdowns": "Rushing touchdowns.",
    "yardsPerRushAttempt": "Yards gained per rushing attempt.",
    "longRushing": "Longest rush of the game, in yards.",
    "receivingYards": "Receiving yards gained.",
    "receivingTouchdowns": "Receiving touchdowns.",
    "yardsPerReception": "Yards gained per reception.",
    "longReception": "Longest reception of the game, in yards.",
    "interceptionYards": "Yards returned on interceptions.",
    "interceptionTouchdowns": "Touchdowns scored on interception returns.",
    "punts": "Punts attempted.",
    "puntYards": "Total punt yards.",
    "longPunt": "Longest punt of the game, in yards.",
    "grossAvgPuntYards": "Gross average yards per punt, before return yardage.",
    "puntsInside20": "Punts downed inside the opponent 20-yard line.",
    "touchbacks": "Punts or kickoffs that resulted in a touchback.",
    "puntReturns": "Punt returns attempted.",
    "puntReturnYards": "Yards gained on punt returns.",
    "puntReturnTouchdowns": "Touchdowns scored on punt returns.",
    "yardsPerPuntReturn": "Yards gained per punt return.",
    "longPuntReturn": "Longest punt return of the game, in yards.",
}

CROSSWALK = {
    "espn_team_id": "ESPN team id for the crosswalk row.",
    "fox_team_id": "Fox Sports team id for the same team.",
    "yahoo_team_id": "Yahoo Sports team id for the same team.",
    "cfbd_team_id": "collegefootballdata.com team id for the same team.",
    "espn_game_id": "ESPN game id for the crosswalk row.",
    "fox_game_id": "Fox Sports game id for the same game.",
    "yahoo_game_id": "Yahoo Sports game id for the same game.",
    "cfbd_game_id": "collegefootballdata.com game id for the same game.",
}


def describe(col: str) -> str | None:
    if col in STATIC:
        return STATIC[col]
    if col in CROSSWALK:
        return CROSSWALK[col]

    # play_participants grid: {role}_player_{id|ids|name|names}
    m = re.fullmatch(r"(.+)_player_(id|ids|name|names)", col)
    if m and m.group(1) in ROLE:
        role, kind = ROLE[m.group(1)], m.group(2)
        if kind == "id":
            return f"ESPN athlete id of {role} -- the FIRST participant in that role on the play."
        if kind == "name":
            return f"Display name of {role} -- the FIRST participant in that role on the play."
        what = "athlete ids" if kind == "ids" else "display names"
        return (
            f"List of the {what} of EVERY participant credited as {role} on the play, so multi-entry "
            "roles such as split sacks or gang tackles are not collapsed to one."
        )

    # percentile/summary metrics, optionally _rank suffixed
    m = re.fullmatch(r"(.+?)(_rank)?", col)
    if m and m.group(1) in METRIC:
        p = METRIC[m.group(1)]
        if m.group(2):
            return f"National rank of the team's {p}, where 1 is best."
        return f"{p[0].upper()}{p[1:]}."
    # bare *_rank over a static stat (yards_rank, success_rank, ...)
    m = re.fullmatch(r"(.+)_rank", col)
    if m:
        base = m.group(1)
        if base in METRIC:
            return f"National rank of the team's {METRIC[base]}, where 1 is best."
        alias = {
            "yards": "total yards",
            "success": "success rate across the team plays",
            "comppct": "completion percentage",
        }
        if base in alias:
            return f"National rank of the team's {alias[base]}, where 1 is best."
        if base in STATIC:
            return f"National rank of the team's {STATIC[base].rstrip('.').lower()}, where 1 is best."
    return None


def main() -> None:
    import yaml

    from tools.codegen import extract_residual_columns as x

    rows = [r for r in x.deferred_columns() if r["league"] == "cfb"]
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    targets = sorted({r["schema"] for r in rows})
    out, unparsed = {}, []
    for t in targets:
        got = {}
        pct = t == "load_cfb_percentiles"
        for c in schemas.get(t, []):
            d = describe(c["name"])
            if d and pct and c["name"] in METRIC:
                # this table reports DISTRIBUTION cut-points, not a team value
                d = f"Value of {METRIC[c['name']]} at the percentile this row reports."
            if d:
                got[c["name"]] = d
        if got:
            out[t] = got
    covered = {(t, c) for t, v in out.items() for c in v}
    for r in rows:
        if (r["schema"], r["col"]) not in covered:
            unparsed.append(f"{r['schema']}.{r['col']}")
    print(f"composed {sum(len(v) for v in out.values())}; still-uncovered {len(unparsed)}")
    for u in sorted(unparsed)[:40]:
        print(f"   {u}")
    with open("_cfbrest_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            {k: dict(sorted(v.items())) for k, v in out.items()}, fh, sort_keys=True, allow_unicode=True, width=120
        )
    print("wrote _cfbrest_descs.yaml")


if __name__ == "__main__":
    main()
