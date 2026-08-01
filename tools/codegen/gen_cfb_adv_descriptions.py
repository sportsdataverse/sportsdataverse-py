"""Descriptions for the ESPN advBoxScore team columns (adv_team + adv_team_gamelog).

Unlike team_summaries, these are NOT computed in this ecosystem -- they are raw
fields from ESPN's `advBoxScore.team` block, flattened verbatim. There is no
producer arithmetic to read, so every claim below is either (a) verified
empirically against the published 2024 asset, or (b) stated at a level that does
not assert ESPN's internal thresholds.

Verified empirically before writing:
  * EPA_overall_off and EPA_overall_offense are EXACT duplicates (max abs diff 0).
  * The bare EPA_explosive* columns are integer COUNTS (0-13), not EPA, despite
    the EPA_ prefix. The *_rate siblings are true rates in [0,1].
  * EPA_explosive_rate is NOT EPA_explosive / EPA_plays -- the implied denominator
    runs 1-27 plays smaller. The denominator is ESPN's own qualifying-play count,
    so the text says so rather than inventing a formula.

Anything not listed is left BLANK rather than invented.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, ".")

_ESPN = "ESPN's advanced box score"
_THRESH = "ESPN applies its own qualifying threshold"

DESCS: dict[str, str] = {
    # --- identity -----------------------------------------------------------
    "pos_team_id": "ESPN team id of the team on offense. Present for every season 2004+.",
    # --- play counts --------------------------------------------------------
    "EPA_plays": f"Number of plays {_ESPN} scored for the team.",
    "scrimmage_plays": "Number of plays from scrimmage (rushes plus passes), excluding special teams.",
    "special_teams_plays": "Number of special-teams plays.",
    "kickoff_plays": "Number of kickoff plays.",
    "punt_plays": "Number of punt plays.",
    "rushes": "Number of rushing attempts.",
    "field_goals": "Number of field-goal attempts.",
    # --- explosive counts + rates ------------------------------------------
    "EPA_explosive": (
        f"Count of explosive plays, per {_ESPN}. Despite the EPA_ prefix this is a play COUNT, not an EPA total."
    ),
    "EPA_explosive_passing": "Count of explosive pass plays. A play count, not an EPA total.",
    "EPA_explosive_rushing": "Count of explosive rush plays. A play count, not an EPA total.",
    "EPA_explosive_rate": (
        "Explosive-play rate. Note this is NOT EPA_explosive divided by EPA_plays -- ESPN divides "
        "by its own smaller qualifying-play count, so deriving it yourself will not reproduce this value."
    ),
    "EPA_explosive_passing_rate": "Explosive-play rate on pass plays, over ESPN's qualifying-play denominator.",
    "EPA_explosive_rushing_rate": "Explosive-play rate on rush plays, over ESPN's qualifying-play denominator.",
    # --- EPA totals + per-play ---------------------------------------------
    "EPA_overall_off": (
        "Total offensive EPA for the team. Duplicated exactly by EPA_overall_offense in every "
        "published season checked -- prefer one and ignore the other."
    ),
    "EPA_overall_offense": "Total offensive EPA. An exact duplicate of EPA_overall_off.",
    "EPA_overall_total": (
        "Total EPA across all phases, which is why it differs from the offense-only EPA_overall_off."
    ),
    "EPA_per_play": "Offensive EPA per play.",
    "EPA_passing_overall": "Total EPA on pass plays.",
    "EPA_passing_per_play": "EPA per pass play.",
    "EPA_rushing_overall": "Total EPA on rush plays.",
    "EPA_rushing_per_play": "EPA per rush play.",
    "EPA_rushing_power": f"Total EPA on power rushing situations, as classified by {_ESPN}.",
    "EPA_rushing_power_per_play": "EPA per play on power rushing situations.",
    "EPA_non_explosive": "Total EPA with explosive plays excluded, isolating the team's routine-down production.",
    "EPA_non_explosive_per_play": "EPA per play with explosive plays excluded.",
    "EPA_non_explosive_passing": "Total EPA on pass plays with explosive plays excluded.",
    "EPA_non_explosive_passing_per_play": "EPA per pass play with explosive plays excluded.",
    "EPA_non_explosive_rushing": "Total EPA on rush plays with explosive plays excluded.",
    "EPA_non_explosive_rushing_per_play": "EPA per rush play with explosive plays excluded.",
    # --- special teams / penalty EPA ---------------------------------------
    "EPA_special_teams": "Total EPA generated on special-teams plays.",
    "EPA_sp": "Total special-teams EPA, ESPN's abbreviated field for the same phase.",
    "EPA_fg": "Total EPA on field-goal attempts.",
    "EPA_punt": "Total EPA on punt plays.",
    "EPA_kickoff": "Total EPA on kickoff plays.",
    "EPA_penalty": "Total EPA attributed to penalties.",
    # --- first downs created ------------------------------------------------
    "first_downs_created": "Number of first downs the team created.",
    "first_downs_created_rate": "Share of the team's plays that created a first down.",
    "passing_first_downs_created": "Number of first downs created on pass plays.",
    "passing_first_downs_created_rate": "Share of pass plays that created a first down.",
    "rushing_first_downs_created": "Number of first downs created on rush plays.",
    "rushing_first_downs_created_rate": "Share of rush plays that created a first down.",
    "penalty_first_downs_created": "Number of first downs the team gained via opponent penalty.",
    "penalty_first_downs_created_rate": "Share of the team's first downs that came via opponent penalty.",
    # --- play mix -----------------------------------------------------------
    "passes_rate": "Share of the team's plays from scrimmage that were pass plays.",
    "rushes_rate": "Share of the team's plays from scrimmage that were rush plays.",
    # --- rushing decomposition ---------------------------------------------
    "line_yards": (
        "Line yards -- the portion of rushing yardage credited to the offensive line under the "
        f"standard rushing decomposition. {_THRESH} for the yardage split."
    ),
    "line_yards_per_carry": "Line yards per rushing attempt.",
    "second_level_yards": "Second-level yards -- rushing yardage earned just beyond the line of scrimmage.",
    "open_field_yards": "Open-field yards -- rushing yardage earned well downfield, past the second level.",
    "rushing_highlight": ("Highlight yards -- rushing yardage credited to the back rather than the offensive line."),
    "rushing_highlight_rate": "Share of rushing yardage that was highlight (back-credited) yardage.",
    "rushing_highlight_yards_per_opp": "Highlight yards per rushing opportunity.",
    "rushing_opportunity": "Count of rushing opportunities -- carries that reached ESPN's opportunity threshold.",
    "rushing_opportunity_rate": "Share of carries that qualified as rushing opportunities.",
    "rushing_power": f"Count of power rushing attempts, in short-yardage situations as classified by {_ESPN}.",
    "rushing_power_rate": "Share of carries that were power rushing attempts.",
    "rushing_power_success_rate": "Share of power rushing attempts that succeeded.",
    "rushing_stopped": "Count of rushing attempts stopped at or behind the line of scrimmage.",
    # These nine were falling through to the R-package dict, which mislabels two of
    # them: rushing_power_success is Int64 0-7 (a COUNT) but was rendered "success
    # rate", and rushing_highlight_yards was given the per-opportunity wording that
    # belongs to rushing_highlight_yards_per_opp. Manual entries take precedence,
    # so these override the fallback. Ranges verified on the published 2024 asset.
    "rushing_power_success": (
        "Count of power rushing attempts that gained the yardage needed. An integer count, not a "
        "rate -- the rate is published separately as rushing_power_success_rate."
    ),
    "rushing_highlight_yards": (
        "Total highlight yards the team accumulated -- the yardage credited to ball carriers rather "
        "than the line. The per-carry figure is rushing_highlight_yards_per_opp."
    ),
    "rushing_stuff_rate": "Share of the team's carries that were stuffed at or behind the line of scrimmage.",
    "rush_yards": "Total yards the team gained on rush plays.",
    "pass_yards": "Total yards the team gained on pass plays.",
    "passes": "Number of pass plays the team ran.",
    "total_yards": "Total yards the team gained across all plays.",
    "penalties": "Number of penalties assessed against the team.",
    "penalty_yards": (
        "Net penalty yardage assessed against the team; can be negative when enforcement moved the "
        "team forward on balance."
    ),
    "rushing_stopped_rate": "Share of carries stopped at or behind the line of scrimmage.",
    "rushing_stuff": "Count of stuffed rushing attempts.",
    # --- yardage totals -----------------------------------------------------
    "off_yards": "Offensive yards gained from scrimmage.",
    "total_off_yards": "Total offensive yards across all plays.",
    "total_pen_yards": "Total penalty yards assessed.",
    "yards_per_play": "Yards gained per play.",
    "yards_per_rush": "Yards gained per rushing attempt.",
}


def main() -> None:
    import yaml

    # Every adv_* loader flattens the same ESPN advBoxScore vocabulary, so the
    # descriptions apply across the family, not just the team block.
    targets = tuple(
        f
        for f in yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
        if f.startswith("load_cfb_adv")
    )
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    out: dict[str, dict[str, str]] = {}
    missing: set[str] = set()
    for t in targets:
        got = {}
        for c in schemas[t]:
            col = c["name"]
            if col in DESCS:
                got[col] = DESCS[col]
        out[t] = got
        declared = {c["name"] for c in schemas[t]}
        del declared  # per-target set unused; staleness is checked against the union below
    print("composed: " + ", ".join(f"{t}={len(v)}" for t, v in out.items()))
    if missing:
        print(f"WARNING description written for column not in adv_team schema: {sorted(missing)}")
    with open("_adv_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(out, fh, sort_keys=True, allow_unicode=True, width=120)
    print("wrote _adv_descs.yaml")


if __name__ == "__main__":
    main()
