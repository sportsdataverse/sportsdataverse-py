"""Compose descriptions for the cfb team_summaries column grid.

The schema is a combinatorial grid, not 378 independent concepts:

    {base}_{off|def|margin}[_{pass|rush}][_rank]

Every base metric below is transcribed from the PRODUCER
(cfbfastR-cfb-data/python/cfb_data_build/team_summaries.py::_summarize_team), so
the text states what is actually computed. Where a base is the mean of an
upstream pbp/ESPN flag, the description says so rather than asserting a
threshold this script cannot verify.

Rank direction is verified from the producer: offense is ranked with
ascending=False and defense with ascending=True (and the "lower is better"
metrics flip again), so rank 1 is BEST for that side of the ball in every case.
Margins rank descending, so 1 = largest margin.

Anything that does not parse is reported and left BLANK -- never invented.
"""

from __future__ import annotations

import re
import sys

sys.path.insert(0, ".")

# base -> (noun phrase, "higher"|"lower"|None for better-direction commentary)
BASES: dict[str, str] = {
    "plays": "plays run",
    "playsgame": "plays run per game",
    "playsdrive": "plays run per drive",
    "drives": "offensive drives",
    "drivesgame": "drives per game",
    "passrate": "share of plays that were pass plays",
    "rushrate": "share of plays that were rush plays",
    "TEPA": "total EPA summed over every play",
    "EPAplay": "EPA per play",
    "EPAdrive": "EPA per drive (total EPA divided by drives)",
    "EPAgame": "EPA per game (total EPA divided by games)",
    "early_down_EPA": "EPA per early-down play",
    "nonExplosiveEpaPerPlay": "EPA per play with explosive plays excluded",
    "yards": "total yards gained",
    "yardsplay": "yards gained per play",
    "yardsgame": "yards gained per game",
    "yardsdrive": "yards gained per drive",
    "success": "success rate -- the share of plays flagged as successful by EPA",
    "red_zone_success": "success rate on red-zone plays",
    "third_down_success": "success rate on third-down plays",
    "late_down_success": "success rate on late-down plays",
    "third_down_distance": "average yards to go on third down",
    "havoc": "havoc rate -- the share of plays carrying the defensive-disruption flag",
    "explosive": "explosive-play rate -- the share of plays carrying the explosive flag",
    "play_stuffed": "stuffed-play rate -- the share of plays carrying the stuffed flag",
    "line_yards": "average line yards credited to the offensive line on rushes",
    "opportunity_rate": "opportunity rate -- the share of rushes carrying the opportunity flag",
    "start_position": "average drive start position, measured in yards from the opponent goal line",
}

SIDE = {
    "off": "with the team on offense",
    "def": "with the team on defense (i.e. allowed to opponents)",
}
PHASE = {"pass": " on pass plays", "rush": " on rush plays"}

_RE = re.compile(r"^(?P<base>.+?)_(?P<side>off|def|margin)(?:_(?P<phase>pass|rush))?(?P<rank>_rank)?$")

# Columns outside the {base}_{side} grid. Each is transcribed from its producer:
#   adj_*/net/strength/valid_games -> sportsdataverse/cfb/cfb_adjusted_epa.py
#   available/total_*_yards        -> team_summaries.py (drive aggregation)
#   fbs_class                      -> team_summaries.py::prepare_for_write
_ADJ = (
    "opponent-adjusted EPA per play from the ridge (RAPM-style) regression on offense/defense "
    "team indicators plus home field -- cfbfastR's adjust_epa adjustment, fit in-sample across "
    "the season, so the value is descriptive of that window rather than predictive"
)
_AVAIL = (
    "Available yards are the yards a drive could theoretically gain, summed from each drive's "
    "starting distance to the opponent goal line"
)
EXTRA: dict[str, str] = {
    "adj_off_epa": f"Offensive {_ADJ}.",
    "adj_def_epa": f"Defensive {_ADJ}. Lower is better -- it is EPA allowed.",
    "net_adj_epa": "Net opponent-adjusted EPA per play: adj_off_epa minus adj_def_epa. Higher is better.",
    "adj_off_epa_rank": "National rank of the team's adj_off_epa, where 1 is best.",
    "adj_def_epa_rank": "National rank of the team's adj_def_epa, where 1 is best (fewest EPA allowed).",
    "net_adj_epa_rank": "National rank of the team's net_adj_epa, 1 = largest net adjusted EPA.",
    "off_strength_faced": (
        "Average opponent-defense strength the team's offense faced, taken as the mean of the "
        "ridge's defensive coefficients across its opponents. Higher means a tougher slate."
    ),
    "def_strength_faced": (
        "Average opponent-offense strength the team's defense faced, taken as the mean of the "
        "ridge's offensive coefficients across its opponents. Higher means a tougher slate."
    ),
    "valid_games": (
        "Number of the team's games that produced both an offensive and a defensive adjusted-EPA "
        "value; teams below two valid games are dropped from the adjusted ratings."
    ),
    "fbs_class": (
        "Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, "
        "derived from conference membership (Notre Dame is classified with the power group). Null "
        "for teams outside FBS."
    ),
    "through_week": (
        "Regular-season week this cumulative snapshot covers -- the row reflects the team's state "
        "through the end of that week. One asset holds every week, so filter on this column."
    ),
    "total_available_yards_off": f"{_AVAIL}. Total available yards on the team's own drives.",
    "total_available_yards_def": f"{_AVAIL}. Total available yards on drives the team defended.",
    "total_gained_yards_off": "Total yards the team actually gained across its own drives.",
    "total_gained_yards_def": "Total yards the team allowed across the drives it defended.",
    "available_yards_pct_off": (
        "Share of available yards the team's offense actually gained (total_gained_yards_off "
        "divided by total_available_yards_off). Higher is better."
    ),
    "available_yards_pct_def": (
        "Share of available yards the team's defense allowed opponents to gain. Lower is better."
    ),
    "available_yards_pct_off_rank": "National rank of the team's offensive available-yards share, where 1 is best.",
    "available_yards_pct_def_rank": "National rank of the team's defensive available-yards share, where 1 is best.",
    "total_available_yards_margin": "Available yards on the team's own drives minus available yards on drives it defended.",
    "total_gained_yards_margin": "Yards the team gained minus yards it allowed.",
    "available_yards_pct_margin": (
        "Available-yards share gained by the offense minus the share allowed by the defense. Higher is better."
    ),
    "total_available_yards_margin_rank": "National rank of total_available_yards_margin, 1 = largest margin.",
    "total_gained_yards_margin_rank": "National rank of total_gained_yards_margin, 1 = largest margin.",
    "available_yards_pct_margin_rank": "National rank of available_yards_pct_margin, 1 = largest margin.",
}


def _sentence_case(s: str) -> str:
    """Upper-case the first letter WITHOUT touching the rest.

    str.capitalize() lower-cases the tail, which turns the "EPA per play" nouns
    into "Epa per play" -- it destroys every acronym in the vocabulary.
    """
    return s[:1].upper() + s[1:] if s else s


def describe(col: str) -> str | None:
    if col in EXTRA:
        return EXTRA[col]
    m = _RE.match(col)
    if not m:
        return None
    base, side, phase, rank = m["base"], m["side"], m["phase"], m["rank"]
    if base not in BASES:
        return None
    noun, ph = BASES[base], PHASE.get(phase or "", "")

    if side == "margin":
        if base == "start_position":
            body = (
                "Field-position margin: the team's own average starting field position minus the "
                "average starting field position it allowed, both measured as yards gained from "
                "their own goal line. Positive means the team started closer to scoring than its "
                "opponents"
            )
        else:
            body = f"Margin in {noun}{ph}: the team's offensive value minus the value it allowed on defense"
        return f"{body}. National rank of that margin, 1 = largest." if rank else f"{body}."

    if rank:
        return f"National rank of the team's {noun}{ph} {SIDE[side]}, where 1 is best."
    return f"{_sentence_case(noun)}{ph}, {SIDE[side]}."


def main() -> None:
    """Enumerate from loader_schemas.yaml, NOT from deferred_columns().

    deferred_columns() shrinks as descriptions land, so driving off it makes the
    generator non-idempotent -- a second run sees zero work and a re-merge would
    wipe the block it just wrote. The declared schema is the stable source.
    """
    import pathlib

    import yaml

    targets = ("load_cfb_team_summaries", "load_cfb_team_summaries_weekly")
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    out: dict[str, dict[str, str]] = {t: {} for t in targets}
    unparsed: list[str] = []
    for t in targets:
        for c in schemas[t]:
            col = c["name"]
            d = describe(col)
            if d is None:
                unparsed.append(f"{t}.{col}")
                continue
            out[t][col] = d

    total = sum(len(v) for v in out.values())
    print(f"composed {total} descriptions across {len(targets)} schemas")
    print(f"UNPARSED (left blank, never invented): {len(unparsed)}")
    for u in sorted(set(c.split(".", 1)[1] for c in unparsed))[:40]:
        print(f"   {u}")

    import yaml

    with open("_summary_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(out, fh, sort_keys=True, allow_unicode=True, width=120)
    print("wrote _summary_descs.yaml")


if __name__ == "__main__":
    main()
