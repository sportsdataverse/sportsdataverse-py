"""Compose descriptions for the cfb team_summaries column grid.

The schema is a combinatorial grid, not 378 independent concepts:

    {base}_{off|def|margin}[_{pass|rush}][_rank|_n]

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
    "opportunity_rate": "opportunity rate -- opportunity-flagged rushes as a share of all plays",
    "start_position": "average drive start position, measured in yards from the opponent goal line",
}

SIDE = {
    "off": "with the team on offense",
    "def": "with the team on defense (i.e. allowed to opponents)",
}
PHASE = {"pass": " on pass plays", "rush": " on rush plays"}

# Bases whose meaning changes by phase split. opportunity_run is
# (rush == 1) & (yds_rushed >= 4) and False -- not null -- on every other play,
# so the producer's mean runs over ALL plays in the split, not over rushes.
PHASE_NOUNS: dict[tuple[str, str | None], str] = {
    ("opportunity_rate", "pass"): "opportunity rate (always 0: only rushes carry the opportunity flag)",
    ("opportunity_rate", "rush"): "opportunity rate -- the share of rushes carrying the opportunity flag",
}

_RE = re.compile(r"^(?P<base>.+?)_(?P<side>off|def|margin)(?:_(?P<phase>pass|rush))?(?P<suffix>_rank|_n)?$")

#: what a team-grid ``_n`` counts -- transcribed from cfbfastR-cfb-data
#: team_summaries.py _TEAM_MEAN_SOURCES / _TEAM_RATIO_DENOMINATORS. It is the
#: split's row count (or n_games/n_drives), computed inside the SAME
#: group_by(team-side[, phase]) as the metric itself -- a team with zero rows
#: in that split has no aggregate row at all, so every column including this
#: one comes back null after the join to the full team roster (see the
#: null-semantics sentence composed below).
N_OF: dict[str, str] = {
    "passrate": "plays",
    "rushrate": "plays",
    "havoc": "plays",
    "explosive": "plays",
    "EPAplay": "plays",
    "yardsplay": "plays",
    "play_stuffed": "plays",
    "success": "plays",
    "opportunity_rate": "plays",
    "red_zone_success": "red-zone plays",
    "third_down_success": "third-down plays",
    "third_down_distance": "third-down plays",
    "late_down_success": "third- and fourth-down plays",
    "early_down_EPA": "early-down plays",
    "start_position": "plays with a known drive start",
    "nonExplosiveEpaPerPlay": "non-explosive plays",
    "line_yards": "rushes",
    "playsgame": "games",
    "EPAgame": "games",
    "yardsgame": "games",
    "drivesgame": "games",
    "EPAdrive": "drives",
    "yardsdrive": "drives",
    "playsdrive": "drives",
}

# Columns outside the {base}_{side} grid. Each is transcribed from its producer:
#   adj_*/net/strength/valid_games -> sportsdataverse/cfb/cfb_adjusted_epa.py
#   available/total_*_yards        -> team_summaries.py (drive aggregation)
#   fbs_class                      -> team_summaries.py::prepare_for_write
#   pts_per_opp_*                  -> team_summaries.py::_drives / _drive_owners
#   turnovers_* / turnover_margin  -> team_summaries.py::_summarize_team, summaries_input.game_giveaways
# explosive_margin(_rank) parses on the grid below (explosive_off - explosive_def).
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
    # cfb_adjusted_epa: off_ = mean(adjmodelOff), joined on the offense the DEFENSE faced;
    # def_ = mean(adjmodelDef), the EPA/play the opposing defenses allow -- so lower is tougher.
    "off_strength_faced": (
        "Average strength of the opposing offenses the team's defense faced: the mean over its games of each "
        "opponent's ridge-fitted offensive EPA per play. Higher means a tougher slate. Null when the team has "
        "fewer than two valid games."
    ),
    "def_strength_faced": (
        "Average strength of the opposing defenses the team's offense faced: the mean over its games of the "
        "EPA per play each opponent's defense is fitted to allow. Lower means a tougher slate. Null when the "
        "team has fewer than two valid games."
    ),
    "off_strength_faced_rank": (
        "National rank of off_strength_faced, where 1 = toughest slate (the strongest opposing offenses). Ties "
        "share the average rank. Null when off_strength_faced is null: unranked, not last."
    ),
    "def_strength_faced_rank": (
        "National rank of def_strength_faced, where 1 = toughest slate (the strongest opposing defenses, i.e. "
        "the lowest def_strength_faced). Ties share the average rank. Null when def_strength_faced is null: "
        "unranked, not last."
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

# Five Factors (cfbfastR-cfb-data#103). Whole-team only: no _pass/_rush split.
_PPO = (
    "Points per scoring opportunity. A scoring opportunity is a drive with a run or pass snap at or inside "
    "the opponent 40, charged only to the drive's owner (ESPN's drive team); it scores its ESPN drive "
    "result, 7 for a touchdown, 3 for a field goal and 0 otherwise"
)
_TOV = (
    "interceptions and lost fumbles on every play, special teams included (a muffed punt counts against "
    "the return team)"
)
for _s, _who, _null, _best in (
    ("off", "the team's own drives", "the team had", "most points per opportunity"),
    ("def", "opponents' drives against the team's defense", "opponents had", "fewest points allowed per opportunity"),
):
    EXTRA |= {
        f"pts_per_opp_{_s}": f"{_PPO}. Counted on {_who}. Null when {_null} no scoring opportunity.",
        f"pts_per_opp_{_s}_rank": (
            f"National rank of pts_per_opp_{_s}, where 1 is best ({_best}). "
            f"Null when pts_per_opp_{_s} is null: unranked, not last."
        ),
        f"pts_per_opp_{_s}_n": (
            f"Sample size behind pts_per_opp_{_s}: the number of scoring opportunities on {_who}. "
            f"0 when there were none, and pts_per_opp_{_s} is then null."
        ),
        f"turnovers_{_s}_n": f"Sample size behind turnovers_{_s}: the number of games it is computed over.",
    }
EXTRA |= {
    "pts_per_opp_margin": "pts_per_opp_off minus pts_per_opp_def. Null when either side is null. Higher is better.",
    "pts_per_opp_margin_rank": (
        "National rank of pts_per_opp_margin, 1 = largest margin. Null when pts_per_opp_margin is null: "
        "unranked, not last."
    ),
    "turnovers_off": f"Giveaways per game: {_TOV}. Lower is better.",
    "turnovers_def": "Takeaways per game: the opponents' giveaways, counted the same way. Higher is better.",
    "turnovers_off_rank": "National rank of turnovers_off, where 1 is best (fewest giveaways per game).",
    "turnovers_def_rank": "National rank of turnovers_def, where 1 is best (most takeaways per game).",
    "turnover_margin": (
        "Turnover margin per game: turnovers_def minus turnovers_off (takeaways minus giveaways). Higher is "
        "better. Spelled singular; there is no turnovers_margin column."
    ),
    "turnover_margin_rank": "National rank of turnover_margin, 1 = largest margin.",
}

# Drive efficiency (CFBE-1d). Whole-team only. Same owner rule and drive.result scoring as pts_per_opp.
_PPD = (
    "Points per drive. A drive is one with at least one run or pass snap (kneel-downs excluded) in an "
    "FBS-vs-FBS game, charged only to its owner: ESPN's drive team, or the team with the most snaps in it "
    "when that label fits none of its snaps. It scores its ESPN drive result, 7 for a touchdown, 3 for a "
    'field goal and 0 otherwise. A drive ESPN labels as a return touchdown ("INT TD", "PUNT RETURN TD") '
    'scores 0, but a return or defensive touchdown on a drive ESPN labels plain "TD" is credited to the '
    "drive's owner, the team that gave it up"
)
for _s, _who, _null, _best, _dir in (
    ("off", "the team's own drives", "the team owned", "most points per drive", "Higher"),
    (
        "def",
        "opponents' drives against the team's defense",
        "opponents owned",
        "fewest points allowed per drive",
        "Lower",
    ),
):
    EXTRA |= {
        f"pts_per_drive_{_s}": f"{_PPD}. Counted on {_who}. {_dir} is better. Null when {_null} no drive.",
        f"pts_per_drive_{_s}_rank": (
            f"National rank of pts_per_drive_{_s}, where 1 is best ({_best}). Ties share the average rank. "
            f"Null when pts_per_drive_{_s} is null: unranked, not last."
        ),
        f"pts_per_drive_{_s}_n": (
            f"Sample size behind pts_per_drive_{_s}: the number of {_who} it is computed over. 0 when there were "
            f"none, and pts_per_drive_{_s} is then null. Can sit slightly below drives_{_s}, which also counts "
            "drive ids holding only a stray snap."
        ),
    }
EXTRA |= {
    "pts_per_drive_margin": (
        "pts_per_drive_off minus pts_per_drive_def: points scored per drive minus points allowed per drive. "
        "Null when either side is null. Higher is better."
    ),
    "pts_per_drive_margin_rank": (
        "National rank of pts_per_drive_margin, 1 = largest margin. Ties share the average rank. Null when "
        "pts_per_drive_margin is null: unranked, not last."
    ),
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
    base, side, phase, suffix = m["base"], m["side"], m["phase"], m["suffix"]
    if base not in BASES:
        return None
    noun, ph = PHASE_NOUNS.get((base, phase), BASES[base]), PHASE.get(phase or "", "")
    rank = suffix == "_rank"

    if suffix == "_n":
        if side == "margin" or base not in N_OF:
            return None
        # line_yards is null on every non-rush play, so its pass split has nothing to average
        always_0 = (
            " Always 0 here: line yards are credited only on rushes." if (base, phase) == ("line_yards", "pass") else ""
        )
        return (
            f"Sample size behind {col[:-2]}: the number of {N_OF[base]} it is computed over{ph}, {SIDE[side]}. "
            f"Null when the team has no rows in that split; read it as 0.{always_0}"
        )

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
