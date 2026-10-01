"""Compose `_rank` + `_pct` descriptions for the cfb PLAYER leader loaders.

Sibling of ``gen_cfb_summary_descriptions.py``, which covers the *team* grid
(``load_cfb_team_summaries``). This one covers the three player-grain loaders --
``load_cfb_passing`` / ``load_cfb_rushing`` / ``load_cfb_receiving`` -- which the
team generator's ``targets`` tuple deliberately does not include.

Two jobs.

1. FIX the existing ``_rank`` descriptions. They currently read "National rank
   of the TEAM's ...", copy-pasted from the team grid, and they are wrong: these
   loaders are player-grain. Measured on the published ``cfb_passing_2025``
   asset -- 567 rows, 566 distinct ``player_id``, ``EPAgame_rank`` spanning
   1..137 and varying across passers within a team for 128 of 136 teams. They
   are ranks among qualified PLAYERS.

2. ADD the ``_pct`` siblings introduced by cfbfastR-cfb-data#50.

Both are mechanical, which is the point: the next ranked metric gets correct
text for free instead of another hand-copied line that drifts.

The metric noun is NOT invented here -- it is lifted from the existing curated
``_rank`` description, which was transcribed from the producer. This script only
corrects the SUBJECT of that sentence and derives the percentile sibling. A
``_rank`` description it cannot parse is reported and skipped, never guessed.

Semantics the ``_pct`` text has to carry, all three from the producer
(``team_summaries.py::_attach_leader_ranks`` / ``_pct``):

* the population is QUALIFIERS, not every player -- 137 of 567 passers qualified
  in 2025, so the median qualifier is nothing like the median passer;
* direction lives in ``_rank`` already, so an ascending metric (interceptions,
  sacks taken, fumbles) needs no separate wording -- 100 is always good;
* a null metric yields a null percentile and is excluded from the denominator,
  so "unknown" never renders as "worst".

Output is a side file for the maintainer to merge into
manual_column_descriptions.yaml, matching the sibling generator's contract.
Enumeration is driven off loader_schemas.yaml (the stable declared schema) so
the script is idempotent -- driving off "what is still undescribed" would make a
second run a no-op and a re-merge would wipe the block it just wrote.
"""

from __future__ import annotations

import pathlib
import re

TARGETS = ("load_cfb_passing", "load_cfb_rushing", "load_cfb_receiving")

# Row subject and the ACTUAL qualifier gate per loader, both transcribed from
# the producer (team_summaries.py `min_expr=` at the three _attach_leader_ranks
# call sites). Naming the threshold rather than saying "qualified" is the whole
# point: "qualified" invites a reader to assume the population is every player,
# and it is not -- 137 of 567 passers cleared the gate in 2025, so the median
# qualifier sits nowhere near the median passer. The existing curated text for
# catchpct_rank already does this; the rest should match it.
SUBJECT = {
    "load_cfb_passing": (
        "passer",
        "passers clearing the leaderboard minimum of 14 dropbacks per team game",
    ),
    "load_cfb_rushing": (
        "rusher",
        "rushers clearing the leaderboard minimum of 6.25 plays per team game",
    ),
    "load_cfb_receiving": (
        "receiver",
        "receivers clearing the leaderboard minimum of 1.875 targets per team game",
    ),
}

# Two accepted shapes, so the script is re-runnable against a tree it has
# already been applied to. Matching only the first would make a second run
# report every column as unparsed and compose nothing -- idempotent on the
# INPUT is not the same as idempotent on its own OUTPUT.
#
#   1. team-grid leftover: "National rank of the team's <noun>, where 1 is best."
#   2. this script's own:  "Rank of the <subject>'s <noun> among <pop>, where 1 is best."
_RANK_PATTERNS = (
    re.compile(
        r"^National rank of the team's (?P<noun>.+?),\s*where 1 is best\.?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^Rank of the \w+'s (?P<noun>.+?) among .+?,\s*where 1 is best\.?$",
        re.IGNORECASE,
    ),
)

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml"
MANUAL = ROOT / "tools" / "codegen" / "manual_column_descriptions.yaml"

#: what a player ``_n`` counts, per loader -- transcribed from
#: cfbfastR-cfb-data/python/cfb_data_build/team_summaries.py::PLAYER_SAMPLE_SIZES.
#: CFB's passer ``success``/``yardsplay`` are counted over completions +
#: incompletions (``att`` -- sacks AND interceptions are excluded from that
#: frame). ``comppct`` counts throws including interceptions but excluding
#: sacks (``att + pass_int``); a dropback is ``att + sacked + pass_int``.
_GAMES = {m: "games" for m in ("EPAgame", "yardsgame", "playsgame")}
PLAYER_N_OF: dict[str, dict[str, str]] = {
    "load_cfb_passing": {
        "EPAplay": "dropbacks (completions, incompletions, sacks and interceptions)",
        "yardsdropback": "dropbacks (completions, incompletions, sacks and interceptions)",
        "comppct": "throws (including interceptions, excluding sacks)",
        "success": "completions and incompletions (interceptions and sacks excluded)",
        "yardsplay": "completions and incompletions (interceptions and sacks excluded)",
        "detmer": "games",
        "detmergame": "games",
        **_GAMES,
    },
    "load_cfb_rushing": {"EPAplay": "carries", "success": "carries", "yardsplay": "carries", **_GAMES},
    "load_cfb_receiving": {
        "EPAplay": "targets",
        "success": "targets",
        "yardsplay": "targets",
        "catchpct": "targets",
        **_GAMES,
    },
}

#: (loader, base) pairs whose CFB count differs from the NFL twin's -- the
#: description must say so (program amendment 14). sdv-py has no NFL summaries
#: loader, so the twin is named by its nfl-data table. Transcribed from
#: nfl-data/python/nfl_team_summaries/build.py PLAYER_SAMPLE_SIZES: success is
#: over ``plays`` (dropbacks), yardsplay over ``att`` (thrown balls -- input.py
#: drops sacks from nflfastR's pass_attempt, interceptions stay in).
NFL_NOTES = {
    ("load_cfb_passing", "success"): "nfl-data's nfl_passing table counts its success_n over dropbacks instead.",
    ("load_cfb_passing", "yardsplay"): (
        "nfl-data's nfl_passing table counts its yardsplay_n over pass attempts instead: "
        "interceptions included, sacks still excluded."
    ),
}


def n_desc(base: str, noun: str, subject: str, *, note: str = "") -> str:
    text = f"Sample size behind {base}: the number of {noun} the {subject}'s value is computed over. 0 where {base} is null."
    return f"{text} {note}" if note else text


# Nouns whose wording is team-grid leftover and wrong on a player frame.
# Verified against the producer: summarize_passer/rusher/receiver group by
# player and take `success=pl.col("success").mean()`, so the mean is over that
# PLAYER's plays, not the team's.
NOUN_FIXUPS = {
    "success rate across the team plays": "success rate across their plays",
}


def _noun(existing: str | None) -> str | None:
    """Metric phrase out of an existing ``_rank`` description, else None."""
    if not existing:
        return None
    m = next(
        (m for p in _RANK_PATTERNS if (m := p.match(existing.strip()))),
        None,
    )
    if not m:
        return None
    noun = NOUN_FIXUPS.get((raw := m.group("noun").strip()), raw)
    # A noun still mentioning the team is team-grid wording that has not been
    # verified against the player producer. Report it rather than ship text
    # that says "team" on a per-player row.
    if "team" in noun.lower():
        return None
    return noun


#: the hand-curated player ``_pct`` shape, for ranks ``_noun`` cannot parse
_PCT_NOUN = re.compile(
    r"^Percentile position \(0-100\) of the player's (?P<noun>.+?) among qualifying players that season\.?$"
)


def _pct_noun(existing: str | None) -> str | None:
    """Metric phrase out of a curated ``_pct`` description, else None."""
    m = _PCT_NOUN.match((existing or "").strip())
    return m.group("noun") if m else None


def rank_desc(noun: str, subject: str, population: str) -> str:
    return f"Rank of the {subject}'s {noun} among {population}, where 1 is best."


#: cohort column -> (phrase after the population, the null cases beyond a null metric).
#: Transcribed from cfbfastR-cfb-data team_summaries.py (#126): _attach_cohort_percentiles
#: with MIN_COHORT_PLAYERS = 10 / MIN_COHORT_TEAMS = 5, and _attach_conference_percentiles,
#: which gives FBS Independents no conference cohort.
COHORTS: dict[str, tuple[str, str]] = {
    "position_group": (
        "at the same position group (position_group)",
        "when position_group is null, or when fewer than 10 qualifiers in the group have the metric",
    ),
    "conference": (
        "in the same conference",
        "for an FBS Independent (not a conference), or when fewer than 5 teams in the conference have the metric",
    ),
}


def pct_desc(noun: str, population: str, *, cohort: str | None = None) -> str:
    """The ``_pct`` text; ``cohort`` gives the ``_pos_pct`` / ``_conf_pct`` variant."""
    head = (
        "where 100 is best. Direction is already encoded in the matching rank, so a "
        "lower-is-better metric still scores 100 at its best."
    )
    if cohort is None:
        return (
            f"Percentile (0-100) of {noun} among {population}, {head} Null when the metric "
            f"is null, and null rows are excluded from the denominator."
        )
    phrase, null_cases = COHORTS[cohort]
    return (
        f"Percentile (0-100) of {noun} among {population} {phrase}, {head} Null when the "
        f"metric is null, {null_cases}. Rows with a null metric are excluded from the denominator."
    )


#: the player tables' cohort column (cfbfastR-cfb-data #126, _attach_position_cohorts)
POSITION_GROUP_DESC = (
    "Position group from the season's ESPN roster (espn_cfb_rosters position_abbreviation): QB; "
    "RB (RB and FB); WR; TE; other for any other listed position. Null when the player is not on "
    "the season roster, is listed without a position ('-'), or is listed under two different "
    "groups. The cohort of the _pos_pct columns."
)


def main() -> None:
    import yaml

    schemas = yaml.safe_load(SCHEMAS.read_text(encoding="utf-8"))
    manual = yaml.safe_load(MANUAL.read_text(encoding="utf-8")) or {}

    out: dict[str, dict[str, str]] = {t: {} for t in TARGETS}
    unparsed: list[str] = []
    pending: list[str] = []

    for t in TARGETS:
        subject, population = SUBJECT[t]
        curated = manual.get(t) or {}
        declared = {(e["name"] if isinstance(e, dict) else str(e)) for e in schemas.get(t, [])}
        for col in sorted(declared):
            if col.endswith("_n"):
                base = col[:-2]
                if base not in PLAYER_N_OF[t]:
                    unparsed.append(f"{t}.{col}")
                    continue
                out[t][col] = n_desc(base, PLAYER_N_OF[t][base], subject, note=NFL_NOTES.get((t, base), ""))
                continue
            if not col.endswith("_rank"):
                continue
            base = col[: -len("_rank")]
            noun = _noun(curated.get(col))
            if noun is None:
                unparsed.append(f"{t}.{col}")
                # the _pos_pct sibling can still take its noun from the curated _pct
                pct_noun = _pct_noun(curated.get(f"{base}_pct"))
                if pct_noun and f"{base}_pos_pct" in declared:
                    out[t][f"{base}_pos_pct"] = pct_desc(pct_noun, population, cohort="position_group")
                continue
            out[t][col] = rank_desc(noun, subject, population)
            # Emit the _pct sibling ONLY when the column is actually declared.
            # tests/codegen/test_manual_descriptions.py::test_no_orphan_manual_entries
            # fails on any manual key without a matching schema column, so a
            # description cannot be written ahead of the column existing --
            # runtime tolerates it (the lookup is manual[schema][col]) but the
            # suite deliberately does not. The _pct columns land when
            # cfbfastR-cfb-data#50's rebuilt assets are published and
            # loader_schemas.yaml is re-captured; this generator then emits them
            # on the next run with no edit here.
            pct_col = f"{base}_pct"
            if pct_col in declared:
                out[t][pct_col] = pct_desc(noun, population)
            else:
                pending.append(f"{t}.{pct_col}")
            # the position-group cohort sibling (cfbfastR-cfb-data #126), same rule
            if f"{base}_pos_pct" in declared:
                out[t][f"{base}_pos_pct"] = pct_desc(noun, population, cohort="position_group")
        if "position_group" in declared:
            out[t]["position_group"] = POSITION_GROUP_DESC

    total = sum(len(v) for v in out.values())
    ranks = sum(1 for v in out.values() for c in v if c.endswith("_rank"))
    pcts = sum(1 for v in out.values() for c in v if c.endswith("_pct"))
    ns = sum(1 for v in out.values() for c in v if c.endswith("_n"))
    print(
        f"composed {total} descriptions across {len(TARGETS)} schemas "
        f"({ranks} corrected _rank, {pcts} new _pct, {ns} new _n)"
    )
    print(f"UNPARSED (skipped, never invented): {len(unparsed)}")
    print(f"PENDING _pct (column not declared yet, skipped): {len(pending)}")
    for u in sorted(unparsed)[:40]:
        print(f"   {u}")

    dest = pathlib.Path("_player_percentile_descs.yaml")
    with dest.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(out, fh, sort_keys=True, allow_unicode=True, width=120)
    print(f"wrote {dest}")


if __name__ == "__main__":
    main()
