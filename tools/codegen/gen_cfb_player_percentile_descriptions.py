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


def rank_desc(noun: str, subject: str, population: str) -> str:
    return f"Rank of the {subject}'s {noun} among {population}, where 1 is best."


def pct_desc(noun: str, population: str) -> str:
    return (
        f"Percentile (0-100) of {noun} among {population}, where 100 is best. "
        f"Direction is already encoded in the matching rank, so a lower-is-better "
        f"metric still scores 100 at its best. Null when the metric is null, and "
        f"null rows are excluded from the denominator."
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
        declared = {
            (e["name"] if isinstance(e, dict) else str(e)) for e in schemas.get(t, [])
        }
        for col in sorted(declared):
            if not col.endswith("_rank"):
                continue
            base = col[: -len("_rank")]
            noun = _noun(curated.get(col))
            if noun is None:
                unparsed.append(f"{t}.{col}")
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

    total = sum(len(v) for v in out.values())
    ranks = sum(1 for v in out.values() for c in v if c.endswith("_rank"))
    pcts = sum(1 for v in out.values() for c in v if c.endswith("_pct"))
    print(f"composed {total} descriptions across {len(TARGETS)} schemas ({ranks} corrected _rank, {pcts} new _pct)")
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
