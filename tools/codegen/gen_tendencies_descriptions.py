"""Compose descriptions for the split families of the six tendencies loaders.

``sportsdataverse.football.tendencies`` emits every split as a grid, not as
independent concepts:

    [def_]{plays|passes|epa|successes|pass_rate|epa_per_play|success_rate}_{split}
    [def_]{games|wins|win_rate}_{context}

so one template per family, times a phrase per split, describes all of it. The
split and context phrases are transcribed from the producer (``_offense_counts``
masks; the ``ctx_*`` definitions are the data-repo wrappers'). Columns outside
the grid keep their hand-written text: the merge is additive, existing wins.

Enumerates from loader_schemas.yaml (the stable declared schema), like
``gen_cfb_summary_descriptions.py``, so a re-run is idempotent. The new split
columns are declared there only when the loader schemas are re-captured from
the republished assets; run this, then ``merge_column_descriptions.py
_tendencies_descs.yaml``, right after that re-capture.
"""

from __future__ import annotations

import sys

sys.path.insert(0, ".")

TARGETS = {
    "load_cfb_team_tendencies": "team",
    "load_cfb_coach_tendencies": "coach",
    "load_cfb_coach_careers": "coach",
    "load_nfl_team_tendencies": "team",
    "load_nfl_coach_tendencies": "coach",
    "load_nfl_coach_careers": "coach",
}

_NEUTRAL = (
    "in situation-neutral situations (win probability between 20% and 80%, in the first four quarters, "
    "outside the final two minutes of a half)"
)
#: split -> the phrase that finishes "Plays ..." (``_offense_counts`` masks)
SPLIT_PHRASES: dict[str, str] = {
    "neutral": _NEUTRAL,
    "d1": "on first down",
    "d2": "on second down",
    "d3": "on third down",
    "d4": "on fourth down",
    "early_down": "on first or second down",
    "standard_down": "on standard downs (the play-by-play standard_down flag)",
    "passing_down": "on passing downs (the play-by-play passing_down flag)",
    "leading": "snapped with the offense ahead on the scoreboard",
    "tied": "snapped with the score tied",
    "trailing": "snapped with the offense behind on the scoreboard",
    "first_half": "in the first two quarters",
    "second_half": "in the third and fourth quarters (overtime belongs to neither half)",
    "d3_short": "on third down with 3 or fewer yards to go",
    "d3_medium": "on third down with 4 to 6 yards to go",
    "d3_long": "on third down with 7 or more yards to go",
    "red_zone": "in the red zone (the play-by-play rz_play flag)",
    "own_half": "snapped in the offense's own half (50 or more yards from the opponent end zone)",
    "opp_half": "snapped in the opponent's half (fewer than 50 yards from the opponent end zone)",
    "one_score": "snapped with the offense within 8 points either way",
}
#: context -> its games, as a plural noun (the definitions are the wrappers')
CONTEXT_GAMES: dict[str, str] = {
    "home": "home games",
    "away": "away games",
    "neutral_site": "neutral-site games",
    "vs_ranked": "games against an opponent ranked at kickoff (ESPN's displayed rank)",
    "after_bye": "games played 13 or more days after the team's previous game",
    "opener": "regular-season openers",
    "one_score_game": "games decided by 8 points or fewer",
}
_NULL = " Null when the denominator is 0."
_WIN = "points for above points against, so a tie is not a win"


def _phrase(split: str) -> str | None:
    if split in SPLIT_PHRASES:
        return SPLIT_PHRASES[split]
    return f"in the team's {CONTEXT_GAMES[split]}" if split in CONTEXT_GAMES else None


def _offense(col: str) -> str | None:
    """The offense text for one grid column, or None when ``col`` is off the grid."""
    for fam in ("epa_per_play", "success_rate", "pass_rate", "successes", "passes", "plays", "epa"):
        if col.startswith(f"{fam}_"):
            s = col[len(fam) + 1 :]
            p = _phrase(s)
            if p is None:
                return None
            return {
                "plays": f"Plays {p}.",
                "passes": f"Pass plays {p}.",
                "epa": f"Play EPA summed over the plays {p}.",
                "successes": f"Plays flagged EPA_success (positive EPA) {p}.",
                "pass_rate": f"passes_{s} / plays_{s}: pass rate {p}.{_NULL}",
                "epa_per_play": f"epa_{s} / plays_{s}: EPA per play {p}.{_NULL}",
                "success_rate": f"successes_{s} / plays_{s}: success rate {p}.{_NULL}",
            }[fam]
    for fam in ("win_rate", "games", "wins"):
        if col.startswith(f"{fam}_") and col[len(fam) + 1 :] in CONTEXT_GAMES:
            c = col[len(fam) + 1 :]
            g = CONTEXT_GAMES[c]
            return {
                "games": f"Distinct {g} in which the offense ran at least one standing scrimmage play.",
                "wins": f"Of games_{c}, the {g} the team won ({_WIN}).",
                "win_rate": f"wins_{c} / games_{c}: win rate in the team's {g}.{_NULL}",
            }[fam]
    return None


def describe(col: str, subject: str = "team") -> str | None:
    """Description for a split-grid column; ``subject`` is ``team`` or ``coach``."""
    if not col.startswith("def_"):
        return _offense(col)
    base = col[4:]
    text = _offense(base)
    if text is None:
        return None
    if text.endswith(_NULL):
        return (
            f"Defense-allowed twin of {base}: {text[: -len(_NULL)]} Computed from the def_ counts; "
            "null when the denominator is 0."
        )
    context = any(base.endswith(f"_{c}") for c in CONTEXT_GAMES)
    side = ", with the game context read from the defending team's side (def_ctx_*)" if context else ""
    return (
        f"Defense-allowed twin of {base} -- the same measure over the opposing offenses' plays while this "
        f"{subject}'s defense was on the field{side}: {text[:1].lower()}{text[1:]}"
    )


def main() -> None:
    import pathlib

    import yaml

    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    out: dict[str, dict[str, str]] = {t: {} for t in TARGETS}
    for t, subject in TARGETS.items():
        for c in schemas.get(t) or []:
            d = describe(c["name"], subject)
            if d is not None:
                out[t][c["name"]] = d
    print(f"composed {sum(len(v) for v in out.values())} split descriptions across {len(TARGETS)} schemas")
    with open("_tendencies_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(out, fh, sort_keys=True, allow_unicode=True, width=120)
    print("wrote _tendencies_descs.yaml")


if __name__ == "__main__":
    main()
