"""Descriptions for the final tail of uncovered loader return-table columns.

Two groups:

1. NHL localized name keys (.cs/.de/.es/.fi/.fr/.sk/.sv). Verified directly
   against the published 2024 penalties asset rather than assumed:
     lastName.cs   'Hagg'   -> 'Hagg' with diacritics  (default is the ASCII fold)
     lastName.sv   'Holmstrom' -> umlauted, BUT 'Hakanpaa' has them STRIPPED
     firstName.es  'Aliaksei' -> 'Alexei', 'Joshua' -> 'Josh'
   So family-name keys are alternate orthography in EITHER direction and
   given-name keys are frequently a different name entirely -- NOT "locale
   spellings", which is what a first pass wrongly claimed.

2. Individual columns, each profiled on the published asset. Four turned out to
   be CONSTANT or redundant and say so, because a column that carries no
   information should not read like a usable signal.
"""

from __future__ import annotations

import pathlib
import re
import sys

sys.path.insert(0, ".")

LOCALE = {
    "cs": "Czech",
    "de": "German",
    "es": "Spanish",
    "fi": "Finnish",
    "fr": "French",
    "sk": "Slovak",
    "sv": "Swedish",
}

ROLE = {
    "committedByPlayer": "the penalized player",
    "drawnBy": "the opposing player credited with drawing the penalty",
    "servedBy": "the player serving the penalty",
}

STATIC: dict[str, str] = {
    # --- nhl scoring / schedule -------------------------------------------
    "highlightClipFr": (
        "NHL video id of the French-language highlight clip for the goal. Stored as Float64 even "
        "though it is a whole 13-digit identifier, so cast before using it as a key."
    ),
    "highlightClipSharingUrlFr": (
        "Shareable nhl.com URL for the French-language highlight clip; its trailing numeric segment "
        "is the same id carried in highlightClipFr."
    ),
    # load_nhl_schedule availability flags. All six are CONSTANT Boolean markers for
    # which sub-feed blocks the source game record carried, and without explicit
    # entries they fell through to the R-package dict -- which matched same-named
    # FOOTBALL columns and described `scoring` as "TD, FG, safety, two-point
    # conversion" on an NHL schedule.
    "scoring": (
        "CONSTANT true: marks that the source game record carried a scoring-summary block. "
        "It is an availability flag, not a count or a scoring event."
    ),
    "penalties": (
        "CONSTANT true: marks that the source game record carried a penalty-summary block. "
        "It is an availability flag, not a penalty count."
    ),
    "shifts": (
        "CONSTANT false in every published row: the shift-chart block is not carried on this "
        "asset. An availability flag, not a shift count."
    ),
    "three_stars": "CONSTANT true: marks that the source game record carried a three-stars block.",
    "game_info": "CONSTANT true: marks that the source game record carried a game-info block.",
    "linescore": (
        "CONSTANT: true on every published row, so it carries no information as shipped. It marks "
        "that a linescore block existed on the source game record."
    ),
    "scratches": "True when the source game record carried a scratches block for the game.",
    "series_letter": (
        "Playoff series identifier letter, populated only for postseason games (88 of 1,400 rows in "
        "2024) and null for the regular season."
    ),
    # --- cfb ---------------------------------------------------------------
    "def_pos_team_id": "ESPN team id of the team on defense. Present for every season 2004+.",
    "kick_returns_yards": "Total yards the team gained returning kickoffs.",
    "st_turnovers_lost": "Turnovers the team lost on special-teams plays.",
    "espn_sourced": (
        "CONSTANT: true on every published row. It records that the row was built from the ESPN "
        "feed rather than an alternate provider, and no other provider is currently used."
    ),
    "odds_source": (
        "Provenance of the spread and over/under used for the game: summary_pickcenter when ESPN's "
        "own pickcenter carried them, core_odds_api when they came from the live odds endpoint, "
        "default when neither resolved, injected when supplied by an offline rebuild."
    ),
    "draft_team_href": (
        "API link to the team that drafted the player. Sparse: absent entirely from the 2023 and "
        "2024 assets and populated on only a small share of 2025 rows."
    ),
    # --- basketball --------------------------------------------------------
    "official_order": "Position of the official within the game's listed officiating crew.",
    "away_timeout_called": "True when the away team called a timeout on the play.",
    "home_timeout_called": "True when the home team called a timeout on the play.",
    "end_period_seconds_remaining": "Seconds left in the period when the play ended.",
    "box_obpm": "Box-score offensive plus/minus for the player, the offensive half of box BPM.",
    "box_dbpm": "Box-score defensive plus/minus for the player, the defensive half of box BPM.",
    "o_adj_rapm": "Offensive regularized adjusted plus/minus after the ridge opponent adjustment.",
    "d_adj_rapm": "Defensive regularized adjusted plus/minus after the ridge opponent adjustment.",
    "athlete_headshot": "URL of the player's headshot image.",
    "group_short_name": "Short display name of the conference or division grouping the row belongs to.",
    "game_sub_label": (
        "Secondary label for special-event games (e.g. 'NBA Abu Dhabi Game'); an empty string for "
        "ordinary games rather than null."
    ),
    "is_neutral": "True when the game was played at a neutral site.",
    "draft_type": (
        "CONSTANT in the published asset: every row reads 'Draft', so it does not currently "
        "distinguish the main draft from any other selection event."
    ),
    "season_2": (
        "REDUNDANT: duplicates the season column on every published row. It exists because the "
        "upstream feed returns the season under a second key."
    ),
    # --- baseball ----------------------------------------------------------
    "proj_xwoba": "Projected expected weighted on-base average for the batter.",
    "x_woba": "Expected weighted on-base average, derived from batted-ball quality rather than outcomes.",
}


def describe(col: str) -> str | None:
    """Compose the description for one column name, or None if it cannot be grounded.

    Args:
        col: the column name as declared in ``loader_schemas.yaml``.

    Returns:
        The description string, or ``None`` when the column is outside this
        generator's vocabulary. Returning ``None`` is meaningful: ``main`` reports
        those columns rather than emitting an invented description for them.

    Example:
        Resolve a localized NHL name key::

            describe("drawnBy.lastName.cs")
    """
    if col in STATIC:
        return STATIC[col]

    # NHL localized name keys, optionally prefixed by a participant role
    # servedBy exposes its locale keys DIRECTLY (servedBy.cs), with no nested
    # firstName/lastName/name segment, so it needs its own branch.
    m = re.fullmatch(r"servedBy\.(default|cs|de|es|fi|fr|sk|sv)", col)
    if m:
        loc = m.group(1)
        if loc == "default":
            return "Abbreviated name, in the feed's default English locale, of the player serving the penalty."
        return (
            f"Alternate abbreviated name (first initial plus family name) for the player serving the "
            f"penalty, published under the NHL feed's {LOCALE[loc]} key, differing from the default by "
            "diacritics or by an alternate given-name form."
        )

    m = re.fullmatch(r"(?:(committedByPlayer|drawnBy|servedBy)\.)?(firstName|lastName|name)\.(default|\w{2})", col)
    if m:
        role_key, kind, loc = m.groups()
        who = ROLE.get(role_key or "", "the player")
        if loc == "default":
            what = {"firstName": "Given name", "lastName": "Family name", "name": "Abbreviated name"}[kind]
            return f"{what} of {who} as published in the NHL feed's default English locale."
        L = LOCALE.get(loc)
        if not L:
            return None
        if kind == "lastName":
            return (
                f"Alternate rendering of {who}'s family name under the NHL feed's {L} key. It differs from the "
                "default in orthography -- usually restoring diacritics the default folds to ASCII, though for "
                "some names it strips them instead -- so treat it as an alternate spelling, not a canonical one."
            )
        if kind == "firstName":
            return (
                f"Alternate given name for {who} under the NHL feed's {L} key. Verified against the data it is "
                "frequently a different name form rather than a re-spelling (Joshua published as Josh, Aliaksei "
                "as Alexei), so it is not a reliable transliteration of the default."
            )
        return (
            f"Alternate abbreviated name for {who} under the NHL feed's {L} key, differing from the default by "
            "diacritics or by an alternate given-name form."
        )

    # bare servedBy.default / *.sweaterNumber
    if col == "servedBy.default":
        return "Abbreviated name, in the feed's default English locale, of the player serving the penalty."
    m = re.fullmatch(r"(committedByPlayer|drawnBy)\.sweaterNumber", col)
    if m:
        return f"Jersey number of {ROLE[m.group(1)]} on the play."
    return None


def main() -> None:
    """Write ``_final_descs.yaml`` for every still-uncovered loader column.

    Reads the deferred-column work-list and the declared schemas, composes what it
    can ground, and prints anything it cannot so the gap is visible rather than
    silently dropped.

    Returns:
        None. Side effect is the ``_final_descs.yaml`` file in the working
        directory, which ``merge_column_descriptions.py`` folds into the manual dict.

    Example:
        Run from the repo root::

            uv run python tools/codegen/gen_final_tail_descriptions.py
    """
    import yaml

    from tools.codegen import extract_residual_columns as x

    # Enumerate the DECLARED schemas of every rendering loader, not the deferred
    # work-list: that list shrinks to empty as descriptions land, so driving off it
    # makes the generator non-idempotent and silently emits nothing once the gap is
    # closed -- which then blocks any correction to an already-covered column.
    rendering = x._rendering_loaders()
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    rows = [{"schema": fn, "col": c["name"]} for fn, cols in schemas.items() if fn in rendering for c in cols]
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    out, unparsed = {}, []
    for r in rows:
        d = describe(r["col"])
        if d:
            out.setdefault(r["schema"], {})[r["col"]] = d
        else:
            unparsed.append(f"{r['schema']}.{r['col']}")
    # also fill the sibling *.default keys these loaders declare, so the family reads consistently
    for fn in list(out):
        for c in schemas.get(fn, []):
            d = describe(c["name"])
            if d:
                out[fn].setdefault(c["name"], d)
    # unparsed is every column outside this generator's vocabulary (most of the
    # repo), so it is counted rather than listed -- the per-column work-list lives
    # in extract_residual_columns, not here.
    print(f"composed {sum(len(v) for v in out.values())}; outside this generator: {len(unparsed)}")
    with open("_final_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            {k: dict(sorted(v.items())) for k, v in out.items()}, fh, sort_keys=True, allow_unicode=True, width=120
        )
    print("wrote _final_descs.yaml")


if __name__ == "__main__":
    main()
