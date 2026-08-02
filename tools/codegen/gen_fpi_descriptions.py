"""Compose returns-table descriptions for the two ESPN FPI loaders.

ESPN ships a ``description`` alongside every FPI field in its own payloads, so
the bulk of this file is not authored text -- it is ESPN's text, fetched and
attached to the matching column. That is deliberate: a composed description
invented here would drift from what the provider actually means by
``gamecontrol`` or ``accomplishment``.

Two payloads are needed because the two loaders come from different endpoints:

  * the weekly team table  -> ``predictives`` / ``efficiencies`` field blocks
  * the per-game matchup   -> the event powerindex entry's ``stats`` block

The handful of columns ESPN does NOT describe are authored below, each one
grounded in an observed property of the published data rather than a guess.

Run:  uv run python tools/codegen/gen_fpi_descriptions.py
Then: uv run python tools/codegen/merge_column_descriptions.py <out.yaml>
"""

from __future__ import annotations

import json
import pathlib
import urllib.request
from typing import Any

import yaml

CORE = "https://sports.core.api.espn.com/v2/sports/football/leagues/college-football"
_UA = {"User-Agent": "Mozilla/5.0 (compatible; sportsdataverse/codegen)"}

# A season/week known to carry a fully-populated FPI table, and a game known to
# have matchup FPI. Pinned rather than "current" so a re-run is reproducible.
_WEEK_URL = f"{CORE}/seasons/2024/types/2/weeks/8/powerindex?limit=5"
_GAME_URL = f"{CORE}/events/401628319/competitions/401628319/powerindex?limit=10"

# Columns ESPN ships no description for. Every one of these is grounded in a
# checked property of the published data, noted in the text where it matters to
# a consumer -- an undocumented sentinel or an always-null column is exactly the
# kind of thing a returns table exists to warn about.
_AUTHORED: dict[str, str] = {
    # Rank-only fields: ESPN publishes the rank but not the underlying
    # percentage (there is no `adjwinpct` / `projectedwpct` column). adjwins and
    # adjlosses ARE published, so the adjusted percentage is derivable.
    "adjwinpctrank": (
        "Rank among FBS teams by adjusted win percentage. ESPN publishes the rank "
        "without the underlying percentage; derive it from adjwins and adjlosses. "
        "0 is an unranked placeholder, not a rank -- it appears where the "
        "underlying value is null."
    ),
    "projectedwpctrank": (
        "Rank among FBS teams by projected win percentage. ESPN publishes the rank "
        "without the underlying percentage; derive it from projectedw and projectedl."
    ),
    "adjavgingamewprank": (
        "Rank among FBS teams by adjavgingamewp (average in-game win probability "
        "adjusted for opponent). Null for most pre-2019 snapshots. 0 is an "
        "unranked placeholder, not a rank."
    ),
    # Vestigial: college football abolished ties in 1996. ESPN still emits the
    # key beside projectedw / projectedl but never a value -- null in all
    # 23,641 published rows, hence the Null dtype.
    "projectedt": (
        "Projected ties. Always null -- college football abolished ties in 1996, "
        "and ESPN emits the key beside projectedw/projectedl without ever "
        "populating it. Retained so the column set matches the upstream payload."
    ),
    # ESPN's own text for these is degenerate -- "Won Games.", "Rank." -- and the
    # returns-table quality gate rejects a description that only restates the
    # column name. Replaced with text that says which of the several similar
    # columns this one actually is, which is the real question a reader has here.
    "numwins": (
        "Actual wins to date at the time of the snapshot. Distinct from projectedw "
        "(full-season projection) and adjwins (opponent-adjusted)."
    ),
    "numlosses": (
        "Actual losses to date at the time of the snapshot. Distinct from projectedl "
        "(full-season projection) and adjlosses (opponent-adjusted)."
    ),
    "numties": (
        "Actual ties to date. Never nonzero -- college football abolished ties in "
        "1996; the column is null or 0 in every published row."
    ),
    # Verified against the data rather than assumed: across 2015-2025 the two rank
    # columns disagree on 140 of 23,591 rows, and in every one of those `rank`
    # equals the rank implied by the published fpi while `fpirank` equals it in
    # none. So `rank` is the trustworthy one and the caveat belongs on both.
    "rank": (
        "FPI rank among FBS teams for this snapshot (1 = best). Prefer this over "
        "fpirank: the two agree on 99.4% of rows, and on the ~0.6% where they "
        "differ, rank is always the one consistent with the published fpi value."
    ),
    "fpirank": (
        "ESPN's FPI rank field. Agrees with rank on 99.4% of rows; on the ~0.6% "
        "where they differ it is stale -- it never matches the rank implied by the "
        "published fpi, while rank always does. Prefer rank."
    ),
    # Producer-side columns (not ESPN fields).
    "run_date_time_key": (
        "ESPN's run key for the snapshot, as an integer timestamp "
        "(e.g. 20241021040000). This is the AS-OF date the snapshot represents, "
        "which is not the same as last_updated (when ESPN computed it); the gap "
        "between the two is what snapshot_is_contemporaneous flags."
    ),
    "snapshot_out_of_sequence": (
        "True when this snapshot was computed AFTER one belonging to a later week "
        "of the same season type -- so it cannot be read as an as-of-that-week "
        "rating. Almost always the week-1 slot, which ESPN overwrites with a "
        "late-season computation (2024 week 1 is stamped 2024-12-15). Filter these "
        "out for any point-in-time or backtest use."
    ),
    "snapshot_is_contemporaneous": (
        "True when the snapshot was computed inside its own season's window "
        "(August of the season year through February of the next), i.e. it is a "
        "live weekly run rather than a retrospective backfill. False for every "
        "row before 2015, which ESPN computed in one pass afterwards. A "
        "retrospective row is a reconstruction, not an as-of-week rating."
    ),
}


def _get(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.load(r)


def espn_descriptions() -> dict[str, str]:
    """Field name -> ESPN's own description, from both FPI payloads."""
    out: dict[str, str] = {}

    def take(name: Any, desc: Any) -> None:
        if name and desc:
            out[str(name)] = str(desc).strip().rstrip(".") + "."

    item = (_get(_WEEK_URL).get("items") or [{}])[0]
    for block in ("predictives", "efficiencies"):
        for f in item.get(block) or []:
            take(f.get("name"), f.get("description"))

    for entry in _get(_GAME_URL).get("items") or []:
        ref = entry.get("$ref") or ""
        # Only follow ESPN's own refs, and over https.
        if ref.startswith(("http://sports.core.api.espn.com/", "https://sports.core.api.espn.com/")):
            entry = _get(ref.replace("http://", "https://", 1))
        for s in entry.get("stats") or []:
            take(s.get("name"), s.get("description"))
    return out


def main() -> int:
    from tools.codegen.generate import _loader_schemas

    descriptions = {**espn_descriptions(), **_AUTHORED}
    schemas = _loader_schemas()

    # Drive off the DECLARED schema, never off the current deferred list: the
    # deferred list shrinks as entries land, so a generator keyed on it stops
    # reproducing its own output and is no longer idempotent.
    out: dict[str, dict[str, str]] = {}
    missing: list[str] = []
    for fn in ("load_cfb_fpi_weekly", "load_cfb_power_index"):
        cols = schemas.get(fn) or []
        block = {}
        for c in cols:
            name = c["name"] if isinstance(c, dict) else c
            if name in descriptions:
                block[name] = descriptions[name]
            else:
                missing.append(f"{fn}.{name}")
        if block:
            out[fn] = block

    dest = pathlib.Path("tools/codegen/_fpi_descriptions.yaml")
    dest.write_text(
        yaml.safe_dump(out, sort_keys=True, allow_unicode=True, width=100),
        encoding="utf-8",
    )
    total = sum(len(v) for v in out.values())
    print(f"wrote {dest}: {total} descriptions across {len(out)} loaders")
    if missing:
        # Not a failure. Shared keys (season / week / team_id / game_id ...) are
        # described once in codegen's cross-loader vocabulary rather than
        # per-loader, so they are correctly absent here -- the coverage ratchet
        # in extract_residual_columns is what decides whether anything is
        # genuinely undescribed.
        print(f"not covered here (expected for shared keys): {sorted(missing)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
