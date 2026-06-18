"""Generate the ``reference:`` block of ``great-docs.yml`` from endpoint metadata.

The Great Docs API reference was hand-curated (8 sports x ~8 names). This module
makes it **generated** so it stays in sync with the wrapper surface and covers
every league declared in ``tools/codegen/endpoints/leagues.yaml`` — including the
ones added after the original curation (men's/women's college hockey, college
baseball/softball, UFL/XFL/CFL, soccer, cricket).

It does NOT enumerate the full ~800-wrapper cross-league surface (that bloats the
Great Docs build, which introspects every listed name live). Instead it pins a
**curated set of high-value short names per league** — the entry points people
actually call — and emits only the ``espn_{prefix}_{short}`` names that are
**real importable top-level exports** of ``sportsdataverse`` (Great Docs fails on
a non-importable name). The hand-authored tail sections (NFL data loaders, the
play-by-play / config classes) are preserved verbatim.

Splice contract: the generated block is written into ``great-docs.yml`` between
the ``# >>> generated reference`` and ``# <<< generated reference`` markers, so
re-running ``python tools/codegen/generate.py --great-docs`` refreshes it in
place without disturbing the rest of the config. ``--great-docs --check`` (via
:func:`great_docs_reference_stale`) fails if the committed block is stale.

Regen command:  python tools/codegen/generate.py --great-docs
Drift check:     python tools/codegen/generate.py --great-docs --check
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import yaml

# Put the repo root FIRST on sys.path so ``import sportsdataverse`` resolves to
# the local editable source — not a stale site-packages install. When run as a
# script, sys.path[0] is this file's dir (tools/codegen/), which would otherwise
# let the installed copy win and silently drop the newer leagues.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ROOT = Path(__file__).resolve().parents[2]
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints"
GREAT_DOCS_YML = ROOT / "great-docs.yml"

BEGIN = "# >>> generated reference (python tools/codegen/generate.py --great-docs)"
END = "# <<< generated reference"

# Curated high-value short names, in display order. Every league is probed for
# these; only the ones that resolve to a real top-level export are emitted. The
# set is intentionally small (the canonical entry points) to keep the Great Docs
# build fast — NOT the full per-league wrapper surface.
CURATED_SHORTS = [
    "teams",
    "scoreboard",
    "schedule",
    "team_roster",
    "player_stats",
    "standings",
    "rankings",
    "calendar",
    "news",
    "injuries",
    "futures",
    "team_record",
]

# Sport slug -> human display heading for the reference section grouping. Mirrors
# the per-sport grouping on the Docusaurus site / the curated original.
SPORT_TITLES = {
    "basketball": "Basketball",
    "football": "Football",
    "baseball": "Baseball",
    "hockey": "Hockey",
    "soccer": "Soccer",
    "cricket": "Cricket",
}

# Per-league display label (prefix -> friendly name) for the section descriptions.
LEAGUE_LABELS = {
    "nba": "NBA",
    "wnba": "WNBA",
    "mbb": "Men's college basketball",
    "wbb": "Women's college basketball",
    "cfb": "College football",
    "nfl": "NFL",
    "mlb": "MLB",
    "nhl": "NHL",
    "mch": "Men's college hockey",
    "wch": "Women's college hockey",
    "college_baseball": "College baseball",
    "college_softball": "College softball",
    "ufl": "UFL",
    "xfl": "XFL",
    "cfl": "CFL",
    "soccer": "Soccer (league-parameterized)",
    "cricket": "Cricket (league-parameterized)",
}

# Sport display order.
SPORT_ORDER = ["football", "basketball", "baseball", "hockey", "soccer", "cricket"]


def _load_leagues() -> list[dict]:
    raw = yaml.safe_load((ENDPOINTS / "leagues.yaml").read_text(encoding="utf-8"))
    return raw["leagues"]


def _importable_names() -> set[str]:
    """The set of top-level ``espn_*`` exports of ``sportsdataverse``."""
    sdv = importlib.import_module("sportsdataverse")
    return {n for n in dir(sdv) if n.startswith("espn_")}


def _league_contents(prefix: str, names: set[str]) -> list[str]:
    """Curated ``espn_{prefix}_{short}`` names that are real top-level exports."""
    return [f"espn_{prefix}_{short}" for short in CURATED_SHORTS if f"espn_{prefix}_{short}" in names]


def build_reference() -> dict:
    """Build the generated ``reference`` dict (sport sections + preserved tail)."""
    names = _importable_names()
    leagues = _load_leagues()

    # Group leagues by sport, preserving leagues.yaml order within a sport. Only
    # one entry per (prefix) — the soccer/cricket aliases (epl, laliga, ...) have
    # NO top-level exports (they are reached via espn_soccer_*(league=...)), so
    # they naturally drop out below.
    by_sport: dict[str, list[dict]] = {}
    for lg in leagues:
        contents = _league_contents(lg["prefix"], names)
        if not contents:
            continue  # alias / param-mode league with no own exports
        by_sport.setdefault(lg["sport"], []).append({"prefix": lg["prefix"], "contents": contents})

    sections: list[dict] = []
    for sport in SPORT_ORDER:
        for entry in by_sport.get(sport, []):
            prefix = entry["prefix"]
            label = LEAGUE_LABELS.get(prefix, prefix.upper())
            sport_word = SPORT_TITLES.get(sport, sport.title())
            sections.append(
                {
                    "title": label,
                    "desc": f"{label} — {sport_word} (ESPN cross-league surface).",
                    "contents": entry["contents"],
                }
            )

    # Preserved hand-authored tail sections (NFL loaders + PBP/config classes).
    sections.append(
        {
            "title": "NFL data loaders",
            "desc": (
                "nflreadpy-parity loaders for nflverse release data (cached, polars by "
                "default). The load_nfl_* prefix disambiguates under the umbrella package."
            ),
            "contents": [
                "load_nfl_pbp",
                "load_nfl_schedule",
                "load_nfl_rosters",
                "load_nfl_teams",
                "load_nfl_nextgen_stats",
                "load_nfl_pfr_advstats",
                "load_nfl_injuries",
                "load_nfl_depth_charts",
                "load_nfl_draft_picks",
                "load_nfl_contracts",
            ],
        }
    )
    sections.append(
        {
            "title": "Play-by-play and config classes",
            "desc": "Core processing classes and the NFL caching config object.",
            "contents": [
                {"name": "CFBPlayProcess", "members": False},
                {"name": "NFLPlayProcess", "members": False},
                "NflConfig",
                "SeasonNotFoundError",
            ],
        }
    )

    return {
        "title": "API reference",
        "desc": (
            "The canonical public surface of sportsdataverse, grouped by sport. Each "
            "espn_<sport>_* function returns a tidy polars DataFrame by default (pass "
            "return_as_pandas=True for pandas). The full ~800-wrapper cross-league "
            "surface is generated dynamically at import; this reference is generated "
            "from tools/codegen/endpoints/leagues.yaml and pins the high-value entry "
            "points per league across every sport."
        ),
        "sections": sections,
    }


def render_reference_block() -> str:
    """Render the marker-delimited ``reference:`` YAML block (begin/end included)."""
    ref = build_reference()
    body = yaml.safe_dump(
        {"reference": ref},
        sort_keys=False,
        default_flow_style=False,
        width=100,
        allow_unicode=True,
    )
    return f"{BEGIN}\n{body.rstrip()}\n{END}\n"


def _split_existing(text: str) -> tuple[str, str]:
    """Return (before, after) the marker block. Raises if markers are missing."""
    if BEGIN not in text or END not in text:
        raise RuntimeError(
            "great-docs.yml is missing the generated-reference markers "
            f"({BEGIN!r} / {END!r}); add them once, then re-run --great-docs."
        )
    before = text[: text.index(BEGIN)]
    after = text[text.index(END) + len(END) :]
    after = after.lstrip("\n")
    return before, after


def splice(text: str) -> str:
    """Return ``text`` with the generated block re-spliced between the markers.

    The result always ends with exactly one trailing newline so it agrees with
    the ``end-of-file-fixer`` pre-commit hook (otherwise the ``--check`` drift
    gate and the hook would fight over a trailing blank line forever).
    """
    before, after = _split_existing(text)
    block = render_reference_block()  # ends with "{END}\n"
    if after:
        return f"{before.rstrip(chr(10))}\n\n{block}{after.rstrip(chr(10))}\n"
    # reference block is the last thing in the file -> single trailing newline
    return f"{before.rstrip(chr(10))}\n\n{block.rstrip(chr(10))}\n"


def write_great_docs_reference() -> Path:
    """Splice the freshly generated reference block into ``great-docs.yml``."""
    text = GREAT_DOCS_YML.read_text(encoding="utf-8")
    GREAT_DOCS_YML.write_text(splice(text), encoding="utf-8", newline="\n")
    return GREAT_DOCS_YML


def great_docs_reference_stale() -> bool:
    """True if the committed reference block differs from a fresh render."""
    text = GREAT_DOCS_YML.read_text(encoding="utf-8")
    return splice(text) != text


if __name__ == "__main__":
    p = write_great_docs_reference()
    print(f"great-docs reference: spliced generated block into {p}")
