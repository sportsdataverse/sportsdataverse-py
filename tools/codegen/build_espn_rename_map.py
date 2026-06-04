"""Produce an ESPN naming-curation worksheet vs the R sister packages.

Automated URL-matching is unreliable here: hoopR/wehoop/cfbfastR build ESPN URLs
dynamically (base-url vars + glue), bundle many functions per file, and use a
different surface granularity than the one-wrapper-per-endpoint generated layer.
So this does NOT fabricate 1:1 guesses. Instead it emits a reviewable worksheet:

* exact-name matches  -> generated name already == an R export (no rename needed),
* generated-only      -> generated endpoints with no exact R name (name + path +
                         host, for you to decide: rename-to-R / keep / qualify),
* R-only              -> R exports with no generated twin (candidate rename targets).

Run:  python tools/codegen/build_espn_rename_map.py
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints"
_DEV = "C:/Users/saiem/Documents/GitHub-Data/sdv-dev"

# league -> (R NAMESPACE path, scopes that apply)
_PKG = {
    "nba": ("hoopR-dev/hoopR", {"universal"}),
    "mbb": ("hoopR-dev/hoopR", {"universal", "ncaa"}),
    "wnba": ("wehoop-dev/wehoop", {"universal"}),
    "wbb": ("wehoop-dev/wehoop", {"universal", "ncaa"}),
    "cfb": ("cfbfastR-dev/cfbfastR", {"universal", "ncaa", "football"}),
}
_API_HOST = {"espn_site_v2": "site_v2", "espn_web_v3": "web_v3", "espn_core_v2": "core_v2"}


def _r_exports(rel: str, prefix: str) -> set:
    txt = Path(f"{_DEV}/{rel}/NAMESPACE").read_text(encoding="utf-8", errors="ignore")
    return set(re.findall(rf"export\((espn_{prefix}_[a-z0-9_]+)\)", txt))


def _generated_for(prefix: str, scopes: set) -> dict:
    """generated name -> (host, path) for endpoints in this league's scopes."""
    out = {}
    for api in ("espn_site_v2", "espn_web_v3", "espn_core_v2"):
        doc = yaml.safe_load((ENDPOINTS / f"{api}.yaml").read_text(encoding="utf-8"))
        for ep in doc["endpoints"]:
            scope = ep.get("scope", "universal")
            if scope not in scopes:
                continue
            host = ep.get("host") or _API_HOST[api]
            out[f"espn_{prefix}_{ep['short']}"] = (host, ep["path"])
    return out


def main() -> int:
    md = ["# ESPN -> R naming curation worksheet (for review)\n"]
    md.append(
        "Behavior of the generated wrappers is fixed (Plan 2, URL-parity-proven). This "
        "worksheet is for deciding **public names** vs the R sister packages. Fill the "
        "`-> ?` column for the generated-only rows: `rename to <r_name>`, `keep`, or "
        "`qualify` (collision fallback).\n",
    )
    totals = {}
    for prefix, (rel, scopes) in _PKG.items():
        rexp = _r_exports(rel, prefix)
        gen = _generated_for(prefix, scopes)
        gnames = set(gen)
        exact = gnames & rexp
        gen_only = sorted(gnames - rexp)
        r_only = sorted(rexp - gnames)
        totals[prefix] = (len(gnames), len(exact), len(gen_only), len(r_only))
        pkg = rel.split("/")[-1]
        md.append(f"\n## {prefix} (vs {pkg})\n")
        md.append(
            f"- generated: {len(gnames)} | exact-name match (no rename): {len(exact)} | "
            f"generated-only: {len(gen_only)} | R-only: {len(r_only)}\n"
        )
        md.append("### generated-only — decide name\n")
        md.append("| generated | host | path | -> ? |")
        md.append("|-----------|------|------|------|")
        for g in gen_only:
            host, path = gen[g]
            md.append(f"| `{g}` | {host} | `{path}` |  |")
        md.append("\n### R-only exports (candidate rename targets / not yet wrapped)\n")
        md.append(", ".join(f"`{n}`" for n in r_only) or "_(none)_")

    md.insert(1, "## Totals\n\n| league | generated | exact match | generated-only | R-only |\n|---|--:|--:|--:|--:|")
    for prefix, (g, e, go, ro) in totals.items():
        md.insert(3, f"| {prefix} | {g} | {e} | {go} | {ro} |")

    out = ROOT / "docs" / "superpowers" / "specs" / "espn-r-naming-worksheet.md"
    out.write_text("\n".join(md) + "\n", encoding="utf-8")
    print("totals (league: gen/exact/gen_only/R_only):")
    for prefix, t in totals.items():
        print(f"  {prefix}: {t}")
    print(f"wrote {out.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
