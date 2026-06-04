"""Draft an ESPN generated-name -> R-export rename map (best-guess, for review).

Matches each generated ESPN endpoint to the R sister-package function (hoopR /
wehoop / cfbfastR) that hits the **same ESPN URL**, keyed by (host-class,
path-suffix-after-sport/league). R URLs are reconstructed by joining the
consecutive string literals of each ``glue``/``paste`` call inside a function's
own block, and indexed by the function's *own* prefix (so hoopR's shared R/ dir
splits cleanly between nba and mbb).

Output is a *best-guess worksheet* for human review, never auto-applied:
  - exact-name match  -> keep (already R-aligned),
  - 1 URL candidate   -> "rename to <r>",
  - >1 candidate      -> "review: a | b" (one raw endpoint feeds several R fns),
  - 0 candidate       -> "keep (no R twin found)".

Run:  python tools/codegen/build_espn_rename_map.py
"""

from __future__ import annotations

import glob
import re
from collections import defaultdict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints"
_DEV = "C:/Users/saiem/Documents/GitHub-Data/sdv-dev"

# league -> (R package rel-path, applicable scopes, sport, league-slug)
_PKG = {
    "nba": ("hoopR-dev/hoopR", {"universal"}, "basketball", "nba"),
    "mbb": ("hoopR-dev/hoopR", {"universal", "ncaa"}, "basketball", "mens-college-basketball"),
    "wnba": ("wehoop-dev/wehoop", {"universal"}, "basketball", "wnba"),
    "wbb": ("wehoop-dev/wehoop", {"universal", "ncaa"}, "basketball", "womens-college-basketball"),
    "cfb": ("cfbfastR-dev/cfbfastR", {"universal", "ncaa", "football"}, "football", "college-football"),
}
_API_HOST = {"espn_site_v2": "site_v2", "espn_web_v3": "web_v3", "espn_core_v2": "core_v2"}

_HOST_CLASS = [  # (class, host+api prefix) longest first
    ("core_v2", "sports.core.api.espn.com/v2/sports"),
    ("web_v3", "site.web.api.espn.com/apis/common/v3/sports"),
    ("fitt_v3", "site.web.api.espn.com/apis/fitt/v3/sports"),
    ("site_v2_alt", "site.api.espn.com/apis/v2/sports"),
    ("site_v2", "site.api.espn.com/apis/site/v2/sports"),
]
_TOKEN = re.compile(r"\{[^}/]+\}")
# structural prefix tokens that don't carry the dataset's identity
_NOISE = {"event", "athlete", "season", "competitor", "site", "core", "web", "info"}


def _high_confidence(short: str, rname: str) -> bool:
    """A rename is high-confidence when a content token of the generated short
    (its identity, minus structural prefixes) also appears in the R name."""
    content = set(short.split("_")) - _NOISE
    return bool(content) and content <= set(rname.split("_"))


_FUNC = re.compile(r"\n(espn_[a-z0-9_]+)\s*<-\s*function")
_LIT_RUN = re.compile(r'((?:"(?:[^"\\]|\\.)*"\s*[,+]?\s*)+)')
_LIT = re.compile(r'"((?:[^"\\]|\\.)*)"')


def _norm(suffix: str) -> str:
    s = suffix.split("?")[0].strip("/").lower()
    return _TOKEN.sub("*", s)


def _gen_index():
    """league -> {generated_name: (host_class, norm_suffix)}."""
    out = defaultdict(dict)
    docs = {api: yaml.safe_load((ENDPOINTS / f"{api}.yaml").read_text(encoding="utf-8")) for api in _API_HOST}
    for prefix, (_rel, scopes, _sport, _slug) in _PKG.items():
        for api, doc in docs.items():
            for ep in doc["endpoints"]:
                if ep.get("scope", "universal") not in scopes:
                    continue
                hc = ep.get("host") or _API_HOST[api]
                path = ep["path"].replace("[", "").replace("]", "")
                suffix = re.sub(r"^/\{sport\}/(leagues/)?\{league\}", "", path)
                out[prefix][f"espn_{prefix}_{ep['short']}"] = (hc, _norm(suffix), ep["path"], hc)
    return out


def _r_index():
    """league -> {(host_class, norm_suffix): set(r_fn_names)}."""
    out = defaultdict(lambda: defaultdict(set))
    seen_dirs = {}
    for prefix, (rel, _scopes, sport, slug) in _PKG.items():
        rdir = f"{_DEV}/{rel}/R"
        files = seen_dirs.setdefault(rdir, sorted(glob.glob(f"{rdir}/*.R")))
        strip_re = re.compile(rf"^/{sport}/(?:leagues/)?{re.escape(slug)}")
        for path in files:
            txt = open(path, encoding="utf-8", errors="ignore").read()
            starts = [(m.group(1), m.start()) for m in _FUNC.finditer(txt)]
            for i, (fn, st) in enumerate(starts):
                if not fn.startswith(f"espn_{prefix}_"):
                    continue
                end = starts[i + 1][1] if i + 1 < len(starts) else len(txt)
                block = txt[st:end]
                for run in _LIT_RUN.findall(block):
                    joined = "".join(_LIT.findall(run))
                    for hc, pre in _HOST_CLASS:
                        idx = joined.find(pre)
                        if idx == -1:
                            continue
                        rest = joined[idx + len(pre) :]
                        m = strip_re.match(rest)
                        if not m:
                            continue
                        ns = _norm(rest[m.end() :])
                        if ns:
                            out[prefix][(hc, ns)].add(fn)
                        break
    return out


def main() -> int:
    gen = _gen_index()
    r = _r_index()
    md = ["# ESPN -> R naming worksheet (best-guess, for review)\n"]
    md.append(
        "Behavior is fixed (Plan 2, URL-parity-proven); this is for **public names**. "
        "Suggestions come from URL matching against hoopR/wehoop/cfbfastR. Edit the "
        "`suggestion` column: `rename to <r>` / `keep` / `qualify`. `review:` rows are "
        "one raw endpoint that feeds several R functions — pick or split.\n",
    )
    totals = {}
    suggested = {}
    for prefix, (rel, _scopes, _sport, _slug) in _PKG.items():
        rexp = _r_export_names(rel, prefix)
        gmap = gen[prefix]
        rmap = r[prefix]
        exact = sum(1 for g in gmap if g in rexp)
        renamed = review = keep = 0
        rows = []
        for g in sorted(gmap):
            hc, ns, path, _ = gmap[g]
            if g in rexp:
                rows.append((g, hc, path, "keep (exact R match)"))
                continue
            cands = sorted(rmap.get((hc, ns), set()) - {g})
            if len(cands) == 1:
                gshort = g.split(f"espn_{prefix}_", 1)[-1]
                conf = "high" if _high_confidence(gshort, cands[0]) else "check"
                sug = f"rename to `{cands[0]}` ({conf})"
                renamed += 1
                if conf == "high":
                    suggested[g] = cands[0]  # only high-confidence go in the auto-applicable map
            elif len(cands) > 1:
                sug = "review: " + " | ".join(f"`{c}`" for c in cands)
                review += 1
            else:
                sug = "keep (no R twin found)"
                keep += 1
            rows.append((g, hc, path, sug))
        totals[prefix] = (len(gmap), exact, renamed, review, keep)
        md.append(f"\n## {prefix} (vs {rel.split('/')[-1]})\n")
        md.append(
            f"- generated {len(gmap)} | exact {exact} | suggested-rename {renamed} | "
            f"review(1->many) {review} | keep {keep}\n",
        )
        md.append("| generated | host | path | suggestion |")
        md.append("|-----------|------|------|------------|")
        for g, hc, path, sug in rows:
            md.append(f"| `{g}` | {hc} | `{path}` | {sug} |")

    md.insert(1, "## Totals\n\n| league | gen | exact | rename | review | keep |\n|---|--:|--:|--:|--:|--:|")
    for prefix, (g, e, rn, rv, k) in totals.items():
        md.insert(3, f"| {prefix} | {g} | {e} | {rn} | {rv} | {k} |")

    out = ROOT / "docs" / "superpowers" / "specs" / "espn-r-naming-worksheet.md"
    out.write_text("\n".join(md) + "\n", encoding="utf-8")
    ymap = ROOT / "tools" / "codegen" / "espn_rename_map.suggested.yaml"
    ymap.write_text(yaml.safe_dump({"rename": dict(sorted(suggested.items()))}, sort_keys=False, width=100), "utf-8")
    print("totals (league: gen/exact/rename/review/keep):")
    for prefix, t in totals.items():
        print(f"  {prefix}: {t}")
    print(f"suggested 1:1 renames: {len(suggested)}")
    print(f"wrote {out.relative_to(ROOT)} + {ymap.relative_to(ROOT)}")
    return 0


def _r_export_names(rel: str, prefix: str) -> set:
    txt = Path(f"{_DEV}/{rel}/NAMESPACE").read_text(encoding="utf-8", errors="ignore")
    return set(re.findall(rf"export\((espn_{prefix}_[a-z0-9_]+)\)", txt))


if __name__ == "__main__":
    raise SystemExit(main())
