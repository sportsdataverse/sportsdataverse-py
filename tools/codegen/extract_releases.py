"""Seed the dataset-loader manifest (releases.yaml) from the live package.

Deterministic + faithful: parses ``sportsdataverse/config.py`` for the
``NAME_URL = SDVRELEASES + "tag/stem_{season}.parquet"`` constants and maps each to
the existing ``load_<league>_<dataset>`` function that uses it (by scanning the
``*_loaders.py`` modules). The emitted manifest therefore reproduces the current
loaders' exact URLs; the generated 404-safe loaders are byte-for-byte URL-faithful
to the hand-written ones (guarded by test_loaders_parity).

Standalone from ``extract.py`` (which imports the retired ESPN factory and is no
longer importable post-retirement).
"""

from __future__ import annotations

import glob
import re
from pathlib import Path
from typing import Dict, List

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "sportsdataverse" / "config.py"
LOADERS_GLOB = str(ROOT / "sportsdataverse" / "*" / "*_loaders.py")

# config.py base-constant name -> releases.yaml base key
_BASE_KEYS = {"SDVRELEASES": "sdv_releases", "SGITHUB": "raw_data"}
_BASES = {
    "sdv_releases": "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/",
    "raw_data": "https://raw.githubusercontent.com/sportsdataverse/",
}

# default min_season per league (best-effort; hand-verify against live assets)
_MIN_SEASON = {"wnba": 2002, "wbb": 2002, "nba": 2002, "mbb": 2002, "cfb": 2003, "nfl": 1999, "nhl": 2011, "pwhl": 2024}

_URL_RE = re.compile(r'^(\w+_URL)\s*=\s*(SDVRELEASES|SGITHUB)\s*\+\s*"([^"]+)"', re.M)
_LOADER_RE = re.compile(r"def (load_\w+)\s*\(")


def _config_constants() -> Dict[str, tuple]:
    """name -> (base_key, relative_url) for every {season} release constant."""
    text = CONFIG.read_text(encoding="utf-8")
    out = {}
    for name, base, rel in _URL_RE.findall(text):
        if "{season}" in rel:
            out[name] = (_BASE_KEYS.get(base, "sdv_releases"), rel)
    return out


def _loader_constant_map() -> Dict[str, str]:
    """load_fn -> the *_URL constant it reads (scans each loader function body)."""
    mapping: Dict[str, str] = {}
    for path in glob.glob(LOADERS_GLOB):
        text = Path(path).read_text(encoding="utf-8")
        # split into function blocks by `def load_...`
        idxs = [(m.group(1), m.start()) for m in _LOADER_RE.finditer(text)]
        for i, (fn, start) in enumerate(idxs):
            end = idxs[i + 1][1] if i + 1 < len(idxs) else len(text)
            body = text[start:end]
            m = re.search(r"(\w+_URL)\.format\(", body)
            if m:
                mapping[fn] = m.group(1)
    return mapping


def build_manifest() -> dict:
    consts = _config_constants()
    fn_const = _loader_constant_map()
    loaders: List[dict] = []
    for fn, const in sorted(fn_const.items()):
        if const not in consts:
            continue  # loader reads a non-{season} / non-release URL
        base_key, rel = consts[const]
        league = fn.split("_", 2)[1] if fn.startswith("load_") else "unknown"
        tag = rel.split("/", 1)[0]
        entry = {"fn": fn, "league": league, "base": base_key, "url": rel, "tag": tag}
        if league in _MIN_SEASON:
            entry["min_season"] = _MIN_SEASON[league]
        entry["example_args"] = {"seasons": 2024}
        loaders.append(entry)
    return {"bases": _BASES, "loaders": loaders}


def write_manifest(path: Path) -> dict:
    doc = build_manifest()
    path.write_text(yaml.safe_dump(doc, sort_keys=False, width=100, allow_unicode=True), encoding="utf-8")
    return doc


def main() -> int:
    out = ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"
    doc = write_manifest(out)
    print(f"releases: wrote {len(doc['loaders'])} loader entries to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
