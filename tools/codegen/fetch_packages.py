"""Snapshot the SportsDataVerse package list (projects.json) the website renders.

Network tool -- run manually to refresh ``tools/codegen/packages.json``; the docs
generator (``generate.render_packages_page``) reads the committed snapshot offline,
so the drift gate never needs the network. The packages page is simply omitted when
no snapshot exists.
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

URL = "https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-web/with-data/frontend/data/projects.json"

DEST = Path(__file__).parent / "packages.json"


def fetch() -> list[dict]:
    with urllib.request.urlopen(URL, timeout=20) as r:  # noqa: S310 (trusted SDV host)
        data = json.loads(r.read())
    items = data if isinstance(data, list) else next(v for v in data.values() if isinstance(v, list))
    pkgs = [
        {
            "name": p["name"],
            "language": p.get("language", ""),
            "url": (p.get("urls") or {}).get("site") or (p.get("urls") or {}).get("repo") or "",
        }
        for p in items
    ]
    DEST.write_text(json.dumps(pkgs, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(pkgs)} packages -> {DEST}")
    return pkgs


if __name__ == "__main__":
    fetch()
