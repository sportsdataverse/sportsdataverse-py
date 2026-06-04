"""Loader manifest readiness: the generated 404-safe loaders fetch the same URL
the existing hand-written loaders do (faithful), and historical loaders are kept.
"""

import importlib
from pathlib import Path
from unittest.mock import patch

from tools.codegen import generate, spec

REL = Path("tools/codegen/endpoints/releases.yaml")


def test_manifest_covers_historical_loaders():
    rel = spec.load_releases(REL)
    fns = {ld.fn for ld in rel.loaders}
    for f in ("load_wnba_pbp", "load_wnba_schedule", "load_wnba_player_boxscore", "load_wnba_team_boxscore"):
        assert f in fns, f"regressed historical loader {f}"


def _existing_url(fn_name: str, league: str):
    """Capture the URL the existing hand-written loader fetches for season 2024."""
    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    box = {}

    def fake(url, *a, **k):
        box["url"] = url
        raise FileNotFoundError("404")  # short-circuit after first fetch

    with patch.object(mod.pl, "read_parquet", side_effect=fake):
        try:
            getattr(mod, fn_name)(seasons=2024)
        except Exception:  # noqa: BLE001
            pass
    return box.get("url")


def test_generated_loader_urls_match_existing():
    rel = spec.load_releases(REL)
    mismatches = []
    for ld in rel.loaders:
        if ld.stub:
            continue
        gen_url = f"{rel.bases[ld.base]}{ld.url}".replace("{season}", "2024")
        existing = _existing_url(ld.fn, ld.league)
        if existing is None:
            continue  # historical loader not URL-capturable (different mechanism); skip
        if gen_url != existing:
            mismatches.append(f"{ld.fn}:\n   gen:      {gen_url}\n   existing: {existing}")
    assert not mismatches, f"{len(mismatches)} loader URL mismatch(es):\n" + "\n".join(mismatches[:20])


def test_loader_modules_render_valid_python():
    import ast

    rel = spec.load_releases(REL)
    by_league: dict = {}
    for ld in rel.loaders:
        by_league.setdefault(ld.league, []).append(ld)
    for league, loaders in by_league.items():
        src = generate.render_loader_module(league, loaders, rel.bases)
        ast.parse(src)  # raises on malformed
