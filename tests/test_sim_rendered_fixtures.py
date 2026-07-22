"""Drift gate for the committed golden rendered-pbp fixtures.

Each (league, provider) pair regenerates its seeded game end-to-end from the
real capture fixtures and must byte-match (as parsed JSON) the committed
file under ``tests/fixtures/sim_rendered/``. A red here means a renderer,
template, or engine change altered the rendered contract — review the diff,
then regenerate deliberately: ``uv run python -m tools.sim_fixtures.build``.
"""

from __future__ import annotations

import json

import pytest

from tools.sim_fixtures import build


def _manifest():
    return build.manifest()


def test_manifest_matches_committed_files() -> None:
    expected = {f"{league}_{provider}.json" for league, provider in _manifest()}
    committed = {p.name for p in build.OUT_DIR.glob("*.json")}
    assert committed == expected, (
        f"missing={sorted(expected - committed)} orphaned={sorted(committed - expected)} — "
        "run: uv run python -m tools.sim_fixtures.build"
    )


@pytest.mark.parametrize("league, provider", _manifest())
def test_rendered_fixture_is_current(league: str, provider: str) -> None:
    path = build.OUT_DIR / f"{league}_{provider}.json"
    committed = json.loads(path.read_text(encoding="utf-8"))
    fresh = build.build_fixture(league, provider)
    assert committed["meta"] == fresh["meta"], f"{path} meta drifted — regenerate"
    if committed["rows"] != fresh["rows"]:
        first = next((i, a, b) for i, (a, b) in enumerate(zip(committed["rows"], fresh["rows"])) if a != b)
        pytest.fail(f"{path} rows drifted — first divergence at row {first[0]}:\n{first[1]}\n{first[2]}")
    # the serialization itself round-trips
    assert json.loads(build.render_fixture_text(fresh)) == fresh
