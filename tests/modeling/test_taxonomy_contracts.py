"""Taxonomy-contract gates — committed type domains vs fixtures and live feeds.

Offline: every committed contract must exactly match the domain rebuilt from
the fixtures (strict validation — a re-captured fixture with an unseen type
goes red until ``uv run python -m tools.taxonomy_contracts.build`` is rerun,
making taxonomy growth a deliberate, reviewed event). Live (gated): the
in-season WNBA feeds validate against the same domains, so a brand-new live
event type surfaces through the weekly cron's ``live-tests:drift`` issue
flow before it becomes a silent parser gap.
"""

from __future__ import annotations

import pytest

from sportsdataverse.modeling.integrity import read_contract, validate_frame
from tests.conftest import skip_if_no_live
from tools.taxonomy_contracts import build


def test_manifest_matches_committed_files() -> None:
    expected = {f"{feed}.contract.json" for feed in build.FEEDS}
    # dataset contracts (dataset_*) share the directory; the taxonomy
    # manifest owns only the feed contracts
    committed = {p.name for p in build.OUT_DIR.glob("*.contract.json") if not p.name.startswith("dataset_")}
    assert committed == expected, (
        f"missing={sorted(expected - committed)} orphaned={sorted(committed - expected)} — "
        "run: uv run python -m tools.taxonomy_contracts.build"
    )


@pytest.mark.parametrize("feed", sorted(build.FEEDS))
def test_fixture_types_conform_to_committed_domain(feed: str) -> None:
    contract = read_contract(build.OUT_DIR / f"{feed}.contract.json")
    domain = contract.columns["event_type"].allowed_values or []
    assert len(domain) >= 3
    frame = build.type_frame(build.FEEDS[feed]())
    assert frame.height > 0
    report = validate_frame(frame, contract, strict=True)
    assert report.ok, (
        f"{feed}: fixture taxonomy drifted from the committed contract — "
        f"{[v.detail for v in report.violations]}; regenerate deliberately via "
        "uv run python -m tools.taxonomy_contracts.build"
    )


@skip_if_no_live
def test_live_scoreboard_states_within_domain() -> None:
    from sportsdataverse.wnba import espn_wnba_scoreboard

    contract = read_contract(build.OUT_DIR / "espn_scoreboard_states.contract.json")
    events = (espn_wnba_scoreboard(return_parsed=False) or {}).get("events") or []
    if not events:
        pytest.skip("no live scoreboard events to validate")
    states = [str(((e.get("status") or {}).get("type") or {}).get("state") or "") for e in events]
    report = validate_frame(build.type_frame([s for s in states if s]), contract, strict=True)
    assert report.ok, [v.detail for v in report.violations]


@skip_if_no_live
def test_live_summary_play_types_within_domain() -> None:
    from sportsdataverse.wnba import espn_wnba_scoreboard, espn_wnba_summary

    contract = read_contract(build.OUT_DIR / "espn_basketball_play_types.contract.json")
    events = (espn_wnba_scoreboard(return_parsed=False) or {}).get("events") or []
    completed = [e for e in events if bool(((e.get("status") or {}).get("type") or {}).get("completed"))]
    if not completed:
        pytest.skip("no completed live games to validate")
    summary = espn_wnba_summary(event_id=str(completed[0].get("id")), return_parsed=False)
    types = [str((p.get("type") or {}).get("text") or "") for p in (summary or {}).get("plays") or []]
    if not types:
        pytest.skip("live summary carried no plays")
    report = validate_frame(build.type_frame([t for t in types if t]), contract, strict=True)
    assert report.ok, [v.detail for v in report.violations]
