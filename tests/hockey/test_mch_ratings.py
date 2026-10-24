"""First test reaching `sportsdataverse.hockey.mch.mch_ratings` (supp-013).

The shim fetches ESPN's MCH scoreboard for every requested date and hands the concatenated events to the
league-agnostic core with `league="mch"`. The scoreboard fetch is replaced by the committed real sample
(`tests/fixtures/league_ports/mch_scoreboard_sample.json`, 194 games; README there) served one date at a
time, so the test proves the date sweep, the event concatenation and the league binding without network.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path


from sportsdataverse.hockey.college_hockey_ratings import college_hockey_ratings

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "league_ports" / "mch_scoreboard_sample.json"


def test_mch_ratings_sweeps_every_date_and_rates_the_mch_league(monkeypatch):
    events = json.loads(FIX.read_text(encoding="utf-8"))
    by_date: dict[str, list[dict]] = {}
    for ev in events:
        by_date.setdefault(ev["date"][:10].replace("-", ""), []).append(ev)
    calls: list[tuple[str, bool]] = []

    def fake_scoreboard(*, dates: str, return_parsed: bool):
        calls.append((dates, return_parsed))
        return {"events": by_date.get(dates, [])}

    leagues: list[str] = []

    def spy_core(evs, *, league, **kw):
        # the MCH and WCH constants happen to rate this sample identically, so the binding is read directly
        leagues.append(league)
        return college_hockey_ratings(evs, league=league, **kw)

    mod = importlib.import_module("sportsdataverse.hockey.mch.mch_ratings")
    monkeypatch.setattr(mod, "espn_mch_scoreboard", fake_scoreboard)
    monkeypatch.setattr(mod, "college_hockey_ratings", spy_core)
    dates = sorted(by_date)
    got = mod.mch_ratings(dates)
    assert calls == [(d, False) for d in dates]
    assert leagues == ["mch"]
    want = college_hockey_ratings(events, league="mch")
    assert got.height >= 60
    key = ["team_id"]
    assert got.sort(key).equals(want.sort(key))
    assert mod.mch_ratings(dates, return_as_pandas=True).shape == got.shape
    print("MCH_RATINGS_OK", got.height, len(dates))
