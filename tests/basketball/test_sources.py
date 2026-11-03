"""Basketball source contract + dispatch (BB-D12): ESPN through the dispatch is byte-identical to the direct path.

Fixtures: the real ESPN captures in tests/fixtures/espn/basketball_pbp/*.json.gz (the
``espn_<lg>_pbp(raw=True)`` shape) and tests/fixtures/espn/summary_<lg>.json (the bare summary,
served as the HTTP body); provenance in tests/fixtures/espn/basketball_pbp/README.md.
"""

from __future__ import annotations

import copy
import gzip
import importlib
import json
from pathlib import Path

import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.basketball.sources import dispatch
from sportsdataverse.basketball.sources.contract import CONTRACT_VERSION, LEAGUES, _validate_summary
from sportsdataverse.basketball.sources.dispatch import (
    SOURCE_ORDER,
    AllSourcesFailed,
    _fallthrough_order,
    _process_game,
)
from sportsdataverse.nba.nba_game_page import game_page_frames, league_pbp_functions

ESPN = Path(__file__).resolve().parents[1] / "fixtures" / "espn"
CAPTURES = sorted((ESPN / "basketball_pbp").glob("*.json.gz"))


def _load(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def _league(path: Path) -> str:
    return path.name.split("_")[0]


def _game_id(summary: dict) -> int:
    return int(summary["header"]["id"])


def _direct(lg: str, summary: dict) -> tuple[dict, dict]:
    """Game on Paper's ESPN path today: ``helper_<lg>_pbp`` then ``game_page_frames``."""
    _, parse = league_pbp_functions(lg)
    final = parse(_game_id(summary), copy.deepcopy(summary))
    return final, game_page_frames(final, league=lg)


def _assert_same(before: tuple[dict, dict], after: dispatch.ProcessedGame) -> None:
    final, frames = before
    assert after.frames.keys() == frames.keys()
    for key, want in frames.items():
        got = after.frames[key]
        if hasattr(want, "schema"):
            assert_frame_equal(got, want)
        else:
            assert got == want, key
    served = {k: v for k, v in after.final.items() if k != "source"}
    assert json.dumps(served, sort_keys=True, default=str) == json.dumps(final, sort_keys=True, default=str)


def test_captures_cover_every_league() -> None:
    assert {_league(p) for p in CAPTURES} == set(LEAGUES)


@pytest.mark.parametrize("path", CAPTURES, ids=lambda p: p.name)
def test_every_real_espn_capture_passes_the_contract(path: Path) -> None:
    report = _validate_summary(_load(path), _league(path))
    assert report.ok and report.gop_ok, report.summary()
    assert report.version == CONTRACT_VERSION and report.n_plays > 100


@pytest.mark.parametrize("path", CAPTURES, ids=lambda p: p.name)
def test_espn_through_dispatch_is_unchanged(path: Path) -> None:
    lg, summary = _league(path), _load(path)
    before = _direct(lg, summary)
    after = _process_game(lg, _game_id(summary), payloads={"espn": copy.deepcopy(summary)})
    _assert_same(before, after)


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._p = payload

    def json(self) -> dict:
        return self._p


@pytest.mark.parametrize("lg", LEAGUES)
def test_espn_fetch_through_dispatch_is_unchanged(monkeypatch: pytest.MonkeyPatch, lg: str) -> None:
    """The fetch path: the bare ESPN summary served as the HTTP body, ``raw=True`` defaulting included."""
    body = json.loads((ESPN / f"summary_{lg}.json").read_text(encoding="utf-8"))
    mod = importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp")
    monkeypatch.setattr(mod, "download", lambda *_a, **_k: _Resp(copy.deepcopy(body)))
    fetch, _ = league_pbp_functions(lg)
    before = _direct(lg, fetch(_game_id(body), raw=True))

    after = _process_game(lg, _game_id(body))

    _assert_same(before, after)
    prov = after.provenance
    assert after.final["source"] is prov
    assert (prov["requested"], prov["served"], prov["fallback"]) == ("espn", "espn", False)
    assert [a["source"] for a in prov["attempts"]] == ["espn"] and prov["attempts"][0]["ok"]
    assert prov["playByPlaySource"] == "full" and prov["native_ids"] == {"espn_event_id": str(_game_id(body))}
    assert prov["contract"]["ok"] and prov["contract"]["version"] == CONTRACT_VERSION
    assert prov["lossy_columns"] == [] and after.health == {"espn": "ok"}


def test_only_espn_is_selectable_until_the_parity_gates_pass() -> None:
    assert dict.fromkeys(LEAGUES, ("espn",)) == SOURCE_ORDER
    assert _fallthrough_order("nba", "espn") == ("espn",)
    with pytest.raises(ValueError, match="unknown source 'live' for nba"):
        _process_game("nba", 401766458, source="live")
    with pytest.raises(ValueError, match="league must be one of"):
        _process_game("nfl", 401766458)


def test_a_registered_alternate_falls_through_to_espn(monkeypatch: pytest.MonkeyPatch) -> None:
    """ESPN is the terminal fallback when a requested alternate fails."""
    path = ESPN / "basketball_pbp" / "nba_401766458.json.gz"
    summary = _load(path)
    monkeypatch.setitem(dispatch.SOURCE_ORDER, "nba", ("espn", "live"))

    def broken(*_args: object) -> dispatch.AdaptedGame:
        raise dispatch.SourceUnavailable("cdn down")

    monkeypatch.setitem(dispatch._ADAPTERS, ("nba", "live"), broken)
    game = _process_game("nba", _game_id(summary), source="live", payloads={"espn": summary})
    assert game.provenance["served"] == "espn" and game.provenance["fallback"] is True
    assert game.health == {"live": "SourceUnavailable: cdn down", "espn": "ok"}


def test_a_contract_failure_is_named_and_raises_all_sources_failed() -> None:
    summary = _load(ESPN / "basketball_pbp" / "wnba_400927398.json.gz")
    summary["plays"] = []  # before tip-off ESPN ships the shell and no plays
    with pytest.raises(AllSourcesFailed) as err:
        _process_game("wnba", _game_id(summary), payloads={"espn": summary})
    assert err.value.attempts[0].error == "contract: missing=['plays[]'] invalid=[]"


def test_a_payload_for_another_game_is_not_served_under_this_id() -> None:
    summary = _load(ESPN / "basketball_pbp" / "wnba_400927398.json.gz")
    with pytest.raises(AllSourcesFailed) as err:
        _process_game("wnba", 401726992, payloads={"espn": summary})
    assert err.value.attempts[0].error == "header.id '400927398' is not the requested game 401726992"


def test_a_fetch_failure_raises_all_sources_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = importlib.import_module("sportsdataverse.mbb.mbb_pbp")

    def boom(*_args: object, **_kwargs: object) -> None:
        raise ConnectionError("espn down")

    monkeypatch.setattr(mod, "download", boom)
    with pytest.raises(AllSourcesFailed, match="espn summary fetch failed"):
        _process_game("mbb", 401812161)


def _mutated(mutate) -> list[str]:
    summary = _load(ESPN / "basketball_pbp" / "mbb_401812161.json.gz")
    mutate(summary)
    report = _validate_summary(summary, "mbb")
    return report.missing + report.invalid + report.gop_missing + [f"warn: {w}" for w in report.warnings]


def _competitors(s: dict) -> list[dict]:
    return s["header"]["competitions"][0]["competitors"]


@pytest.mark.parametrize(
    ("mutate", "want"),
    [
        (lambda s: [p.pop("homeScore") for p in s["plays"]], "plays[].homeScore"),
        (lambda s: [p.update(homeScore=None) for p in s["plays"]], "plays[].homeScore: present but null on every row"),
        (lambda s: s.pop("pickcenter"), "pickcenter"),
        (lambda s: s["boxscore"].pop("players"), "boxscore.players"),
        (lambda s: [c.pop("score") for c in _competitors(s)], "competitors[].score"),
        (lambda s: [c["team"].pop("displayName") for c in _competitors(s)], "competitors[].team.displayName"),
        (lambda s: _competitors(s)[0]["team"].update(id="-2"), "competitors[0].team.id '-2' is negative"),
        (lambda s: _competitors(s)[0].update(homeAway="away"), "competitors.homeAway must be exactly"),
        (lambda s: s["plays"][5].update(clock={"displayValue": "1:2:3"}), 'clock.displayValue not "MM:SS"'),
        (lambda s: s["plays"][5].update(clock={"displayValue": "99:99"}), 'clock.displayValue not "MM:SS"'),
        (lambda s: _competitors(s)[0].update(homeAway=1), "competitors.homeAway must be exactly"),
        (
            lambda s: s["header"]["competitions"][0].update(neutralSite=None),
            "header.competitions[0].neutralSite: present but null",
        ),
        (lambda s: s["plays"][5]["team"].update(id="1610612737"), "play team ids not in header competitors"),
        (lambda s: s.update(format={}), "warn: format.regulation.periods absent"),
        (lambda s: [p.pop("coordinate", None) for p in s["plays"]], "warn: plays[].coordinate.x absent"),
    ],
)
def test_the_contract_names_the_missing_or_wrong_field(mutate, want: str) -> None:
    assert any(f.startswith(want) for f in _mutated(mutate)), _mutated(mutate)


def test_warnings_alone_do_not_fail_the_contract() -> None:
    summary = _load(ESPN / "basketball_pbp" / "mbb_401812161.json.gz")
    summary["format"] = {}  # the key is required; its regulation is repaired
    assert _validate_summary(summary, "mbb").ok


def test_unknown_league_and_non_dict() -> None:
    with pytest.raises(ValueError, match="league must be one of"):
        _validate_summary({}, "nfl")
    assert _validate_summary([], "nba").missing == ["<summary is not a dict>"]
