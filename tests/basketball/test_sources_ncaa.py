"""BB-D16: the stats.ncaa.org adapter (``source="ncaa"``, MBB / WBB) against ESPN on 16 real 2026-27 games.

The corpus (``tests/basketball/fixtures/ncaa_parity/``, provenance in its README) is 8 men's and 8 women's
finals: each contest's published rows from the Data API and ESPN's ``espn_<lg>_pbp(raw=True)`` dict of the same
game. Both run through the dispatch leg a served game takes (``helper_<lg>_pbp`` + ``game_page_frames``); the
harness pairs the plays by alignment (stats.ncaa.org has no event number) and holds every game to the pinned
:data:`~sportsdataverse.basketball.sources.parity.NCAA_GATES`. The hard gates are not met yet, so "ncaa" is not
in ``SOURCE_ORDER``; the test that says so names the gates that block it.
"""

from __future__ import annotations

import copy
import fnmatch
import gzip
import json
from collections.abc import Callable
from functools import lru_cache, cache
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse.basketball.sources import dispatch, parity
from sportsdataverse.basketball.sources.contract import KNOWN_LOSSY, _validate_summary
from sportsdataverse.football.sources.dispatch import SourceUnavailable
from sportsdataverse.mbb.ncaa_pbp import to_espn_summary as ncaa
from tests.basketball import test_sources_live as d13

CORPUS = Path(__file__).resolve().parent / "fixtures" / "ncaa_parity"


def _json(path: Path) -> Any:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _teams() -> dict[str, dict[str, dict]]:
    return _json(CORPUS / "teams_2027.json.gz")


def _cases() -> list[tuple[str, int, str]]:
    """(league, ESPN id, contest id), in file order."""
    out = []
    for f in sorted(CORPUS.glob("ncaa_*.json.gz")):
        lg, contest = f.name.split("_")[1], f.name.split("_")[2].split(".")[0]
        out.append((lg, int(_json(f)["pbp"][0]["espn_game_id"]), contest))
    return out


CASES = _cases()
IDS = [f"{lg}_{eid}" for lg, eid, _ in CASES]


def _payloads(i: int) -> dict[str, Any]:
    """Fresh (mutable) native payloads for CASES[i]."""
    lg, eid, contest = CASES[i]
    return {
        "espn": _json(CORPUS / f"espn_{lg}_{eid}.json.gz"),
        "ncaa": {"contest": _json(CORPUS / f"ncaa_{lg}_{contest}.json.gz"), "teams": copy.deepcopy(_teams()[lg])},
    }


@cache
def _legs(i: int) -> dict[str, tuple[dict, dict]]:
    lg, eid, _ = CASES[i]
    pay = _payloads(i)
    out = {}
    for src in ("espn", "ncaa"):
        _, _, final, frames = dispatch._run_source(lg, eid, dispatch._adapter_for(lg, src), pay[src])
        out[src] = (final, frames)
    return out


@cache
def _report(i: int) -> parity.ParityReport:
    legs = _legs(i)
    return parity._compare_frames(CASES[i][0], CASES[i][1], "ncaa", legs["espn"], legs["ncaa"])


# --------------------------------------------------------------------------- the corpus


def test_corpus_is_the_one_the_gates_were_derived_from() -> None:
    """16 finished D-I games, none twice, with the situations the gates' comments name."""
    leagues = [lg for lg, _, _ in CASES]
    assert (leagues.count("mbb"), leagues.count("wbb")) == (8, 8)
    assert len({eid for _, eid, _ in CASES}) == 16
    for i, (lg, eid, contest) in enumerate(CASES):
        pay = _payloads(i)
        assert pay["espn"]["header"]["competitions"][0]["status"]["type"]["completed"] is True
        rows = pay["ncaa"]["contest"]["pbp"]
        assert {r["espn_game_id"] for r in rows} == {str(eid)} and {r["contest_id"] for r in rows} == {contest}
        assert {r["season"] for r in rows} == {2027}
    periods = {IDS[i]: max(r["period"] for r in _payloads(i)["ncaa"]["contest"]["pbp"]) for i in range(16)}
    assert periods["mbb_401917515"] == 3  # an overtime
    files = list(CORPUS.glob("*.json.gz"))
    assert len(files) == 2 * 16 + 1 and sum(f.stat().st_size for f in files) < 3_000_000


# --------------------------------------------------------------------------- the gates


@pytest.mark.parametrize("i", range(len(CASES)), ids=IDS)
def test_every_game_passes_every_pinned_gate(i: int) -> None:
    report = _report(i)
    assert report.check() == [], (IDS[i], report.check())
    assert report.n_plays_espn > 350 and report.n_players >= 15 and report.n_shots > 90 and report.n_ft_tech > 20


@pytest.mark.parametrize("i", range(len(CASES)), ids=IDS)
def test_the_adapted_summary_meets_the_contract_and_game_on_paper(i: int) -> None:
    lg, eid, _ = CASES[i]
    adapted = ncaa._ncaa_adapter(lg, eid, dispatch.SourceContext(payload=_payloads(i)["ncaa"]))
    report = _validate_summary(adapted.summary, lg)
    assert report.gop_ok, report.summary()
    assert adapted.native_ids == {"espn_event_id": str(eid), "contest_id": CASES[i][2]}


def test_no_gate_is_looser_than_the_corpus_needs() -> None:
    """Each bound sits within its stated margin of the corpus worst case (0.01 on a share, 0.05 ft on a distance)."""
    observed = [_report(i).metrics() for i in range(len(CASES))]
    assert parity.GATES[("mbb", "ncaa")] is parity.GATES[("wbb", "ncaa")] is parity.NCAA_GATES
    for name, (lo, hi) in parity.NCAA_GATES.items():
        values = [m[name] for m in observed]
        slack = 0.05 if name.endswith("_ft") else 0.01
        if lo is not None:
            assert 0 <= min(values) - lo <= slack, (name, lo, min(values))
        if hi is not None:
            assert 0 <= hi - max(values) <= (slack if not name.endswith("mismatches") else 0), (name, hi, max(values))


def test_ncaa_is_not_selectable_until_the_hard_gates_are_exact() -> None:
    """The measured reason "ncaa" is not in SOURCE_ORDER. When this fails, the adapter or the pipeline got
    better: add "ncaa" to SOURCE_ORDER for that league and tighten NCAA_GATES."""
    breached: dict[str, set[str]] = {}
    for i in range(len(CASES)):
        for fail in _report(i).check(parity.HARD_GATES):
            if (name := fail.split()[0]) in parity.HARD_GATES:
                breached.setdefault(name, set()).add(IDS[i])
    assert set(breached) == {"team_box_mismatches", "player_minutes_agreement", "ft_tech_order_agreement"}
    assert breached["team_box_mismatches"] == {"mbb_401920769", "wbb_401926100"}
    assert len(breached["player_minutes_agreement"]) == 16  # stint minutes, not the official box's
    for lg in ("mbb", "wbb"):
        assert "ncaa" not in dispatch.SOURCE_ORDER[lg]
        with pytest.raises(ValueError, match="unknown source 'ncaa'"):
            dispatch._process_game(lg, CASES[0][1], source="ncaa")


def test_period_scores_and_points_are_exact() -> None:
    for i in range(len(CASES)):
        r = _report(i)
        assert r.period_score_mismatches == [] and r.player_points_agreement == 1.0, IDS[i]


def test_known_lossy_is_exactly_what_the_corpus_measures() -> None:
    """KNOWN_LOSSY is measured, never guessed: every gap is covered and every pattern matches a gap."""
    games = []
    for i in range(len(CASES)):
        espn, ours = _legs(i)["espn"][1], _legs(i)["ncaa"][1]
        games.append(({**ours, "pbp": parity._align_play_ids(espn["pbp"], ours["pbp"])}, espn))
    measured = d13._measured_lossy(games)
    patterns = KNOWN_LOSSY[("mbb", "ncaa")]
    assert KNOWN_LOSSY[("wbb", "ncaa")] is patterns
    assert {c for c in measured if not any(fnmatch.fnmatch(c, p) for p in patterns)} == set()
    assert [p for p in patterns if not any(fnmatch.fnmatch(c, p) for c in measured)] == []


# --------------------------------------------------------------------------- what the adapter repairs


def test_score_columns_listed_away_first_are_swapped_back() -> None:
    """WBB 401913054 (Arizona at Stanford): stats.ncaa.org's score columns are Arizona's first."""
    i = IDS.index("wbb_401913054")
    lg, eid, _ = CASES[i]
    adapted = ncaa._ncaa_adapter(lg, eid, dispatch.SourceContext(payload=_payloads(i)["ncaa"]))
    assert any("swapped back" in n for n in adapted.notes)
    comps = {c["homeAway"]: c["score"] for c in adapted.summary["header"]["competitions"][0]["competitors"]}
    espn = {c["homeAway"]: c["score"] for c in _payloads(i)["espn"]["header"]["competitions"][0]["competitors"]}
    assert comps == espn == {"home": "69", "away": "84"}


def test_a_player_the_roster_lacks_keeps_his_box_line() -> None:
    """MBB 401917515: a player with no stats.ncaa.org id is boxed under their name; their assists and
    turnovers stay theirs, not the team's."""
    i = IDS.index("mbb_401917515")
    j = _legs(i)["espn"][1]["team_box"].join(_legs(i)["ncaa"][1]["team_box"], on="team_id", suffix="_src")
    for col in ("assists", "team_turnovers", "turnovers"):
        assert j.filter(pl.col(col).ne_missing(pl.col(f"{col}_src"))).is_empty(), col


def test_shots_land_in_espns_frame() -> None:
    """x = 25 + shot_x: the mirrored frame puts the median error near 25 ft, not half a foot."""
    for i in range(len(CASES)):
        assert _report(i).shot_error_median_ft < 0.6


# --------------------------------------------------------------------------- mutation checks


def _wrap(monkeypatch: pytest.MonkeyPatch, name: str, after: Callable[[Any], Any]) -> None:
    original = getattr(ncaa, name)
    monkeypatch.setattr(ncaa, name, lambda *a, **k: after(original(*a, **k)))


def _mirror_shots(out: tuple) -> tuple:
    for p in out[0]:
        if p["shootingPlay"] and p["coordinate"]["x"] > -1000 and p["type"]["text"] != "MadeFreeThrow":
            p["coordinate"]["x"] = 50 - p["coordinate"]["x"]
    return out


def _unswapped(out: tuple) -> tuple:
    rows, _ = out
    for r in rows:
        r["home_score"], r["away_score"] = r["away_score"], r["home_score"]
    return rows, False


def _one_too_many(sides: Any) -> Any:
    """Each side's rebounds, assists, steals and blocks one too many: 12 team-box cells, over the 4-cell bound."""
    players, totals = sides
    for t in totals.values():
        for k in ("orb", "drb", "ast", "stl", "blk"):
            t[k] += 1
    return players, totals


MUTATIONS: dict[str, tuple[str, str, Callable[[pytest.MonkeyPatch], None]]] = {
    "shots mirrored across the long axis": (
        "wbb_401918712",
        "shot_error_median_ft",
        lambda mp: _wrap(mp, "_plays", _mirror_shots),
    ),
    "score columns left away-first": (
        "wbb_401913054",
        "period_score_mismatches",
        lambda mp: _wrap(mp, "_normalise", _unswapped),
    ),
    "every team total one too many": (
        "mbb_401920344",
        "team_box_mismatches",
        lambda mp: _wrap(mp, "_tally", _one_too_many),
    ),
    "a point added to every player": (
        "wbb_401918712",
        "player_points_agreement",
        lambda mp: _wrap(mp, "_player_stats", lambda s: [s[0], str(int(s[1]) + 1), *s[2:]]),
    ),
}


@pytest.mark.parametrize("mutation", MUTATIONS)
def test_a_broken_adapter_breaches_its_gate(monkeypatch: pytest.MonkeyPatch, mutation: str) -> None:
    game, gate, apply = MUTATIONS[mutation]
    i = IDS.index(game)
    assert _report(i).check() == []
    apply(monkeypatch)
    fails = parity._compare_game(CASES[i][0], CASES[i][1], _payloads(i), source="ncaa").check()
    assert any(f.startswith(gate) for f in fails), (mutation, fails)


# --------------------------------------------------------------------------- serving


def test_a_contest_of_another_game_is_refused() -> None:
    pay = _payloads(0)["ncaa"]
    with pytest.raises(SourceUnavailable, match="not 1"):
        ncaa._ncaa_adapter(CASES[0][0], 1, dispatch.SourceContext(payload=pay))


def test_no_payload_and_no_data_api_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDV_DATA_API_URL", raising=False)
    with pytest.raises(SourceUnavailable, match="no contest payload"):
        ncaa._ncaa_adapter("mbb", CASES[0][1], dispatch.SourceContext())


class _Resp:
    def __init__(self, body: dict):
        self.status_code, self._body = 200, body
        self.text = json.dumps(body)

    def json(self) -> dict:
        return self._body


def test_the_data_api_read_rebuilds_the_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no payload, the contest is read from the Data API by ESPN game id, page by page (stubbed: no network)."""
    i = 0
    lg, eid, _ = CASES[i]
    contest = _payloads(i)["ncaa"]["contest"]
    calls: list[tuple[str, dict]] = []

    def transport(url: str, params: dict, headers: dict, **_: Any) -> _Resp:
        calls.append((url.rsplit("/", 1)[1], params))
        table = url.rsplit("/", 1)[1].removeprefix("ncaa_")
        rows = contest["player_crosswalk" if table == "player_crosswalk" else table]
        if table == "player_crosswalk":
            rows = [r for r in rows if str(r["ncaa_team_id"]) == params["ncaa_team_id"]]
        return _Resp({"data": rows[params["offset"] : params["offset"] + params["limit"]]})

    monkeypatch.setattr("sportsdataverse.dl_utils.download", transport)
    got = ncaa._fetch_contest(lg, eid, "https://example.invalid")
    assert [c[0] for c in calls[:3]] == ["ncaa_pbp", "ncaa_shots", "ncaa_player_box"]
    assert all(p.get("espn_game_id") == str(eid) for _, p in calls[:3])
    assert {k: len(v) for k, v in got.items()} == {k: len(contest[k]) for k in got}
    a = ncaa._ncaa_to_espn_summary(got, espn_id=eid, league=lg, teams=_teams()[lg])
    b = ncaa._ncaa_to_espn_summary(contest, espn_id=eid, league=lg, teams=_teams()[lg])
    assert a == b


def test_ncaa_through_dispatch_once_it_is_selectable(monkeypatch: pytest.MonkeyPatch) -> None:
    i = IDS.index("wbb_401918712")
    lg, eid, _ = CASES[i]
    monkeypatch.setitem(dispatch.SOURCE_ORDER, lg, ("espn", "ncaa"))
    out = dispatch._process_game(lg, eid, source="ncaa", payloads={"ncaa": _payloads(i)["ncaa"]})
    prov = out.provenance
    assert prov["served"] == "ncaa" and prov["fallback"] is False and out.health == {"ncaa": "ok"}
    assert prov["lossy_columns"] == list(KNOWN_LOSSY[(lg, "ncaa")]) and prov["contract"]["ok"]
    assert out.frames["pbp"].height > 350 and out.frames["player_box"].height >= 15
