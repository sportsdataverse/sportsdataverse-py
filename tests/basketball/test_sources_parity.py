"""BB-D15: the ESPN-vs-live parity gates, on 27 real captured games, decide whether "live" is selectable.

The corpus is the three BB-D13 games (``tests/basketball/fixtures/live_adapter/``, loaded through
``test_sources_live``'s own helpers) plus 24 more captured for this item (``tests/basketball/fixtures/parity/``,
provenance in its README): 18 NBA 2025-26 and 9 WNBA 2026 games, with overtime up to four extra periods,
NBA Cup knockout and international neutral sites, the playoffs and play-in, technicals, flagrants and two
ejections. Every game must pass every gate in :data:`~sportsdataverse.basketball.sources.parity.GATES`;
the mutation tests break the adapter one way at a time and watch the gate that guards it go red.
"""

from __future__ import annotations

import fnmatch
import math
from collections.abc import Callable
from functools import lru_cache
from typing import Any

import polars as pl
import pytest

from sportsdataverse.basketball.sources import dispatch, parity
from sportsdataverse.basketball.sources.contract import KNOWN_LOSSY
from sportsdataverse.basketball.sources.idmap import IDMAP_SCHEMA
from sportsdataverse.nba.live_pbp import to_espn_summary as live
from tests.basketball import test_sources_live as d13

CORPUS = d13.TESTS / "basketball" / "fixtures" / "parity"


@lru_cache(maxsize=1)
def _corpus_idmap() -> pl.DataFrame:
    return pl.DataFrame(d13._json(CORPUS / "idmap_rows.json.gz"), schema=IDMAP_SCHEMA)


def _cases() -> list[tuple[str, int, str]]:
    """(league, ESPN id, liveData id): the BB-D13 games, then the corpus in id-map order."""
    games = _corpus_idmap().filter(pl.col("kind") == "game").sort("league", "espn_id")
    return [(g[0], g[1], g[2]) for g in d13.GAMES] + [
        (r["league"], int(r["espn_id"]), r["source_id"]) for r in games.iter_rows(named=True)
    ]


CASES = _cases()
IDS = [f"{lg}_{eid}" for lg, eid, _ in CASES]


def _payloads(i: int) -> dict[str, Any]:
    """Fresh (mutable) native payloads for CASES[i]: ESPN's raw summary and liveData + its id map."""
    lg, eid, lid = CASES[i]
    if i < len(d13.GAMES):
        return {"espn": d13._espn(d13.GAMES[i]), "live": d13._payload(d13.GAMES[i])}
    return {
        "espn": d13._json(CORPUS / f"espn_{lg}_{eid}.json.gz"),
        "live": {
            "playbyplay": d13._json(CORPUS / f"playbyplay_{lid}.json.gz"),
            "boxscore": d13._json(CORPUS / f"boxscore_{lid}.json.gz"),
            "idmap": _corpus_idmap(),
        },
    }


@lru_cache(maxsize=None)
def _legs(i: int) -> dict[str, tuple[dict, dict]]:
    """``{source: (helper_<lg>_pbp final, game_page_frames)}`` through the dispatch leg a served game takes."""
    lg, eid, _ = CASES[i]
    pay = _payloads(i)
    out = {}
    for src in ("espn", "live"):
        _, _, final, frames = dispatch._run_source(lg, eid, dispatch._adapter_for(lg, src), pay[src])
        out[src] = (final, frames)
    return out


@lru_cache(maxsize=None)
def _report(i: int) -> parity.ParityReport:
    legs = _legs(i)
    return parity._compare_frames(CASES[i][0], CASES[i][1], "live", legs["espn"], legs["live"])


# --------------------------------------------------------------------------- the corpus


def test_corpus_is_the_one_the_gates_were_derived_from() -> None:
    """27 finished games, none twice, covering every situation the row names."""
    leagues = [lg for lg, _, _ in CASES]
    assert (leagues.count("nba"), leagues.count("wnba")) == (18, 9)
    assert len({eid for _, eid, _ in CASES}) == len(CASES)
    situations: dict[str, set[int]] = {k: set() for k in ("ot", "neutral", "playoff", "play_in", "tech", "eject")}
    for i, (_, eid, _) in enumerate(CASES):
        pay = _payloads(i)
        comp = pay["espn"]["header"]["competitions"][0]
        assert comp["status"]["type"]["completed"] is True
        assert pay["live"]["boxscore"]["game"]["gameStatus"] == 3
        actions = pay["live"]["playbyplay"]["game"]["actions"]
        flags = {
            "ot": max(a["period"] for a in actions) > 4,
            "neutral": comp["neutralSite"] is True,
            "playoff": pay["espn"]["header"]["season"]["type"] == 3,
            "play_in": pay["espn"]["header"]["season"]["type"] == 5,
            "tech": any(a.get("subType") == "technical" for a in actions),
            "eject": any(a["actionType"] == "ejection" for a in actions),
        }
        for k, on in flags.items():
            if on:
                situations[k].add(eid)
    assert {k: len(v) for k, v in situations.items()} == {
        "ot": 8,
        "neutral": 7,
        "playoff": 8,
        "play_in": 2,
        "tech": 18,
        "eject": 1,
    }
    assert 401857031 in situations["ot"]  # WNBA, four overtimes
    files = list(CORPUS.glob("*.json.gz"))
    assert len(files) == 3 * 24 + 1 and sum(f.stat().st_size for f in files) < 15_000_000


# --------------------------------------------------------------------------- the gates


@pytest.mark.parametrize("i", range(len(CASES)), ids=IDS)
def test_every_game_passes_every_gate(i: int) -> None:
    report = _report(i)
    assert report.check() == [], (IDS[i], report.check())
    assert report.n_plays_espn > 350 and report.n_players >= 15 and report.n_shots > 120 and report.n_ft_tech > 20


@pytest.mark.parametrize("i", range(len(CASES)), ids=IDS)
def test_the_equalised_prior_is_espns(i: int) -> None:
    """The second WP correlation gives the live plays ESPN's own starting point, so only the plays differ."""
    prior = _report(i).prior
    assert prior["source_equalised"] == pytest.approx(prior["espn"], abs=1e-12)


def test_no_gate_is_looser_than_the_corpus_needs() -> None:
    """Every bound sits within its stated margin of the corpus worst case: 0.01 on a share, 0.05 ft on a distance.

    A bound moved further out without the corpus moving fails here, so a gate can only be loosened by a
    corpus that needs it (and a comment saying why), never quietly.
    """
    observed = [_report(i).metrics() for i in range(len(CASES))]
    for (lg, src), gates in parity.GATES.items():
        assert src == "live" and lg in ("nba", "wnba")
        for name, (lo, hi) in gates.items():
            values = [m[name] for m in observed]
            slack = 0.05 if name.endswith("_ft") else 0.01
            if lo is not None:
                assert 0 <= min(values) - lo <= slack, (name, lo, min(values))
            if hi is not None:
                assert 0 <= hi - max(values) <= slack, (name, hi, max(values))


def test_known_lossy_is_exactly_what_the_corpus_measures() -> None:
    """KNOWN_LOSSY is measured, never guessed: each pattern matches a gap on the corpus and covers all of them."""
    measured = d13._measured_lossy((_legs(i)["live"][1], _legs(i)["espn"][1]) for i in range(len(CASES)))
    for lg in ("nba", "wnba"):
        patterns = KNOWN_LOSSY[(lg, "live")]
        assert {c for c in measured if not any(fnmatch.fnmatch(c, p) for p in patterns)} == set(), lg
        assert [p for p in patterns if not any(fnmatch.fnmatch(c, p) for c in measured)] == [], lg


def test_compare_game_runs_both_sources_through_dispatch() -> None:
    """The public entry point fetches nothing when both payloads are given and matches the cached legs."""
    i = IDS.index("wnba_401856986")
    assert parity._compare_game(CASES[i][0], CASES[i][1], _payloads(i)) == _report(i)


# --------------------------------------------------------------------------- mutation checks

#: NBA 401866759 (play-in, two ejections, technicals) and NBA 401810002 (Mexico City; FG 34-80 = 42.5%).
EJECTIONS, HALF_PCT = IDS.index("nba_401866759"), IDS.index("nba_401810002")


def _wrap(monkeypatch: pytest.MonkeyPatch, name: str, after: Callable[[Any], Any]) -> None:
    original = getattr(live, name)
    monkeypatch.setattr(live, name, lambda *a, **k: after(original(*a, **k)))


def _stretch_shots(row: dict) -> dict:
    if row["coordinate"]["x"] > -1000:
        row["coordinate"] = {"x": 25 + (row["coordinate"]["x"] - 25) * 1.2, "y": row["coordinate"]["y"] * 1.2}
    return row


def _last_basket_a_period_late(out: tuple) -> tuple:
    """The last basket of period 2 logged after the period ends: a boundary off by one play."""
    plays = out[0]
    last = max(i for i, p in enumerate(plays) if p["period"]["number"] == 2 and p["scoringPlay"])
    end = next(i for i, p in enumerate(plays) if i > last and p["type"]["text"] == "End Period")
    moved = plays.pop(last)
    moved["period"] = {**moved["period"], "number": 3}
    plays.insert(end, moved)
    return out


def _clock_stopped(row: dict) -> dict:
    """Every clock read as the period's last second: the score is right, the time remaining is not."""
    row["clock"] = {"displayValue": "0.0"}
    return row


def _free_throw_first(out: tuple) -> tuple:
    plays = out[0]
    t = next(i for i, p in enumerate(plays) if "Technical" in p["type"]["text"] and "Free" not in p["type"]["text"])
    f = next(i for i, p in enumerate(plays) if i > t and "Free Throw" in p["type"]["text"])
    plays.insert(t, plays.pop(f))
    return out


def _drop_every_25th(out: tuple) -> tuple:
    return ([p for k, p in enumerate(out[0]) if k % 25 != 24], *out[1:])


def _half_to_even(stats: list[dict]) -> list[dict]:
    by = {s["name"]: s["displayValue"] for s in stats}
    for pct, pair in (
        ("fieldGoalPct", "fieldGoalsMade-fieldGoalsAttempted"),
        ("threePointFieldGoalPct", "threePointFieldGoalsMade-threePointFieldGoalsAttempted"),
    ):
        made, att = (int(x) for x in by[pair].split("-"))
        by[pct] = str(round(100 * made / att))
    return [{"name": k, "displayValue": v} for k, v in by.items()]


def _point_more(stats: list[str]) -> list[str]:
    stats[1] = str(int(stats[1]) + 1)
    return stats


MUTATIONS: dict[str, tuple[int, str, Callable[[pytest.MonkeyPatch], None]]] = {
    "shot coordinates 20% off": (
        EJECTIONS,
        "shot_error_median_ft",
        lambda mp: _wrap(mp, "_play_row", _stretch_shots),
    ),
    "the last basket of a period a period late": (
        EJECTIONS,
        "period_score_mismatches",
        lambda mp: _wrap(mp, "_plays", _last_basket_a_period_late),
    ),
    "the clock stopped at 0.0": (
        EJECTIONS,
        "wp_corr_equal_prior",
        lambda mp: _wrap(mp, "_play_row", _clock_stopped),
    ),
    "a free throw ahead of its technical": (
        EJECTIONS,
        "ft_tech_order_agreement",
        lambda mp: _wrap(mp, "_plays", _free_throw_first),
    ),
    "every 25th play dropped": (
        EJECTIONS,
        "play_count_ratio",
        lambda mp: _wrap(mp, "_plays", _drop_every_25th),
    ),
    "percentages rounded half to even": (
        HALF_PCT,
        "team_box_mismatches",
        lambda mp: _wrap(mp, "_team_statistics", _half_to_even),
    ),
    "minutes truncated, not rounded": (
        EJECTIONS,
        "player_minutes_agreement",
        lambda mp: mp.setattr(live, "_minutes", lambda v: (m := live._CLOCK.match(str(v or ""))) and m[1] or "0"),
    ),
    "a point added to every player": (
        EJECTIONS,
        "player_points_agreement",
        lambda mp: _wrap(mp, "_player_stats", _point_more),
    ),
}


@pytest.mark.parametrize("mutation", MUTATIONS)
def test_a_broken_adapter_breaches_its_gate(monkeypatch: pytest.MonkeyPatch, mutation: str) -> None:
    i, gate, apply = MUTATIONS[mutation]
    assert _report(i).check() == []  # the unbroken adapter passes on this game
    apply(monkeypatch)
    fails = parity._compare_game(CASES[i][0], CASES[i][1], _payloads(i)).check()
    assert any(f.startswith(gate) for f in fails), (mutation, fails)


def test_a_player_missing_from_the_id_map_breaches_the_pairing_gate() -> None:
    lg, eid, lid = CASES[EJECTIONS]
    box = _payloads(EJECTIONS)["live"]["boxscore"]["game"]["homeTeam"]["players"]
    starters = {str(p["personId"]) for p in box if p.get("starter") == "1"}
    pay = _payloads(EJECTIONS)
    pay["live"]["idmap"] = _corpus_idmap().filter(~((pl.col("kind") == "player") & pl.col("source_id").is_in(starters)))
    fails = parity._compare_game(lg, eid, pay).check()
    assert any(f.startswith("player_paired_share") for f in fails), fails


# --------------------------------------------------------------------------- the harness itself


def test_check_names_an_unmeasured_metric() -> None:
    """A NaN (a metric the harness could not compute) compares False against every bound: it must fail."""
    report = parity.ParityReport("nba", 1, "live")
    fails = report.check()
    assert len(fails) == len(parity.LIVE_GATES) - 2  # the two mismatch counts are 0 on an empty report
    assert all("not measured" in f for f in fails)
    assert all(math.isnan(v) for k, v in report.metrics().items() if not k.endswith("mismatches"))


def test_join_keys_must_share_a_dtype() -> None:
    a = pl.DataFrame({"id": [1, 2]})
    with pytest.raises(ValueError, match="dtype differs"):
        parity._pair(a, a.with_columns(pl.col("id").cast(pl.Utf8)), "id")


def test_an_unregistered_source_is_refused() -> None:
    with pytest.raises(ValueError, match="no adapter registered"):
        parity._compare_game("mbb", 401830342, {}, source="live")


def test_a_source_that_cannot_be_served_cannot_pass() -> None:
    pay = _payloads(0)
    pay["live"] = {**pay["live"], "idmap": _corpus_idmap().filter(pl.col("kind") != "game")}
    with pytest.raises(dispatch._SourceFailed, match="not in the id map"):
        parity._compare_game(CASES[0][0], CASES[0][1], pay)
