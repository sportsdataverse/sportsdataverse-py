"""H1 nightly parity harness (``tools/validation/source_parity.py``) — offline, 0 network.

The only payload used is the committed CLE @ JAX summary, injected on both sides; the
alternate is a stub adapter that hands the same summary back, so the identity case must score
a perfect pairing. Everything else is unit-tested on small frames, because the failure modes
that matter (a NaN correlation passing a floor, a state key pairing a Timeout against a pass)
are not visible in a happy-path integration run.
"""

from __future__ import annotations

import math

import polars as pl
import pytest

from sportsdataverse.football.sources import dispatch
from sportsdataverse.football.sources.dispatch import AdaptedGame
from tools.validation import source_parity as sp

from .conftest import NFL_GAME_ID


# --------------------------------------------------------------------------- configuration


def test_floors_file_is_loadable_and_well_formed():
    floors = sp.load_floors()
    assert floors["defaults"]["join_rate"] > 0
    for key, cfg in floors["sources"].items():
        league, _, source = key.partition(".")
        assert league in ("nfl", "cfb"), key
        assert source
        assert cfg.get("join", "state") in ("id", "state"), key


def test_unknown_source_is_measured_but_cannot_trip_a_correlation_alert():
    """An adapter registers the day its PR merges; it must be swept before its floors exist."""
    cfg = sp.source_config(sp.load_floors(), "nfl", "brand_new_source")
    assert cfg["floors"] == {}
    assert cfg["join"] == "state"  # the safe default: never assume ESPN play ids
    summary = _summary(games_ok=1, pooled={"EPA": {"r": 0.1, "n": 100}})
    assert [a["rule"] for a in sp.alerts_for(summary, cfg)] == []


def test_registered_alternates_reads_dispatch_and_never_returns_espn():
    for league in ("nfl", "cfb"):
        alts = sp.registered_alternates(league)
        assert "espn" not in alts
        assert set(alts) <= set(dispatch.SOURCE_ORDER[league])
    assert "shield" in sp.registered_alternates("nfl")


# --------------------------------------------------------------------------- odds


def test_one_closing_line_goes_into_both_paths(nfl_summary):
    odds, how = sp.espn_odds(nfl_summary)
    assert set(odds) == {"gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable"}
    assert how in ("espn_pickcenter", "processor_default")


def test_no_pickcenter_falls_back_to_the_processor_default_on_both_sides(nfl_summary):
    odds, how = sp.espn_odds({**nfl_summary, "pickcenter": []})
    assert how == "processor_default"
    assert odds == sp.DEFAULT_ODDS


# --------------------------------------------------------------------------- pairing


def test_state_join_drops_admin_rows_and_ambiguous_keys():
    key = sp.STATE_KEYS["cfb"]
    frame = pl.DataFrame(
        {
            "type.text": ["Pass Reception", "Timeout", "Rush", "Rush"],
            "period": [1, 1, 2, 2],
            "start.down": [1, 1, 3, 3],
            "start.distance": [10, 10, 2, 2],
            "start.yardsToEndzone": [75, 75, 40, 40],
            "start.pos_team.id": ["1", "1", "2", "2"],
        }
    )
    kept = sp._snaps(frame, key)
    # the Timeout carries the preceding snap's state (dropped), and the two identical Rush
    # rows cannot identify a play, so both go rather than one being paired wrongly
    assert kept.height == 1
    assert kept["type.text"].to_list() == ["Pass Reception"]


def test_identity_parity_is_perfect(nfl_summary, no_network, monkeypatch):
    """The same summary down both paths: every play pairs and every metric agrees exactly."""

    def stub(league, espn_id, ctx):
        return AdaptedGame(summary=nfl_summary, native_ids={"stub": "1"})

    monkeypatch.setitem(dispatch._ADAPTERS, ("nfl", "shield"), stub)
    row, pairs = sp.compare_game("nfl", NFL_GAME_ID, "shield", join="id", payloads={"espn": nfl_summary})

    assert "error" not in row, row.get("error")
    assert row["served"] == "shield"
    assert row["n_paired"] == row["n_espn"] > 100
    assert row["join_rate"] == 1.0
    assert row["r_EPA"] == pytest.approx(1.0)
    assert row["exact_EPA"] == pytest.approx(1.0)
    assert row["divergence"] == "{}"
    assert pairs is not None and pairs.height == row["n_paired"]


def test_an_unavailable_source_is_recorded_not_answered_by_espn(nfl_summary, no_network, monkeypatch):
    """``fallthrough=False`` is load-bearing: a fallen-through game would score 1.0 vs itself."""

    def down(league, espn_id, ctx):
        raise dispatch.SourceUnavailable("stub down")

    monkeypatch.setitem(dispatch._ADAPTERS, ("nfl", "shield"), down)
    row, pairs = sp.compare_game("nfl", NFL_GAME_ID, "shield", join="id", payloads={"espn": nfl_summary})
    assert row["unavailable"] is True
    assert pairs is None
    assert "r_EPA" not in row


# --------------------------------------------------------------------------- classification


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"EPA_cand": None}, "null_metric"),
        ({"type.text_cand": "Sack"}, "type_label_differs"),
        ({"start.distance_cand": 7}, "down_distance_differs"),
        ({"start.yardsToEndzone_cand": 60}, "start_spot_differs"),
        ({"end.yardsToEndzone_cand": 60}, "end_spot_differs"),
        ({}, "same_state_metric_differs"),
    ],
)
def test_classify_names_the_disagreement(overrides, expected):
    base = {
        "EPA": 1.0,
        "EPA_cand": -1.0,
        "type.text": "Rush",
        "type.text_cand": "Rush",
        "start.down": 1,
        "start.down_cand": 1,
        "start.distance": 10,
        "start.distance_cand": 10,
        "start.yardsToEndzone": 75,
        "start.yardsToEndzone_cand": 75,
        "end.yardsToEndzone": 70,
        "end.yardsToEndzone_cand": 70,
    }
    assert sp._classify({**base, **overrides}) == expected


def test_divergence_only_counts_rows_over_the_threshold():
    paired = pl.DataFrame(
        {
            "EPA": [1.0, 1.0, 1.0],
            "EPA_cand": [1.0, 0.9, -1.0],  # 0.0, 0.1, 2.0 apart
            "type.text": ["Rush", "Rush", "Rush"],
            "type.text_cand": ["Rush", "Rush", "Sack"],
        }
    )
    assert sp.divergence_classes(paired) == {"type_label_differs": 1}


def test_pooled_is_over_every_play_not_a_mean_of_per_game_r():
    small = pl.DataFrame({"EPA": [1.0, 2.0], "EPA_cand": [1.0, 2.0]})
    big = pl.DataFrame({"EPA": [1.0, 2.0, 3.0, 4.0], "EPA_cand": [4.0, 3.0, 2.0, 1.0]})
    stats = sp.pooled([small, big])["EPA"]
    assert stats["n"] == 6
    assert stats["exact_share"] == pytest.approx(2 / 6)
    assert stats["r"] < 1.0  # the 4 disagreeing plays outweigh the 2 agreeing ones


# --------------------------------------------------------------------------- alert rules


def _summary(**kw):
    base = {
        "date": "2026-09-13",
        "league": "nfl",
        "source": "shield",
        "games": 10,
        "games_ok": 10,
        "games_unavailable": 0,
        "unavailable_share": 0.0,
        "n_espn_plays": 1000,
        "n_paired_plays": 1000,
        "join_rate": 1.0,
        "pooled": {"EPA": {"r": 0.999, "n": 1000}},
        "alerts": [],
    }
    base.update(kw)
    return base


def test_correlation_floor_alert():
    cfg = {"floors": {"EPA": 0.97}, "join_rate": 0.9, "unavailable_share": 0.5}
    assert sp.alerts_for(_summary(), cfg) == []
    fired = sp.alerts_for(_summary(pooled={"EPA": {"r": 0.5, "n": 1000}}), cfg)
    assert [a["rule"] for a in fired] == ["correlation_floor"]
    assert fired[0]["metric"] == "EPA"


def test_a_nan_correlation_fails_the_floor():
    """pl.corr is NaN on a zero-variance column — a source whose WP model never ran and
    ships a constant must not sail past every floor on ``got < floor`` being False."""
    cfg = {"floors": {"EPA": 0.9}, "join_rate": 0.9, "unavailable_share": 0.5}
    for value in (float("nan"), None):
        fired = sp.alerts_for(_summary(pooled={"EPA": {"r": value, "n": 1000}}), cfg)
        assert [a["rule"] for a in fired] == ["correlation_floor"]
    # and a missing metric entirely
    assert [a["rule"] for a in sp.alerts_for(_summary(pooled={}), cfg)] == ["correlation_floor"]


def test_join_rate_and_unavailable_alerts():
    cfg = {"floors": {}, "join_rate": 0.9, "unavailable_share": 0.5}
    assert [a["rule"] for a in sp.alerts_for(_summary(join_rate=0.5, n_paired_plays=500), cfg)] == ["join_rate"]
    fired = sp.alerts_for(_summary(games_unavailable=6, unavailable_share=0.6, games_ok=4), cfg)
    assert "source_unavailable" in [a["rule"] for a in fired]


def test_a_day_with_no_finals_raises_nothing():
    cfg = {"floors": {"EPA": 0.99}, "join_rate": 0.9, "unavailable_share": 0.5}
    empty = _summary(games=0, games_ok=0, join_rate=0.0, n_espn_plays=0, n_paired_plays=0, pooled={})
    assert sp.alerts_for(empty, cfg) == []


# --------------------------------------------------------------------------- telemetry


def test_telemetry_is_fail_open_without_keys(monkeypatch):
    monkeypatch.delenv("SDV_INGEST_KEY", raising=False)
    monkeypatch.delenv("SDV_DATA_ADMIN_KEY", raising=False)
    monkeypatch.setattr(sp, "ADMIN_KEY_FILE", sp.Path("/nonexistent/key"))
    out = sp.post_telemetry([_summary()])
    assert out["posted"] == 0 and "skipped" in out


def test_summaries_and_alerts_both_reach_the_store(monkeypatch):
    sent: list[dict] = []

    class _Resp:
        status_code = 202

        @staticmethod
        def json():
            return {"accepted": 99}

    def fake_post(url, json, headers, timeout):  # noqa: A002
        sent.append({"url": url, "events": json["events"], "headers": headers})
        return _Resp()

    monkeypatch.setenv("SDV_INGEST_KEY", "ingest")
    monkeypatch.setenv("SDV_DATA_ADMIN_KEY", "admin")
    import requests

    monkeypatch.setattr(requests, "post", fake_post)
    summary = _summary(
        alerts=[
            {
                "rule": "correlation_floor",
                "source": "nfl.shield",
                "metric": "EPA",
                "observed": 0.5,
                "threshold": 0.97,
                "detail": "x",
            }
        ]
    )
    sp.post_telemetry([summary], base_url="http://api")
    tables = {e["table"] for e in sent[0]["events"]}
    assert tables == {"client_event", "error_log"}
    alert_row = next(e["row"] for e in sent[0]["events"] if e["table"] == "error_log")
    assert alert_row["service"] == "source_parity"  # what GET /v1/admin/errors filters on
    assert "correlation_floor" in alert_row["message"]
    assert all(math.isfinite(e["row"]["value"]) for e in sent[0]["events"] if e["table"] == "client_event")
