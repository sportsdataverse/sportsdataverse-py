"""Roster-talent tests (T2.2 Phase 1).

Task 1.1 pins the recruit-class loader's normalization of the 247 RDB recruit feed
(``sports247_recruits``) into per-recruit rows, monkeypatched to a synthetic payload
so no network is hit. The real column names are confirmed live during implementation.
"""

from __future__ import annotations

import sys

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent, load_recruit_classes

_mod = sys.modules["sportsdataverse.cfb.cfb_roster_talent"]


def _fake_recruit_page(*, year: int, page: int | None = None, **kwargs: object) -> pl.DataFrame:
    """Two committed recruits + one uncommitted (dropped); empty after page 1 (ends paging)."""
    if page and page > 1:
        return pl.DataFrame(schema={"key": pl.Int64})
    return pl.DataFrame(
        {
            "key": [46112955, 46133902, 46128573],  # 247 recruit key (Int64)
            "first_name": ["Dante", "Karmello", "Rem"],
            "last_name": ["Moore", "English", "Uncommitted"],
            "composite_star_rating": [4.0, 5.0, 3.0],
            "composite_rating": [94.5, 98.1, 84.2],
            "primary_position": ["QB", "WR", "OT"],
            # committed team key is Float64 in the RDB -> must cast Int64->Utf8, not "71.0"
            "committed_institution_team_key": [71.0, 71.0, None],
            "committed_institution_full_name": ["Michigan Wolverines", "Michigan Wolverines", None],
        }
    )


def test_loader_normalizes_to_per_recruit_contract(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "sports247_recruits", _fake_recruit_page)
    out = load_recruit_classes(2023, division="fbs")

    assert out.columns == ["season", "team_id", "team", "recruit_id", "player_name", "stars", "grade", "position"]
    assert out.height == 2  # the uncommitted recruit (null team) is dropped
    assert out.schema["team_id"] == pl.Utf8
    assert out.schema["recruit_id"] == pl.Utf8
    assert out.schema["stars"] == pl.Int64
    assert out.schema["grade"] == pl.Float64
    # float team key -> clean integer string, never "71.0"
    assert out["team_id"].unique().to_list() == ["71"]
    assert out["recruit_id"].to_list() == ["46112955", "46133902"]
    assert out["player_name"].to_list() == ["Dante Moore", "Karmello English"]
    assert out["season"].unique().to_list() == [2023]


def test_loader_multi_season_concats(monkeypatch) -> None:
    monkeypatch.setattr(_mod, "sports247_recruits", _fake_recruit_page)
    out = load_recruit_classes([2022, 2023], division="fbs")
    assert set(out["season"].unique().to_list()) == {2022, 2023}
    assert out.height == 4  # 2 committed recruits x 2 seasons


def test_loader_empty_returns_documented_schema(monkeypatch) -> None:
    """Empty in -> typed empty out, but it must NOT be silent (see below)."""
    monkeypatch.setattr(_mod, "sports247_recruits", lambda **k: pl.DataFrame(schema={"key": pl.Int64}))
    with pytest.warns(UserWarning, match="returned no rows"):
        out = load_recruit_classes(2023)
    assert out.height == 0
    assert out.schema["team_id"] == pl.Utf8 and out.schema["stars"] == pl.Int64


def test_empty_season_warns_because_classes_are_never_empty(monkeypatch) -> None:
    """A season yielding zero recruits is a FETCH FAILURE, not an empty class.

    This is the regression test for a bug that ran undetected: _PAGE_SIZE was
    500, which exceeds what the 247 RDB serves inside the 3s client timeout, so
    every page raised curl(28), no frames accumulated, and `cfb_roster_talent`
    returned (0, 7) for every season -- indistinguishable from "247 has no
    recruits". Every test here monkeypatches the feed, so none of them ever
    exercised the real page size, and the failure was invisible.
    """
    monkeypatch.setattr(_mod, "sports247_recruits", lambda **k: pl.DataFrame(schema={"key": pl.Int64}))
    with pytest.warns(UserWarning, match="fetch or schema failure"):
        load_recruit_classes(2023)


def test_schema_drift_raises_instead_of_returning_empty(monkeypatch) -> None:
    """One renamed 247 column must fail loudly, not yield an empty talent table.

    The predecessor returned a well-formed zero-row frame on any missing
    required column, so a feed rename would have produced empty talent forever
    with no error and no warning.
    """
    drifted = pl.DataFrame(
        {
            "key": [1],
            "composite_star_rating": [4.0],
            "composite_rating": [95.0],
            "primary_position": ["QB"],
            # committed_institution_* renamed away
            "school_team_key": [71],
            "school_full_name": ["Michigan Wolverines"],
        }
    )
    monkeypatch.setattr(_mod, "sports247_recruits", lambda **k: drifted)
    with pytest.raises(ValueError, match="no recognisable institution columns"):
        load_recruit_classes(2023)


def test_page_size_stays_within_the_measured_serving_limit() -> None:
    """_PAGE_SIZE must stay at or below what 247 actually serves in time.

    Measured against the live feed: 50/100/250 return in full, 500 times out at
    the 3s client budget. A unit test cannot hit the network, but it can pin
    the constant so the next person to "optimise" it upward has to read why.
    """
    assert _mod._PAGE_SIZE <= 250, _mod._PAGE_SIZE


def test_blue_chip_window_rollup() -> None:
    from sportsdataverse.cfb.cfb_roster_talent import blue_chip_ratio

    # team A: 2020 class = 2 blue-chip of 4; 2021 class = 4 blue-chip of 4
    rec = pl.DataFrame(
        {
            "season": [2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021],
            "team_id": ["A"] * 8,
            "recruit_id": [f"r{i}" for i in range(8)],
            "stars": [5, 4, 3, 3, 4, 4, 5, 4],
            "grade": [95.0] * 8,
            "position": ["QB"] * 8,
        }
    )
    out = blue_chip_ratio(rec, window=4, division="fbs").sort("season")
    # season 2020 window sees only the 2020 class: 2/4 = 0.5
    row20 = out.filter(pl.col("season") == 2020).row(0, named=True)
    assert row20["n_recruits"] == 4 and row20["n_blue_chip"] == 2
    assert abs(row20["blue_chip_ratio"] - 0.5) < 1e-9
    # season 2021 window sees 2020+2021: (2+4)/(4+4) = 0.75
    row21 = out.filter(pl.col("season") == 2021).row(0, named=True)
    assert abs(row21["blue_chip_ratio"] - 0.75) < 1e-9


def _two_class_recruits() -> pl.DataFrame:
    # team A: 2020 class stars [5, 4] -> 170 pts; 2021 class [4] -> 70 pts
    # team B: 2021 class [3] -> 45 pts
    return pl.DataFrame(
        {
            "season": [2020, 2020, 2021, 2021],
            "team_id": ["A", "A", "A", "B"],
            "team": ["Team A", "Team A", "Team A", "Team B"],
            "recruit_id": ["r1", "r2", "r3", "r4"],
            "stars": [5, 4, 4, 3],
            "grade": [98.0, 92.0, 91.0, 85.0],
            "position": ["QB", "EDGE", "WR", "OT"],
        }
    )


def test_talent_composite_weighted_sum_and_rank(monkeypatch) -> None:
    from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent

    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: _two_class_recruits())
    out = cfb_roster_talent(2021, division="fbs")
    # Rank decay (0.75) then class-recency weights (1.0, 0.9, ...):
    #   A 2021 class [4*]        -> 70 * 1**-0.75                  =  70.000
    #   A 2020 class [5*, 4*]    -> 100 * 1**-0.75 + 70 * 2**-0.75 = 141.622
    #   A total  = 1.0 * 70.000 + 0.9 * 141.622                    = 197.460
    # The pre-decay flat sum was 223.0; the second signee in the 2020 class is
    # now worth 2**-0.75 = 0.5946 of his face value.
    row_a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert abs(row_a["talent_composite"] - 197.4600241225857) < 1e-9
    row_b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert abs(row_b["talent_composite"] - 45.0) < 1e-9
    # dense rank desc within season + blue_chip_ratio joined in
    assert row_a["talent_rank"] == 1 and row_b["talent_rank"] == 2
    assert abs(row_a["blue_chip_ratio"] - 1.0) < 1e-9  # 3/3 blue chips
    assert row_a["n_recruits"] == 3
    assert out.schema["team_id"] == pl.Utf8


def test_talent_composite_247_override(monkeypatch) -> None:
    from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent

    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: _two_class_recruits())
    ora = pl.DataFrame(
        {"season": [2021], "team_id": ["A"], "talent_247": [999.5]},
        schema={"season": pl.Int64, "team_id": pl.Utf8, "talent_247": pl.Float64},
    )
    out = cfb_roster_talent(2021, division="fbs", composite_247=ora)
    row_a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert abs(row_a["talent_composite"] - 999.5) < 1e-9  # 247 overrides
    row_b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert abs(row_b["talent_composite"] - 45.0) < 1e-9  # ESPN-derived fallback


def test_loader_prefers_signed_institution_over_committed(monkeypatch) -> None:
    # r1 signed with team 71 but its committed_* drifted to 99 (decommit/transfer);
    # r2 never signed (signed_* null) -> falls back to committed 71.
    def _page(**kwargs):
        if kwargs.get("page", 1) > 1:
            return pl.DataFrame(schema={"key": pl.Int64})
        return pl.DataFrame(
            {
                "key": [1, 2],
                "committed_institution_team_key": [99.0, 71.0],
                "committed_institution_full_name": ["Wrong U", "Michigan Wolverines"],
                "signed_institution_team_key": [71.0, None],
                "signed_institution_full_name": ["Michigan Wolverines", None],
                "composite_star_rating": [4, 3],
                "composite_rating": [95.0, 88.0],
                "primary_position": ["QB", "WR"],
            }
        )

    monkeypatch.setattr(_mod, "sports247_recruits", _page)
    out = load_recruit_classes(2023)
    assert out["team_id"].unique().to_list() == ["71"]
    assert out["team"].unique().to_list() == ["Michigan Wolverines"]


def test_excess_signees_do_not_inflate_talent(monkeypatch) -> None:
    """Signees past the scholarship limit must not add to `talent_composite`.

    Regression test for a bug that only REAL data exposed. `class_points` was
    an uncapped sum of star points, so a team whose 247 page lists preferred
    walk-ons out-earned one that lists only scholarship signees. On the live
    2021-2024 feed that put Air Force 7th nationally off 200 signees at a
    0.000 blue-chip ratio, and Washington State 6th off 142 at 0.035. Every
    prior test used 1-2 recruits per class, where capped and uncapped agree
    exactly, so nothing caught it.

    The assertion is the cap's actual guarantee -- excess signees are inert --
    NOT "quality beats quantity". A full class of 25 three-stars really does
    carry more talent than five five-stars; encoding that opinion instead
    would be a test of my assumption rather than of the code.
    """
    top25 = {
        "season": [2023] * 25,
        "team_id": ["T"] * 25,
        "team": ["Team T"] * 25,
        "recruit_id": [f"r{i}" for i in range(25)],
        "stars": [4] * 25,
        "grade": [92.0] * 25,
        "position": ["OL"] * 25,
    }
    lean = pl.DataFrame(top25)
    # Same top 25, plus 175 walk-on-tier signees appended.
    padded = pl.concat(
        [
            lean,
            pl.DataFrame(
                {
                    "season": [2023] * 175,
                    "team_id": ["T"] * 175,
                    "team": ["Team T"] * 175,
                    "recruit_id": [f"w{i}" for i in range(175)],
                    "stars": [2] * 175,
                    "grade": [70.0] * 175,
                    "position": ["OL"] * 175,
                }
            ),
        ]
    )

    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: lean)
    lean_pts = cfb_roster_talent(2023, division="fbs")["talent_composite"][0]
    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: padded)
    padded_pts = cfb_roster_talent(2023, division="fbs")["talent_composite"][0]

    assert padded_pts == pytest.approx(lean_pts), f"175 walk-on signees moved talent from {lean_pts} to {padded_pts}"

    # And the cap is load-bearing: uncapped, the padding inflates the total.
    uncapped = cfb_roster_talent(2023, division="fbs", max_class_size=10_000)
    assert uncapped["talent_composite"][0] > lean_pts, "cap is inert -- test proves nothing"


def test_all_uncommitted_page_is_skipped_not_treated_as_drift(monkeypatch) -> None:
    """A page where no recruit has committed is data, not schema drift.

    The institution fields are nested objects that json_normalize expands into
    `<prefix>_team_key` / `<prefix>_full_name`. When the object is null for
    EVERY row on a page there is nothing to expand, so those flattened columns
    are absent and only a bare all-null `committed_institution` survives.
    Measured on the live feed: page 1 of 2024 carries
    `committed_institution_full_name` (95/250 null) alongside an all-null
    Float64 `committed_institution`; a deeper, all-uncommitted page carries
    only the latter.

    Requiring the flattened names unconditionally turned such a page into a
    hard ValueError that took the whole loader down. The rows would all be
    dropped anyway (null team), so the page is skipped quietly.
    """
    unexpanded = pl.DataFrame(
        {
            "key": [1, 2],
            "composite_star_rating": [3.0, 3.0],
            "composite_rating": [85.0, 84.0],
            "primary_position": ["QB", "WR"],
            "committed_institution": [None, None],  # all-null => never expanded
            "current_institution_full_name": ["Some HS", "Other HS"],
        },
        schema_overrides={"committed_institution": pl.Float64},
    )
    monkeypatch.setattr(_mod, "sports247_recruits", lambda **k: unexpanded)
    with pytest.warns(UserWarning, match="none resolved to a committed or signed school"):
        out = load_recruit_classes(2023)
    assert out.height == 0
    assert out.schema["team_id"] == pl.Utf8  # documented schema preserved


def test_signed_beats_committed_when_both_flattened(monkeypatch) -> None:
    """The coalesce order survives the rewrite: signed_* wins over committed_*."""
    both = pl.DataFrame(
        {
            "key": [1],
            "composite_star_rating": [4.0],
            "composite_rating": [95.0],
            "primary_position": ["QB"],
            "committed_institution_team_key": [99.0],
            "committed_institution_full_name": ["Wrong U"],
            "signed_institution_team_key": [71.0],
            "signed_institution_full_name": ["Michigan Wolverines"],
        }
    )
    calls = {"n": 0}

    def _page(**k):
        calls["n"] += 1
        return both if calls["n"] == 1 else pl.DataFrame(schema={"key": pl.Int64})

    monkeypatch.setattr(_mod, "sports247_recruits", _page)
    out = load_recruit_classes(2023)
    assert out["team_id"].to_list() == ["71"]
    assert out["team"].to_list() == ["Michigan Wolverines"]


def test_core_field_drift_still_raises(monkeypatch) -> None:
    """A renamed per-recruit field raises on the core branch.

    The institution branch and the core branch raise for different reasons and
    with different messages; both need cover, or splitting the check silently
    drops one of them.
    """
    drifted = pl.DataFrame(
        {
            "key": [1],
            "star_rating": [4.0],  # composite_star_rating renamed away
            "composite_rating": [95.0],
            "primary_position": ["QB"],
            "committed_institution_team_key": [71.0],
            "committed_institution_full_name": ["Michigan Wolverines"],
        }
    )
    monkeypatch.setattr(_mod, "sports247_recruits", lambda **k: drifted)
    with pytest.raises(ValueError, match="missing required columns"):
        load_recruit_classes(2023)


def test_transient_fetch_failure_is_retried(monkeypatch) -> None:
    """One connection reset must not abort a multi-page load.

    A four-season load is 60+ requests; 247 resets connections under sustained
    paging (`curl (56) Recv failure`), and without retry a single reset partway
    through discarded the whole multi-minute fetch.
    """
    monkeypatch.setenv("SDV_PY_247_BACKOFF", "0")  # no real sleeping in tests
    monkeypatch.setenv("SDV_PY_247_DELAY", "0")
    calls = {"n": 0}

    def _flaky(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("Failed to perform, curl: (56) Recv failure")
        if calls["n"] == 2:
            return _fake_recruit_page(year=2023, page=1)
        return pl.DataFrame(schema={"key": pl.Int64})

    monkeypatch.setattr(_mod, "sports247_recruits", _flaky)
    out = load_recruit_classes(2023)
    assert out.height == 2, out  # the retry recovered the page
    assert calls["n"] >= 2


def test_exhausted_retries_warn_rather_than_raise(monkeypatch) -> None:
    """A page that never comes back degrades loudly, not silently or fatally."""
    monkeypatch.setenv("SDV_PY_247_BACKOFF", "0")
    monkeypatch.setenv("SDV_PY_247_RETRIES", "2")

    def _dead(**kwargs):
        raise ConnectionError("Failed to perform, curl: (56) Recv failure")

    monkeypatch.setattr(_mod, "sports247_recruits", _dead)
    with pytest.warns(UserWarning) as rec:
        out = load_recruit_classes(2023)
    msgs = " ".join(str(w.message) for w in rec)
    assert "failed after 2 attempts" in msgs
    assert "returned no rows" in msgs  # the season-level signal still fires
    assert out.height == 0


def test_retry_does_not_swallow_schema_drift(monkeypatch) -> None:
    """Drift raises on the FIRST page -- the retry wraps the fetch only.

    Retrying a ValueError would turn a loud, actionable schema error into three
    slow attempts and then a confusing warning.
    """
    monkeypatch.setenv("SDV_PY_247_BACKOFF", "0")
    calls = {"n": 0}

    def _drifted(**kwargs):
        calls["n"] += 1
        return pl.DataFrame({"key": [1], "composite_rating": [95.0], "primary_position": ["QB"]})

    monkeypatch.setattr(_mod, "sports247_recruits", _drifted)
    with pytest.raises(ValueError, match="missing required columns"):
        load_recruit_classes(2023)
    assert calls["n"] == 1, f"drift was retried {calls['n']}x -- retry must wrap the fetch only"


def test_env_overrides_ignore_unusable_values(monkeypatch) -> None:
    """A typo'd env var warns and falls back; it must not crash the loader."""
    monkeypatch.setenv("SDV_PY_247_RETRIES", "not-a-number")
    monkeypatch.setenv("SDV_PY_247_DELAY", "0")
    monkeypatch.setattr(_mod, "sports247_recruits", _fake_recruit_page)
    with pytest.warns(UserWarning, match="is not numeric"):
        out = load_recruit_classes(2023)
    assert out.height == 2


def test_recruits_injection_skips_the_feed(monkeypatch) -> None:
    """`recruits=` bypasses the 247 fetch entirely.

    This is the seam the cfbfastR-cfb-data producer compiles through: a class
    is immutable once signed, but the composite spans 4 seasons, so fetching
    live re-pulled the same frozen classes once per target season (~20 min a
    call). Passing a pre-loaded frame must not touch the network at all.
    """

    def _boom(**kwargs):
        raise AssertionError("the feed was called despite recruits= being supplied")

    monkeypatch.setattr(_mod, "sports247_recruits", _boom)
    monkeypatch.setattr(_mod, "load_recruit_classes", _boom)
    out = cfb_roster_talent(2021, division="fbs", recruits=_two_class_recruits())
    row_a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert abs(row_a["talent_composite"] - 197.4600241225857) < 1e-9  # same math as the live path


def test_recruits_injection_matches_the_live_path(monkeypatch) -> None:
    """Injected and fetched inputs must produce identical output.

    If these ever diverge, the published dataset and a live call disagree --
    which is the whole risk of having an offline producer at all.
    """
    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: _two_class_recruits())
    fetched = cfb_roster_talent(2021, division="fbs")
    injected = cfb_roster_talent(2021, division="fbs", recruits=_two_class_recruits())
    assert fetched.sort("team_id").equals(injected.sort("team_id"))


def test_rank_decay_discounts_the_tail_of_a_class(monkeypatch) -> None:
    """A class's Nth signee is worth less than its 1st, and decay controls how much.

    Regression test for the volume artefact: a flat sum made talent track class
    SIZE, so on the live feed Air Force ranked 7th nationally off ~200 signees
    with a 0.000 blue-chip ratio. `rank_decay` is the fix, and its default was
    selected by sweeping against Spearman with actual wins (peak at 0.75), not
    chosen by taste.
    """
    # one team, one class: five identical 4-stars. Under a flat sum all five are
    # worth the same; under decay each successive one is worth strictly less.
    rec = pl.DataFrame(
        {
            "season": [2021] * 5,
            "team_id": ["A"] * 5,
            "team": ["Team A"] * 5,
            "recruit_id": [f"r{i}" for i in range(5)],
            "stars": [4] * 5,
            "grade": [90.0] * 5,
            "position": ["QB"] * 5,
        }
    )
    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: rec)

    flat = cfb_roster_talent(2021, division="fbs", rank_decay=0.0)["talent_composite"][0]
    decayed = cfb_roster_talent(2021, division="fbs", rank_decay=0.75)["talent_composite"][0]
    steeper = cfb_roster_talent(2021, division="fbs", rank_decay=1.5)["talent_composite"][0]

    assert flat > decayed > steeper, (flat, decayed, steeper)
    # flat = 5 * 70; decayed = 70 * sum(i**-0.75 for i in 1..5)
    assert abs(flat - 350.0) < 1e-9
    expected = 70.0 * sum(i**-0.75 for i in range(1, 6))
    assert abs(decayed - expected) < 1e-9


def test_rank_decay_zero_restores_the_flat_sum(monkeypatch) -> None:
    """`rank_decay=0.0` must reproduce the pre-decay behaviour exactly.

    Keeps the change auditable: anyone comparing against a historical build can
    turn the curve off and get the old number back.
    """
    monkeypatch.setattr(_mod, "load_recruit_classes", lambda *a, **k: _two_class_recruits())
    out = cfb_roster_talent(2021, division="fbs", rank_decay=0.0)
    row_a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert abs(row_a["talent_composite"] - 223.0) < 1e-9  # the documented pre-decay value
