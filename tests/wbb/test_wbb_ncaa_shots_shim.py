"""Smoke tests for the wbb box-score + shots shims (orchestrator-written).

The heavy validation lives in tests/wbb/test_wbb_ncaa_box_stats_parity.py and
tests/wbb/test_wbb_ncaa_shots_parity.py (which exercise the shared cores with
the quarters ``period_model`` directly); these tests only pin the shim
bindings — delegation identity and the (4, 600, 300) bind in the driver.
"""

from __future__ import annotations

import polars as pl
from polars.testing import assert_frame_equal

from sportsdataverse.mbb.mbb_ncaa_box_stats import parse_ncaa_bb_box
from sportsdataverse.mbb.mbb_ncaa_shots import parse_ncaa_bb_shots
from sportsdataverse.wbb.wbb_ncaa_box_stats import ncaa_wbb_box_scores
from sportsdataverse.wbb.wbb_ncaa_shots import (
    ncaa_wbb_join_pbp_shots,
    ncaa_wbb_shot_locations,
)
from tests.mbb._bigballr_oracle import HTML_DIR, load_oracle_pbp


class _FakeFetcher:
    def fetch_game_box(self, gid: object) -> str:
        return (HTML_DIR / f"box_{gid}.html").read_text(encoding="utf-8")

    def fetch_game_individual_stats(self, gid: object) -> str:
        return (HTML_DIR / f"individual_stats_{gid}.html").read_text(encoding="utf-8")


def test_wbb_box_scores_shim_delegates() -> None:
    got = ncaa_wbb_box_scores(["5722355"], fetcher=_FakeFetcher())
    expected = parse_ncaa_bb_box(
        (HTML_DIR / "individual_stats_5722355.html").read_text(encoding="utf-8"),
        "5722355",
    )
    assert isinstance(got, pl.DataFrame)
    assert_frame_equal(got, expected)


def test_wbb_shot_locations_binds_quarter_model() -> None:
    got = ncaa_wbb_shot_locations(["5728709"], fetcher=_FakeFetcher())
    expected = parse_ncaa_bb_shots(
        (HTML_DIR / "box_5728709.html").read_text(encoding="utf-8"),
        "5728709",
        period_model=(4, 600, 300),
    )
    assert isinstance(got, pl.DataFrame)
    assert_frame_equal(got, expected)
    # quarters bind, not halves: the 1-OT game must reach past 2400s
    assert got["game_seconds"].max() > 2400


def test_wbb_join_pbp_shots_delegates() -> None:
    pbp = load_oracle_pbp("wbb").filter(pl.col("game_id") == "5728709")
    shots = ncaa_wbb_shot_locations(["5728709"], fetcher=_FakeFetcher())
    joined = ncaa_wbb_join_pbp_shots(pbp, shots)
    assert isinstance(joined, pl.DataFrame)
    assert joined.height == pbp.height


def test_wbb_shot_locations_season_pins_the_period_model(monkeypatch) -> None:
    """``season=`` resolves the era through ``wbb_period_model``; no season keeps quarters.

    Shot locations only exist on stats.ncaa.org from 2019, so there is no halves-era
    capture to parse: the check records the model handed to the (real-fixture-fed)
    parser rather than counting rows (rules/wbb.yaml#wbb-2016-four-quarters).
    """
    from sportsdataverse.mbb import mbb_ncaa_shots as mod  # the shared driver's parse call (B-L26)

    seen: list = []

    def _spy(html, gid, *, period_model):
        seen.append(period_model)
        return parse_ncaa_bb_shots(html, gid, period_model=period_model)

    monkeypatch.setattr(mod, "parse_ncaa_bb_shots", _spy)
    ncaa_wbb_shot_locations(["5728709"], fetcher=_FakeFetcher())
    ncaa_wbb_shot_locations(["5728709"], season=2015, fetcher=_FakeFetcher())
    ncaa_wbb_shot_locations(["5728709"], season="2015-16", fetcher=_FakeFetcher())
    assert seen == [(4, 600, 300), (2, 1200, 300), (4, 600, 300)]


class _RecordingFetcher(_FakeFetcher):
    def __init__(self) -> None:
        self.calls: "list[object]" = []

    def fetch_game_box(self, gid: object) -> str:
        self.calls.append(gid)
        return super().fetch_game_box(gid)


def test_wbb_shot_locations_float_contest_ids_are_integer_strings() -> None:
    """A float id fetches ``5728709``, never ``5728709.0``; NaN / None are dropped (R853-2)."""
    fetcher = _RecordingFetcher()
    got = ncaa_wbb_shot_locations([5728709.0, float("nan"), None], fetcher=fetcher)
    assert fetcher.calls == ["5728709"]
    assert got["game_id"].unique().to_list() == ["5728709"]


def test_wbb_shot_locations_float_season_pins_halves(monkeypatch) -> None:
    """``season=2015.0`` resolves the halves model, not "2015.0" -> quarters (R853-5)."""
    from sportsdataverse.mbb import mbb_ncaa_shots as mod  # the shared driver's parse call (B-L26)

    seen: list = []

    def _spy(html, gid, *, period_model):
        seen.append(period_model)
        return parse_ncaa_bb_shots(html, gid, period_model=period_model)

    monkeypatch.setattr(mod, "parse_ncaa_bb_shots", _spy)
    ncaa_wbb_shot_locations(["5728709"], season=2015.0, fetcher=_FakeFetcher())
    assert seen == [(2, 1200, 300)]
