"""ESPN NHL pbp clock columns on real captures (task 2.15, C-NF-4).

ESPN's NHL ``clock.displayValue`` is ELAPSED time in the period. The oracle is that raw clock
and, for the 2024 Stanley Cup Final G7, the NHL API's own ``timeRemaining`` on the same goals.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl import nhl_pbp

FIXTURES = Path(__file__).parent.parent / "fixtures"
CAPTURES = {
    # 2024 SCF G7 FLA-EDM, postseason, regulation only.
    401675111: FIXTURES / "espn" / "summary_nhl.json",
    # 2024 WCF G1 DAL-EDM, postseason, double overtime (20-minute OT periods).
    401672983: FIXTURES / "nhl_espn_pbp" / "summary_401672983.json",
    # 2025-01-11 MTL-DAL, regular season, 5-minute overtime and shootout.
    401688272: FIXTURES / "nhl_espn_pbp" / "summary_401688272.json",
}
COLS = [f"{edge}.{scope}_seconds_remaining" for edge in ("start", "end") for scope in ("period", "game")]


def _plays(game_id: int, monkeypatch: pytest.MonkeyPatch) -> pl.DataFrame:
    summary = json.loads(CAPTURES[game_id].read_text(encoding="utf-8"))
    monkeypatch.setattr(nhl_pbp, "_download_json", lambda *a, **k: summary)
    plays = pl.DataFrame(nhl_pbp.espn_nhl_pbp(game_id)["plays"], infer_schema_length=None)
    assert plays.height == len(summary["plays"])
    return plays.select("period.number", "clock.displayValue", "seasonType", *COLS)


@pytest.mark.parametrize("game_id", sorted(CAPTURES))
def test_clock_remaining_end_not_after_start(game_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    df = _plays(game_id, monkeypatch)
    for scope in ("period", "game"):
        start, end = pl.col(f"start.{scope}_seconds_remaining"), pl.col(f"end.{scope}_seconds_remaining")
        assert df.filter(end > start).height == 0, scope
        # The game's last row has no next row; every other row has an end.
        assert df[f"end.{scope}_seconds_remaining"].null_count() == 1
        assert df[f"end.{scope}_seconds_remaining"][-1] is None


@pytest.mark.parametrize("game_id", sorted(CAPTURES))
def test_clock_remaining_decreases_within_period(game_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    df = _plays(game_id, monkeypatch).with_columns(
        d=pl.col("start.game_seconds_remaining").diff().over("period.number"),
        last=pl.col("period.number") != pl.col("period.number").shift(-1),
        next_start=pl.col("start.game_seconds_remaining").shift(-1),
        next_pstart=pl.col("start.period_seconds_remaining").shift(-1),
    )
    assert df.filter(pl.col("d") > 0).height == 0
    # Within a period a row ends where the next starts; the last row of a period ends at 0:00.
    mid = df.filter(pl.col("last") == False)
    assert (mid["end.game_seconds_remaining"] == mid["next_start"]).all()
    assert (mid["end.period_seconds_remaining"] == mid["next_pstart"]).all()
    assert (df.filter(pl.col("last") == True)["end.period_seconds_remaining"].drop_nulls() == 0).all()


@pytest.mark.parametrize("game_id", sorted(CAPTURES))
def test_clock_remaining_is_period_length_less_elapsed(game_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    df = _plays(game_id, monkeypatch)
    mm = df["clock.displayValue"].str.split(":").list
    elapsed = mm.get(0).cast(pl.Int64) * 60 + mm.get(1).cast(pl.Int64)
    remaining = df["start.period_seconds_remaining"].cast(pl.Int64)
    total = elapsed + remaining
    periods = df["period.number"]
    postseason = int(df["seasonType"][0]) == 3
    for p, length in {1: 1200, 2: 1200, 3: 1200, 4: 1200 if postseason else 300, 5: 1200 if postseason else 0}.items():
        assert (total.filter(periods == p) == length).all(), p
    assert df["start.game_seconds_remaining"][0] == 3600
    # Regulation offsets: period p's game clock = (3 - p) * 1200 + its period clock.
    reg = df.filter(pl.col("period.number") <= 3)
    off = reg["start.game_seconds_remaining"] - reg["start.period_seconds_remaining"]
    assert (off == (3 - reg["period.number"]) * 1200).all()


def test_clock_remaining_matches_nhl_api_goals(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same game, independent feed: the NHL API's ``timeRemaining`` on each goal."""
    api = json.loads((FIXTURES / "nhl_api_web" / "pbp_2024_scf_g7.json").read_text(encoding="utf-8"))

    def secs(s: str) -> int:
        m, x = s.split(":")
        return 60 * int(m) + int(x)

    want = [
        (p["periodDescriptor"]["number"], secs(p["timeRemaining"])) for p in api["plays"] if p["typeDescKey"] == "goal"
    ]
    summary = json.loads(CAPTURES[401675111].read_text(encoding="utf-8"))
    monkeypatch.setattr(nhl_pbp, "_download_json", lambda *a, **k: summary)
    goals = pl.DataFrame(nhl_pbp.espn_nhl_pbp(401675111)["plays"], infer_schema_length=None).filter(
        pl.col("type.text") == "Goal"
    )
    got = [(int(p), int(s)) for p, s in goals.select("period.number", "start.period_seconds_remaining").rows()]
    assert want == got == [(1, 933), (1, 796), (2, 289)]


# ESPN capture -> (NHL API play-by-play of the same game, floor of plays matched on (period, elapsed)).
NHL_API = {
    401675111: (FIXTURES / "nhl_api_web" / "pbp_2024_scf_g7.json", 329),
    401672983: (FIXTURES / "nhl_espn_pbp" / "nhl_api_pbp_2023030321.json", 437),
    401688272: (FIXTURES / "nhl_espn_pbp" / "nhl_api_pbp_2024020673.json", 357),
}


@pytest.mark.parametrize("game_id", sorted(NHL_API))
def test_clock_remaining_matches_nhl_api_time_remaining(game_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every NHL API play's (period, timeInPeriod) found in ESPN has ESPN's remaining == its timeRemaining."""
    path, floor = NHL_API[game_id]
    api = json.loads(path.read_text(encoding="utf-8"))

    def secs(s: str) -> int:
        m, x = s.split(":")
        return 60 * int(m) + int(x)

    df = _plays(game_id, monkeypatch)
    mm = df["clock.displayValue"].str.split(":").list
    elapsed = (mm.get(0).cast(pl.Int64) * 60 + mm.get(1).cast(pl.Int64)).to_list()
    espn = dict(zip(zip(df["period.number"].to_list(), elapsed), df["start.period_seconds_remaining"].to_list()))
    pairs = [
        ((p["periodDescriptor"]["number"], secs(p["timeInPeriod"])), secs(p["timeRemaining"])) for p in api["plays"]
    ]
    matched = [(k, r) for k, r in pairs if k in espn]
    assert len(matched) >= floor
    assert [(k, r) for k, r in matched if espn[k] != r] == []
