"""Clock seconds on the committed ESPN basketball captures (task 2.16: C-M21, C-L7, C-NF-1 invariant half).

Every capture is a verbatim ESPN summary run offline through its league's processor
(provenance: ``tests/fixtures/espn/basketball_pbp/README.md`` and the ``summary_<lg>.json``
captures). The invariants are :mod:`sportsdataverse.validation.pbp_invariants` group 13,
the gate a basketball producer runs before it publishes; ``start.*`` comes from the raw
clock, so they judge the processor against ESPN's clock, never against stored output.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import mbb_pbp
from sportsdataverse.nba import nba_pbp
from sportsdataverse.validation import pbp_invariants as inv
from sportsdataverse.wbb import wbb_pbp
from sportsdataverse.wnba import wnba_pbp

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "espn"
MODULES = {"nba": nba_pbp, "wnba": wnba_pbp, "mbb": mbb_pbp, "wbb": wbb_pbp}
CAPTURES = [
    ("mbb", "basketball_pbp/mbb_401830342.json.gz"),  # 2OT
    ("mbb", "basketball_pbp/mbb_401856600.json.gz"),
    ("mbb", "summary_mbb.json"),
    ("nba", "basketball_pbp/nba_260312029.json.gz"),
    ("nba", "summary_nba.json"),
    ("wnba", "basketball_pbp/wnba_400927398.json.gz"),
    ("wnba", "summary_wnba.json"),  # OT
    ("wbb", "summary_wbb.json"),
    ("wbb", "basketball_pbp/wbb_400509734.json.gz"),  # 2013-14 halves era, OT
]
SCOPES = {"mbb": ("period", "game"), "nba": ("quarter", "half", "game")}
# the release's clock-part dtypes: NBA / WNBA clocks carry tenths under a minute
CLOCK_DTYPES = {"mbb": pl.Int32, "wbb": pl.Int32, "nba": pl.Float64, "wnba": pl.Float64}


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return copy.deepcopy(self._payload)


def _summary(name: str) -> dict:
    path = FIX / name
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads(path.read_text(encoding="utf-8"))


def _process(monkeypatch, lg: str, summary: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The public ``espn_<lg>_pbp`` on a capture: (returned plays, the features frame it was built from)."""
    mod = MODULES[lg]
    frames = []
    features = getattr(mod, f"helper_{lg}_pbp_features")

    def spy(game_id, pbp_txt, init):
        out = features(game_id, pbp_txt, init)
        frames.append(out["plays"])
        return out

    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(summary))
    monkeypatch.setattr(mod, f"helper_{lg}_pbp_features", spy)
    # mbb / nba default to the release frame; these checks read the processed dict
    kw = {"return_parsed": False} if lg in ("mbb", "nba") else {}
    res = getattr(mod, f"espn_{lg}_pbp")(game_id=int(summary["header"]["id"]), **kw)
    return pl.from_dicts(res["plays"], infer_schema_length=None), frames[0]


def _seconds_rules(plays: pl.DataFrame) -> list[inv.RuleResult]:
    return [r for r in inv.evaluate(plays) if r.rule.startswith("seconds.")]


@pytest.mark.parametrize(("lg", "name"), CAPTURES)
def test_end_le_start_on_capture(monkeypatch, lg: str, name: str) -> None:
    plays, _ = _process(monkeypatch, lg, _summary(name))
    rules = _seconds_rules(plays)
    scopes = SCOPES.get(lg, SCOPES["nba"])
    assert {r.rule for r in rules} == {
        f"seconds.{s}_{kind}"
        for s in scopes
        for kind in ("end_not_after_start", "end_is_next_start", "period_last_row_ends_at_zero")
    }
    assert all(r.n_checked > 0 for r in rules)
    # every period but the last is judged; the game's final row has a null end (the processor
    # leaves it open), so it leaves the denominator rather than passing (R844-10)
    periods = plays["period.number"].n_unique()
    last_row = [r for r in rules if r.rule.endswith("_period_last_row_ends_at_zero")]
    assert {r.n_checked for r in last_row} == {periods - 1}
    assert [(r.rule, r.n_violations, r.samples) for r in rules if r.n_violations] == []


def test_end_le_start_where_the_audit_found_it(monkeypatch) -> None:
    # C-M21's rows: the first play of Q2/Q3/Q4 (9:41, 9:48, 9:42) ended at the full period
    # length, 1800/1200/600, after it started. Each now ends where ESPN's next play starts.
    summary = _summary("basketball_pbp/wnba_400927398.json.gz")
    plays, _ = _process(monkeypatch, "wnba", summary)
    raw = summary["plays"]
    firsts = [i for i in range(1, len(raw)) if raw[i]["period"]["number"] != raw[i - 1]["period"]["number"]]
    assert [raw[i]["clock"]["displayValue"] for i in firsts] == ["9:41", "9:48", "9:42"]
    for i in firsts:
        m, s = raw[i + 1]["clock"]["displayValue"].split(":")
        left_in_game = 600 * (4 - raw[i]["period"]["number"])
        assert plays["end.game_seconds_remaining"][i] == left_in_game + 60 * int(m) + int(s)
        assert plays["end.game_seconds_remaining"][i] <= plays["start.game_seconds_remaining"][i]
        # the period before it ends at its own 0:00, not at this play's clock
        assert plays["end.quarter_seconds_remaining"][i - 1] == 0
        assert plays["end.game_seconds_remaining"][i - 1] == left_in_game + 600


def test_end_after_start_fires_on_the_first_row_override(monkeypatch) -> None:
    # negative control on the real game: put the old period-boundary fill back (a period's
    # first row ends at the full period length) and the invariant names C-M21's three rows
    plays, _ = _process(monkeypatch, "wnba", _summary("basketball_pbp/wnba_400927398.json.gz"))
    first = (pl.col("period.number") != pl.col("period.number").shift(1)) & (pl.col("period.number") > 1)
    old = plays.with_columns(
        pl.when(first)
        .then(600 * (5 - pl.col("period.number")))
        .otherwise(pl.col("end.game_seconds_remaining"))
        .alias("end.game_seconds_remaining")
    )
    fired = {r.rule: r for r in _seconds_rules(old) if r.n_violations}
    assert set(fired) == {"seconds.game_end_not_after_start", "seconds.game_end_is_next_start"}
    assert [row["clock.displayValue"] for row in fired["seconds.game_end_not_after_start"].samples] == [
        "9:41",
        "9:48",
        "9:42",
    ]


def test_end_after_start_fires_on_the_previous_rows_start(monkeypatch) -> None:
    # negative control for the published-file shape (C-NF-1): end = the PREVIOUS row's start
    plays, _ = _process(monkeypatch, "nba", _summary("basketball_pbp/nba_260312029.json.gz"))
    shifted = plays.with_columns(pl.col("start.game_seconds_remaining").shift(1).alias("end.game_seconds_remaining"))
    fired = {r.rule: r for r in _seconds_rules(shifted)}
    moved = plays.filter(pl.col("start.game_seconds_remaining").diff() != 0).height
    assert fired["seconds.game_end_not_after_start"].n_violations == moved
    assert fired["seconds.game_end_is_next_start"].n_violations > plays.height // 2


@pytest.mark.parametrize("lg", ["mbb", "nba", "wnba", "wbb"])
def test_clock_parts_carry_the_release_dtypes(monkeypatch, lg: str) -> None:
    # C-L7: Float32 minutes / seconds left float noise in the published seconds (56.1 -> 2216.1001)
    name = next(n for league, n in CAPTURES if league == lg)
    _, frame = _process(monkeypatch, lg, _summary(name))
    assert (frame.schema["clock.minutes"], frame.schema["clock.seconds"]) == (pl.Int32, CLOCK_DTYPES[lg])


def test_period_last_row_rule_fires_on_a_clamp(monkeypatch) -> None:
    # R841-1: a producer that skips the period-boundary fill and clamps end = min(next start,
    # start) passes the other two rules (within a period the clock only runs down), but each
    # period's last row then ends at its own start or the next period's, never at its 0:00
    plays, _ = _process(monkeypatch, "wnba", _summary("basketball_pbp/wnba_400927398.json.gz"))
    scopes = ("quarter", "half", "game")
    clamped = plays.with_columns(
        pl.min_horizontal(
            pl.col(f"start.{s}_seconds_remaining").shift(-1), pl.col(f"start.{s}_seconds_remaining")
        ).alias(f"end.{s}_seconds_remaining")
        for s in scopes
    )
    fired = {r.rule: r for r in _seconds_rules(clamped) if r.n_violations}
    # every period here ends on an "End of ..." row logged at 0:00, so the quarter clock's
    # clamp lands on the right value; the half and game clocks carry the next period's start
    assert set(fired) == {f"seconds.{s}_period_last_row_ends_at_zero" for s in ("half", "game")}
    # Q1-Q4 end: the game's final row counts as a period's last (R844-9), and the clamp leaves
    # it at its own start, 0:00, so it passes; the half clock is right at the Q2 -> Q3
    # boundary, where the clamp lands on the row's own start, 0:00 of the half
    assert {k: (r.n_checked, r.n_violations) for k, r in fired.items()} == {
        "seconds.half_period_last_row_ends_at_zero": (4, 2),
        "seconds.game_period_last_row_ends_at_zero": (4, 3),
    }


def test_halves_era_capture_ends_each_half_at_zero(monkeypatch) -> None:
    # R841-2: the 2013-14 WBB capture plays two 20-minute halves and an overtime; the shared
    # period-end formula ends each on the period clock's 0:00 (20:00 and 5:00 periods)
    plays, _ = _process(monkeypatch, "wbb", _summary("basketball_pbp/wbb_400509734.json.gz"))
    assert plays["period.number"].max() == 3
    ends = plays.filter(pl.col("period.number") != pl.col("period.number").shift(-1)).head(2)
    assert ends["end.quarter_seconds_remaining"].to_list() == [0, 0]
    assert ends["end.game_seconds_remaining"].to_list() == [1200, 0]
