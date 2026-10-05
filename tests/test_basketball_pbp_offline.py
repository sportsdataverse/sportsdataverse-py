"""Offline real-data locks for the ESPN basketball pbp producers (nba / wnba / mbb / wbb).

Every payload is a verbatim ESPN summary (or its ``pickcenter`` array) from the
committed ``tests/fixtures/espn/summary_*.json`` captures or the hoopR / wehoop raw
stores; provenance in ``tests/fixtures/espn/basketball_pbp/README.md``.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mbb import mbb_pbp
from sportsdataverse.nba import nba_pbp
from sportsdataverse.wbb import wbb_pbp
from sportsdataverse.wnba import wnba_pbp

FIX = Path(__file__).parent / "fixtures" / "espn"
BB = FIX / "basketball_pbp"
MODULES = {"nba": nba_pbp, "wnba": wnba_pbp, "mbb": mbb_pbp, "wbb": wbb_pbp}
DEFAULT_OU = {"nba": 215.5, "wnba": 165.5, "mbb": 142.0, "wbb": 130.5}
PICKCENTER = json.loads((BB / "pickcenter.json").read_text(encoding="utf-8"))
TEAM_TIMEOUT_TYPES = {"RegularTimeOut", "ShortTimeOut", "Full Timeout", "Short Timeout", "No Timeout", "Reset Timeout"}


def _raw(name: str) -> dict:
    with gzip.open(BB / name, "rt", encoding="utf-8") as f:
        return json.load(f)


def _pick(lg: str, pickcenter) -> tuple:
    init = getattr(MODULES[lg], f"helper_{lg}_pickcenter")({"pickcenter": copy.deepcopy(pickcenter)})
    keys = ("gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable")
    return tuple(np.asarray(init[k]).reshape(-1)[0].item() for k in keys)


@pytest.mark.parametrize(
    ("lg", "game_id", "expected"),
    [
        # one provider: was (2.5, default OU, True, False) for every one of these
        ("mbb", "401856600", (-6.5, 146.5, True, True)),  # DraftKings, MICH -6.5
        ("nba", "401809238", (-4.5, 241.5, True, True)),  # DraftKings, 2025-26
        ("wnba", "401320565", (2.5, 161.0, False, True)),  # Caesars, away favored
        ("wbb", "401468165", (8.0, 147.5, False, True)),  # Caesars, away favored
        # several providers: the values the pre-fix `> 1` path already produced
        ("mbb", "400766104", (-6.5, 132.5, True, True)),
        ("nba", "400578293", (-9.0, 191.0, True, True)),
        # providers in str(provider.id) order: teamrankings "1002" ahead of Caesars "45"
        # (an int sort would read Caesars' -15.5 / 158.5)
        ("mbb", "401364342", (-13.0, 160.5, True, True)),
    ],
)
def test_pickcenter_reads_the_provider(lg: str, game_id: str, expected: tuple) -> None:
    assert _pick(lg, PICKCENTER[lg][game_id]) == expected


@pytest.mark.parametrize(
    ("lg", "game_id", "home_line"),
    [
        ("mbb", "330582427", 17.5),  # consensus UNCA (home) -17.5; the home line was -17.5
        ("nba", "401430219", 4.5),  # consensus MIA (home) -4.5; the home line was -4.5
    ],
)
def test_spread_and_favorite_come_from_the_same_provider(lg: str, game_id: str, home_line: float) -> None:
    # a record-only teamrankings row sorts first: no spread, favorite False for both
    # teams. Its flag used to be paired with consensus' spread.
    spread, _, home_favorite, available = _pick(lg, PICKCENTER[lg][game_id])
    # homeTeamSpread as helper_<lg>_pbp_features builds it
    assert (abs(spread) if home_favorite else -abs(spread), available) == (home_line, True)


@pytest.mark.parametrize("lg", list(MODULES))
@pytest.mark.parametrize(
    "pickcenter",
    [[], {}, None, PICKCENTER["mbb"]["400587253"]],  # last: a lone teamrankings entry with no spread
    ids=["empty", "missing", "null", "no-spread"],
)
def test_pickcenter_without_a_spread_keeps_defaults(lg: str, pickcenter) -> None:
    assert _pick(lg, pickcenter) == (2.5, DEFAULT_OU[lg], True, False)


def test_mbb_one_provider_spread_reaches_every_play() -> None:
    raw = _raw("mbb_401856600.json.gz")
    out = mbb_pbp.helper_mbb_pbp(401856600, copy.deepcopy(raw))
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    # MICH (home) -6.5; the released final.json carried 2.5 / unavailable
    assert plays["gameSpread"].unique().to_list() == [6.5]
    assert plays["homeTeamSpread"].unique().to_list() == [6.5]
    assert plays["gameSpreadAvailable"].unique().to_list() == [True]
    # 5 ShortTimeOut + 1 RegularTimeOut (a full timeout, missed before); 8 TV timeouts excluded
    assert out["timeouts"] == _expected_timeouts(raw["plays"], list(out["timeouts"]), 1)
    assert sum(len(v) for t in out["timeouts"].values() for v in t.values()) == 6


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def _expected_timeouts(plays: list, team_ids: list, first_half_periods: int) -> dict:
    out = {t: {"1": [], "2": []} for t in team_ids}
    for p in plays:
        team = (p.get("team") or {}).get("id")
        if p["type"]["text"] in TEAM_TIMEOUT_TYPES and team is not None and int(team) in out:
            half = "1" if p["period"]["number"] <= first_half_periods else "2"
            out[int(team)][half].append(int(p["id"]))
    return out


@pytest.mark.parametrize(
    ("lg", "first_half_periods", "n_team_timeouts"),
    [
        ("nba", 2, 9),  # 9 "Full Timeout"; the old ShortTimeOut-only filter found 0
        ("wnba", 2, 15),  # 13 "Full Timeout" + 2 "No Timeout"; "Official Timeout" excluded
        ("mbb", 1, 6),  # 6 ShortTimeOut; the 8 OfficialTVTimeOut excluded
        ("wbb", 2, 4),  # 4 ShortTimeOut; the 4 OfficialTVTimeOut excluded
    ],
)
def test_timeouts_map_matches_the_plays(monkeypatch, lg: str, first_half_periods: int, n_team_timeouts: int) -> None:
    summary = json.loads((FIX / f"summary_{lg}.json").read_text(encoding="utf-8"))
    mod = MODULES[lg]
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(summary)))
    out = getattr(mod, f"espn_{lg}_pbp")(game_id=int(summary["header"]["id"]))
    teams = list(out["timeouts"])
    assert out["timeouts"] == _expected_timeouts(summary["plays"], teams, first_half_periods)
    assert sum(len(v) for t in out["timeouts"].values() for v in t.values()) == n_team_timeouts


def test_nba_timeout_goes_to_the_calling_team_not_a_substring_match() -> None:
    # 2006 MEM vs PHI: "Memphis 20 Sec. timeout" contains "phi", so the name match
    # alone put every Memphis timeout in Philadelphia's list too.
    raw = _raw("nba_260312029.json.gz")
    out = nba_pbp.helper_nba_pbp(260312029, copy.deepcopy(raw))
    assert out["timeouts"] == _expected_timeouts(raw["plays"], list(out["timeouts"]), 2)
    mem, phi = (set(sum(out["timeouts"][t].values(), [])) for t in (29, 20))
    assert mem and phi and not (mem & phi)


def test_nba_name_fallback_matches_whole_words_only() -> None:
    # the same game with every play's team removed, so only the name fallback runs:
    # "Memphis 20 Sec. timeout" must not match PHI inside "Memphis"
    raw = _raw("nba_260312029.json.gz")
    expected = _expected_timeouts(raw["plays"], [29, 20], 2)
    for p in raw["plays"]:
        p.pop("team", None)
    assert nba_pbp.helper_nba_pbp(260312029, raw)["timeouts"] == expected


def test_wnba_timeout_without_a_team_name_goes_to_the_play_team() -> None:
    # 2017 CHI @ MIN: two RegularTimeOut plays read only " Full timeout". The name
    # match credits them to nobody; the play's team.id says CHI (19) and MIN (8).
    raw = _raw("wnba_400927398.json.gz")
    out = wnba_pbp.helper_wnba_pbp(400927398, copy.deepcopy(raw))
    assert out["timeouts"] == _expected_timeouts(raw["plays"], list(out["timeouts"]), 2)
    assert 400927398184 in out["timeouts"][19]["1"] and 400927398402 in out["timeouts"][8]["2"]


def test_mbb_overtime_end_seconds_agree_in_every_overtime() -> None:
    # 2OT game: on the first play of 2OT (4:36), end.period_seconds_remaining took the
    # next play's start (272) while end.game_seconds_remaining was set to 300.
    out = mbb_pbp.helper_mbb_pbp(401830342, _raw("mbb_401830342.json.gz"))
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    second_half_on = plays.filter(pl.col("period.number") >= 2)
    assert second_half_on["end.period_seconds_remaining"].to_list() == (
        second_half_on["end.game_seconds_remaining"].to_list()
    )
    first_of_ot = plays.filter(
        (pl.col("period.number") >= 3) & (pl.col("period.number") != pl.col("period.number").shift(1))
    )
    assert first_of_ot["period.number"].to_list() == [3, 4]
    assert first_of_ot["end.period_seconds_remaining"].to_list() == [300, 300]


def test_mbb_bare_seconds_clock_parses() -> None:
    # hardening: no real MBB feed carries a tenths clock, the NBA/WNBA/WBB paths accept one
    raw = _raw("mbb_401830342.json.gz")
    raw["plays"][-1]["clock"]["displayValue"] = "23.4"
    last = mbb_pbp.helper_mbb_pbp(401830342, raw)["plays"][-1]
    assert (last["clock.minutes"], last["clock.seconds"], last["start.period_seconds_remaining"]) == (0, 23, 23)
