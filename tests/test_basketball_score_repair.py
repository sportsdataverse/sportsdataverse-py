"""Running-score rebuild in the shared ESPN basketball pbp builder (nba / wnba / mbb / wbb).

Every payload is a real ESPN summary: the committed ``summary_*.json`` captures or verbatim
copies of the hoopR / wehoop raw stores (provenance in ``fixtures/espn/basketball_pbp/README.md``).
Two tests mutate a real payload in-test (a dropped header score, nulled per-play scores) to
reach branches no committed game exercises; they say so where they do it.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import mbb_pbp
from sportsdataverse.mbb.mbb_play_by_play import helper_mbb_play_by_play
from sportsdataverse.nba import nba_pbp
from sportsdataverse.nba.nba_play_by_play import helper_nba_play_by_play
from sportsdataverse.wbb import wbb_pbp
from sportsdataverse.wbb.wbb_play_by_play import helper_wbb_play_by_play
from sportsdataverse.wnba import wnba_pbp
from sportsdataverse.wnba.wnba_play_by_play import helper_wnba_play_by_play

FIX = Path(__file__).parent / "fixtures" / "espn"
BB = FIX / "basketball_pbp"
MODULES = {"nba": nba_pbp, "wnba": wnba_pbp, "mbb": mbb_pbp, "wbb": wbb_pbp}
BUILDERS = {
    "nba": helper_nba_play_by_play,
    "wnba": helper_wnba_play_by_play,
    "mbb": helper_mbb_play_by_play,
    "wbb": helper_wbb_play_by_play,
}


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def _raw(name: str) -> dict:
    if name.endswith(".gz"):
        with gzip.open(BB / name, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def _final(lg: str, name: str, monkeypatch: pytest.MonkeyPatch) -> dict:
    """The processed payload the producers store and the builder reads (espn_<lg>_pbp output)."""
    raw = _raw(name)
    mod = MODULES[lg]
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(raw)))
    # mbb / nba default to the release frame; the builder reads the processed dict
    kw = {"return_parsed": False} if lg in ("mbb", "nba") else {}
    return getattr(mod, f"espn_{lg}_pbp")(game_id=int(raw["header"]["id"]), **kw)


def _espn(final: dict) -> pl.DataFrame:
    """ESPN's own per-play scores, in the payload's array order."""
    return pl.DataFrame(
        {
            "home": [p.get("homeScore") for p in final["plays"]],
            "away": [p.get("awayScore") for p in final["plays"]],
        },
        schema={"home": pl.Int32, "away": pl.Int32},
        strict=False,
    )


def _build(lg: str, final: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    return BUILDERS[lg](final), _espn(final)


def _at_seq(df: pl.DataFrame, seq: int) -> tuple[int, int]:
    row = df.filter(pl.col("sequence_number") == seq).row(0, named=True)
    return row["home_score"], row["away_score"]


def _last(df: pl.DataFrame) -> tuple[int, int]:
    row = df.row(df.height - 1, named=True)
    return row["home_score"], row["away_score"]


def _changed_rows(out: pl.DataFrame, espn: pl.DataFrame) -> int:
    return int(((out["home_score"] != espn["home"]) | (out["away_score"] != espn["away"])).sum())


def _never_falls(out: pl.DataFrame) -> bool:
    return bool(
        (out["home_score"].diff().drop_nulls() >= 0).all() and (out["away_score"].diff().drop_nulls() >= 0).all()
    )


def test_late_keyed_play_rebases_every_later_row(monkeypatch: pytest.MonkeyPatch) -> None:
    out, espn = _build("nba", _final("nba", "nba_401766458.json.gz", monkeypatch))
    assert out.height == 496 and out.columns[-1] == "score_source"
    assert out["score_source"].unique().to_list() == ["rebuilt"]
    # array order: the late-keyed seq 738 sits before seq 658 and is never moved
    seqs = out["sequence_number"].to_list()
    assert seqs.index(738) == 438 and seqs.index(658) == 439
    assert _at_seq(out, 738) == (109, 87)
    assert _at_seq(out, 658) == _at_seq(out, 659) == (109, 87)  # ESPN: 108-87
    assert _at_seq(out, 660) == (111, 87)  # ESPN: 110-87
    assert _last(out) == (120, 95)  # ESPN: 119-95; header final 120-95
    assert _changed_rows(out, espn) == 57
    assert _never_falls(out)


def test_made_free_throws_with_zero_score_value_count_one(monkeypatch: pytest.MonkeyPatch) -> None:
    final = _final("nba", "nba_400829116.json.gz", monkeypatch)
    fts = [p for p in final["plays"] if p.get("scoringPlay") and "Free Throw" in (p.get("type.text") or "")]
    assert fts and all((p.get("scoreValue") or 0) == 0 for p in fts)  # the 2013-2019 feed shape
    out, espn = _build("nba", final)
    assert out["score_source"].unique().to_list() == ["rebuilt"]
    assert _last(out) == (114, 105) and (espn["home"][-1], espn["away"][-1]) == (112, 105)
    assert _never_falls(out)


def test_existing_nba_fixture_rebuilds_its_one_dip(monkeypatch: pytest.MonkeyPatch) -> None:
    out, espn = _build("nba", _final("nba", "summary_nba.json", monkeypatch))
    assert out.height == 450 and out["score_source"].unique().to_list() == ["rebuilt"]
    assert _changed_rows(out, espn) == 17
    assert (espn["home"][90], espn["away"][90]) == (26, 22) and _at_seq(out, 128) == (26, 24)
    assert _last(out) == (111, 96)
    assert _never_falls(out)


def test_rebuild_that_misses_the_final_keeps_every_espn_value(monkeypatch: pytest.MonkeyPatch) -> None:
    out, espn = _build("nba", _final("nba", "nba_401704638.json.gz", monkeypatch))
    assert out.height == 531
    assert out["score_source"].unique().to_list() == ["espn"]
    assert _changed_rows(out, espn) == 0
    assert _last(out) == (104, 140)  # ESPN's last row; the scoring plays only sum to 104-139


def test_missing_official_final_keeps_espn(monkeypatch: pytest.MonkeyPatch) -> None:
    final = _final("nba", "nba_401766458.json.gz", monkeypatch)
    # In-test mutation of the real payload: a pre-tip live header carries no score.
    final["header"]["competitions"][0]["competitors"][0].pop("score")
    out, espn = _build("nba", final)
    assert out["score_source"].unique().to_list() == ["espn"]
    assert _changed_rows(out, espn) == 0 and _last(out) == (119, 95)


def test_null_espn_scores_are_filled_when_rebuilt(monkeypatch: pytest.MonkeyPatch) -> None:
    final = _final("nba", "nba_401766458.json.gz", monkeypatch)
    # In-test mutation of the real payload: ten rows with no per-play home score.
    for p in final["plays"][100:110]:
        p["homeScore"] = None
    out, espn = _build("nba", final)
    assert espn["home"].null_count() == 10
    assert out["score_source"].unique().to_list() == ["rebuilt"]
    assert out["home_score"].null_count() == 0 and _last(out) == (120, 95)
    assert out.schema["home_score"] == pl.Int32 and out.schema["score_source"] == pl.String


@pytest.mark.parametrize(
    ("lg", "name", "rows", "periods", "final"),
    [
        ("wnba", "summary_wnba.json", 418, 5, (67, 62)),  # quarters + OT
        ("mbb", "summary_mbb.json", 294, 2, (75, 60)),  # halves
        ("wbb", "summary_wbb.json", 360, 4, (87, 75)),  # quarters
        ("mbb", "mbb_401830342.json.gz", 426, 4, (75, 81)),  # halves + 2OT
    ],
)
def test_clean_feeds_rebuild_to_espn_exactly(
    monkeypatch: pytest.MonkeyPatch, lg: str, name: str, rows: int, periods: int, final: tuple[int, int]
) -> None:
    out, espn = _build(lg, _final(lg, name, monkeypatch))
    assert out.height == rows and out["period_number"].max() == periods
    assert out["score_source"].unique().to_list() == ["rebuilt"]
    assert _changed_rows(out, espn) == 0 and _last(out) == final


def test_wnba_halves_era_stale_base(monkeypatch: pytest.MonkeyPatch) -> None:
    # 2004-06-18, two 20-minute halves: ESPN ends 61-69, official 63-69
    out, espn = _build("wnba", _final("wnba", "wnba_240618004.json.gz", monkeypatch))
    assert out["period_number"].max() == 2 and out["score_source"].unique().to_list() == ["rebuilt"]
    assert (espn["home"][-1], espn["away"][-1]) == (61, 69) and _last(out) == (63, 69)
    assert (espn["home"][116], espn["away"][116]) == (12, 22) and _at_seq(out, 119) == (14, 22)
    assert _changed_rows(out, espn) == 245
    assert _never_falls(out)


def test_mbb_neutral_site_scores_follow_the_header_sides(monkeypatch: pytest.MonkeyPatch) -> None:
    final = _final("mbb", "mbb_401829457.json.gz", monkeypatch)
    assert final["header"]["competitions"][0]["neutralSite"] is True
    out, espn = _build("mbb", final)
    assert out["home_team_id"].unique().to_list() == [155]  # UND, header home, 58 points
    assert (espn["home"][-1], espn["away"][-1]) == (75, 58)  # ESPN's rows carry CCU in homeScore
    assert out["score_source"].unique().to_list() == ["rebuilt"] and _last(out) == (58, 75)
    first = out.filter(pl.col("scoring_play") == True).row(0, named=True)
    assert first["team_id"] == 155 and (first["home_score"], first["away_score"]) == (3, 0)
    assert _changed_rows(out, espn) == 533
    assert _never_falls(out)


def test_wbb_halves_era_late_free_throws(monkeypatch: pytest.MonkeyPatch) -> None:
    # 2013-14, two halves: ESPN ends 52-54, official 54-56; the last two rows were never re-based
    out, espn = _build("wbb", _final("wbb", "wbb_400550784.json.gz", monkeypatch))
    assert out["period_number"].max() == 2 and out["score_source"].unique().to_list() == ["rebuilt"]
    assert (espn["home"][-1], espn["away"][-1]) == (52, 54) and _last(out) == (54, 56)
    assert _changed_rows(out, espn) == 2
    assert _never_falls(out)
