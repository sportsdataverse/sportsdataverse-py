"""In-season NCAA -> ESPN game crosswalk: built from parsed contests, stamped back into them.

The 2026-27 schedule pages were captured before tip-off (no contest ids, no ESPN team
ids), so the schedules-only NCAA side built 0 contests and every published 2027 row
had a null ``espn_game_id``.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.scrape.ncaa import espn_game_xwalk as xwalk


def _tree(tmp_path, contests):
    """A raw tree with a schedule_master, a pre-season schedules parquet and parsed contests."""
    lg = tmp_path / "mbb"
    (lg / "json").mkdir(parents=True)
    (lg / "schedules" / "parquet").mkdir(parents=True)
    pl.DataFrame(
        {"contest_id": [c for c, *_ in contests] + ["999"], "season": ["2027"] * len(contests) + ["2026"]}
    ).write_parquet(lg / "schedule_master.parquet")
    # Pre-season capture: contest ids and ESPN team ids not posted yet.
    pl.DataFrame(
        {
            "contest_id": [None],
            "game_date": ["11/03/2026"],
            "team": ["NJIT"],
            "home": ["NJIT"],
            "away": ["Saint Peter's"],
            "espn_team_id": [None],
            "opponent_espn_team_id": [None],
        },
        schema={
            "contest_id": pl.Utf8,
            "game_date": pl.Utf8,
            "team": pl.Utf8,
            "home": pl.Utf8,
            "away": pl.Utf8,
            "espn_team_id": pl.Utf8,
            "opponent_espn_team_id": pl.Utf8,
        },
    ).write_parquet(lg / "schedules" / "parquet" / "2027.parquet")
    for cid, date, home, away in contests:
        parsed = {
            "contest_id": cid,
            "linescore": [{"contest_id": cid, "game_date": f"{date} 04:00 PM", "espn_game_id": None}],
            "pbp": [{"game_date": date, "espn_game_id": None}],
            "teams": [
                {"team": "H", "espn_team_id": home, "side": "home", "espn_game_id": None},
                {"team": "A", "espn_team_id": away, "side": "away", "espn_game_id": None},
            ],
        }
        (lg / "json" / f"{cid}.json").write_text(json.dumps(parsed), encoding="utf-8")
    return tmp_path


def test_parsed_side_keys_the_season_from_parsed_contests(tmp_path):
    root = _tree(tmp_path, [("6615603", "11/08/2026", "2885", "2612")])
    side = xwalk.parsed_schedule_side(root, "mbb", 2027)
    assert side.rows() == [("6615603", pl.Series(["2026-11-08"]).str.to_date()[0], "2885", "2612")]
    assert xwalk.parsed_schedule_side(root, "mbb", 2025).height == 0


def test_build_matches_and_stamp_backfills_parsed_ids(tmp_path, monkeypatch):
    root = _tree(tmp_path, [("6615603", "11/08/2026", "2885", "2612"), ("6615604", "11/08/2026", "1", "2")])
    espn = pl.DataFrame(
        {
            "espn_game_id": ["401920865"],
            "game_date": pl.Series(["2026-11-08"]).str.to_date(),
            "home_espn_team_id": ["2885"],
            "away_espn_team_id": ["2612"],
        }
    )
    monkeypatch.setattr(xwalk, "espn_schedule_side", lambda league, season: espn)
    frame = xwalk.build_season_xwalk(root, "mbb", 2027)
    assert dict(zip(frame["contest_id"], frame["espn_game_id"])) == {"6615603": "401920865", "6615604": None}

    xwalk.write_season_xwalk(root, "mbb", 2027, frame)
    assert xwalk.stamp_parsed(root, "mbb", 2027) == 1
    parsed = json.loads((root / "mbb" / "json" / "6615603.json").read_text(encoding="utf-8"))
    assert {r["espn_game_id"] for r in parsed["linescore"] + parsed["pbp"] + parsed["teams"]} == {"401920865"}
    untouched = json.loads((root / "mbb" / "json" / "6615604.json").read_text(encoding="utf-8"))
    assert untouched["teams"][0]["espn_game_id"] is None
    # Idempotent: nothing left to stamp.
    assert xwalk.stamp_parsed(root, "mbb", 2027) == 0


# --- Real 2026-27 data -------------------------------------------------------
# ``mbb_2027_parsed.json``: three contests from ncaa-mbb-hoops-raw ``mbb/json/``
# (linescore row 1 and ``teams`` kept verbatim; every id null, as published).
# ``mbb_2027_espn_schedule.json``: ESPN's 2027 MBB schedule release, the dates
# around those games. 6615603 is NJIT vs Saint Peter's (exact match), 6626395
# is Harvard at UChicago, a home side with no NCAA-side ESPN id (single-team match),
# and 6640165 has no ESPN game.
_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "ncaa_espn_game_xwalk"
_REAL_EXPECTED = {
    "6615603": ("401920865", "exact"),
    "6626395": ("401920355", "single_team"),
    "6640165": (None, None),
}


def _real_tree(tmp_path, monkeypatch):
    lg = tmp_path / "mbb"
    (lg / "json").mkdir(parents=True)
    contests = json.loads((_FIXTURES / "mbb_2027_parsed.json").read_text(encoding="utf-8"))
    pl.DataFrame({"contest_id": list(contests), "season": ["2027"] * len(contests)}).write_parquet(
        lg / "schedule_master.parquet"
    )
    for cid, parsed in contests.items():
        (lg / "json" / f"{cid}.json").write_text(json.dumps(parsed), encoding="utf-8")
    espn = pl.DataFrame(
        json.loads((_FIXTURES / "mbb_2027_espn_schedule.json").read_text(encoding="utf-8")),
        schema={
            "espn_game_id": pl.Utf8,
            "game_date": pl.Utf8,
            "home_espn_team_id": pl.Utf8,
            "away_espn_team_id": pl.Utf8,
        },
    ).with_columns(pl.col("game_date").str.to_date())
    monkeypatch.setattr(xwalk, "espn_schedule_side", lambda league, season: espn)
    return tmp_path


def _ids(root, cid):
    parsed = json.loads((root / "mbb" / "json" / f"{cid}.json").read_text(encoding="utf-8"))
    return {r.get("espn_game_id") for f in ("linescore", "teams") for r in parsed[f]}


def test_real_2027_contests_match_without_schedule_pages(tmp_path, monkeypatch):
    # No schedules parquet at all (WBB 2027 had none): the parsed side alone builds it.
    root = _real_tree(tmp_path, monkeypatch)
    frame = xwalk.build_season_xwalk(root, "mbb", 2027)
    got = {r["contest_id"]: (r["espn_game_id"], r["match_method"]) for r in frame.iter_rows(named=True)}
    assert got == _REAL_EXPECTED


def test_real_2027_stamp_backfills_and_clears_stale_ids(tmp_path, monkeypatch):
    root = _real_tree(tmp_path, monkeypatch)
    # An earlier build had stamped a (now rejected) id onto the unmatched contest.
    stale = json.loads((root / "mbb" / "json" / "6640165.json").read_text(encoding="utf-8"))
    for row in stale["linescore"] + stale["teams"]:
        row["espn_game_id"] = "401999999"
    (root / "mbb" / "json" / "6640165.json").write_text(json.dumps(stale), encoding="utf-8")

    xwalk.write_season_xwalk(root, "mbb", 2027, xwalk.build_season_xwalk(root, "mbb", 2027))
    assert xwalk.stamp_parsed(root, "mbb", 2027) == 3
    for cid, (espn_game_id, _) in _REAL_EXPECTED.items():
        assert _ids(root, cid) == {espn_game_id}, cid
    assert xwalk.stamp_parsed(root, "mbb", 2027) == 0
