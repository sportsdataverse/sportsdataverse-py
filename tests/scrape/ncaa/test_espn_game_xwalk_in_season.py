"""In-season NCAA -> ESPN game crosswalk: built from parsed contests, stamped back into them.

The 2026-27 schedule pages were captured before tip-off (no contest ids, no ESPN team
ids), so the schedules-only NCAA side built 0 contests and every published 2027 row
had a null ``espn_game_id``.
"""

from __future__ import annotations

import json

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
            "game_date": ["11/03/2026 07:00 PM"],
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
