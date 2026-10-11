"""Per-game integrity screen: unparsed games, duplicated rows, frozen clocks, truncation, down skips, garbled names."""

from __future__ import annotations

import json

import polars as pl
import pytest

from sportsdataverse.validation.findings import CheckContext, Severity
from tools.validation.checks import pbp_integrity as pi


def _ctx(**kw):
    return CheckContext(domain="cfb", dataset="toy", schema={}, **kw)


def _game(n=30, game_id=1, season=2024, **over):
    """A clean game: n scrimmage rushes spread over four quarters, ending at 0:00 of Q4."""
    rows = []
    for i in range(n):
        period = 1 + (4 * i) // n
        secs = 900 - ((i * 4 * 900 // n) % 900) if i < n - 1 else 0
        rows.append(
            {
                "game_id": game_id,
                "season": season,
                "id": f"{game_id}-{i}",
                "game_play_number": i + 1,
                "period": period,
                "clock.displayValue": f"{secs // 60}:{secs % 60:02d}",
                "drive.id": f"{game_id}-d{i // 3}",
                "pos_team_id": 10,
                "start.down": 1 + i % 3,
                "rush": True,
                **{flag: False for flag in pi._FLAGS if flag != "rush"},
                **{name: None for name in pi._NAME_COLUMNS},
            }
        )
    frame = pl.DataFrame(rows)
    return frame.with_columns(**{k: pl.lit(v) for k, v in over.items()}) if over else frame


def _ind(frame):
    return pi.game_indicators(frame).row(0, named=True)


def test_clean_game_raises_nothing():
    r = _ind(_game())
    assert not r["unparsed"] and r["duplicate_rows"] == 0 and not r["frozen_clock"] and not r["truncated"]
    assert r["down_skips"] == 0 and r["garbled_names"] == 0


def test_mostly_flagless_game_is_unparsed():
    assert _ind(_game().with_columns(rush=pl.lit(False)))["unparsed"]


def test_duplicated_play_ids_are_counted():
    g = _game()
    assert _ind(pl.concat([g, g.head(5)]))["duplicate_rows"] == 5


def test_clock_stuck_at_zero_is_frozen():
    g = _game()
    stuck = g.with_columns(
        pl.when(pl.col("game_play_number") <= 10)
        .then(pl.lit("0:00"))
        .otherwise(pl.col("clock.displayValue"))
        .alias("clock.displayValue")
    )
    assert _ind(stuck)["frozen_clock"]  # 10 of 30 plays at 0:00
    assert not _ind(g)["frozen_clock"]  # only the final whistle


def test_game_ending_mid_fourth_quarter_is_truncated():
    g = _game()
    # drop the plays after 8:23 of the fourth quarter
    cut = g.filter(~((pl.col("period") == 4) & (pl.col("game_play_number") > 26)))
    cut = cut.with_columns(
        pl.when(pl.col("game_play_number") == 26)
        .then(pl.lit("8:23"))
        .otherwise(pl.col("clock.displayValue"))
        .alias("clock.displayValue")
    )
    assert _ind(cut)["truncated"]
    assert not _ind(g)["truncated"]


def test_down_jump_inside_a_drive_is_a_skip():
    g = _game().with_columns(
        pl.when(pl.col("game_play_number") == 2).then(3).otherwise(pl.col("start.down")).alias("start.down")
    )
    # play 1 is 1st down, play 2 now 3rd down in the same drive -> one skip (1 -> 3)
    assert _ind(g)["down_skips"] == 1


@pytest.mark.parametrize(
    "name, garbled",
    [
        ("Jeff Blanchardtackled b", True),
        ("Smith to the Colo", True),
        ("Jones, clock:13", True),
        ("#49 Adams", True),
        ("Jeff Blanchard", False),
        ("Jaxon Smith-Njigba", False),
        ("D'Andre Swift", False),
        ("Chris Van de Water", False),
        ("Smith Jr.", False),
    ],
)
def test_garbled_name_rule(name, garbled):
    g = _game(n=8).with_columns(pl.lit(name).alias("kickoff_return_player_name"))
    assert (_ind(g)["garbled_names"] > 0) is garbled


def test_count_above_ceiling_warns_with_example_games(tmp_path, monkeypatch):
    monkeypatch.setattr(pi, "_FLOOR_DIR", tmp_path)
    (tmp_path / "toy_integrity.json").write_text(json.dumps({"ceilings": {"unparsed": {"2024": 0}}}), encoding="utf-8")
    frame = pl.concat([_game(game_id=g).with_columns(rush=pl.lit(False)) for g in (1, 2, 3)])
    warns = [f for f in pi.run("toy", frame, _ctx()) if f.severity is Severity.WARN]
    assert len(warns) == 1
    assert warns[0].locator == {"indicator": "unparsed", "season": 2024}
    assert warns[0].actual == 3 and "1, 2, 3" in warns[0].message


def test_count_at_ceiling_is_info_only(tmp_path, monkeypatch):
    monkeypatch.setattr(pi, "_FLOOR_DIR", tmp_path)
    (tmp_path / "toy_integrity.json").write_text(json.dumps({"ceilings": {"unparsed": {"2024": 3}}}), encoding="utf-8")
    frame = pl.concat([_game(game_id=g).with_columns(rush=pl.lit(False)) for g in (1, 2, 3)])
    findings = pi.run("toy", frame, _ctx())
    assert findings and all(f.severity is Severity.INFO for f in findings)


def test_missing_columns_or_ceilings_skip(tmp_path, monkeypatch):
    monkeypatch.setattr(pi, "_FLOOR_DIR", tmp_path)
    assert pi.run("toy", _game(), _ctx()) == []
    (tmp_path / "toy_integrity.json").write_text(json.dumps({"ceilings": {}}), encoding="utf-8")
    assert pi.run("toy", pl.DataFrame({"game_id": [1]}), _ctx()) == []


def test_committed_cfb_pbp_integrity_ceilings_are_real():
    payload = json.loads(pi.floor_path("cfb_pbp").read_text(encoding="utf-8"))
    assert set(payload["ceilings"]) == set(pi.INDICATORS)


def test_possession_change_inside_one_drive_id_is_not_a_skip():
    """ESPN drive ids can span both teams: a turnover hands the next down to the other offense."""
    g = _game().with_columns(
        pl.when(pl.col("game_play_number") == 2).then(3).otherwise(pl.col("start.down")).alias("start.down"),
        pl.when(pl.col("game_play_number") >= 2).then(20).otherwise(pl.col("pos_team_id")).alias("pos_team_id"),
    )
    assert _ind(g)["down_skips"] == 0
