import datetime as dt

import polars as pl

from sportsdataverse.mlb.mlb_stolen_base import (
    mlb_stolen_base_value,
    predict_sb_success,
    sb_attempts_from_pitches,
    sb_success_surface,
)


def _pitches_with_des():
    # Real-capture des shape: attempts are narrated as a trailing clause on
    # a DIFFERENT batter's terminal pitch, not their own `events` value.
    return pl.DataFrame(
        {
            "des": [
                "Someone strikes out swinging. Runner A steals (3) 2nd base.",
                "Someone walks. Runner B caught stealing 2nd, catcher C to second baseman D to catcher C.",
                "Someone flies out.",
            ],
            "fielder_2": [9, 9, 9],
            "on_1b": [100.0, 200.0, None],
            "on_2b": [None, None, None],
            "on_3b": [None, None, None],
        }
    )


def test_sb_attempts_from_pitches_success_and_caught():
    out = sb_attempts_from_pitches(_pitches_with_des())
    assert out.schema["runner_id"] == pl.Utf8
    assert set(out.columns) == {"game_date", "runner_id", "catcher_id", "base", "outcome"}
    assert out.height == 2
    success_row = out.filter(pl.col("outcome") == "success").row(0, named=True)
    caught_row = out.filter(pl.col("outcome") == "caught").row(0, named=True)
    assert success_row["runner_id"] == "100"
    assert success_row["base"] == "2B"
    assert caught_row["runner_id"] == "200"
    assert caught_row["catcher_id"] == "9"


def test_sb_attempts_from_pitches_empty_input():
    out = sb_attempts_from_pitches(pl.DataFrame(schema={"des": pl.Utf8}))
    assert out.height == 0
    assert set(out.columns) == {"game_date", "runner_id", "catcher_id", "base", "outcome"}


def _sb_inputs():
    att = pl.DataFrame(
        {
            "runner_id": ["1", "1", "1", "1"],
            "catcher_id": ["9", "9", "9", "9"],
            "base": ["2B"] * 4,
            "outcome": ["success", "success", "success", "caught"],
        }
    )
    spd = pl.DataFrame({"runner_id": ["1"], "sprint_speed": [29.0]})
    pop = pl.DataFrame({"catcher_id": ["9"], "pop_2b_sba": [2.0]})
    return att, spd, pop


def test_sb_surface_laplace():
    att, spd, pop = _sb_inputs()
    s = sb_success_surface(att, spd, pop, alpha=2.0)
    assert s.height == 1
    row = s.row(0, named=True)
    # 3 successes / 4 attempts, Laplace(2): (3+2)/(4+4) = 0.625
    assert abs(row["p_success"] - 0.625) < 1e-9


def test_sb_surface_empty_and_dtype_guard():
    att, spd, pop = _sb_inputs()
    assert att.schema["runner_id"] == spd.schema["runner_id"]
    assert att.schema["catcher_id"] == pop.schema["catcher_id"]
    s = sb_success_surface(pl.DataFrame(schema={"runner_id": pl.Utf8}), spd, pop)
    assert s.height == 0
    assert set(s.columns) == {"speed_b", "pop_b", "base", "p_success", "n"}


def test_stolen_base_value_schema_and_empty():
    att, spd, pop = _sb_inputs()
    out = mlb_stolen_base_value(att, spd, pop)
    assert out.schema["runner_id"] == pl.Utf8
    assert set(out.columns) == {"runner_id", "attempts", "p_success_mean", "sb_run_value"}
    row = out.row(0, named=True)
    assert row["attempts"] == 4

    empty = mlb_stolen_base_value(pl.DataFrame(schema={"runner_id": pl.Utf8}), spd, pop)
    assert empty.height == 0
    assert set(empty.columns) == {"runner_id", "attempts", "p_success_mean", "sb_run_value"}


def test_predict_sb_success_as_of_boundary_excludes_future():
    history = pl.DataFrame(
        {
            "game_date": [dt.date(2024, 6, 1), dt.date(2024, 6, 2)],
            "outcome": ["caught", "caught"],
            "sprint_speed": [29.0, 29.0],
            "pop_2b_sba": [2.0, 2.0],
            "base": ["2B", "2B"],
        }
    )
    upcoming = pl.DataFrame({"runner_id": ["1"], "sprint_speed": [29.0], "pop_2b_sba": [2.0], "base": ["2B"]})

    before = predict_sb_success(upcoming, history, cutoff_date=dt.date(2024, 6, 15))

    # Appending a future (post-cutoff) success must NOT change the surface --
    # the boundary excludes it entirely, not just down-weight it.
    history_with_future = pl.concat(
        [
            history,
            pl.DataFrame(
                {
                    "game_date": [dt.date(2024, 6, 20)],
                    "outcome": ["success"],
                    "sprint_speed": [29.0],
                    "pop_2b_sba": [2.0],
                    "base": ["2B"],
                }
            ),
        ]
    )
    after = predict_sb_success(upcoming, history_with_future, cutoff_date=dt.date(2024, 6, 15))

    assert before.row(0, named=True)["p_success"] == after.row(0, named=True)["p_success"]
    # All-caught history -> low (Laplace-smoothed, not zero) success probability.
    assert before.row(0, named=True)["p_success"] < 0.5
