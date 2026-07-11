import polars as pl

from sportsdataverse.mlb.mlb_baserunning import advancement_opportunities, mlb_baserunning_value


def _events_frame():
    # game_pk=1: PA1 single with runner on 1st (id 100) -> ends up on 3rd (took the extra base).
    # PA2 (next PA): pre-state on_1b/2b/3b reflects the post-state of PA1.
    return pl.DataFrame(
        {
            "game_pk": [1, 1],
            "at_bat_number": [1, 2],
            "on_1b": [100.0, None],
            "on_2b": [None, None],
            "on_3b": [None, 100.0],
            "events": ["single", "strikeout"],
        }
    )


def test_advancement_opportunities_first_to_third():
    out = advancement_opportunities(_events_frame())
    assert out.schema["runner_id"] == pl.Utf8
    assert set(out.columns) == {"runner_id", "opp_type", "took_extra"}
    row = out.filter(pl.col("opp_type") == "first_to_third").row(0, named=True)
    assert row["runner_id"] == "100"
    assert row["took_extra"] == 1


def test_advancement_opportunities_empty_input():
    out = advancement_opportunities(pl.DataFrame(schema={"game_pk": pl.Int64}))
    assert out.height == 0
    assert set(out.columns) == {"runner_id", "opp_type", "took_extra"}


def _baserunning_inputs():
    # runner 200 (fast): 5 first-to-third opportunities, takes the extra base every time.
    # runner 300 (slow, baseline pool): 5 opportunities, holds every time.
    n = 5
    # Build two-PA-per-game frames: PA1 = single w/ runner on 1st, PA2 = next PA carrying post-state.
    rows = []
    gpk = 1
    for runner, reaches_third in [(200.0, True), (300.0, False)]:
        for _ in range(n):
            rows.append(
                {"game_pk": gpk, "at_bat_number": 1, "on_1b": runner, "on_2b": None, "on_3b": None, "events": "single"}
            )
            rows.append(
                {
                    "game_pk": gpk,
                    "at_bat_number": 2,
                    "on_1b": None,
                    "on_2b": None,
                    "on_3b": (runner if reaches_third else None),
                    "events": "strikeout",
                }
            )
            gpk += 1
    events = pl.DataFrame(rows)
    sprint_speed = pl.DataFrame({"runner_id": ["200", "300"], "sprint_speed": [30.0, 25.0]})
    return events, sprint_speed


def test_baserunning_runs_fast_runner_higher():
    events, sprint_speed = _baserunning_inputs()
    # Coarse speed_bin so both runners share one bin -- the "expected" rate
    # reflects the shared pool, not each runner's own rate alone (a runner
    # alone in a bin is otherwise self-referential: 0 by construction).
    out = mlb_baserunning_value(events, sprint_speed, speed_bin=50.0)
    assert out.schema["runner_id"] == pl.Utf8
    assert set(out.columns) == {"runner_id", "opportunities", "extra_bases_above_expected", "baserunning_runs"}
    r200 = out.filter(pl.col("runner_id") == "200").row(0, named=True)
    r300 = out.filter(pl.col("runner_id") == "300").row(0, named=True)
    assert r200["opportunities"] == 5 and r300["opportunities"] == 5
    assert r200["baserunning_runs"] > r300["baserunning_runs"]


def test_baserunning_empty_input_returns_schema():
    events, sprint_speed = _baserunning_inputs()
    out = mlb_baserunning_value(pl.DataFrame(schema={"game_pk": pl.Int64}), sprint_speed)
    assert out.height == 0
    assert set(out.columns) == {"runner_id", "opportunities", "extra_bases_above_expected", "baserunning_runs"}
