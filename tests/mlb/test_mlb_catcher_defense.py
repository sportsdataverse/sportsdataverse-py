import polars as pl

from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_blocking, mlb_catcher_throwing


def _blocking_frame(*, use_des: bool = True):
    # 10 dirt pitches for catcher 1, all blocked (no WP/PB charged).
    # 10 dirt pitches for catcher 2, 5 blocked + 5 wild-pitches.
    n = 10
    plate_z = [1.0] * (n * 2)
    fielder_2 = [1] * n + [2] * n
    on_1b = [123] * (n * 2)
    data = {
        "plate_z": plate_z,
        "sz_top": [3.5] * (n * 2),
        "sz_bot": [1.5] * (n * 2),
        "fielder_2": fielder_2,
        "on_1b": on_1b,
    }
    if use_des:
        # Real-capture shape: WP is narrated in `des`, never a clean `events` value.
        des = [None] * n + [None] * 5 + ["Someone strikes out swinging. Wild pitch by pitcher X."] * 5
        data["des"] = des
    else:
        events = [None] * n + [None] * 5 + ["wild_pitch"] * 5
        data["events"] = events
    return pl.DataFrame(data)


def test_blocking_runs_catcher_ordering_and_schema():
    out = mlb_catcher_blocking(_blocking_frame())
    assert out.schema["catcher_id"] == pl.Utf8
    assert set(out.columns) == {"catcher_id", "block_opps", "blocks_above_expected", "blocking_runs"}
    c1 = out.filter(pl.col("catcher_id") == "1").row(0, named=True)
    c2 = out.filter(pl.col("catcher_id") == "2").row(0, named=True)
    assert c1["block_opps"] == 10
    assert c2["block_opps"] == 10
    assert c1["blocking_runs"] > 0
    assert c1["blocking_runs"] > c2["blocking_runs"]


def test_blocking_runs_events_fallback_when_no_des():
    # Older feeds without `des` fall back to the `events` column.
    out = mlb_catcher_blocking(_blocking_frame(use_des=False))
    c1 = out.filter(pl.col("catcher_id") == "1").row(0, named=True)
    c2 = out.filter(pl.col("catcher_id") == "2").row(0, named=True)
    assert c1["blocking_runs"] > c2["blocking_runs"]


def test_blocking_empty_input_returns_schema():
    out = mlb_catcher_blocking(pl.DataFrame(schema={"plate_z": pl.Float64}))
    assert out.height == 0
    assert set(out.columns) == {"catcher_id", "block_opps", "blocks_above_expected", "blocking_runs"}


def _throwing_inputs():
    # catcher 9: fast pop time (1.90), throws out 3/4 attempts.
    # catcher 8: slow pop time (2.20), throws out 1/4 attempts.
    outcome = (["caught"] * 3 + ["success"] * 1) + (["caught"] * 1 + ["success"] * 3)
    catcher_id = (["9"] * 4) + (["8"] * 4)
    sb_attempts = pl.DataFrame({"catcher_id": catcher_id, "outcome": outcome})
    poptime = pl.DataFrame({"catcher_id": ["9", "8"], "pop_2b_sba": [1.90, 2.20]})
    return sb_attempts, poptime


def test_throwing_runs_fast_pop_catcher_higher():
    sb_attempts, poptime = _throwing_inputs()
    # Coarse pop_bin_width so both catchers share one bin -- the "expected"
    # CS rate reflects the shared pool, not each catcher's own rate alone
    # (a catcher alone in a bin is otherwise self-referential: 0 by construction).
    out = mlb_catcher_throwing(sb_attempts, poptime, pop_bin_width=3.0)
    assert out.schema["catcher_id"] == pl.Utf8
    assert set(out.columns) == {"catcher_id", "attempts", "cs_above_expected", "throwing_runs"}
    c9 = out.filter(pl.col("catcher_id") == "9").row(0, named=True)
    c8 = out.filter(pl.col("catcher_id") == "8").row(0, named=True)
    assert c9["attempts"] == 4 and c8["attempts"] == 4
    assert c9["throwing_runs"] > c8["throwing_runs"]


def test_throwing_dtype_guard_and_empty():
    sb_attempts, poptime = _throwing_inputs()
    assert sb_attempts.schema["catcher_id"] == poptime.schema["catcher_id"]
    out = mlb_catcher_throwing(pl.DataFrame(schema={"catcher_id": pl.Utf8}), poptime)
    assert out.height == 0
    assert set(out.columns) == {"catcher_id", "attempts", "cs_above_expected", "throwing_runs"}
