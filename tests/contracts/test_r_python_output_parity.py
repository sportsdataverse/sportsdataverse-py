import polars as pl

from tools.validation import cli
from tools.validation.checks import r_python_output_parity as rpp

KEYS = ("game_id",)


def _run(r, py, **kw):
    return rpp.run("d", r, py, KEYS, "nfl", **kw)


def _checks(findings):
    return [f.message for f in findings]


def test_identical_frames_are_clean():
    f = pl.DataFrame({"game_id": [1, 2], "epa": [0.5, -0.25], "team": ["KC", "BUF"]})
    assert _run(f, f.clone()) == []


def test_missing_join_key_stops_immediately():
    r = pl.DataFrame({"epa": [0.5]})
    py = pl.DataFrame({"game_id": [1], "epa": [0.5]})
    findings = _run(r, py)
    assert len(findings) == 1, "must not attempt any comparison without the key"
    assert "join key(s) absent" in findings[0].message
    assert findings[0].severity.value == "error"


def test_join_key_dtype_mismatch_is_named_not_misreported_as_missing_rows():
    """The whole point: a Utf8-vs-Int64 key matches nothing, and reporting that
    as 'no shared rows' sends the reader hunting for missing data."""
    r = pl.DataFrame({"game_id": ["1", "2"], "epa": [0.5, 0.25]})
    py = pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.25]})
    findings = _run(r, py)
    assert len(findings) == 1
    msg = findings[0].message
    assert "dtype disagreement" in msg
    assert "String" in msg and "Int64" in msg
    # It must NOT have gone on to claim the row sets differ.
    assert not any("row sets differ" in m for m in _checks(findings))


def test_row_set_divergence_reports_both_directions():
    r = pl.DataFrame({"game_id": [1, 2, 3], "epa": [0.1, 0.2, 0.3]})
    py = pl.DataFrame({"game_id": [2, 3, 4], "epa": [0.2, 0.3, 0.4]})
    findings = _run(r, py)
    row = next(f for f in findings if "row sets differ" in f.message)
    assert "1 key group(s) only in R" in row.message
    assert "1 only in Python" in row.message
    assert row.needs_judgment is True


def test_no_shared_keys_fails_loudly_instead_of_passing_vacuously():
    r = pl.DataFrame({"game_id": [1], "epa": [0.1]})
    py = pl.DataFrame({"game_id": [9], "epa": [999.0]})
    findings = _run(r, py)
    assert any("no shared key groups" in f.message for f in findings)
    # and it must not have emitted a clean bill of health for `epa`
    assert not any("epa" in f.message and "disagrees" in f.message for f in findings)


def test_column_set_divergence_is_a_warning_not_an_error():
    """Bundling differences are legitimate (R emits schedules inside the pbp
    stage), so this informs rather than fails."""
    r = pl.DataFrame({"game_id": [1], "epa": [0.1], "r_only": [1]})
    py = pl.DataFrame({"game_id": [1], "epa": [0.1], "py_only": [2]})
    findings = _run(r, py)
    col = next(f for f in findings if "column sets differ" in f.message)
    assert col.severity.value == "warn"
    assert "['r_only']" in col.message and "['py_only']" in col.message


def test_shared_column_dtype_divergence_is_reported():
    r = pl.DataFrame({"game_id": [1], "yards": [10]})
    py = pl.DataFrame({"game_id": [1], "yards": [10.0]})
    findings = _run(r, py)
    assert any("differ in dtype" in f.message for f in findings)


def test_numeric_divergence_beyond_tolerance_is_flagged_with_a_sample():
    r = pl.DataFrame({"game_id": [1, 2], "epa": [0.50, 0.20]})
    py = pl.DataFrame({"game_id": [1, 2], "epa": [0.50, 0.99]})
    findings = _run(r, py)
    val = next(f for f in findings if "'epa' disagrees" in f.message)
    assert "1 of 2 shared row(s)" in val.message
    assert val.sample and val.sample[0]["game_id"] == 2
    assert val.needs_judgment is True


def test_numeric_divergence_within_tolerance_is_clean():
    r = pl.DataFrame({"game_id": [1], "epa": [0.5000000]})
    py = pl.DataFrame({"game_id": [1], "epa": [0.5000001]})
    assert _run(r, py, tolerance=1e-5) == []


def test_null_on_one_side_only_is_a_divergence():
    """Null-vs-value must not slip through: `null - 0.5` is null, so a naive
    abs()>tol comparison would drop the row rather than flag it."""
    r = pl.DataFrame({"game_id": [1], "epa": [None]}, schema={"game_id": pl.Int64, "epa": pl.Float64})
    py = pl.DataFrame({"game_id": [1], "epa": [0.5]})
    findings = _run(r, py)
    assert any("'epa' disagrees" in f.message for f in findings)


def test_string_divergence_is_flagged():
    r = pl.DataFrame({"game_id": [1], "team": ["KC"]})
    py = pl.DataFrame({"game_id": [1], "team": ["KAN"]})
    findings = _run(r, py)
    assert any("'team' disagrees" in f.message for f in findings)


def test_ignored_columns_are_not_compared():
    r = pl.DataFrame({"game_id": [1], "epa": [0.1], "built_at": ["2026-01-01"]})
    py = pl.DataFrame({"game_id": [1], "epa": [0.1], "built_at": ["2026-08-06"]})
    assert _run(r, py, ignore_columns=("built_at",)) == []


def _write_pair(tmp_path, r, py):
    rp, pp = tmp_path / "r.parquet", tmp_path / "py.parquet"
    r.write_parquet(rp)
    py.write_parquet(pp)
    return str(rp), str(pp)


def test_cli_compare_exits_1_on_divergence(tmp_path, capsys):
    rp, pp = _write_pair(
        tmp_path,
        pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.2]}),
        pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.9]}),
    )
    code = cli.main(
        [
            "compare",
            "--dataset",
            "d",
            "--domain",
            "nfl",
            "--r-parquet",
            rp,
            "--py-parquet",
            pp,
            "--join-keys",
            "game_id",
        ]
    )
    assert code == 1
    assert "disagrees between the R and Python pipelines" in capsys.readouterr().out


def test_cli_compare_exits_0_when_the_pipelines_agree(tmp_path):
    frame = pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.2]})
    rp, pp = _write_pair(tmp_path, frame, frame.clone())
    code = cli.main(
        [
            "compare",
            "--dataset",
            "d",
            "--domain",
            "nfl",
            "--r-parquet",
            rp,
            "--py-parquet",
            pp,
            "--join-keys",
            "game_id",
        ]
    )
    assert code == 0


def test_cli_compare_honours_ignore_columns(tmp_path):
    rp, pp = _write_pair(
        tmp_path,
        pl.DataFrame({"game_id": [1], "epa": [0.5], "built_at": ["a"]}),
        pl.DataFrame({"game_id": [1], "epa": [0.5], "built_at": ["b"]}),
    )
    argv = [
        "compare",
        "--dataset",
        "d",
        "--domain",
        "nfl",
        "--r-parquet",
        rp,
        "--py-parquet",
        pp,
        "--join-keys",
        "game_id",
    ]
    assert cli.main(argv) == 1, "sanity: the stamp diverges when not ignored"
    assert cli.main([*argv, "--ignore-columns", "built_at"]) == 0


def test_neither_side_is_described_as_wrong():
    """The gate's premise is that a divergence is a review item, not a verdict.
    Wording that blames one pipeline would pre-empt the human decision."""
    r = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    py = pl.DataFrame({"game_id": [1, 3], "epa": [0.1, 0.9]})
    for f in _run(r, py):
        assert "dropped from" not in f.message
        assert "should be" not in f.message
