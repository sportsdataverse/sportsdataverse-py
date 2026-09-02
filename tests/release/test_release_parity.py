"""Parity tests for ``sportsdataverse.release`` vs the ``sportsdataversedata`` R package.

Canonical R source: sportsdataverse/sportsdataverse-data v0.0.11 —
``R/upload.R`` (``sportsdataverse_save`` L100-188, ``sportsdataverse_upload``
L11-44, sidecar helpers L46-80), ``R/gh_cli.R`` (``gh_cli_release_upload``
L21-73, ``gh_cli_release_tags`` L76-93, ``gh_cli_release_assets`` L97-128,
``.cli_parse_json`` L172-178), ``R/zzz.R`` (env-var retry wrapping L1-27).

Golden fixtures under ``tests/fixtures/release/`` were produced by running the
REAL R functions (see ``make_fixtures.R`` + ``README.md`` there for
provenance).  Parity bars:

* file outputs of ``sportsdataverse_save``: semantic frame equality (same
  reader on both files) — byte parity is NOT the bar because fwrite/polars
  legitimately differ on bool casing (``TRUE``/``true``) and quoting style;
* parquet metadata: exact match on ``sportsdataverse_type``, key-presence for
  ``sportsdataverse_timestamp`` (value is capture-time-dependent);
* ``gh_cli_release_assets``: exact frame equality with ``size_string``
  compared whitespace-stripped — R right-justifies the strings across the
  vector (a ``format()`` display artifact), the Python port emits unpadded
  values on purpose;
* ``_size_string``: exact per-value equality against the rlang oracle;
* gh command construction: token-for-token, except one-file-per-invocation
  (deliberate divergence: ``gh release upload`` with many files silently
  drops large assets — known SDV gotcha).

All tests run offline: the gh subprocess chokepoint ``release._invoke_gh`` is
monkeypatched.
"""

from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse import release
from sportsdataverse._rds import write_rds

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "release"

PKG_FUNCTION = "sportsdataverse::load_parity_frame()"


def r_parity_frame() -> pl.DataFrame:
    """The exact input frame built in make_fixtures.R (test_df)."""
    return pl.DataFrame(
        {
            # includes a float-stringified and a space-padded season: R's
            # as.integer parses character via double, so these must coerce
            "season": ["2023", "2024.0", " 2025"],
            "week": [1.0, 2.0, 18.0],
            "game_id": [401547401, 401547402, 401547403],
            "team": ["Green Bay", "St. Louis, MO", 'The "Team"'],
            "epa": [0.123456789, -1.5, None],
            "home": [True, False, None],
            "note": ["plain", None, "trailing space "],
        }
    )


@pytest.fixture()
def gh_recorder(monkeypatch):
    """Replace the gh subprocess chokepoint; record every arg list."""
    calls: list[list[str]] = []

    def fake_invoke(args, **kwargs):
        calls.append(list(args))
        return ""

    monkeypatch.setattr(release, "_invoke_gh", fake_invoke)
    return calls


@pytest.fixture()
def saved_files(gh_recorder, tmp_path) -> list[Path]:
    """Run the full save() flow offline; return written data-file paths."""
    return release.sportsdataverse_save(
        r_parity_frame(),
        file_name="parity_frame",
        sportsdataverse_type="Parity fixture frame",
        release_tag="test-tag",
        pkg_function=PKG_FUNCTION,
        file_types=("rds", "csv", "csv.gz", "parquet"),
    )


# ---------------------------------------------------------------------------
# sportsdataverse_save file outputs (upload.R L100-188)
# ---------------------------------------------------------------------------


def _by_suffix(files: list[Path], suffix: str) -> Path:
    matches = [f for f in files if str(f).endswith(suffix)]
    assert len(matches) == 1, f"expected exactly one {suffix} in {files}"
    return matches[0]


def test_save_csv_parity(saved_files):
    got = pl.read_csv(_by_suffix(saved_files, ".csv"))
    expected = pl.read_csv(FIXTURE_DIR / "parity_frame.csv")
    assert_frame_equal(got, expected)


def test_save_csv_gz_parity(saved_files):
    with gzip.open(_by_suffix(saved_files, ".csv.gz")) as f:
        got = pl.read_csv(f.read())
    with gzip.open(FIXTURE_DIR / "parity_frame.csv.gz") as f:
        expected = pl.read_csv(f.read())
    assert_frame_equal(got, expected)


def test_save_parquet_parity(saved_files):
    got = pl.read_parquet(_by_suffix(saved_files, ".parquet"))
    expected = pl.read_parquet(FIXTURE_DIR / "parity_frame.parquet")
    # season/week must carry the as.integer coercion => Int32, like R.
    assert got.schema["season"] == pl.Int32
    assert got.schema["week"] == pl.Int32
    # game_id stays as the caller supplied it (Int64 in Python, Int32 from R's
    # integer literal) — compare values, not dtypes, for non-coerced columns.
    assert_frame_equal(got, expected, check_dtypes=False)


def test_save_parquet_metadata(saved_files):
    got = pl.read_parquet_metadata(_by_suffix(saved_files, ".parquet"))
    expected = pl.read_parquet_metadata(FIXTURE_DIR / "parity_frame.parquet")
    assert got["sportsdataverse_type"] == expected["sportsdataverse_type"]
    assert "sportsdataverse_timestamp" in got


def test_save_rejects_r_only_file_types(gh_recorder):
    with pytest.raises(ValueError, match="qs"):
        release.sportsdataverse_save(
            r_parity_frame(),
            file_name="x",
            sportsdataverse_type="t",
            release_tag="tag",
            pkg_function="f()",
            file_types=("qs",),
        )


# ---------------------------------------------------------------------------
# rds writing (sportsdataverse/_rds.py vs R saveRDS)
# ---------------------------------------------------------------------------


def test_save_writes_gzipped_rds(saved_files):
    rds = _by_suffix(saved_files, ".rds")
    header = rds.read_bytes()[:2]
    assert header == b"\x1f\x8b"  # gzip magic, like saveRDS(compress = TRUE)


def test_rds_byte_golden(tmp_path):
    """The Python writer reproduces R's saveRDS bytes exactly.

    ``rds_golden.rds`` is the save()-coerced parity frame serialized by R
    4.5.3 with a fixed timestamp attribute and ``compress = FALSE``. The
    14-byte serialization header ("X\\n" + 3 version ints) is skipped so an
    R upgrade at fixture-regeneration time can't break the comparison.
    """
    coerced = r_parity_frame().with_columns(
        pl.col("season").str.strip_chars().cast(pl.Float64).cast(pl.Int32),
        pl.col("week").cast(pl.Int32),
    )
    out = tmp_path / "py_golden_check.rds"
    write_rds(
        coerced,
        out,
        attributes={
            "sportsdataverse_type": "Parity fixture frame",
            "sportsdataverse_timestamp": datetime(2026, 7, 12, 14, 0, 0, tzinfo=timezone.utc),
        },
        compress=False,
    )
    got = out.read_bytes()
    expected = (FIXTURE_DIR / "rds_golden.rds").read_bytes()
    assert got[14:] == expected[14:]


def test_rds_class_byte_golden(tmp_path):
    """The Python writer reproduces R's saveRDS bytes for a LEAGUE-CLASSED frame.

    ``rds_golden_classed.rds`` is what the real producer chain emits:
    ``hoopR:::make_hoopR_data()`` stamps the class + ``hoopR_*`` attrs, then
    ``sportsdataverse_save()`` appends its own pair -- the attribute order on
    every published release asset.

    The class is load-bearing, not cosmetic: hoopR/wehoop register S3 methods
    on it (``print.hoopR_data``), so an rds written without it prints
    differently for every downstream user.
    """
    coerced = r_parity_frame().with_columns(
        pl.col("season").str.strip_chars().cast(pl.Float64).cast(pl.Int32),
        pl.col("week").cast(pl.Int32),
        # test_df's game_id carries R's L suffix -- integer, not double
        pl.col("game_id").cast(pl.Int32),
    )
    ts = datetime(2026, 7, 12, 14, 0, 0, tzinfo=timezone.utc)
    out = tmp_path / "py_classed_check.rds"
    write_rds(
        coerced,
        out,
        cls=["hoopR_data", "tbl_df", "tbl", "data.table", "data.frame"],
        attributes={
            "hoopR_timestamp": ts,
            "hoopR_type": "ESPN NBA parity from hoopR data repository",
            "sportsdataverse_type": "Parity fixture frame",
            "sportsdataverse_timestamp": ts,
        },
        compress=False,
    )
    expected = (FIXTURE_DIR / "rds_golden_classed.rds").read_bytes()
    assert out.read_bytes()[14:] == expected[14:]


def test_rds_default_class_is_unchanged(tmp_path):
    """Omitting ``cls`` must still emit a bare data.frame -- the pre-existing
    byte-golden depends on it, so the new arg cannot shift the default."""
    out = tmp_path / "default.rds"
    write_rds(pl.DataFrame({"a": [1]}), out, compress=False)
    assert b"data.frame" in out.read_bytes()
    assert b"hoopR_data" not in out.read_bytes()


@pytest.mark.parametrize("bad", [[], ["hoopR_data"], ["data.frame", "tbl_df"]])
def test_rds_rejects_class_without_trailing_data_frame(tmp_path, bad):
    """R only dispatches data.frame methods when data.frame closes the chain;
    an rds that loses it stops behaving like a data.frame on read."""
    with pytest.raises(ValueError):
        write_rds(pl.DataFrame({"a": [1]}), tmp_path / "x.rds", cls=bad)


def test_rds_rejects_nested_columns(tmp_path):
    nested = pl.DataFrame({"plays": [[1, 2], [3]]})
    with pytest.raises(ValueError, match="nested"):
        write_rds(nested, tmp_path / "x.rds")


# ---------------------------------------------------------------------------
# sidecar files (upload.R L46-80) + upload flow (upload.R L11-44)
# ---------------------------------------------------------------------------


def test_upload_appends_sidecar_files(gh_recorder, tmp_path):
    data_file = tmp_path / "some.parquet"
    data_file.write_bytes(b"x")

    release.sportsdataverse_upload([data_file], tag="test-tag", pkg_function=PKG_FUNCTION)

    uploaded = [Path(c[3]).name for c in gh_recorder if c[:2] == ["release", "upload"]]
    assert uploaded[0] == "some.parquet"
    # order matters: data files first, then timestamp, then package_function
    assert uploaded[1:] == [
        "timestamp.txt",
        "timestamp.json",
        "package_function.txt",
        "package_function.json",
    ]


def test_sidecar_file_shapes(gh_recorder, tmp_path, monkeypatch):
    written: dict[str, str] = {}

    def fake_invoke(args, **kwargs):
        for a in args:
            p = Path(a)
            if p.is_file() and p.suffix in {".txt", ".json"}:
                written[p.name] = p.read_text()
        return ""

    monkeypatch.setattr(release, "_invoke_gh", fake_invoke)
    data_file = tmp_path / "d.csv"
    data_file.write_text("a\n1\n")
    release.sportsdataverse_upload([data_file], tag="test-tag", pkg_function=PKG_FUNCTION)

    # same JSON shape as the R fixtures (single-key objects, auto_unbox)
    assert list(json.loads(written["timestamp.json"])) == ["last_updated"]
    assert json.loads(written["package_function.json"]) == {"package_function": PKG_FUNCTION}
    assert written["package_function.txt"].strip() == PKG_FUNCTION
    # txt and json carry the same timestamp value
    assert json.loads(written["timestamp.json"])["last_updated"] == written["timestamp.txt"].strip()


def test_upload_retries_on_failure(monkeypatch, tmp_path):
    attempts: list[int] = []

    def flaky_invoke(args, **kwargs):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("boom")
        return ""

    monkeypatch.setattr(release, "_invoke_gh", flaky_invoke)
    monkeypatch.setenv("SPORTSDATAVERSE.UPLOAD.MAX_TIMES", "5")
    monkeypatch.setenv("SPORTSDATAVERSE.UPLOAD.PAUSE_BASE", "0.001")
    monkeypatch.setenv("SPORTSDATAVERSE.UPLOAD.PAUSE_MIN", "0.001")

    data_file = tmp_path / "d.csv"
    data_file.write_text("a\n1\n")
    release.sportsdataverse_upload([data_file], tag="t")
    # R's insistently() re-runs the WHOLE upload per attempt: attempts 1 and 2
    # die on the first gh call; attempt 3 uploads data + timestamp.txt +
    # timestamp.json => 2 failed calls + 3 successful calls.
    assert len(attempts) == 5


def test_upload_insist_off_raises_immediately(monkeypatch, tmp_path):
    attempts: list[int] = []

    def failing_invoke(args, **kwargs):
        attempts.append(1)
        raise RuntimeError("boom")

    monkeypatch.setattr(release, "_invoke_gh", failing_invoke)
    monkeypatch.setenv("SPORTSDATAVERSE.UPLOAD.INSIST", "false")

    data_file = tmp_path / "d.csv"
    data_file.write_text("a\n1\n")
    with pytest.raises(RuntimeError):
        release.sportsdataverse_upload([data_file], tag="t")
    assert len(attempts) == 1


# ---------------------------------------------------------------------------
# gh_cli_release_upload (gh_cli.R L21-73)
# ---------------------------------------------------------------------------


def test_release_upload_command_tokens(gh_recorder, tmp_path):
    f1 = tmp_path / "a.parquet"
    f2 = tmp_path / "b.csv"
    f1.write_bytes(b"x")
    f2.write_bytes(b"y")

    assert release.gh_cli_release_upload([f1, f2], tag="my-tag") is True

    # R builds: gh release upload <tag> <files...> -R <repo> --clobber
    # Python uploads one file per invocation (multi-file upload drops large
    # assets) but keeps the same tokens otherwise.
    assert gh_recorder == [
        ["release", "upload", "my-tag", str(f1), "-R", release.DEFAULT_REPO, "--clobber"],
        ["release", "upload", "my-tag", str(f2), "-R", release.DEFAULT_REPO, "--clobber"],
    ]


def test_release_upload_no_clobber(gh_recorder, tmp_path):
    f1 = tmp_path / "a.parquet"
    f1.write_bytes(b"x")
    release.gh_cli_release_upload([f1], tag="t", overwrite=False)
    assert "--clobber" not in gh_recorder[0]


def test_release_upload_skips_missing_files(gh_recorder, tmp_path):
    present = tmp_path / "here.csv"
    present.write_text("a\n")
    missing = tmp_path / "gone.csv"

    assert release.gh_cli_release_upload([present, missing], tag="t") is True
    assert [c[3] for c in gh_recorder] == [str(present)]


def test_release_upload_all_missing_returns_false(gh_recorder, tmp_path):
    # R: warns then `return(invisible(FALSE))` (gh_cli.R L40-43)
    assert release.gh_cli_release_upload([tmp_path / "nope.csv"], tag="t") is False
    assert gh_recorder == []


# ---------------------------------------------------------------------------
# gh_cli_release_tags (gh_cli.R L76-93) — derived-by-inspection payload
# ---------------------------------------------------------------------------


def test_release_tags(monkeypatch):
    payload = '[{"tagName":"nfl_espn_qbr"},{"tagName":"espn_cfb_pbp"}]'
    calls: list[list[str]] = []

    def fake_invoke(args, **kwargs):
        calls.append(list(args))
        return payload

    monkeypatch.setattr(release, "_invoke_gh", fake_invoke)
    assert release.gh_cli_release_tags() == ["nfl_espn_qbr", "espn_cfb_pbp"]
    assert calls == [["release", "list", "-R", release.DEFAULT_REPO, "--json", "tagName"]]


# ---------------------------------------------------------------------------
# gh_cli_release_assets (gh_cli.R L97-128) — golden R fixture
# ---------------------------------------------------------------------------


def test_release_assets_parity(monkeypatch):
    raw = (FIXTURE_DIR / "assets_raw.json").read_text()
    # the filter branch must actually be exercised by this payload
    assert "timestamp" in raw

    monkeypatch.setattr(release, "_invoke_gh", lambda args, **kw: raw)
    got = release.gh_cli_release_assets("espn_cfb_pbp")

    expected = pl.read_csv(FIXTURE_DIR / "assets_expected.csv")
    assert got.columns == expected.columns
    got = got.with_columns(pl.col("size_string").str.strip_chars())
    expected = expected.with_columns(pl.col("size_string").str.strip_chars())
    assert_frame_equal(got, expected, check_dtypes=False)


def test_release_assets_empty_payload(monkeypatch):
    monkeypatch.setattr(release, "_invoke_gh", lambda args, **kw: '{"assets": []}')
    got = release.gh_cli_release_assets("whatever")
    assert got.height == 0
    assert got.columns == ["name", "size", "downloads", "last_update", "url", "size_string"]


# ---------------------------------------------------------------------------
# _size_string vs rlang::as_bytes (per-value oracle)
# ---------------------------------------------------------------------------


def test_size_string_parity():
    oracle = pl.read_csv(FIXTURE_DIR / "sizes_expected.csv")
    for size, expected in oracle.iter_rows():
        assert release._size_string(int(size)) == expected, size


# ---------------------------------------------------------------------------
# ANSI stripping (gh_cli.R .cli_parse_json L172-178)
# ---------------------------------------------------------------------------


def test_parse_json_strips_ansi(monkeypatch):
    ansi_payload = '\x1b[32m[{"tagName":\x1b[39m"x"}]'
    monkeypatch.setattr(release, "_invoke_gh", lambda args, **kw: ansi_payload)
    assert release.gh_cli_release_tags() == ["x"]


# ---------------------------------------------------------------------------
# GH_TOKEN fallback (zzz.R L31-33)
# ---------------------------------------------------------------------------


def test_gh_token_fallback_from_github_pat(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_PAT", "pat-value")
    env = release._gh_env()
    assert env["GH_TOKEN"] == "pat-value"

    monkeypatch.setenv("GH_TOKEN", "explicit")
    assert release._gh_env()["GH_TOKEN"] == "explicit"
    assert os.environ.get("GH_TOKEN") == "explicit"


def test_write_release_sidecars_writes_all_four(tmp_path):
    """The public writer emits the same four files the R upload attaches."""
    from sportsdataverse.release import write_release_sidecars

    paths = write_release_sidecars(tmp_path, "hoopR::load_nba_pbp()")

    assert [p.name for p in paths] == [
        "timestamp.txt",
        "timestamp.json",
        "package_function.txt",
        "package_function.json",
    ]
    assert json.loads((tmp_path / "package_function.json").read_text()) == {"package_function": "hoopR::load_nba_pbp()"}
    assert "last_updated" in json.loads((tmp_path / "timestamp.json").read_text())


def test_write_release_sidecars_omits_package_function_when_none(tmp_path):
    from sportsdataverse.release import write_release_sidecars

    paths = write_release_sidecars(tmp_path)

    assert [p.name for p in paths] == ["timestamp.txt", "timestamp.json"]
    assert not (tmp_path / "package_function.json").exists()


def test_upload_release_sidecars_pushes_four_through_the_runner():
    """A producer's own gh runner uploads the same four files, one per call."""
    from sportsdataverse.release import upload_release_sidecars

    calls: list[list[str]] = []
    names = upload_release_sidecars(
        "espn_nba_pbp",
        runner=calls.append,
        pkg_function="hoopR::load_nba_pbp()",
        repo="sportsdataverse/sportsdataverse-data",
    )

    assert names == [
        "timestamp.txt",
        "timestamp.json",
        "package_function.txt",
        "package_function.json",
    ]
    assert len(calls) == 4
    for call, name in zip(calls, names):
        assert call[:3] == ["release", "upload", "espn_nba_pbp"]
        assert call[3].endswith(name)
        assert call[4:] == ["--repo", "sportsdataverse/sportsdataverse-data", "--clobber"]


def test_upload_release_sidecars_cleans_up_its_temp_dir():
    from sportsdataverse.release import upload_release_sidecars

    paths: list[str] = []
    upload_release_sidecars("t", runner=lambda a: paths.append(a[3]))

    assert len(paths) == 2  # timestamp pair only
    assert not any(Path(p).exists() for p in paths)


def test_write_release_sidecars_defaults_to_now(tmp_path):
    """A live publish stamps the current moment -- unchanged behaviour."""
    from sportsdataverse.release import write_release_sidecars

    write_release_sidecars(tmp_path)
    stamped = json.loads((tmp_path / "timestamp.json").read_text())["last_updated"]

    assert stamped.startswith(datetime.now().strftime("%Y-%m-%d")) or stamped.startswith(
        (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    )


def test_write_release_sidecars_honours_as_of(tmp_path):
    """A back-fill records when the data actually moved, not when we stamped it.

    Stamping a tag whose assets last changed in 2023 with today's date would
    assert the data moved today -- a worse answer than the missing one.
    """
    from sportsdataverse.release import write_release_sidecars

    write_release_sidecars(tmp_path, as_of=datetime(2023, 3, 5, 13, 4, 56, tzinfo=timezone.utc))
    stamped = json.loads((tmp_path / "timestamp.json").read_text())["last_updated"]

    assert stamped.startswith("2023-03-05")
    assert (tmp_path / "timestamp.txt").read_text().strip() == stamped


def test_as_of_reads_a_naive_datetime_as_utc(tmp_path):
    """GitHub's asset timestamps are UTC and parse naive; do not let them drift."""
    from sportsdataverse.release import write_release_sidecars

    write_release_sidecars(tmp_path, as_of=datetime(2023, 3, 5, 13, 4, 56))
    naive = json.loads((tmp_path / "timestamp.json").read_text())["last_updated"]

    write_release_sidecars(tmp_path, as_of=datetime(2023, 3, 5, 13, 4, 56, tzinfo=timezone.utc))
    aware = json.loads((tmp_path / "timestamp.json").read_text())["last_updated"]

    assert naive == aware


def test_upload_release_sidecars_passes_as_of_through():
    from sportsdataverse.release import upload_release_sidecars

    seen: dict[str, str] = {}

    def _runner(argv: list[str]) -> None:
        path = Path(argv[3])
        seen[path.name] = path.read_text()

    upload_release_sidecars(
        "ncaa_baseball_pbp",
        runner=_runner,
        pkg_function="sportsdataverse.baseball.load_ncaa_baseball_pbp()",
        as_of=datetime(2023, 3, 5, 13, 4, 56, tzinfo=timezone.utc),
    )

    assert json.loads(seen["timestamp.json"])["last_updated"].startswith("2023-03-05")
