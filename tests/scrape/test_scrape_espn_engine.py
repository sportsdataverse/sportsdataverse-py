"""The shared ESPN ``-raw`` engine: league config, ids, capture columns, master.

Ported from ``wehoop-wbb-raw``'s in-repo suite (the only one of the four ESPN
raw repos that had one) and extended with the league parameterization. The WBB
assertions are kept verbatim on purpose: they are the parity oracle proving the
lift did not change behavior for the repo the code came from.
"""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.scrape.espn import league_config as lc
from sportsdataverse.scrape.espn.cli import season_args, str2bool
from sportsdataverse.scrape.espn.ids import to_int64, with_int64_ids
from sportsdataverse.scrape.espn.master import build_coverage, build_master
from sportsdataverse.scrape.espn.paths import raw_github_url
from sportsdataverse.scrape.espn.persist import (
    is_error_payload,
    scan_for_error_payloads,
    write_payload,
)
from sportsdataverse.scrape.espn.schedule import add_capture_columns

# --- league config -----------------------------------------------------------


@pytest.mark.parametrize("key", ["nba", "mbb", "wnba", "wbb"])
def test_every_league_resolves(key):
    assert lc.by_key(key).key == key


def test_unknown_league_names_the_valid_set():
    """The caller is usually a shell driver passing --league from a cron line."""
    with pytest.raises(ValueError, match="mbb, nba, wbb, wnba"):
        lc.by_key("ncaam")


@pytest.mark.parametrize("key,expected", [("nba", False), ("mbb", False), ("wnba", True), ("wbb", True)])
def test_officials_exist_only_for_the_womens_leagues(key, expected):
    stems = [stem for stem, _ in lc.by_key(key).families]
    assert ("officials_json" in stems) is expected


@pytest.mark.parametrize("key", ["nba", "mbb", "wnba", "wbb"])
def test_game_json_raw_is_never_flagged(key):
    """It is written by the same call as game_json, so a second flag could only
    ever lie about a divergence that cannot happen."""
    config = lc.by_key(key)
    assert "game_json_raw" not in config.flagged


@pytest.mark.parametrize("key", ["nba", "mbb", "wnba", "wbb"])
def test_every_flagged_stem_is_a_real_family(key):
    config = lc.by_key(key)
    stems = {stem for stem, _ in config.families}
    assert set(config.flagged) <= stems


def test_each_league_owns_a_distinct_repo():
    repos = [lc.by_key(k).repo for k in ("nba", "mbb", "wnba", "wbb")]
    assert len(set(repos)) == 4


# --- the league keyword is required, not defaulted ---------------------------


def test_add_capture_columns_requires_league_as_a_keyword():
    """A defaulted league is how a well-formed capture lands under the wrong
    league's tree -- wrong data, no error. Guarded structurally, because the
    NCAA extraction shipped exactly that bug (a capture CLI with no --league).
    """
    sig = inspect.signature(add_capture_columns)
    league = sig.parameters["league"]
    assert league.kind is inspect.Parameter.KEYWORD_ONLY
    assert league.default is inspect.Parameter.empty


def test_calling_without_a_league_is_a_typeerror():
    with pytest.raises(TypeError):
        add_capture_columns(pl.DataFrame({"game_id": [1]}), root=".")  # type: ignore[call-arg]


# --- ids ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "values,dtype",
    [
        ([401811123], pl.Int32),
        ([401811123], pl.Int64),
        (["401811123"], pl.Utf8),
        ([401811123.0], pl.Float64),
    ],
    ids=["int32", "int64", "utf8", "float64"],
)
def test_every_source_dtype_lands_on_int64(values, dtype):
    out = to_int64(pl.Series("game_id", values, dtype=dtype))
    assert out.dtype == pl.Int64
    assert out[0] == 401811123


def test_nulls_survive_canonicalization():
    out = to_int64(pl.Series("game_id", [None, 401811123], dtype=pl.Int64))
    assert out.null_count() == 1


def test_lossy_float_refuses_rather_than_truncating():
    with pytest.raises(ValueError, match="lossy"):
        to_int64(pl.Series("game_id", [401811123.5]))


def test_non_numeric_string_refuses():
    with pytest.raises(ValueError, match="non-numeric"):
        to_int64(pl.Series("game_id", ["not-an-id"]))


def test_with_int64_ids_skips_absent_columns():
    df = pl.DataFrame({"game_id": [1]}, schema={"game_id": pl.Int32})
    out = with_int64_ids(df, "game_id", "venue_id")
    assert out.schema["game_id"] == pl.Int64
    assert "venue_id" not in out.columns


# --- per-season capture columns ----------------------------------------------

WBB_COLUMNS = [
    "game_json_url",
    "game_json_raw_url",
    "game_rosters_json_url",
    "officials_json_url",
    "has_game_json",
    "has_game_rosters_json",
    "has_officials_json",
]


def _tree(tmp_path: Path, league: str) -> Path:
    for stem, segments in lc.by_key(league).families:
        (tmp_path / league / Path(*segments)).mkdir(parents=True, exist_ok=True)
    (tmp_path / league / "json" / "final" / "401811123.json").write_text("{}")
    (tmp_path / league / "game_rosters" / "json" / "401811123.json").write_text("{}")
    return tmp_path


def _schedule() -> pl.DataFrame:
    return pl.DataFrame({"game_id": [401811123, 401811124]}, schema={"game_id": pl.Int32})


def test_adds_every_column(tmp_path):
    out = add_capture_columns(_schedule(), root=_tree(tmp_path, "wbb"), league="wbb")
    for column in WBB_COLUMNS:
        assert column in out.columns


def test_a_league_without_officials_gets_no_officials_columns(tmp_path):
    out = add_capture_columns(_schedule(), root=_tree(tmp_path, "nba"), league="nba")
    assert "officials_json_url" not in out.columns
    assert "has_officials_json" not in out.columns
    assert "has_game_json" in out.columns


def test_urls_are_league_and_repo_scoped(tmp_path):
    out = add_capture_columns(_schedule(), root=_tree(tmp_path, "nba"), league="nba")
    url = out["game_json_url"][0]
    assert "/hoopR-nba-raw/" in url
    assert url.endswith("nba/json/final/401811123.json")


def test_urls_emitted_for_every_row_even_when_the_file_is_absent(tmp_path):
    out = add_capture_columns(_schedule(), root=_tree(tmp_path, "wbb"), league="wbb")
    assert out["game_json_url"].null_count() == 0
    assert out["game_json_url"][1].endswith("wbb/json/final/401811124.json")


def test_has_flags_reflect_what_is_on_disk(tmp_path):
    out = add_capture_columns(_schedule(), root=_tree(tmp_path, "wbb"), league="wbb")
    assert out["has_game_json"].to_list() == [True, False]
    assert out["has_game_rosters_json"].to_list() == [True, False]
    assert out["has_officials_json"].to_list() == [False, False]


def test_game_id_is_canonicalized_to_int64(tmp_path):
    out = add_capture_columns(_schedule(), root=_tree(tmp_path, "wbb"), league="wbb")
    assert out.schema["game_id"] == pl.Int64


def test_url_never_contains_a_float_artifact(tmp_path):
    """A float-origin id stringifies as "123.0" and addresses nothing."""
    df = pl.DataFrame({"game_id": [401811123.0]})
    out = add_capture_columns(df, root=_tree(tmp_path, "wbb"), league="wbb")
    assert ".0.json" not in out["game_json_url"][0]
    assert out["game_json_url"][0].endswith("401811123.json")


def test_a_league_config_may_be_passed_directly(tmp_path):
    out = add_capture_columns(_schedule(), root=_tree(tmp_path, "wbb"), league=lc.WBB)
    assert out["has_game_json"].to_list() == [True, False]


def test_raw_github_url_shape():
    assert raw_github_url("wehoop-wbb-raw", "wbb", "json", "final", "1.json") == (
        "https://raw.githubusercontent.com/sportsdataverse/wehoop-wbb-raw/main/wbb/json/final/1.json"
    )


# --- CLI contract ------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("false", False),
        ("False", False),
        ("FALSE", False),
        (" false ", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("", False),
        ("true", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        (True, True),
        (False, False),
    ],
)
def test_str2bool_parses_shell_strings(raw, expected):
    assert str2bool(raw) is expected


def test_unrecognised_text_does_not_trigger_a_rescrape():
    """A typo in a cron definition must not re-fetch the whole archive."""
    assert str2bool("ture") is False
    assert str2bool("maybe") is False


def test_rescrape_defaults_to_false():
    assert season_args(["--start_year", "2026"]).rescrape is False


def test_rescrape_default_is_overridable_for_migrating_repos():
    """nba/mbb/wnba-raw shipped default=True; they migrate without changing
    cron behavior, then flip it in a separate reviewable commit."""
    assert season_args(["--start_year", "2026"], rescrape_default=True).rescrape is True
    assert season_args(["--start_year", "2026", "-r", "false"], rescrape_default=True).rescrape is False


def test_rescrape_false_string_stays_false():
    assert season_args(["--start_year", "2026", "-r", "false"]).rescrape is False


def test_rescrape_true_string_is_honoured():
    assert season_args(["--start_year", "2026", "-r", "true"]).rescrape is True


def test_end_year_defaults_to_start_year():
    assert season_args(["--start_year", "2026"]).end_year == 2026


def test_explicit_range_is_preserved():
    args = season_args(["-s", "2007", "-e", "2013"])
    assert (args.start_year, args.end_year) == (2007, 2013)


def test_start_year_is_required():
    with pytest.raises(SystemExit):
        season_args([])


# --- write guard -------------------------------------------------------------

SPRING_ERROR = {
    "error": "Not Found",
    "message": "",
    "path": "/apis/site/v2/...",
    "status": 404,
    "timestamp": "2026-01-01T00:00:00Z",
}
ESPN_ERROR = {"code": 404, "detail": "no data"}
GOOD = {"results": {"stats": []}, "team": {"id": "52"}}


@pytest.mark.parametrize(
    "payload",
    [SPRING_ERROR, ESPN_ERROR, {}, [], None, ""],
    ids=["spring", "espn", "empty-dict", "list", "none", "empty-str"],
)
def test_error_and_empty_payloads_are_recognized(payload):
    assert is_error_payload(payload) is True


@pytest.mark.parametrize(
    "payload",
    [GOOD, {"items": [], "count": 0}, {"categories": [], "teams": {}}],
    ids=["team_stats", "officials-envelope", "player_season_stats"],
)
def test_real_payloads_are_not_errors(payload):
    assert is_error_payload(payload) is False


def test_an_empty_but_valid_collection_is_not_an_error():
    """A zero-row Core v2 page is a real answer: this team had no officials.
    Treating it as an error would re-scrape it forever."""
    assert is_error_payload({"count": 0, "items": [], "pageCount": 0}) is False


def test_write_refuses_an_error_payload(tmp_path):
    path = tmp_path / "2948.json"
    assert write_payload(path, ESPN_ERROR) is False
    assert not path.exists()


def test_write_persists_a_real_payload(tmp_path):
    path = tmp_path / "52.json"
    assert write_payload(path, GOOD) is True
    assert json.loads(path.read_text(encoding="utf-8")) == GOOD


def test_write_preserves_an_archives_existing_byte_format(tmp_path):
    """These archives are committed to git. The hoopR/wehoop trees were written
    with indent=0; adopting the guard must not reformat them, or any later
    rewrite churns the diff of every file it touches."""
    compact = tmp_path / "compact.json"
    indented = tmp_path / "indented.json"
    write_payload(compact, GOOD)
    write_payload(indented, GOOD, indent=0)
    assert "\n" not in compact.read_text(encoding="utf-8")
    assert "\n" in indented.read_text(encoding="utf-8")
    assert json.loads(compact.read_text(encoding="utf-8")) == json.loads(indented.read_text(encoding="utf-8"))


def test_write_creates_missing_parents(tmp_path):
    path = tmp_path / "2026" / "52.json"
    assert write_payload(path, GOOD) is True
    assert path.exists()


def test_write_never_truncates_a_good_file_with_a_bad_one(tmp_path):
    """The failure that actually matters: a good capture overwritten later by
    an error response, turning a working season into a silent gap."""
    path = tmp_path / "52.json"
    write_payload(path, GOOD)
    assert write_payload(path, SPRING_ERROR) is False
    assert json.loads(path.read_text(encoding="utf-8")) == GOOD


def test_scan_finds_error_payloads(tmp_path):
    (tmp_path / "2007").mkdir()
    (tmp_path / "2007" / "2948.json").write_text(json.dumps(ESPN_ERROR), encoding="utf-8")
    (tmp_path / "2007" / "52.json").write_text(json.dumps(GOOD), encoding="utf-8")
    found = scan_for_error_payloads(tmp_path, "*/*.json")
    assert [p.name for p in found] == ["2948.json"]


def test_scan_reports_unreadable_files(tmp_path):
    (tmp_path / "broken.json").write_text("{not json", encoding="utf-8")
    assert [p.name for p in scan_for_error_payloads(tmp_path, "*.json")] == ["broken.json"]


# --- master + coverage --------------------------------------------------------


def _season(season: int, n: int, captured: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [900000 + season * 100 + i for i in range(n)],
            "season": [season] * n,
            "season_type": [2] * n,
            "date": ["2025-11-0%d" % (i % 9 + 1) for i in range(n)],
            "has_game_json": [i < captured for i in range(n)],
            "has_game_rosters_json": [False] * n,
            "has_officials_json": [False] * n,
        }
    )


def test_master_is_the_union_of_seasons():
    master = build_master([_season(2025, 4, 2), _season(2026, 6, 6)])
    assert master.height == 10
    assert set(master["season"].unique().to_list()) == {2025, 2026}


def test_master_pins_one_column_order_across_ragged_inputs():
    a = _season(2025, 2, 1)
    b = _season(2026, 2, 2).with_columns(pl.lit(1200).alias("venue_capacity"))
    assert build_master([a, b]).columns == build_master([b, a]).columns


def test_a_column_missing_from_one_season_is_null_filled():
    a = _season(2025, 2, 1)
    b = _season(2026, 2, 2).with_columns(pl.lit(1200).alias("venue_capacity"))
    master = build_master([a, b])
    assert master["venue_capacity"].null_count() == 2


def test_master_game_id_is_int64():
    assert build_master([_season(2026, 2, 2)]).schema["game_id"] == pl.Int64


def test_build_master_refuses_an_empty_input():
    with pytest.raises(ValueError, match="at least one"):
        build_master([])


def test_coverage_is_one_row_per_season_and_type():
    coverage = build_coverage(build_master([_season(2025, 4, 2), _season(2026, 6, 6)]))
    assert coverage.height == 2
    row = coverage.filter(pl.col("season") == 2025).to_dicts()[0]
    assert row["n_games"] == 4
    assert row["pct_json_captured"] == pytest.approx(0.5)


def test_coverage_has_a_pct_column_per_flag():
    coverage = build_coverage(build_master([_season(2026, 2, 1)]))
    for column in (
        "pct_has_game_json",
        "pct_has_game_rosters_json",
        "pct_has_officials_json",
    ):
        assert column in coverage.columns


def test_coverage_carries_the_date_range():
    coverage = build_coverage(build_master([_season(2026, 4, 4)]))
    row = coverage.to_dicts()[0]
    assert row["first_date"] <= row["last_date"]


def test_coverage_refuses_a_frame_with_no_season_keys():
    with pytest.raises(ValueError, match="neither season"):
        build_coverage(pl.DataFrame({"game_id": [1]}))


# --- no module may hardcode a league -----------------------------------------


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """Ids of every Constant node that is a docstring rather than a value.

    Scanning source text cannot distinguish the two: the engine's docstrings
    legitimately *name* the leagues while documenting the ``league=`` argument,
    and a line-based check flags that prose as a hardcoded literal.
    """
    ids: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
            if isinstance(first.value.value, str):
                ids.add(id(first.value))
    return ids


def test_no_engine_module_hardcodes_a_league_literal():
    """The failure this guards is silent: a well-formed capture written under
    the wrong league's tree. Only league_config.py may name a league.

    Checked on the AST, and only on non-docstring string constants -- comments
    and documentation are free to name leagues, executable code is not.
    """
    keys = {"nba", "mbb", "wnba", "wbb"}
    engine = Path(lc.__file__).parent
    offenders = []
    for module in sorted(engine.glob("*.py")):
        if module.name in ("league_config.py", "__init__.py"):
            continue
        tree = ast.parse(module.read_text(encoding="utf-8"))
        docstrings = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or id(node) in docstrings:
                continue
            if isinstance(node.value, str) and node.value in keys:
                offenders.append(f"{module.name}:{node.lineno}: {node.value!r}")
    assert offenders == [], offenders
