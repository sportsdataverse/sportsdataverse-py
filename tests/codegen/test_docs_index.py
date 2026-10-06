import hashlib
import io
import json
import sqlite3
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from sdv_docs.schema import ASSET, MANIFEST
from tools.codegen import build_docs_index as B
from tools.codegen import spec


def test_doc_anchors_maps_function_headings(tmp_path):
    root = tmp_path / "docs"
    (root / "nhl/reference/loaders").mkdir(parents=True)
    (root / "bchl").mkdir()
    (root / "nhl/reference/loaders/pbp.md").write_text(
        "# NHL\n\n## load_nhl_pbp\n\n### Returns {#load_nhl_pbp-returns}\n"
    )
    (root / "bchl/index.md").write_text("### bchl_pbp {#bchl_pbp}\n")
    (root / "index.md").write_text("## intro_fn\n")
    a = B.doc_anchors(root)
    assert a["load_nhl_pbp"] == "https://py.sportsdataverse.org/docs/nhl/reference/loaders/pbp#load_nhl_pbp"
    assert a["bchl_pbp"] == "https://py.sportsdataverse.org/docs/bchl#bchl_pbp"
    assert a["intro_fn"] == "https://py.sportsdataverse.org/docs/#intro_fn"
    assert "Returns" not in a


def test_doc_anchors_on_the_committed_tree_finds_split_loader_pages():
    assert B.doc_anchors()["load_nhl_pbp"].endswith("/docs/nhl/reference/loaders/pbp#load_nhl_pbp")


def test_header_comments_keep_the_espn_cdn_probe_matrix():
    notes = B.header_comments(B.G.ENDPOINTS / "espn_cdn.yaml")
    assert "include_prefixes" in notes and "playbyplay" in notes
    assert not notes.startswith("api:")


def test_endpoint_row_serializes_params():
    p = spec.Param(python_name="team_id", api="team", required=True, description="ESPN team id.")
    ep = SimpleNamespace(summary="Team roster.", path_params=[p], query_params=[])
    row = B.endpoint_row(
        "espn_site_v2", "https://x/{league}/teams/{team_id}/roster", ep, "espn_nba_team_roster", "", "https://spec"
    )
    assert row[:4] == ("espn_site_v2", "GET", "https://x/{league}/teams/{team_id}/roster", "Team roster.")
    assert json.loads(row[4]) == [
        {"name": "team_id", "api": "team", "type": "str", "required": True, "description": "ESPN team id."}
    ]
    assert row[5:] == ("espn_nba_team_roster", "", "codegen", "https://spec")


@pytest.mark.xdist_group("docs_index_build")
def test_codegen_wrappers_cover_espn_and_flat():
    rows = B.Rows()
    w = B.codegen_wrappers("abc1234", rows)
    assert (w["espn_nba_team_roster"].kind, w["espn_nba_team_roster"].league) == ("espn", "nba")
    assert (w["wnba_stats_shotchartdetail"].kind, w["wnba_stats_shotchartdetail"].league) == ("flat", "wnba")
    assert sum(1 for x in w.values() if x.kind == "espn") > 3000
    cdn = [r for r in rows.endpoints if r[0] == "espn_cdn"]
    assert cdn and "playbyplay" in cdn[0][6]
    assert (
        cdn[0][8]
        == "https://github.com/sportsdataverse/sportsdataverse-py/blob/abc1234/tools/codegen/endpoints/espn_cdn.yaml"
    )
    roster = [r for r in rows.endpoints if r[0] == "espn_site_v2" and "espn_nba_team_roster" in r[5].split()]
    assert len(roster) == 1 and roster[0][2].startswith("https://site.api.espn.com/")
    shot = [r for r in rows.endpoints if r[0] == "wnba_stats" and r[5] == "wnba_stats_shotchartdetail"]
    assert len(shot) == 1 and shot[0][2].startswith("https://stats.wnba.com")


FIX = Path(__file__).parent / "fixtures" / "docs_index"


def _tarball(files: dict) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(f"sdv-swagger-abc123/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def test_parse_swagger_tarball_keeps_top_level_specs_only():
    mini = (FIX / "mini.openapi.yaml").read_bytes()
    specs = B.parse_swagger_tarball(
        _tarball(
            {
                "mini.openapi.yaml": mini,
                "README.md": b"# x",
                "notes.yaml": b"a: 1",
                "sub/other.openapi.yaml": mini,
                "broken.yaml": b"a: [1",
                "spec.json": b'{"swagger": "2.0", "paths": {"/x": {}}}',
            }
        )
    )
    assert sorted(specs) == ["mini.openapi.yaml", "spec.json"]


def test_openapi_rows_resolve_refs_and_servers():
    rows = B.Rows()
    B.openapi_rows({"mini.openapi.yaml": yaml.safe_load((FIX / "mini.openapi.yaml").read_text())}, "abc123", rows)
    ((api, method, path, summary, params_json, wrapper, notes, source, url),) = rows.endpoints
    assert (api, method, path, summary, wrapper, source) == (
        "Mini API",
        "GET",
        "https://api.example.com/v1/teams/{team_id}/roster",
        "Team roster",
        "",
        "openapi",
    )
    params = json.loads(params_json)
    assert [p["name"] for p in params] == ["team_id", "season"] and params[1]["required"] is True
    assert notes == "Returns the roster."
    assert url == "https://github.com/saiemgilani/sdv-swagger/blob/abc123/mini.openapi.yaml"


def test_parse_pkgdown_llms_reads_the_package_index_only():
    rows = B.Rows()
    n = B.parse_pkgdown_llms("hoopR", (FIX / "hoopr_llms_excerpt.txt").read_text(encoding="utf-8"), rows)
    assert n == 3
    assert rows.functions[0] == (
        "load_nba_pbp",
        "r",
        "hoopR",
        None,
        None,
        "function",
        "NBA Data Functions",
        "Load hoopR NBA play-by-play",
        None,
        "https://hoopR.sportsdataverse.org/reference/load_nba_pbp.md",
    )
    assert rows.functions[1][0] == "load_nba_team_box" and rows.functions[1][7] == "Load hoopR NBA play-by-play"
    assert rows.functions[2][7] == "Update or create a hoopR NBA play-by-play database"


@pytest.mark.xdist_group("docs_index_build")
def test_offline_build_end_to_end(tmp_path):
    db = B.build(tmp_path, offline=True)
    assert db == tmp_path / ASSET
    manifest = json.loads((tmp_path / MANIFEST).read_text())
    assert manifest["sha256"] == hashlib.sha256(db.read_bytes()).hexdigest() and manifest["size"] == db.stat().st_size
    con = sqlite3.connect(db)

    def one(sql, *args):
        return con.execute(sql, args).fetchone()

    assert one("SELECT kind, league, doc_url FROM functions WHERE name='load_nhl_pbp' AND lang='python'") == (
        "loader",
        "nhl",
        "https://py.sportsdataverse.org/docs/nhl/reference/loaders/pbp#load_nhl_pbp",
    )
    assert one("SELECT type FROM columns WHERE function='load_nhl_pbp' AND name='event_type'") == ("String",)
    assert one("SELECT count(*) FROM columns WHERE function='load_cfb_drives' AND name='drive_id'") == (1,)
    assert one("SELECT tag, min_season FROM datasets WHERE loader='load_nhl_shifts'") == ("nhl_shifts", 2025)
    assert one(
        "SELECT r_function, match FROM equivalents WHERE py_function='load_nfl_pbp' AND r_package='nflreadr'"
    ) == ("load_pbp", "alias")
    assert one("SELECT match FROM equivalents WHERE py_function='load_nba_pbp' AND r_package='hoopR'") == ("identity",)
    assert one("SELECT kind, league FROM functions WHERE name='espn_nba_team_roster'") == ("espn", "nba")
    assert one("SELECT kind, league FROM functions WHERE name='wnba_stats_shotchartdetail'") == ("flat", "wnba")
    assert one("SELECT count(*) FROM params WHERE function='load_nhl_pbp' AND name='seasons'") == (1,)
    assert "--offline" in one("SELECT value FROM meta WHERE key='skipped'")[0]
    counts = {
        t: one(f"SELECT count(*) FROM {t}")[0]
        for t in ("functions", "columns", "endpoints", "datasets", "equivalents", "search")
    }
    assert counts["functions"] > 3000 and counts["columns"] > 10000 and counts["endpoints"] > 300
    assert counts["datasets"] >= 300 and counts["equivalents"] > 100 and counts["search"] > counts["columns"]
    assert one("SELECT count(*) FROM functions WHERE lang='r'") == (0,)


def test_parse_pkgdown_llms_reads_inline_colon_titles():
    # pkgdown's other layout (r.sportsdataverse.org, 2026-10-05): the title follows the colon on the same line.
    text = (
        "# sportsdataverse\n\n# Package index\n\n## All functions\n\n"
        "- [`sportsdataverse_update()`](https://r.sportsdataverse.org/reference/sportsdataverse_update.md)\n"
        "  : Update sportsdataverse packages\n"
        "- [`sportsdataverse_logo()`](https://r.sportsdataverse.org/reference/sportsdataverse_logo.md)\n"
        "  : The sportsdataverse logo, using **ASCII** or Unicode characters\n"
    )
    rows = B.Rows()
    assert B.parse_pkgdown_llms("sportsdataverse", text, rows) == 2
    assert [(f[0], f[6], f[7]) for f in rows.functions] == [
        ("sportsdataverse_update", "All functions", "Update sportsdataverse packages"),
        ("sportsdataverse_logo", "All functions", "The sportsdataverse logo, using **ASCII** or Unicode characters"),
    ]


def _meta_rows():
    return B.Rows(meta={"schema_version": "1", "built_at": "2026-01-01T00:00:00Z", "sdv_py_commit": "abc"})


def test_write_failure_leaves_no_manifest_or_tmp(tmp_path):
    (tmp_path / MANIFEST).write_text("{}")  # a previous release's manifest
    rows = _meta_rows()
    rows.functions.append(("too", "few"))  # wrong tuple width -> the insert raises
    with pytest.raises(sqlite3.Error):
        B.write(rows, tmp_path)
    assert not (tmp_path / MANIFEST).exists() and not list(tmp_path.glob("*.tmp"))


def test_write_over_previous_release_refreshes_manifest(tmp_path):
    B.write(_meta_rows(), tmp_path)
    rows = _meta_rows()
    rows.meta["extra"] = "x" * 50_000  # make the second DB differ in size and content
    db = B.write(rows, tmp_path)
    manifest = json.loads((tmp_path / MANIFEST).read_text())
    assert manifest["sha256"] == hashlib.sha256(db.read_bytes()).hexdigest() and manifest["size"] == db.stat().st_size
    assert not list(tmp_path.glob("*.tmp"))


def _stub_offline_parts(monkeypatch):
    for name in ("codegen_wrappers", "python_functions"):
        monkeypatch.setattr(B, name, lambda *a, **k: {})
    monkeypatch.setattr(B, "doc_anchors", lambda: {})
    monkeypatch.setattr(B, "python_columns", lambda *a, **k: None)


def test_online_build_adds_swagger_and_pkgdown_rows(tmp_path, monkeypatch):
    _stub_offline_parts(monkeypatch)
    monkeypatch.setattr(B, "swagger_sha", lambda: "deadbeef")
    monkeypatch.setattr(B, "fetch_swagger", lambda sha: {})
    monkeypatch.setattr(B, "R_LLMS", {"hoopR": "https://example.test/llms.txt"})
    llms = "# hoopR\n\n# Package index\n\n## Loaders\n\n- [`load_x()`](https://example.test/reference/load_x.md)\n  : Load x\n"
    monkeypatch.setattr(B, "http_get", lambda url, *a, **k: llms.encode())
    db = B.build(tmp_path, offline=False)
    con = sqlite3.connect(db)
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert meta["sdv_swagger_sha"] == "deadbeef" and "pkgdown_fetched" in meta and "skipped" not in meta
    assert con.execute("SELECT summary FROM functions WHERE lang='r' AND name='load_x'").fetchone() == ("Load x",)


def test_online_build_raises_when_llms_parses_to_nothing(tmp_path, monkeypatch):
    _stub_offline_parts(monkeypatch)
    monkeypatch.setattr(B, "swagger_sha", lambda: "deadbeef")
    monkeypatch.setattr(B, "fetch_swagger", lambda sha: {})
    monkeypatch.setattr(B, "R_LLMS", {"hoopR": "https://example.test/llms.txt"})
    monkeypatch.setattr(B, "http_get", lambda url, *a, **k: b"# nothing here\n")
    with pytest.raises(RuntimeError, match="https://example.test/llms.txt"):
        B.build(tmp_path, offline=False)
    assert not (tmp_path / MANIFEST).exists()


def test_main_writes_index_and_prints_summary(tmp_path, monkeypatch, capsys):
    seen = {}

    def fake_build(out, offline=False):
        seen["offline"] = offline
        return B.write(_meta_rows(), out)

    monkeypatch.setattr(B, "build", fake_build)
    assert B.main(["--out", str(tmp_path), "--offline"]) == 0
    assert seen["offline"] is True and (tmp_path / MANIFEST).exists()
    assert capsys.readouterr().out.startswith(f"wrote {tmp_path / ASSET}")
