import io
import json
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

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
