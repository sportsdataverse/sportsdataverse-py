"""Loader spec + 404-safe load_module template."""

import ast

import polars as pl

from tools.codegen import generate, spec


def test_load_releases_and_render(tmp_path):
    y = tmp_path / "releases.yaml"
    y.write_text(
        "bases:\n  sdv_releases: 'https://github.com/sportsdataverse/sportsdataverse-data/releases/download/'\n"
        "loaders:\n"
        "  - fn: load_wnba_shots\n    base: sdv_releases\n    url: 'espn_wnba_shots/shot_locations_{season}.parquet'\n"
        "    tag: espn_wnba_shots\n    min_season: 2002\n    league: wnba\n    example_args: { seasons: 2024 }\n",
        encoding="utf-8",
    )
    rel = spec.load_releases(y)
    assert rel.bases["sdv_releases"].endswith("/download/")
    src = generate.render_loader_module("wnba", [ld for ld in rel.loaders if ld.league == "wnba"], rel.bases)
    ast.parse(src)
    assert "def load_wnba_shots(" in src
    assert "_read_release_parquet" in src
    assert "shot_locations_{season}.parquet" in src  # absolute URL inlined with {season}


def test_generated_loader_is_404_safe(tmp_path):
    y = tmp_path / "releases.yaml"
    y.write_text(
        "bases:\n  sdv_releases: 'https://x/'\n"
        "loaders:\n  - fn: load_wnba_shots\n    base: sdv_releases\n    url: 'espn_wnba_shots/s_{season}.parquet'\n"
        "    tag: espn_wnba_shots\n    min_season: 2002\n    league: wnba\n    example_args: { seasons: 2024 }\n",
        encoding="utf-8",
    )
    rel = spec.load_releases(y)
    src = generate.render_loader_module("wnba", rel.loaders, rel.bases)
    ns: dict = {}
    exec(compile(src, "gen_wnba_loaders", "exec"), ns)  # noqa: S102

    def fake_read(url):
        return pl.DataFrame({"x": [1]}) if "2023" in url else None  # 2024 -> missing

    ns["_read_release_parquet"] = fake_read  # rebind the loader's imported helper
    out = ns["load_wnba_shots"](seasons=[2023, 2024])
    assert out.shape[0] == 1  # 2024 skipped, not crashed


def test_generated_loader_min_season_guard(tmp_path):
    y = tmp_path / "releases.yaml"
    y.write_text(
        "bases:\n  sdv_releases: 'https://x/'\n"
        "loaders:\n  - fn: load_wnba_shots\n    base: sdv_releases\n    url: 's_{season}.parquet'\n"
        "    tag: espn_wnba_shots\n    min_season: 2002\n    league: wnba\n",
        encoding="utf-8",
    )
    rel = spec.load_releases(y)
    src = generate.render_loader_module("wnba", rel.loaders, rel.bases)
    ns: dict = {}
    exec(compile(src, "gen", "exec"), ns)  # noqa: S102
    try:
        ns["load_wnba_shots"](seasons=1999)
        raise AssertionError("expected SeasonNotFoundError")
    except Exception as e:  # noqa: BLE001
        assert "2002" in str(e)


def test_stub_loader_raises(tmp_path):
    y = tmp_path / "releases.yaml"
    y.write_text(
        "bases:\n  sdv_releases: 'https://x/'\n"
        "loaders:\n  - fn: load_mlb_pbp\n    base: sdv_releases\n    url: 'x_{season}.parquet'\n"
        "    tag: mlb_pbp\n    league: mlb\n    stub: true\n    stub_message: 'No release yet; use the live wrappers.'\n",
        encoding="utf-8",
    )
    rel = spec.load_releases(y)
    src = generate.render_loader_module("mlb", rel.loaders, rel.bases)
    ns: dict = {}
    exec(compile(src, "gen", "exec"), ns)  # noqa: S102
    try:
        ns["load_mlb_pbp"](seasons=2024)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError as e:
        assert "live wrappers" in str(e)
