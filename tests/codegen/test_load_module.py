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


def test_generated_single_asset_loader_has_no_seasons_and_is_404_safe(tmp_path):
    """A url with no ``{season}`` token is one asset for the whole dataset (e.g. a
    career table): the loader takes no ``seasons``, reads once, and an absent asset
    is an empty frame plus a warning -- the same 404-safe contract, minus the loop."""
    import inspect
    import warnings

    y = tmp_path / "releases.yaml"
    y.write_text(
        "bases:\n  sdv_releases: 'https://x/'\n"
        "loaders:\n  - fn: load_cfb_coach_careers\n    base: sdv_releases\n"
        "    url: 'espn_cfb_coach_careers/coach_careers.parquet'\n"
        "    tag: espn_cfb_coach_careers\n    league: cfb\n",
        encoding="utf-8",
    )
    rel = spec.load_releases(y)
    src = generate.render_loader_module("cfb", rel.loaders, rel.bases)
    ns: dict = {}
    exec(compile(src, "gen_cfb_loaders", "exec"), ns)  # noqa: S102
    fn = ns["load_cfb_coach_careers"]
    assert list(inspect.signature(fn).parameters) == ["return_as_pandas"]
    # the Args block documents no seasons argument (the returns table may still list a
    # `seasons` COLUMN -- a career row counts them)
    assert "load_cfb_coach_careers()" in fn.__doc__ and "seasons: an int" not in fn.__doc__

    seen: list = []

    def fake_read(url):
        seen.append(url)
        return pl.DataFrame({"coach": ["x"]})

    ns["_read_release_parquet"] = fake_read
    assert fn().shape == (1, 1)
    assert seen == ["https://x/espn_cfb_coach_careers/coach_careers.parquet"]

    ns["_read_release_parquet"] = lambda url: None
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        out = fn()
    assert out.shape == (0, 0) and any("no published asset" in str(x.message) for x in w)


def test_seasonal_loader_docstring_still_documents_seasons(tmp_path):
    """Guard the other side of the single-asset branch: a tokened url keeps the
    ``seasons`` argument, its floor, and the ``fn(seasons=...)`` example."""
    from tools.codegen import spec as _spec

    ld = _spec.Loader(
        fn="load_cfb_team_tendencies",
        league="cfb",
        base="sdv_releases",
        url="espn_cfb_team_tendencies/team_tendencies_{season}.parquet",
        tag="espn_cfb_team_tendencies",
        min_season=2004,
    )
    doc = generate._build_loader_docstring(ld)
    assert "seasons: an int or iterable of seasons (>= 2004)." in doc
    assert "SeasonNotFoundError" in doc and "load_cfb_team_tendencies(seasons=2024)" in doc
    single = _spec.Loader(
        fn="load_cfb_coach_careers",
        league="cfb",
        base="sdv_releases",
        url="espn_cfb_coach_careers/coach_careers.parquet",
        tag="espn_cfb_coach_careers",
    )
    doc = generate._build_loader_docstring(single)
    assert "seasons: an int" not in doc and "Raises:" not in doc and "load_cfb_coach_careers()" in doc


def test_loaders_page_example_call_follows_the_season_token():
    template = generate.render.ENV.get_template("loaders_page.md.jinja")
    base = {
        "notes": "",
        "tag": "t",
        "tag_url": "",
        "url": "",
        "automation": {"repo": "", "workflow": ""},
        "return_table": "",
        "example_seasons": 2024,
    }
    page = template.render(
        prefix="cfb",
        sidebar_position=1,
        loaders=[
            {**base, "fn": "load_cfb_coach_careers", "single": True},
            {**base, "fn": "load_cfb_team_tendencies", "single": False},
            {**base, "fn": "load_cfb_legacy"},  # hand-built dicts without the key still render
        ],
    )
    assert "load_cfb_coach_careers()" in page
    assert "load_cfb_team_tendencies(seasons=2024)" in page
    assert "load_cfb_legacy(seasons=2024)" in page
