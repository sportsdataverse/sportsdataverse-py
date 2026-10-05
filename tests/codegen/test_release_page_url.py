"""Docs source links: raw_data-based loaders link their repo, not a nonexistent release tag."""

from tools.codegen import generate

BASES = {
    "sdv_releases": "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/",
    "raw_data": "https://raw.githubusercontent.com/sportsdataverse/",
}


def test_release_base_links_tag_page():
    url = generate._release_page_url(BASES, "sdv_releases", "espn_cfb_pbp/x.parquet", "espn_cfb_pbp")
    assert url == "https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_pbp"


def test_raw_data_base_links_source_repo():
    url = generate._release_page_url(BASES, "raw_data", "cfbfastR-data/main/rosters/x.parquet", "cfbfastR-data")
    assert url == "https://github.com/sportsdataverse/cfbfastR-data"
