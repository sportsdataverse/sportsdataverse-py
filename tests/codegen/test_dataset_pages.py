"""releases.yaml ``dataset_page``: each loader with one links its producer's dataset page, and the hand-written
basketball loaders carry the ``Source:`` release-tag line the generated ones get."""

from __future__ import annotations

import importlib
import inspect
import re

import pytest

from tools.codegen import generate, spec

_LOADERS = spec.load_releases(generate.ENDPOINTS / "releases.yaml").loaders
_PAGED = [ld for ld in _LOADERS if ld.dataset_page]
_PRODUCERS = {"mbb": "hoopR-mbb-data", "nba": "hoopR-nba-data", "wbb": "wehoop-wbb-data", "wnba": "wehoop-wnba-data"}
_TAG = "https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/"


def test_every_espn_basketball_producer_page_is_linked() -> None:
    assert len(_PAGED) == 62
    for ld in _PAGED:
        repo, dataset = ld.dataset_page.split("/")
        assert repo == _PRODUCERS[ld.league], ld.fn
        assert re.fullmatch(r"[a-z_]+", dataset), ld.fn


@pytest.mark.parametrize("ld", _PAGED, ids=lambda ld: ld.fn)
def test_the_docstring_and_loaders_page_link_the_dataset_page(ld: spec.Loader) -> None:
    repo, dataset = ld.dataset_page.split("/")
    url = f"https://github.com/sportsdataverse/{repo}/blob/main/docs/datasets/{dataset}.md"
    doc = inspect.getdoc(getattr(importlib.import_module(f"sportsdataverse.{ld.league}"), ld.fn))
    assert f"Dataset page: {url}" in doc
    assert f"[dataset page]({url})" in generate.render_loaders_page(ld.league)


@pytest.mark.parametrize("module", ["sportsdataverse.nba.nba_loaders_extra", "sportsdataverse.wnba.wnba_loaders_extra"])
def test_hand_written_basketball_loaders_name_their_release_tag(module: str) -> None:
    mod = importlib.import_module(module)
    for name in mod.__all__:
        obj = getattr(mod, name)
        if not (name.startswith("load_") and callable(obj)):
            continue
        assert re.search(rf"^Source: {re.escape(_TAG)}\w+$", inspect.getdoc(obj), re.M), name
