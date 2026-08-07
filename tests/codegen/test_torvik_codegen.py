"""Generator-level checks for the Bart Torvik flat-API families.

Per-league parser/wrapper behaviour lives in ``tests/mbb/test_mbb_torvik.py`` and
``tests/wbb/test_wbb_bart_wbb.py``; this module only asserts what the *generator*
is responsible for: the wrappers exist under their canonical names and the
``raw_types`` declaration in the endpoint YAML reaches the emitted annotation
(barttorvik.com serves CSV, so the raw payload is ``str``, not a JSON ``Dict``).
"""

from __future__ import annotations

import inspect


def test_generated_wrappers_importable():
    from sportsdataverse.mbb import parse_torvik_csv, torvik_ratings, torvik_team_factors
    from sportsdataverse.wbb import bart_wbb_ratings

    assert all(callable(f) for f in (torvik_ratings, torvik_team_factors, bart_wbb_ratings, parse_torvik_csv))


def test_raw_type_union_includes_str():
    """``raw_types: [Dict, str]`` in the YAML must reach the generated signature."""
    from sportsdataverse.mbb import torvik_ratings, torvik_team_factors
    from sportsdataverse.wbb import bart_wbb_ratings

    for fn in (torvik_ratings, torvik_team_factors, bart_wbb_ratings):
        ret = inspect.signature(fn).return_annotation
        assert ret == "Union[pl.DataFrame, pd.DataFrame, Dict, str]", (fn.__name__, ret)


def test_docstrings_carry_the_full_contract():
    from sportsdataverse.mbb import torvik_ratings, torvik_team_factors
    from sportsdataverse.wbb import bart_wbb_ratings

    for fn in (torvik_ratings, torvik_team_factors, bart_wbb_ratings):
        doc = inspect.getdoc(fn) or ""
        for section in ("Args:", "Returns:", "Raises:", "Example:", "See Also:"):
            assert section in doc, (fn.__name__, section)
        # the Example must be runnable from a clean session
        assert f"import {fn.__name__}" in doc
        # and the raw contract must not promise JSON
        assert "raw CSV response body" in doc
