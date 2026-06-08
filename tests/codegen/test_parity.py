"""Inventory parity: generated nba functions reproduce the factory's real param surface.

The *bound* factory wrappers (``sportsdataverse.nba.espn_nba_*``) are opaque
``(*args, return_parsed, return_as_pandas, **kwargs)`` closures for parser-registered
endpoints -- the real parameter names live only on the underlying ``_common_espn`` core
functions. (Exposing those names concretely is the whole point of this redesign.) So
parity is checked against the core functions: the generated signature must list exactly
the core fn's params, minus the ``sport``/``league`` slugs the generator fixes.
"""

import importlib.util
import inspect
from pathlib import Path

import pytest

from sportsdataverse import _common_espn as ce

# Pre-retirement migration gate (see test_parity_full): the factory core fns it
# compares against have been retired. Skips cleanly unless they're present.
if not hasattr(ce, "_site_v2_scoreboard"):
    pytest.skip("ESPN factory retired; parity is a pre-retirement migration gate", allow_module_level=True)

from tools.codegen import generate  # noqa: E402

OUT = Path("tools/codegen/_generated")

# generated fn name -> the core fn it should reproduce
CORE = {
    "espn_nba_scoreboard": ce._site_v2_scoreboard,
    "espn_nba_teams_site": ce._site_v2_teams,
    "espn_nba_standings": ce._site_v2_alt_standings,
}


def _load(mod_path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, mod_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _named_params(fn, drop=()):
    return [
        n
        for n, p in inspect.signature(fn).parameters.items()
        if n not in drop and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    ]


def test_generated_nba_params_match_core_functions():
    generate.build()
    gen = _load(OUT / "nba_espn_ext.py", "_gen_parity")
    for gen_name, core_fn in CORE.items():
        assert hasattr(gen, gen_name), f"generated missing {gen_name}"
        core_params = _named_params(core_fn, drop=("sport", "league"))
        # the generated fn adds the return_parsed/return_as_pandas shim kwargs; drop them
        # so we compare the real API param surface.
        gen_params = _named_params(getattr(gen, gen_name), drop=("return_parsed", "return_as_pandas"))
        assert gen_params == core_params, f"{gen_name}: {gen_params} != core {core_params}"
