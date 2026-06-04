"""Faithful-extraction parity: generated NHL/MLB flat modules reproduce the
hand-written functions' exact request (URL + non-None params), for minimal
(required-only) and maximal (all-args) calls.

This is a *readiness gate* proving the extract-from-hand-written regeneration is
behavior-preserving for the regular majority (152/175 functions). A small set of
genuinely-irregular functions are documented in ``_IRREGULAR`` and excluded -- they
use shapes the single-URL-builder codegen can't represent faithfully (3-way
mutually-exclusive branches, a record value embedded mid-segment, MLB conditional
``_csv`` query inclusion). Those stay hand-written until the codegen grows support
or they are curated by hand; the gate guards everything else.
"""

# Functions whose hand-written logic the URL-builder codegen cannot reproduce
# faithfully (kept hand-written; see module docstring).
_IRREGULAR = {
    "sportsdataverse.nhl.nhl_api_web": {"nhl_web_scoreboard"},  # team/date/now 3-way branch
    "sportsdataverse.nhl.nhl_records": {
        "nhl_records_coach_milestone_wins",  # value embedded mid-segment (fewest-games-to-{n}-wins)
        "nhl_records_consecutive_goal_seasons",
        "nhl_records_games_played_streak_skaters",
        "nhl_records_fastest_goals",
        "nhl_records_fastest_goals_both_teams",
        "nhl_records_comeback_wins",  # scope-conditional path
    },
    "sportsdataverse.mlb.mlb_api": {  # conditional _csv query inclusion / multi-param shaping
        "mlb_api_schedule",
        "mlb_api_pbp_live",
        "mlb_api_pbp",
        "mlb_api_pbp_diff",
        "mlb_api_teams",
        "mlb_api_team_stats",
        "mlb_api_team_leaders",
        "mlb_api_person_stats",
        "mlb_api_standings",
        "mlb_api_stats",
        "mlb_api_stats_leaders",
        "mlb_api_stats_streaks",
        "mlb_api_divisions",
        "mlb_api_seasons",
        "mlb_api_draft_prospects",
        "mlb_api_attendance",
    },
}

import importlib
import importlib.util
import inspect
import tempfile
from pathlib import Path

import pytest
import yaml

from tools.codegen import extract_native as en
from tools.codegen import generate, spec

# (dotted module, fn prefix, base host, name_pattern, parser_module)
_MODULES = [
    (
        "sportsdataverse.nhl.nhl_api_web",
        "nhl_web_",
        "https://api-web.nhle.com",
        "nhl_web_{short}",
        "nhl.nhl_api_web_parsers",
    ),
    (
        "sportsdataverse.nhl.nhl_edge",
        "nhl_edge_",
        "https://api-web.nhle.com",
        "nhl_edge_{short}",
        "nhl.nhl_edge_parsers",
    ),
    (
        "sportsdataverse.nhl.nhl_stats_rest",
        "nhl_stats_rest_",
        "https://api.nhle.com/stats/rest",
        "nhl_stats_rest_{short}",
        "nhl.nhl_stats_rest_parsers",
    ),
    (
        "sportsdataverse.nhl.nhl_records",
        "nhl_records_",
        "https://records.nhl.com/site/api",
        "nhl_records_{short}",
        "nhl.nhl_records_parsers",
    ),
    ("sportsdataverse.mlb.mlb_api", "mlb_api_", "https://statsapi.mlb.com", "mlb_api_{short}", "mlb.mlb_api_parsers"),
]


def _gen_module(dotted, prefix, base, name_pattern, parser_module):
    eps = en.extract_module(dotted, prefix, base, name_pattern)
    doc = {
        "api": dotted.rsplit(".", 1)[-1],
        "host": base,
        "name_pattern": name_pattern,
        "module": dotted.rsplit(".", 1)[-1],
        "parser_module": parser_module,
        "runtime_imports": ["_get", "format_nhl_season"],
        "endpoints": eps,
    }
    yp = Path(tempfile.mktemp(suffix=".yaml"))
    yp.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    api = spec.load_flat_api(yp, {})
    src = generate.render_flat_module(api)
    gp = Path(tempfile.mktemp(suffix=".py"))
    gp.write_text(src, encoding="utf-8")
    s = importlib.util.spec_from_file_location(gp.stem, gp)
    mod = importlib.util.module_from_spec(s)
    s.loader.exec_module(mod)
    return mod, {e["short"] for e in eps}


def _cap(call, sink_owner, sink_name):
    box = {}

    def fake(url=None, params=None, **kw):
        box["url"] = url
        box["params"] = {k: v for k, v in (params or {}).items() if v is not None}
        return None

    orig = getattr(sink_owner, sink_name)
    setattr(sink_owner, sink_name, fake)
    try:
        call()
    finally:
        setattr(sink_owner, sink_name, orig)
    return box


@pytest.mark.parametrize("dotted,prefix,base,name_pattern,parser_module", _MODULES)
def test_native_generated_matches_handwritten(dotted, prefix, base, name_pattern, parser_module):
    import sportsdataverse._codegen_runtime as rt

    hw = importlib.import_module(dotted)
    gen, shorts = _gen_module(dotted, prefix, base, name_pattern, parser_module)

    irregular = _IRREGULAR.get(dotted, set())
    mismatches = []
    for name, hwfn in vars(hw).items():
        if not name.startswith(prefix) or not callable(hwfn) or name.startswith("_"):
            continue
        if name in irregular:
            continue
        gfn = getattr(gen, name, None)
        if gfn is None:
            mismatches.append(f"{name}: missing in generated")
            continue
        sigp = [
            p for p in inspect.signature(hwfn).parameters.values() if p.name != "kwargs" and p.kind != p.VAR_KEYWORD
        ]
        for maximal in (False, True):
            args = {
                p.name: (en._val(p.name) if (maximal or p.default is inspect.Parameter.empty) else p.default)
                for p in sigp
                if (maximal or p.default is inspect.Parameter.empty)
            }
            try:
                want = _cap(lambda: hwfn(**args), hw, "download")
                got = _cap(lambda: gfn(**args), rt, "download")
            except Exception as e:  # noqa: BLE001
                mismatches.append(f"{name} (max={maximal}) raised: {e}")
                continue
            if got.get("url") != want.get("url"):
                mismatches.append(f"{name} (max={maximal}) URL:\n   hw:  {want.get('url')}\n   gen: {got.get('url')}")
            elif got.get("params") != want.get("params"):
                mismatches.append(
                    f"{name} (max={maximal}) PARAMS: hw={want.get('params')} gen={got.get('params')}",
                )
    assert not mismatches, f"{len(mismatches)} parity mismatch(es) in {dotted}:\n" + "\n".join(mismatches[:30])
