"""League-parameterization tests for the NCAA hoops sweep engine.

The behavioral suites (fixtures, parsers, capture flow) stay in the two -raw
repos and run against this engine — they are the parity harness. What is pinned
here is the seam the extraction exists to protect: a league is always passed
in, never assumed.
"""

import inspect
import pathlib

import pytest

from sportsdataverse.scrape.ncaa import (
    capture,
    datasets,
    discover,
    espn_game_xwalk,
    identity,
    league_config,
    parse,
    rosters,
)

ENGINE_DIR = pathlib.Path(capture.__file__).parent


def test_configs_resolve_and_carry_the_period_split() -> None:
    assert league_config.by_league("mbb") is league_config.MBB
    assert league_config.by_league("wbb") is league_config.WBB
    # men's halves vs women's quarters -- the one rule that is genuinely
    # different rather than merely a different token.
    assert league_config.MBB.periods == 2
    assert league_config.WBB.periods == 4
    with pytest.raises(KeyError):
        league_config.by_league("nba")


@pytest.mark.parametrize(
    ("fn", "name"),
    [
        (capture.capture_contests, "capture_contests"),
        (discover.discover_season, "discover_season"),
        (rosters.capture_rosters, "capture_rosters"),
        (parse.parse_and_write, "parse_and_write"),
        (datasets.build_teams, "build_teams"),
        (identity.enrich_parsed, "enrich_parsed"),
    ],
)
def test_league_is_a_required_keyword(fn, name: str) -> None:
    """No public entry point may default its league.

    A shared engine that defaults to one league is how a women's run silently
    reads men's data. Before this extraction the MBB repo's capture CLI
    hardcoded both the schedule-master path and the capture league to "mbb",
    so it could not be pointed at the other league at all.
    """
    param = inspect.signature(fn).parameters.get("league")
    assert param is not None, f"{name} takes no league"
    assert param.default is inspect.Parameter.empty, f"{name} defaults its league"
    assert param.kind is inspect.Parameter.KEYWORD_ONLY, f"{name} league must be keyword-only"


def test_no_module_hardcodes_a_league_literal() -> None:
    """Source-level guard for the bug class the extraction removed.

    Cheap to check, and the failure it prevents (a league literal creeping back
    into a path or a call site) is silent at runtime: it produces a well-formed
    capture pointed at the wrong league's tree.
    """
    offenders = []
    for path in sorted(ENGINE_DIR.glob("*.py")):
        if path.name == "league_config.py":
            continue  # the one file whose job IS declaring the league tokens
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or '"""' in stripped:
                continue
            for bad in ('league="mbb"', 'league="wbb"', '/ "mbb" /', '/ "wbb" /'):
                if bad in stripped:
                    offenders.append(f"{path.name}:{i}: {stripped[:80]}")
    assert not offenders, "league literals must come from the caller:\n" + "\n".join(offenders)


def test_cli_entry_points_take_their_binding_league() -> None:
    """Each repo shim passes its league into the CLI; none may assume one."""
    for mod in (capture, discover, parse, rosters, datasets, espn_game_xwalk):
        main = getattr(mod, "_main")
        param = inspect.signature(main).parameters.get("default_league")
        assert param is not None, f"{mod.__name__}._main takes no default_league"
        assert param.default is inspect.Parameter.empty, f"{mod.__name__}._main defaults its league"


def test_parse_stage_can_target_a_season_and_reprocess() -> None:
    """The parse stage must be re-runnable, not just resumable.

    Skip-if-exists alone means a parser fix or a later identity backfill can
    never reach already-parsed games — which is exactly how the MBB tree ended
    up with null player_ids/clean_names for 2024-2026 while the pipeline had
    long since learned to fill them.
    """
    import argparse
    from unittest import mock

    captured: dict = {}

    def fake_parse_args(self, *a, **k):
        captured["opts"] = {act.dest: act for act in self._actions}
        raise SystemExit(0)

    with mock.patch.object(argparse.ArgumentParser, "parse_args", fake_parse_args):
        with pytest.raises(SystemExit):
            parse._main("mbb", "/tmp/root-not-used-by-this-test")

    opts = captured["opts"]
    assert "season" in opts, "parse stage cannot target a season"
    assert opts["season"].__class__.__name__ == "_AppendAction", "--season must be repeatable"
    assert "force" in opts, "parse stage cannot reprocess existing output"
    assert opts["force"].const is True, "--force must be a flag"


def test_no_module_infers_a_repo_root_from_its_own_location() -> None:
    """The engine lives in sdv-py; a -raw repo's root is NOT derivable from it.

    The lifted modules originally defaulted `--root` to
    ``Path(__file__).resolve().parents[1]`` -- correct when they lived in
    ``<repo>/python/``, and silently wrong once they moved here: every stage
    CLI pointed at ``sportsdataverse/scrape/`` and reported success having
    done nothing (`bundles=0`, `EXIT=0`). The caller supplies the root.
    """
    offenders = []
    for path in sorted(ENGINE_DIR.glob("*.py")):
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "parents[1]" in line and not line.lstrip().startswith(("#", '"')):
                offenders.append(f"{path.name}:{i}: {line.strip()[:80]}")
    assert not offenders, "root must come from the caller:\n" + "\n".join(offenders)


def test_cli_entry_points_require_a_root() -> None:
    for mod in (capture, discover, parse, rosters, datasets, espn_game_xwalk):
        param = inspect.signature(mod._main).parameters.get("default_root")
        assert param is not None, f"{mod.__name__}._main takes no default_root"
        assert param.default is inspect.Parameter.empty, f"{mod.__name__}._main defaults its root"
