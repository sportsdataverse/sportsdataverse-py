"""Gates for the football / MLB / NHL text renderers — canonical dialects.

Each league renders its OWN feed's formulaic text: NFL = GSIS GameCenter
(clock prefix, formations, tackler credits), CFB = ESPN college sentences,
MLB = statsapi result descriptions over the real lineups, NHL = RTSS
report lines with sweater numbers and fitted infraction/stoppage pools.
"""

from __future__ import annotations

import json
import pathlib
import re

import numpy as np
import pytest

# ------------------------------------------------------------------ football


@pytest.fixture(scope="module")
def nfl_summary() -> dict:
    return json.loads(pathlib.Path("tests/fixtures/espn/summary_nfl.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def cfb_summary() -> dict:
    return json.loads(pathlib.Path("tests/fixtures/espn/summary_cfb.json").read_text(encoding="utf-8"))


def test_football_names_fit_from_real_text(nfl_summary: dict, cfb_summary: dict) -> None:
    from sportsdataverse.nfl.nfl_drive_sim import football_names_from_espn

    nfl = football_names_from_espn(nfl_summary)
    # the real Super Bowl LIX participants, incl. fitted tackler pools
    assert nfl.home.abbr == "PHI" and nfl.home.passer == "J.Hurts"
    assert nfl.away.abbr == "KC" and nfl.away.passer == "P.Mahomes"
    assert any(n == "S.Barkley" for n, _ in nfl.home.rushers)
    assert any(n == "T.Kelce" for n, _ in nfl.away.receivers)
    assert nfl.home.tacklers and nfl.away.tacklers
    # the real CFP championship participants via the college full-name shapes
    cfb = football_names_from_espn(cfb_summary)
    assert cfb.home.passer == "Riley Leonard"
    assert cfb.away.passer == "Will Howard"
    assert any(n == "Quinshon Judkins" for n, _ in cfb.away.rushers)


_CLOCK = r"\(\d+:\d\d\) "
_SG = r"(\(Shotgun\) )?"
GSIS_TEXT = {
    "rush": re.compile(
        _CLOCK + _SG + r"\S+ (left tackle|up the middle|right end) to [A-Z]+ \d+ for -?\d+ yards \(.+\)\.$"
    ),
    "pass_complete": re.compile(
        _CLOCK + _SG + r"\S+ pass (short|deep) (left|middle|right) to .+ for -?\d+ yards \(.+\)\.$"
    ),
    "pass_incomplete": re.compile(
        _CLOCK + _SG + r"\S+ pass incomplete (short|deep) (left|middle|right) intended for \S+\.$"
    ),
    "sack": re.compile(_CLOCK + r"\S+ sacked at [A-Z]+ \d+ for -?\d+ yards \(.+\)\.$"),
    "punt": re.compile(_CLOCK + r"\S+ punts \d+ yards to [A-Z]+ \d+\.$"),
    "fg_good": re.compile(_CLOCK + r"\S+ \d+ yard field goal is GOOD\.$"),
    "fg_miss": re.compile(_CLOCK + r"\S+ \d+ yard field goal is No Good\.$"),
    "interception": re.compile(
        _CLOCK + _SG + r"\S+ pass (short|deep) (left|middle|right) intended for \S+ INTERCEPTED by .+ at [A-Z]+ \d+\.$"
    ),
    "touchdown": re.compile(r"^[A-Z]+ TOUCHDOWN \(kick (is good|failed)\)\.$"),
    "kickoff": re.compile(
        r"^\S+ kicks \d+ yards from [A-Z]+ 35 to "
        r"(end zone, Touchback to the [A-Z]+ \d+\.|[A-Z]+ \d+\. .+ to [A-Z]+ \d+ for \d+ yards \(.+\)\.)$"
    ),
    "end_period": re.compile(r"^END QUARTER [1-3]$"),
    "end_game": re.compile(r"^END GAME$"),
    "two_minute_warning": re.compile(r"^Two-Minute Warning$"),
    "penalty": re.compile(r"^PENALTY on [A-Z]+-.+, .+, \d+ yards, enforced at [A-Z]+ \d+ - No Play\.$"),
    "timeout_team": re.compile(r"^Timeout #[1-3] by [A-Z]+ at \d\d:\d\d\.$"),
    "timeout_official": re.compile(r"^Official Timeout at \d\d:\d\d\.$"),
}
COLLEGE_TEXT = {
    "rush": re.compile(r"^.+ run for (\d+ yds|a loss of \d+ yards?) to the [A-Z]+ \d+( for a 1ST down)?$"),
    "pass_complete": re.compile(r"^.+ pass complete to .+ for -?\d+ yds to the [A-Z]+ \d+( for a 1ST down)?$"),
    "pass_incomplete": re.compile(r"^.+ pass incomplete to .+$"),
    "sack": re.compile(r"^.+ sacked by .+ for a loss of \d+ yards to the [A-Z]+ \d+$"),
    "punt": re.compile(r"^.+ punt for \d+ yds$"),
    "fg_good": re.compile(r"^.+ \d+ yd FG GOOD$"),
    "fg_miss": re.compile(r"^.+ \d+ yd FG MISSED$"),
    "touchdown": re.compile(r"^[A-Z]+ TOUCHDOWN, kick (good|failed)$"),
    "kickoff": re.compile(r"^.+ kickoff for \d+ yds (for a touchback|, .+ return for \d+ yds to the [A-Z]+ \d+)$"),
    "end_period": re.compile(r"^End of (1st|2nd|3rd) Quarter$"),
    "end_game": re.compile(r"^End of 4th Quarter$"),
    "penalty": re.compile(r"^PENALTY [A-Z]+ .+ \(.+\) \d+ yards to the [A-Z]+ \d+, NO PLAY\.$"),
    "timeout_team": re.compile(r"^Timeout .+, clock \d\d:\d\d$"),
}
BOUNDARY_CLASSES = {"kickoff", "end_period", "end_game", "two_minute_warning", "timeout_team", "timeout_official"}


@pytest.mark.parametrize(
    "league, provider, formulas",
    [("nfl", "nfl_gsis", GSIS_TEXT), ("cfb", "espn", COLLEGE_TEXT)],
)
def test_football_rendered_text_formulas(league, provider, formulas, nfl_summary, cfb_summary) -> None:
    from sportsdataverse.nfl.nfl_drive_sim import (
        SNAP_CLASSES,
        build_football_shelf,
        football_names_from_espn,
        plays_from_espn_drives,
        render_football_pbp,
        simulate_football_game_pbp,
    )

    summary = nfl_summary if league == "nfl" else cfb_summary
    shelf = build_football_shelf(plays_from_espn_drives(summary))
    names = football_names_from_espn(summary)
    rng = np.random.default_rng(7)
    _, pbp = simulate_football_game_pbp(shelf, rng, college_ot=(league == "cfb"))
    rendered = render_football_pbp(pbp, names, rng, provider=provider)
    sim_rows = [r for r in rendered if r["play_class"] not in BOUNDARY_CLASSES]
    assert len(sim_rows) == len(pbp)
    assert rendered[0]["play_class"] == "kickoff"  # the opening kickoff
    assert rendered[-1]["play_class"] == "end_game"
    assert sum(1 for r in rendered if r["play_class"] == "kickoff") >= 3
    assert any(r["play_class"] == "end_period" for r in rendered)
    if league == "nfl":
        assert any(r["play_class"] == "two_minute_warning" for r in rendered)
        assert any(r["play_class"] == "timeout_official" for r in rendered)
    assert any(r["play_class"] == "penalty" for r in rendered)  # the fitted no-play node
    assert any(r["play_class"] == "timeout_team" for r in rendered)
    dd_re = re.compile(r"^(1st|2nd|3rd|4th) & \d+ at [A-Z]+ \d+$")
    for row in rendered:
        assert row["text"]
        pattern = formulas.get(row["play_class"])
        if pattern:
            assert pattern.match(row["text"]), (row["play_class"], row["text"])
        if row["play_class"] in SNAP_CLASSES:
            assert dd_re.match(row["down_distance_text"]), row["down_distance_text"]


# ----------------------------------------------------------------------- MLB


def test_mlb_renderer_rotates_real_lineups() -> None:
    from sportsdataverse.mlb.mlb_at_bat_sim import (
        at_bats_from_pbp,
        build_at_bat_pmf,
        lineups_from_pbp,
        render_mlb_game_pbp,
    )

    payload = json.loads(pathlib.Path("tests/fixtures/mlb_api/play_by_play_745282.json").read_text(encoding="utf-8"))
    lineups = lineups_from_pbp(payload)
    assert len(lineups["away"]) >= 9 and len(lineups["home"]) >= 9
    assert lineups["away"][0][1] == "Brendan Donovan"  # the real leadoff
    assert len(lineups["away_pitchers"]) >= 2 and len(lineups["home_pitchers"]) >= 2  # real arms
    pmf = build_at_bat_pmf(at_bats_from_pbp(payload))
    (away, home), rows = render_mlb_game_pbp(pmf, lineups, np.random.default_rng(7))
    (away2, home2), rows2 = render_mlb_game_pbp(pmf, lineups, np.random.default_rng(7))
    assert [r["text"] for r in rows] == [r["text"] for r in rows2]  # deterministic
    pa_rows = [r for r in rows if r["kind"] == "pa"]
    event_rows = [r for r in rows if r["kind"] == "event"]
    formula = re.compile(
        r"^.+ (strikes out swinging|called out on strikes|walks|singles on|doubles on|triples on|homers \(\d+\)|"
        r"grounds out|flies out|grounds into a double play|out on a sacrifice fly|"
        r"reaches on a fielding error|caught stealing).*\.$"
    )
    for row in pa_rows:
        assert row["batter_name"]
        assert formula.match(row["text"]), row["text"]
        # named runner sentences reconcile with the engine run ledger
        named = row["text"].count(" scores.")
        expected = int(row["runs_on_play"]) - (1 if row["outcome"] == "hr" else 0)
        assert named == expected, row
    event_formulas = {
        "game_advisory": re.compile(r"^Status Change - In Progress$"),
        "batter_timeout": re.compile(r"^Batter Timeout\.$"),
        "mound_visit": re.compile(r"^Mound Visit\.$"),
        "pitching_sub": re.compile(r"^Pitching Change: .+ replaces .+\.$"),
        "offensive_sub": re.compile(r"^Offensive Substitution: Pinch-hitter .+ replaces .+\.$"),
    }
    assert event_rows and event_rows[0]["event"] == "game_advisory"
    assert any(r["event"] == "pitching_sub" for r in event_rows)  # real bullpen order
    assert any(r["event"] == "offensive_sub" for r in event_rows)  # real bench enters late
    for row in event_rows:
        assert event_formulas[row["event"]].match(row["text"]), row
    # rotation replay: the starting nine cycles, and every offensive sub
    # replaces exactly the slot that was due up
    order9 = [name for _, name in lineups["away"][:9]]
    expected = []
    idx = 0
    for r in rows:
        if r["kind"] == "event" and r["event"] == "offensive_sub" and r["is_top"]:
            incoming, outgoing = re.match(
                r"^Offensive Substitution: Pinch-hitter (.+) replaces (.+)\.$", r["text"]
            ).groups()
            assert order9[idx % 9] == outgoing  # the due batter is the one replaced
            order9[idx % 9] = incoming
        elif r["kind"] == "pa" and r["is_top"] and r["outcome"] != "other_out":
            expected.append(order9[idx % 9])
            idx += 1
    away_rows = [r for r in pa_rows if r["is_top"] and r["outcome"] != "other_out"]
    assert [r["batter_name"] for r in away_rows] == expected


# ----------------------------------------------------------------------- NHL


def test_nhl_rtss_renderer_uses_real_rosters() -> None:
    from sportsdataverse.nhl.nhl_game_sim import (
        build_nhl_shelf,
        events_from_nhl_pbp,
        nhl_event_shares_from_pbp,
        nhl_names_from_pbp,
        nhl_render_context_from_pbp,
        render_nhl_game_pbp,
    )

    payload = json.loads(pathlib.Path("tests/fixtures/nhl_api_web/pbp_2024_scf_g7.json").read_text(encoding="utf-8"))
    names = nhl_names_from_pbp(payload)
    context = nhl_render_context_from_pbp(payload)
    assert len(context["numbers"]) == 40
    assert context["abbr"] == ("FLA", "EDM")
    assert "high-sticking" in [p for p, _ in context["penalties"]]
    assert 0 < context["delayed_rate"] <= 1.5  # fitted from the real flags
    shares = nhl_event_shares_from_pbp(payload)
    shelf = build_nhl_shelf(events_from_nhl_pbp(payload))
    final, rows = render_nhl_game_pbp(shelf, shares, names, np.random.default_rng(3), context)
    final2, rows2 = render_nhl_game_pbp(shelf, shares, names, np.random.default_rng(3), context)
    assert [r["text"] for r in rows] == [r["text"] for r in rows2]  # deterministic
    rtss = {
        "shot_on_goal": re.compile(r"^(FLA|EDM) ONGOAL - #\d+ [A-Z'\-]+, Wrist, Off\. Zone, \d+ ft\.$"),
        "goal": re.compile(
            r"^(FLA|EDM) #\d+ [A-Z'\-]+\(\d+\), Wrist, Off\. Zone, \d+ ft\."
            r"( Assists: #\d+ [A-Z'\-]+\(\d+\)(; #\d+ [A-Z'\-]+\(\d+\))?)?$"
        ),
        "missed_shot": re.compile(r"^(FLA|EDM) #\d+ [A-Z'\-]+, Wrist, Wide of Net, Off\. Zone, \d+ ft\.$"),
        "blocked_shot": re.compile(
            r"^(FLA|EDM) #\d+ [A-Z'\-]+ BLOCKED BY (FLA|EDM) #\d+ [A-Z'\-]+, Wrist, Def\. Zone\.$"
        ),
        "hit": re.compile(r"^(FLA|EDM) #\d+ [A-Z'\-]+ HIT (FLA|EDM) #\d+ [A-Z'\-]+, Def\. Zone\.$"),
        "faceoff": re.compile(r"^(FLA|EDM) won (Neu|Off|Def)\. Zone - EDM #\d+ [A-Z'\-]+ vs FLA #\d+ [A-Z'\-]+$"),
        "giveaway": re.compile(r"^(FLA|EDM) GIVEAWAY - #\d+ [A-Z'\-]+, Def\. Zone\.$"),
        "takeaway": re.compile(r"^(FLA|EDM) TAKEAWAY - #\d+ [A-Z'\-]+, Neu\. Zone\.$"),
        "penalty": re.compile(r"^(FLA|EDM) #\d+ [A-Z'\-]+, [\w \-]+ \(2 min\)$"),
        "period_start": re.compile(r"^Period Start- Local time: \d+:\d\d EDT$"),
        "period_end": re.compile(r"^Period End- Local time: \d+:\d\d EDT$"),
        "game_end": re.compile(r"^Game End- Local time: \d+:\d\d EDT$"),
        "delayed_penalty": re.compile(r"^Delayed Penalty - (FLA|EDM)$"),
    }
    for row in rows:
        pattern = rtss.get(row["event"])
        if pattern:
            assert pattern.match(row["text"]), (row["event"], row["text"])
        elif row["event"] == "stoppage":
            assert row["text"].isupper()
    assert rows[0]["event"] == "period_start"
    assert rows[-1]["event"] == "game_end"
    goals = [r for r in rows if r["event"] == "goal"]
    if goals:
        assert any(" Assists: " in r["text"] for r in goals)  # fitted credit pools
    # a blocked shot always pairs OPPOSING teams
    for row in rows:
        if row["event"] == "blocked_shot":
            teams = re.findall(r"(FLA|EDM)", row["text"])
            assert teams[0] != teams[1], row["text"]


# ------------------------------------------------------------- registry ----


def test_provider_registry_and_custom_override() -> None:
    """Play text is parameterized by provider: register + render a house style."""
    from sportsdataverse._common.play_text import (
        get_templates,
        register_provider,
        registered_providers,
    )

    assert {"nba_stats", "espn", "ncaa_stats"} <= set(registered_providers("basketball"))
    assert {"nfl_gsis", "espn"} <= set(registered_providers("football"))
    assert "mlb_statsapi" in registered_providers("baseball")
    assert "nhl_rtss" in registered_providers("hockey")
    # aliases resolve to the canonical providers
    assert get_templates("basketball", "v3") == get_templates("basketball", "nba_stats")
    assert get_templates("football", "college") == get_templates("football", "espn")
    with pytest.raises(KeyError, match="Unknown provider"):
        get_templates("basketball", "nope")

    # a custom provider inherits a base and overrides one formula...
    register_provider(
        "basketball",
        "housestyle_test",
        {"made_shot": "{name} buries the {subtype} ({pts} PTS)"},
        base="nba_stats",
    )
    tpl = get_templates("basketball", "housestyle_test")
    assert tpl["missed_shot"] == get_templates("basketball", "nba_stats")["missed_shot"]

    # ...and the renderer picks it up by name
    import json as json_mod
    import pathlib as pathlib_mod

    import polars as pl

    from sportsdataverse.nba.nba_possession_sim import PlayerAttribution, build_shelf, possessions_from_pbp
    from sportsdataverse.nba.nba_possession_sim.render import (
        player_names_from_pbp,
        simulate_game_actions,
    )

    frames = []
    for gid in ("0022100001", "0022300001"):
        payload = json_mod.loads(
            pathlib_mod.Path(f"tests/fixtures/nba_engine/{gid}/playbyplayv3.json").read_text(encoding="utf-8")
        )
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    raw = pl.concat(frames, how="diagonal_relaxed")
    shelf = build_shelf(possessions_from_pbp(raw))
    from sportsdataverse.nba.nba_possession_sim.shelf import player_game_logs_from_pbp

    logs = player_game_logs_from_pbp(raw)
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1])
    _, actions = simulate_game_actions(
        shelf, att, player_names_from_pbp(raw), np.random.default_rng(7), provider="housestyle_test"
    )
    makes = [a for a in actions if a["action_type"] == "Made Shot"]
    assert makes and all("buries the" in a["description"] for a in makes)
    # untouched formulas still render the base provider's shapes
    rebounds = [a for a in actions if a["action_type"] == "Rebound" and a["person_id"]]
    assert rebounds and all("REBOUND (Off:" in a["description"] for a in rebounds)
    # clean up: a test registration must not leak into the process registry
    # (the golden-fixture matrix is pinned, but keep the registry pristine)
    from sportsdataverse._common import play_text

    play_text._REGISTRY["basketball"].pop("housestyle_test", None)
    assert "housestyle_test" not in registered_providers("basketball")
