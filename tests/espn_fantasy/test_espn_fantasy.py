"""ESPN Fantasy wrappers + parsers, offline against the committed (redacted) captures."""

from __future__ import annotations

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

from tests.conftest import skip_if_no_live

FIX = pathlib.Path(__file__).parent.parent / "fixtures" / "espn_fantasy"

# (parser, table, fixture) for every wrapper's parser.
CASES = [
    ("parse_espn_fantasy_games", "games", "games.json"),
    ("parse_espn_fantasy_seasons", "seasons", "ffl/seasons.json"),
    ("parse_espn_fantasy_pro_teams", "pro_teams", "ffl/pro_team_schedules.json"),
    ("parse_espn_fantasy_pro_schedule", "pro_schedule", "fba/pro_team_schedules.json"),
    ("parse_espn_fantasy_players", "players", "fba/players.json"),
    ("parse_espn_fantasy_league_directory", "league_directory", "fba/league_directory.json"),
    ("parse_espn_fantasy_scoring", "scoring", "ffl/default_settings.json"),
    ("parse_espn_fantasy_scoring", "scoring", "fba/league_mSettings.json"),
    ("parse_espn_fantasy_player_news", "player_news", "fba/player_news.json"),
    ("parse_espn_fantasy_league_teams", "league_teams", "fba/league_mTeam.json"),
    ("parse_espn_fantasy_league_rosters", "league_rosters", "fba/league_mRoster.json"),
    ("parse_espn_fantasy_league_matchups", "league_matchups", "fba/league_mMatchupScore.json"),
    ("parse_espn_fantasy_league_boxscores", "league_boxscores", "fba/league_mBoxscore.json"),
    ("parse_espn_fantasy_league_draft", "league_draft", "fba/league_mDraftDetail.json"),
    ("parse_espn_fantasy_league_transactions", "league_transactions", "fba/league_mTransactions2.json"),
    ("parse_espn_fantasy_player_pool", "player_pool", "fba/league_kona_player_info.json"),
]
PARSERS = sorted({c[0] for c in CASES})
GAMES = ["ffl", "fba", "flb", "fhl", "wfba"]
TEXT_IDS = {"transaction_id", "now_id"}  # a UUID and ESPN's "23-54107746" content id: text by design


def _p():
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_parsers

    return espn_fantasy_parsers


def load(rel: str):
    return json.loads((FIX / rel).read_text(encoding="utf-8"))


@pytest.mark.parametrize("parser,table,fixture", CASES)
def test_fixture_parses_to_the_documented_schema(parser, table, fixture):
    P = _p()
    df = getattr(P, parser)(load(fixture))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert dict(df.schema) == P.SCHEMAS[table]
    # every documented column is read from the body: a misspelled key would leave it all-null
    assert [c for c in df.columns if df[c].null_count() == df.height] == []
    for col, dtype in df.schema.items():
        if col.endswith("_id") and col not in TEXT_IDS:
            assert dtype == pl.Int64, col


@pytest.mark.parametrize("game", GAMES)
@pytest.mark.parametrize(
    "parser,fixture",
    [
        ("parse_espn_fantasy_seasons", "seasons.json"),
        ("parse_espn_fantasy_pro_teams", "pro_team_schedules.json"),
        ("parse_espn_fantasy_pro_schedule", "pro_team_schedules.json"),
        ("parse_espn_fantasy_players", "players.json"),
        ("parse_espn_fantasy_league_directory", "league_directory.json"),
        ("parse_espn_fantasy_scoring", "default_settings.json"),
        ("parse_espn_fantasy_player_news", "player_news.json"),
    ],
)
def test_every_game_parses_with_one_parser(game, parser, fixture):
    df = getattr(_p(), parser)(load(f"{game}/{fixture}"))
    assert df.height > 0
    assert df.select(pl.all().is_null().all()).row(0).count(True) < df.width  # not an all-null frame


@pytest.mark.parametrize("parser", PARSERS)
@pytest.mark.parametrize(
    "payload",
    [None, {}, [], "", 7, [1, 2], {"messages": ["x"], "details": []}, {"teams": "x", "schedule": 5, "players": None}],
)
def test_an_unusable_body_is_a_typed_zero_row_frame(parser, payload):
    P = _p()
    df = getattr(P, parser)(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    table = next(t for p, t, _ in CASES if p == parser)
    assert dict(df.schema) == P.SCHEMAS[table]


def test_wrong_typed_values_become_null_with_a_drift_warning_not_an_exception():
    from sportsdataverse.errors import SchemaDriftWarning

    body = {
        "id": "x",
        "seasonId": 2027,
        "draftDetail": {"picks": [{"id": 1, "teamId": "3", "bidAmount": 1.5, "playerId": "--5", "roundId": 2**70}]},
    }
    with pytest.warns(SchemaDriftWarning, match="league_id"):
        df = _p().parse_espn_fantasy_league_draft(body)
    row = df.row(0, named=True)
    assert row["league_id"] is None and row["pick_id"] == 1 and row["team_id"] == 3
    assert row["bid_amount"] is None and row["player_id"] is None and row["round_id"] is None


def test_a_list_member_without_its_id_is_not_a_row():
    P = _p()
    assert P.parse_espn_fantasy_games([{}, {"abbrev": "FFL"}]).height == 0
    assert P.parse_espn_fantasy_league_teams({"id": 1, "teams": [{}]}).height == 0
    assert P.parse_espn_fantasy_league_draft({"id": 1, "draftDetail": {"picks": [{"teamId": 2}]}}).height == 0


@pytest.mark.parametrize("parser", PARSERS)
def test_pandas_keeps_ids_as_nullable_integers(parser):
    P = _p()
    fixture = next(f for p, _t, f in CASES if p == parser)
    pdf = getattr(P, parser)(load(fixture), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0
    for col in pdf.columns:
        if col.endswith("_id") and col not in TEXT_IDS:
            assert str(pdf[col].dtype) == "Int64", col


def test_no_parser_emits_a_member_or_owner_column():
    for table, schema in _p().SCHEMAS.items():
        assert not [c for c in schema if "owner" in c or "member" in c], table


def test_committed_fixtures_carry_no_account_guids_or_member_records():
    import re

    guid = re.compile(r"\{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\}")
    for f in FIX.rglob("*.json"):
        text = f.read_text(encoding="utf-8")
        assert not guid.search(text), f
        assert '"members"' not in text, f


def test_pro_schedule_keeps_each_pro_game_once():
    df = _p().parse_espn_fantasy_pro_schedule(load("fba/pro_team_schedules.json"))
    assert df["pro_game_id"].is_unique().all()
    assert df["game_date"].is_sorted()
    assert df.filter(pl.col("home_pro_team_id") == pl.col("away_pro_team_id")).height == 0


def test_boxscores_are_player_rows_for_the_requested_period():
    raw = load("fba/league_mBoxscore.json")
    df = _p().parse_espn_fantasy_league_boxscores(raw)
    assert set(df["side"]) == {"home", "away"}
    assert df["scoring_period_id"].unique().to_list() == [raw["scoringPeriodId"]]
    assert df["player_id"].null_count() == 0
    # a slot placeholder without a player is skipped
    empty = {
        "schedule": [
            {"id": 1, "home": {"teamId": 1, "rosterForCurrentScoringPeriod": {"entries": [{"lineupSlotId": 0}]}}}
        ]
    }
    assert _p().parse_espn_fantasy_league_boxscores(empty).height == 0


def test_matchups_keep_a_bye_with_a_null_away_team():
    df = _p().parse_espn_fantasy_league_matchups(
        {
            "id": 5,
            "seasonId": 2027,
            "schedule": [{"id": 1, "matchupPeriodId": 1, "winner": "HOME", "home": {"teamId": 2, "totalPoints": 10.5}}],
        }
    )
    assert df.row(0, named=True)["away_team_id"] is None and df["home_total_points"][0] == 10.5


def test_transactions_are_one_row_per_item_and_one_for_an_itemless_move():
    P = _p()
    raw = load("fba/league_mTransactions2.json")
    df = P.parse_espn_fantasy_league_transactions(raw)
    assert df.height == sum(max(len(t.get("items") or []), 1) for t in raw["transactions"])
    lone = P.parse_espn_fantasy_league_transactions(
        {"id": 1, "transactions": [{"id": "u", "type": "ROSTER", "items": []}]}
    )
    assert lone.height == 1 and lone["item_type"][0] is None


def test_epoch_millisecond_dates_are_utc_datetimes():
    df = _p().parse_espn_fantasy_league_directory(load("fba/league_directory.json"))
    assert df.schema["draft_date"] == pl.Datetime("ms", time_zone="UTC")
    assert df["draft_date"].dt.year().min() >= 2026


def test_scoring_reads_a_league_and_a_default_template_alike():
    P = _p()
    lg = P.parse_espn_fantasy_scoring(load("fba/league_mSettings.json"))
    df = P.parse_espn_fantasy_scoring(load("ffl/default_settings.json"))
    assert lg["league_id"].unique().to_list() == [1535462463]
    assert df["league_id"].unique().to_list() == [1]  # the template id
    assert df["stat_id"].null_count() == 0 and df["points"].null_count() == 0


# ---- runtime + wrappers ------------------------------------------------------------------------


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status
        self.text = json.dumps(body)
        self.headers = {"content-type": "application/json"}
        self.content = self.text.encode()

    def json(self):
        return self._body


@pytest.fixture
def fake(monkeypatch):
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_runtime as rt

    calls = []

    def install(body, status=200):
        def download(url, params=None, headers=None, **kw):
            calls.append({"url": url, "params": params, "headers": headers, **kw})
            return _Resp(body, status)

        monkeypatch.setattr(rt, "download", download)
        return calls

    return install


def test_filter_travels_as_a_header_not_a_query_param(fake):
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_player_pool

    calls = fake(load("fba/league_kona_player_info.json"))
    df = espn_fantasy_player_pool("fba", 2027, 1535462463)
    assert df.height == 3
    (call,) = calls
    assert call["url"].endswith("/games/fba/seasons/2027/segments/0/leagues/1535462463")
    assert call["params"] == {"view": "kona_player_info"}
    assert json.loads(call["headers"]["X-Fantasy-Filter"])["players"]["limit"] == 50
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_runtime as rt

    assert "Cookie" not in call["headers"]
    assert call["session"] is rt.SESSION and call["cache_ttl"] == 0  # cookie-refusing session, never cached
    assert rt.SESSION.cookies._policy.allowed_domains() == ()  # every domain refused


def test_a_dict_filter_is_json_encoded_and_a_scoring_period_is_a_query_param(fake):
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_boxscores
    from sportsdataverse.providers.espn_fantasy.espn_fantasy_runtime import _get

    calls = fake(load("fba/league_mBoxscore.json"))
    espn_fantasy_league_boxscores("fba", 2027, 1535462463, scoring_period_id=37)
    assert calls[-1]["params"] == {"view": "mBoxscore", "scoringPeriodId": 37}
    _get("https://x", params={"X-Fantasy-Filter": {"a": 1}})
    assert calls[-1]["headers"]["X-Fantasy-Filter"] == '{"a":1}' and calls[-1]["params"] == {}


def test_a_private_league_raises_a_named_asset_fetch_error(fake):
    from sportsdataverse.errors import AssetFetchError
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_teams

    fake({"messages": ["You are not authorized to view this League."]}, status=401)
    with pytest.raises(AssetFetchError, match="private"):
        espn_fantasy_league_teams("ffl", 2026, 336358)


def test_a_bad_filter_is_a_value_error(fake):
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_players

    fake({"messages": ["Filter: Limit request must be accompanied by a sort"]}, status=400)
    with pytest.raises(ValueError):
        espn_fantasy_players("ffl", 2026, fantasy_filter='{"limit":5}')


def test_an_unknown_game_is_refused_before_any_request(fake):
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_seasons

    calls = fake([])
    with pytest.raises(ValueError, match="game must be one of"):
        espn_fantasy_seasons("nfl")
    assert calls == []


def test_player_news_reads_the_site_host(fake):
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_player_news

    calls = fake(load("fba/player_news.json"))
    assert espn_fantasy_player_news("fba", 5104157).height == 3
    assert calls[0]["url"] == "https://site.api.espn.com/apis/fantasy/v3/games/fba/news/players"
    assert calls[0]["params"] == {"playerId": 5104157}


def test_build_query_drops_only_the_filter():
    from sportsdataverse.providers.espn_fantasy.espn_fantasy_runtime import _build_query

    assert _build_query({"view": "mTeam", "X-Fantasy-Filter": "{}"}) == {"view": "mTeam"}
    assert _build_query(None) == {}


def test_every_wrapper_is_exported_and_has_a_schema():
    import yaml

    import sportsdataverse.providers.espn_fantasy as ef

    root = pathlib.Path(__file__).parents[2] / "tools" / "codegen"
    doc = yaml.safe_load((root / "endpoints" / "espn_fantasy.yaml").read_text("utf-8"))
    for ep in doc["endpoints"]:
        assert callable(getattr(ef, f"espn_fantasy_{ep['short']}"))
        schema = yaml.safe_load((root / "schemas" / f"{ep['returns_schema']}.yaml").read_text("utf-8"))
        assert schema["schema"] == f"espn_fantasy_{ep['short']}" and schema["columns"]
    assert {ep["parser"] for ep in doc["endpoints"]} == set(PARSERS)


@skip_if_no_live
def test_live_games_and_public_league_directory():
    from sportsdataverse.providers.espn_fantasy import espn_fantasy_games, espn_fantasy_league_directory

    games = espn_fantasy_games()
    assert {"FFL", "FBA"} <= set(games["game_abbrev"])
    fba = games.filter(pl.col("game_abbrev") == "FBA").row(0, named=True)
    lobby = espn_fantasy_league_directory("fba", fba["current_season_id"])
    assert lobby.schema["league_id"] == pl.Int64
