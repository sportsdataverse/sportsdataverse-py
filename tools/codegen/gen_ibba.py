"""Generate the ``ibba`` flat-API endpoint YAML + returns-schemas (Israeli Premier League, basket.co.il ASMX).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_feb.py`` (itself
``gen_asa.py`` with the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/ibba.yaml`` -- 16 wrappers over the spec's 16 ASMX ops (one wrapper per op;
  :data:`ENDPOINTS` is the source of truth). Every ASMX parameter is required by the host, so a wrapper always
  sends the op's full list; ``0`` means "all" where the host says so and is the default there.
  ``season`` is sent as the host's ``cYear`` unchanged (the END year is already the sdv key).
* ``tools/codegen/schemas/native/ibba/<short>.yaml`` -- one returns-schema per wrapper: the columns of its
  parser run over its committed fixture (FULL bodies in ``tests/fixtures/ibba/full/`` where one was fetched, the
  refs captures otherwise). Each parser emits a fixed column list with dtypes pinned by name
  (``ibba_parsers.SPECS``), so a fixture cannot mistype a rate. The schemas carry ``description: ''``
  (descriptions never live in ``schemas/**``).
* ``tools/codegen/ibba_column_descriptions.snippet.yaml`` -- ``{ibba_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (gitignored). An empty, short or filler text fails the run.
* ``tests/fixtures/ibba/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

``gen_ibba_segev.py`` reuses :func:`emit` for the second spec.

Run: ``python tools/codegen/gen_ibba.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.ibba import ibba_parsers as P

HOST = "https://basket.co.il/ws/ws.asmx"
SPEC = "ibba.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "ibba"
SNIPPET = ROOT / "tools" / "codegen" / "ibba_column_descriptions.snippet.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

ACCESS = (
    "Access: unofficial and reverse-engineered, keyless, for internal use. The "
    "wrappers read basket.co.il's ASP.NET web service (the ops its own site calls) and space requests at least one "
    "second apart; keep crawls slow and incremental. Payloads are not redistributed as release assets. Contact "
    "details (e-mail, phone, fax) and coaches' dates of birth are dropped by the parsers."
)
SEASON_NOTE = (
    "Season and ids. ``season`` is the END year and the host's own ``cYear`` (2026 = 2025-26): no mapping. "
    "``team_id`` and ``player_id`` are per-SEASON registrations (Maccabi Tel Aviv = team 1109 in 2025-26, 2111 in "
    "2026-27; the ASMX host has no cross-season person id: the stable key is the Segev player id in "
    "``ibba_segev_*``). ``team_uid`` is the stable club id (Maccabi = 10). ``ibba_games.segev_game_id`` joins "
    "``ibba_segev_*`` ``game_id`` from 2025-26."
)
COVERAGE = (
    "Coverage: league box scores from 2004-05 (``season=2005``); ``external_id`` / ``segev_game_id`` only from "
    "2025-26; the women's league and Liga Leumit are NOT on this host (they are on Genius Sports org IBBA and in "
    "Segev). Every ASMX value is a string and Hebrew text is HTML-entity encoded; the parsers cast ids, counts "
    "and dates and unescape text. An unknown id answers an empty body (NoDataError); box scores of unfinished "
    "games are refused."
)

SEASON = "Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged."
TEAM_UID = (
    "Required. IBBA club id, stable across seasons (Maccabi Tel Aviv = 10; ``team_uid`` in ibba_teams). Use 0 for "
    "all clubs only where the route says so."
)
TEAM_ID = (
    "Required. Team-season id (new every season; Maccabi = 1109 in 2025-26): ``team_id`` in ibba_teams, "
    "ibba_games ``home_team_id`` / ``away_team_id`` or ibba_box_score."
)
PLAYER_ID = (
    "Required. Player-season id (new every season; Jaylen Hoard = 21912 in 2025-26): ``player_id`` in ibba_players."
)
GAME_ID = "Required. IBBA game id (e.g. 26392): ``game_id`` in ibba_games."
IS_AVG = "True returns per-game averages (decimals); False (default) the season totals."
IS_PO = "True returns the playoff games only; False (default) the regular season."


def _p(
    name: str, query_key: str, typ: str, description: str, *, required: bool = False, default: Any = None
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": query_key, "type": typ, "description": description}
    if required:
        out["required"] = True
    elif default is not None:
        out["default"] = default
    return out


_SEASON = _p("season", "cYear", "int", SEASON, required=True)
_TUID = _p("team_uid", "team_uid", "int", TEAM_UID, required=True)
_TID = _p("team_id", "team_id", "int", TEAM_ID, required=True)
_BOARD = "Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup. {extra}"

Entry = Tuple[str, Callable[[], Dict[str, Any]], str, List[Dict[str, Any]], Dict[str, Any]]


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / "asmx" / f"{name}.json").read_text(encoding="utf-8"))


#: short -> (spec path, fixture loader, summary, extra_params, example_args)
ENDPOINTS: Dict[str, Entry] = {
    "configuration": (
        "/Configuration",
        lambda: _cap("Configuration@2026"),
        "Season settings: the season label, round counters and the standings cut line.",
        [_SEASON],
        {"season": 2026},
    ),
    "boards": (
        "/Boards",
        lambda: _cap("Boards@2026"),
        "Competitions (boards) of a season: league, cup, playoff rounds, play-in, Supercup.",
        [_SEASON],
        {"season": 2026},
    ),
    "boards_ex": (
        "/BoardsEx",
        lambda: _cap("BoardsEx@10_2026"),
        "External competitions a club played in during a season (European cups, national team, pre-season).",
        [_TUID, _SEASON],
        {"team_uid": 10, "season": 2026},
    ),
    "teams": (
        "/Teams",
        lambda: _full("Teams"),
        "Teams of a board and season with their standings line and club profile fields.",
        [
            _SEASON,
            _p("board_id", "board_id", "int", _BOARD.format(extra="Default 5."), default=5),
        ],
        {"season": 2026},
    ),
    "team": (
        "/TeamById",
        lambda: _cap("TeamById@1109_10_2026"),
        "One team-season: standings line and club profile (colours, sponsors, social links, arena id).",
        [_TID, _TUID, _SEASON],
        {"team_id": 1109, "team_uid": 10, "season": 2026},
    ),
    "players": (
        "/Players",
        lambda: _full("Players"),
        "The roster of a team-season: jersey, names, position, height and birth date.",
        [_TID, _TUID, _SEASON],
        {"team_id": 1109, "team_uid": 10, "season": 2026},
    ),
    "player": (
        "/PlayerById",
        lambda: _cap("PlayerById@21912_2026"),
        "One player-season row: jersey, team, names and photo.",
        [_p("player_id", "player_id", "int", PLAYER_ID, required=True), _SEASON],
        {"player_id": 21912, "season": 2026},
    ),
    "coaches": (
        "/Coaches",
        lambda: _cap("Coaches@1109_10_2026"),
        "The coaching staff of a team-season (birth dates are dropped).",
        [_TID, _TUID, _SEASON],
        {"team_id": 1109, "team_uid": 10, "season": 2026},
    ),
    "box_score": (
        "/Boxscore",
        lambda: _full("Boxscore"),
        "Box score of a finished game: player lines, a team-credited line per side and the TOTAL line per side.",
        [_p("game_id", "game_id", "int", GAME_ID, required=True), _SEASON],
        {"game_id": 26392, "season": 2026},
    ),
    "player_stats": (
        "/PlayerStatsById",
        lambda: _cap("PlayerStatsById@21912_2026_true_false"),
        "A player-season's totals, or per-game averages with is_averages=True.",
        [
            _p("player_id", "player_id", "int", PLAYER_ID, required=True),
            _SEASON,
            _p("is_averages", "isAverages", "bool", IS_AVG, default=False),
            _p("is_playoff", "isPlayOff", "bool", IS_PO, default=False),
        ],
        {"player_id": 21912, "season": 2026},
    ),
    "stats_types": (
        "/StatsTypes",
        lambda: _cap("StatsTypes"),
        "The stat categories ibba_leaders accepts (the stat_type codes) with their Hebrew names.",
        [],
        {},
    ),
    "leaders": (
        "/LeaderByType",
        lambda: _cap("LeaderByType@pts_2026_0"),
        "The top five players of a season in one stat category, for all teams or one team.",
        [
            _p(
                "stat_type",
                "stat_type",
                "str",
                "Required. Stat category code from ibba_stats_types: VAL, pts, isr_pts, plusminus, dunk, ASS, TR, "
                "[OR], DR, isr_TR, ST, [TO], ASSTO, BKF, FF, [min], p2, p3, p1, p32T, p22T, p12T, PTSMIN, TRMIN, ASSMIN.",
                required=True,
            ),
            _SEASON,
            _p(
                "team_id",
                "team_id",
                "int",
                "Team-season id to restrict the list to one team; 0 (default) for all teams.",
                default=0,
            ),
        ],
        {"stat_type": "pts", "season": 2026},
    ),
    "team_stats": (
        "/TeamStatsById",
        lambda: _cap("TeamStatsById@1109_2026_true_false"),
        "A team-season's totals, or per-game averages with is_averages=True.",
        [
            _TID,
            _SEASON,
            _p("is_averages", "isAverages", "bool", IS_AVG, default=False),
            _p("is_playoff", "isPlayOff", "bool", IS_PO, default=False),
        ],
        {"team_id": 1109, "season": 2026},
    ),
    "games": (
        "/Games",
        lambda: _full("Games"),
        "Games (schedule and results) of a season filtered by board, round, team and game; 0 means any.",
        [
            _SEASON,
            _p("board_id", "board_id", "int", _BOARD.format(extra="0 (default) for every board."), default=0),
            _p(
                "board_round",
                "board_round",
                "int",
                "Round (game number) within the board; 0 (default) for every round.",
                default=0,
            ),
            _p("team_id", "team_id", "int", "Team-season id; 0 (default) for every team.", default=0),
            _p("team_uid", "team_uid", "int", "IBBA club id; 0 (default) for every club.", default=0),
            _p("game_id", "game_id", "int", "IBBA game id; 0 (default) for every game.", default=0),
        ],
        {"season": 2026, "board_id": 5, "board_round": 1},
    ),
    "games_ex": (
        "/GamesEx",
        lambda: _cap("GamesEx@0_10_2026"),
        "A club's games of a season in external competitions (BoardsEx boards: European cups, pre-season).",
        [
            _TUID,
            _SEASON,
            _p(
                "board_id",
                "board_id",
                "int",
                "External board id from ibba_boards_ex (1001 EuroLeague, 1003 Champions League, ...); 0 (default) for all.",
                default=0,
            ),
        ],
        {"team_uid": 10, "season": 2026},
    ),
    "games_by_team": (
        "/GamesByTeam",
        lambda: _cap("GamesByTeam@10_2026"),
        "Every game of a club in a season, domestic and external, in one frame.",
        [_TUID, _SEASON],
        {"team_uid": 10, "season": 2026},
    ),
}

#: Descriptions of the columns the parsers rename or derive. Season-stat routes, the box score and the
#: games frames qualify some of them per wrapper in :data:`LOCAL_BY`.
LOCAL: Dict[str, str] = {
    "season": "Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping.",
    "season_label": "Season label as 'YYYY-YY' ('2025-26', '2004-05').",
    "red_line": "Standings position the site draws its cut line under (12 in 2025-26, 10 in 2004-05); meaning inferred.",
    "active_board_id": "Board id the site treats as the active competition (17 = final series in 2025-26, 5 = league "
    "in 2004-05); meaning inferred.",
    "next_round": "Next round (game number) to be played; meaning inferred.",
    "total_rounds": "Rounds in the regular season (32 in 2025-26, 22 in 2004-05); meaning inferred.",
    "board_id": "Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, "
    "33 play-in, 34 Supercup, 11 the 2004-05 semifinal group; 1001+ external competitions (1001 EuroLeague, 1003 "
    "Champions League, 1005 pre-season, 1008 EuroBasket, 1012 EuroCup, 1013 State Cup).",
    "board_name_he": "Competition name in Hebrew, HTML-unescaped.",
    "board_title_he": "Sponsor-branded competition name in Hebrew ('ליגת Winner סל' = Winner League).",
    "category_he": "Competition format in Hebrew: 'טבלה' = table, 'נוק אאוט' = knockout, 'סדרה' = series.",
    "team_uid": "IBBA CLUB id, stable across seasons (Maccabi Tel Aviv = 10): the team_uid parameter of the roster "
    "routes. Not the per-season team_id.",
    "team_id": "Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. "
    "Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, "
    "ibba_box_score and the games' home / away ids.",
    "team_name_he": "Team name in Hebrew, HTML-unescaped.",
    "team_short_name_he": "Short team name in Hebrew, HTML-unescaped.",
    "team_name": "Team name in English as registered for the season.",
    "team_short_name": "Short team name in English.",
    "place": "Rank in the standings of the board (1 = first).",
    "games": "Games played (in the standings or in the season stats).",
    "wins": "Games won in the season so far (standings line).",
    "losses": "Games lost in the season so far (standings line).",
    "points_for": "Points scored over the games played.",
    "points_against": "Points conceded over the games played.",
    "table_points": "Standings points (2 per win, 1 per loss in the league table).",
    "logo_url": "Absolute URL of the team logo.",
    "photo_path": "Site-relative path of the photo (team or player picture); prefix with https://www.basket.co.il.",
    "address_he": "Club address in Hebrew, HTML-unescaped.",
    "sponsors_he": "Club sponsors in Hebrew, comma-separated.",
    "facebook_url": "Club Facebook page URL; null when none.",
    "twitter_url": "Club Twitter / X profile URL; null when none.",
    "instagram_url": "Club Instagram profile URL; null when none.",
    "youtube_url": "Club YouTube channel URL; null when none.",
    "ticket_url": "Ticket-sales URL; null when none.",
    "subscribe_url": "Season-ticket subscription URL; null when none.",
    "coaches_he": "Coaching staff as one Hebrew text ('מאמן: ..., עוזר מאמן: ...').",
    "website_url": "Club website URL.",
    "icon_path": "Site-relative path of the small team icon.",
    "icon3_path": "Site-relative path of the third team icon variant.",
    "arena_id": "Home arena id in the IBBA site's own arena table (no arena route is modelled).",
    "address": "Club address in English, HTML-unescaped.",
    "colors_he": "Team colours in Hebrew.",
    "colors": "Team colours in English.",
    "sponsors": "Club sponsors in English, comma-separated.",
    "about_he": "Club profile in Hebrew as HTML markup (honours, history); not stripped.",
    "about": "Club profile in English as HTML markup; null when the club has none.",
    "diary_url": "Calendar-subscription (add-to-diary) URL of the club's fixtures.",
    "player_id": "Player-season id (key space: IBBA player registrations; a NEW id every season, e.g. Jaylen Hoard = "
    "21912 in 2025-26 and 25997 in 2026-27; the ASMX host has no cross-season person id). Joins ibba_players, "
    "ibba_player, ibba_box_score and ibba_leaders; null on the team-credited and TOTAL box rows.",
    "jersey": "Jersey number as a zero-padded string ('02'); a box score also uses '_TEAM' and 'TOTAL' for its two "
    "non-player lines.",
    "player_name_he": "Player name in Hebrew, HTML-unescaped.",
    "player_name": "Player name in English, HTML-unescaped.",
    "is_replaced": "True when the person was replaced during the season (left the roster or staff).",
    "is_local": "True for an Israeli (local) player; the isr_ leader categories count only these.",
    "height_m": "Height in metres (1.93).",
    "position_he": "Position in Hebrew.",
    "position": "Position code: PG, G, G-F, F, F-C or C.",
    "birth_date": "Date of birth (public player bio); null when not published.",
    "coach_id": "Coach id in the IBBA coach table (per team-season registration as far as the samples show; "
    "stability across seasons is unverified).",
    "coach_type": "Staff role code: 1 = head coach, 2 = assistant coach (inferred from the Maccabi Tel Aviv sample).",
    "coach_name_he": "Coach name in Hebrew, HTML-unescaped.",
    "coach_name": "Coach name in English.",
    "sort_order": "Display order of the coach within the staff list (mOrder).",
    "game_id": "IBBA game id (key space: basket.co.il games). Joins ibba_games to ibba_box_score; in ibba_games_ex "
    "it is an EXTERNAL game id (the ig_ space, e.g. 8888 for a pre-season game).",
    "round": "Round (game number) within the board; null for external games.",
    "starter": "True when the player started the game.",
    "minutes": "Minutes played, whole minutes; a TOTAL line holds the team's player-minutes (200 plus 25 per overtime).",
    "pts": "Points scored by the player or team (free throws included).",
    "fg2m": "Two-point field goals made.",
    "fg2a": "Two-point field goals attempted (verified equal to Segev made + missed).",
    "fg2_pct": "Two-point field-goal percentage, 0-100.",
    "fg3m": "Three-point field goals made.",
    "fg3a": "Three-point field goals attempted.",
    "fg3_pct": "Three-point field-goal percentage, 0-100.",
    "ftm": "Free throws made.",
    "fta": "Free throws attempted.",
    "ft_pct": "Free-throw percentage, 0-100.",
    "dreb": "Defensive rebounds.",
    "oreb": "Offensive rebounds.",
    "reb": "Total rebounds.",
    "fouls_drawn": "Fouls DRAWN: fouls opponents committed against the player or team (the wire's fouls_against; "
    "verified equal to Segev f_drawn).",
    "fouls": "Fouls COMMITTED by the player or team (the wire's fouls_for; verified equal to Segev f).",
    "steals": "Steals made by the player or team.",
    "turnovers": "Turnovers (the wire's misspelt tournovers).",
    "assists": "Assists credited to the player or team.",
    "blocks": "Shots the player or team blocked.",
    "blocks_against": "Own shots that were blocked.",
    "val": "Efficiency rating (valuation; verified equal to Segev rate); -1 on 2004-05 team-credited lines.",
    "plus_minus": "Plus-minus; 0 on every line of the 2004-05 sample (not recorded then, inferred).",
    "dunks": "Dunks made by the player or team.",
    "row_type": "Kind of box line: player, team (actions credited to the team: team rebounds and turnovers) or total "
    "(the team's TOTAL line, equal to players plus team lines).",
    "stat_type": "Stat category code to pass to ibba_leaders as stat_type (bracketed codes such as '[OR]' are literal).",
    "stat_name_he": "Stat category name in Hebrew.",
    "value": "The leader's value in the requested category (per-game average, percentage or count).",
    "game_date": "Local Israel date of the game.",
    "home_team_id": "Home team-season id (team1 on the wire; verified home against Segev). On external boards "
    "(board_id >= 1001) an external-team id (the ig_team_ space), not a team-season.",
    "away_team_id": "Away team-season id (team2 on the wire). On external boards an external-team id.",
    "home_team_he": "Home team name in Hebrew, HTML-unescaped.",
    "away_team_he": "Away team name in Hebrew, HTML-unescaped.",
    "home_team": "Home team name in English; null for external games, which carry Hebrew names only.",
    "away_team": "Away team name in English; null for external games.",
    "home_logo_url": "Absolute URL of the home team logo.",
    "away_logo_url": "Absolute URL of the away team logo.",
    "home_score": "Home final score; 0 before the game is played.",
    "away_score": "Away final score; 0 before the game is played.",
    "board_name": "Competition name in English ('Winner League'); null for external boards.",
    "tip_off": "Tip-off 'HH:MM', local Israel time, as text; null in 2004-05. It can disagree with Segev (game 26646: "
    "20:15 here, 19:30 in Segev).",
    "venue_he": "Venue in Hebrew, HTML-unescaped; null in 2004-05.",
    "venue": "Venue in English, HTML-unescaped; null in 2004-05.",
    "referees_he": "Referees' names in Hebrew, comma-separated; ', , ' when unknown.",
    "attendance": "Attendance; 0 when unknown.",
    "overtime": "True when the game went to overtime.",
    "overtimes": "Number of overtime periods played.",
    "home_score_by_period": "Home points per period, pipe-separated ('18|23|30|21'); null for external games.",
    "away_score_by_period": "Away points per period, pipe-separated ('29|26|27|22'); null for external games.",
    "tickets_url": "Ticketing URL; null when none.",
    "external_id": "The wire's ExternalID: the Segev game id (a string) for league games from 2025-26, a news URL for "
    "pre-season games, null before 2025-26.",
    "segev_game_id": "external_id read as an integer Segev game id (digits only), else null; joins ibba_segev_* "
    "game_id. Derived by the parser.",
    "is_external": "True when board_id >= 1001 (an external competition, whose team ids are not team-seasons). "
    "Derived by the parser.",
}
_SEASON_STATS = " Season total, or the per-game average when is_averages is true."
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "box_score": {
        "round": "Round (game number) of the game within its board.",
        "board_id": "Board id of the game (5 = league).",
    },
    "player_stats": {
        c: LOCAL[c] + _SEASON_STATS
        for c in P.SPECS["player_stats"]
        if c in LOCAL
        and c.startswith(
            (
                "fg",
                "ft",
                "pts",
                "min",
                "dreb",
                "oreb",
                "reb",
                "fouls",
                "steals",
                "turn",
                "ass",
                "blocks",
                "val",
                "plus",
                "dunks",
            )
        )
    },
    "team_stats": {
        c: LOCAL[c] + _SEASON_STATS
        for c in P.SPECS["team_stats"]
        if c in LOCAL
        and c.startswith(
            (
                "fg",
                "ft",
                "pts",
                "min",
                "dreb",
                "oreb",
                "reb",
                "fouls",
                "steals",
                "turn",
                "ass",
                "blocks",
                "val",
                "plus",
                "dunks",
            )
        )
    },
    "leaders": {
        "games": "Games the leader played.",
        "team_id": "Team-season id of the leader's team.",
        "jersey": "The leader's jersey number as a zero-padded string.",
    },
    "stats_types": {},
}


def describe(short: str, col: str) -> str:
    return LOCAL_BY.get(short, {}).get(col) or LOCAL.get(col, "")


def emit(
    *,
    stem: str,
    spec_file: str,
    refs_source: str,
    doc: Dict[str, Any],
    endpoints: Dict[str, Entry],
    specs: Dict[str, Any],
    describe_fn: Callable[[str, str], str],
    parser_prefix: str,
    snippet: Path,
    frames: Callable[[str, Entry], pl.DataFrame],
    check_routes: Callable[[Dict[str, Any]], None],
    fixed: Dict[str, Dict[str, Any]] | None = None,
) -> None:
    """Write ``endpoints/<stem>.yaml``, the native schemas and the description snippet for one stem."""
    refs = refs_dir(refs_source, spec_file)
    check_routes(load_spec(refs, spec_file))
    assert set(specs) == set(endpoints), "every parser spec needs an endpoint and the reverse"
    eps: List[Dict[str, Any]] = []
    for short, (path, _fix, summary, params, example) in endpoints.items():
        names = re.findall(r"\{([^}]+)\}", path)
        assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
        ep: Dict[str, Any] = {
            "short": short,
            "summary": summary,
            "path": path,
            "parser": f"{parser_prefix}{short}",
            "returns_schema": f"native/{stem}/{short}",
            "example_args": copy.deepcopy(example),
        }
        if fixed and short in fixed:
            ep["fixed_params"] = dict(fixed[short])  # constant query parameters, never arguments
        if params:
            ep["extra_params"] = copy.deepcopy(params)
        eps.append(ep)
    write_yaml(ROOT / f"tools/codegen/endpoints/{stem}.yaml", {**doc, "endpoints": eps})
    schema_dir = ROOT / f"tools/codegen/schemas/native/{stem}"
    rewrite_schema_dir(schema_dir)
    snippet_doc: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in endpoints.items():
        cols = columns_from_frame(frames(short, entry), [], leaf_fallback=False)
        block = snippet_doc.setdefault(f"{stem}_{short}", {})
        for col in cols:
            text = describe_fn(short, col["name"]).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or BANNED.match(text):
                bad.append(f"{stem}_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"{stem}_{short}", "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"{stem}: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(snippet, snippet_doc)
    print(f"{stem}: {len(endpoints)} endpoints, {sum(map(len, snippet_doc.values()))} descriptions -> {snippet.name}")


def main() -> None:
    def check(spec: Dict[str, Any]) -> None:
        differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
        if differs:
            raise SystemExit(f"ibba: spec paths and ENDPOINTS differ: {sorted(differs)}")
        SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")

    doc = {
        "api": "ibba",
        "host": HOST,
        "name_pattern": "ibba_{short}",
        "module": "ibba",
        "parser_module": "basketball.ibba.ibba_parsers",
        "getter_module": "sportsdataverse.basketball.ibba.ibba_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (box_score, games, teams) name other families
        "schema_prefix": "ibba_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": 'the raw body (``dict``): the ASMX body ``{"<key>": [row, ...]}`` (every value a string) '
            "plus ``_request`` (the op and the wire parameters sent)",
            "raises": [
                "sportsdataverse.errors.NoDataError: the host has nothing for the request (HTTP 404, an empty body "
                "for an unknown id, an empty list).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(a non-200, an error or changed envelope, a row for another season, team, player, board or game, "
                "a box score of an unfinished game).",
            ],
            "notes": [ACCESS, SEASON_NOTE, COVERAGE],
            "see_also": [
                {
                    "name": "Israel Basketball Association",
                    "url": "https://basket.co.il",
                    "note": "the league site whose web service these wrappers read",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball (R)",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball (R)",
                },
            ],
        },
    }
    emit(
        stem="ibba",
        spec_file=SPEC,
        refs_source="ibba",
        doc=doc,
        endpoints=ENDPOINTS,
        specs=P.SPECS,
        describe_fn=describe,
        parser_prefix="parse_ibba_",
        snippet=SNIPPET,
        frames=lambda short, entry: getattr(P, f"parse_ibba_{short}")(entry[1]()),
        check_routes=check,
    )


if __name__ == "__main__":
    main()
