"""``soccer/_frames.rows_to_frame`` keeps nullable dtypes -- on the committed captures of every family.

A missing value must stay a null: a nullable boolean column is ``Boolean`` (never the
string ``"nan"``), a nullable integer column is ``Int64`` (never promoted to ``Float64``),
and no column of any family that builds its frames here carries a literal ``"nan"`` /
``"NaN"`` / ``"None"`` string.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, List, Tuple

import polars as pl
import pytest

from sportsdataverse.euroleague.euroleague_parsers import (
    parse_euroleague,
    parse_euroleague_boxscore,
    parse_euroleague_header,
    parse_euroleague_pbp,
    parse_euroleague_points,
)
from sportsdataverse.soccer._frames import rows_to_frame
from sportsdataverse.soccer.asa_parsers import parse_asa, parse_asa_goals_added
from sportsdataverse.soccer.fifa_parsers import parse_fifa
from sportsdataverse.soccer.fotmob_parsers import parse_fotmob
from sportsdataverse.soccer.mls.mls_api_parsers import parse_mls_api, parse_mls_match, parse_mls_standings
from sportsdataverse.soccer.nwsl.nwsl_api_parsers import (
    parse_nwsl_lineups,
    parse_nwsl_sdp,
    parse_nwsl_standings,
    parse_nwsl_stats,
)
from sportsdataverse.soccer.uefa_parsers import parse_uefa

FIXTURES = Path(__file__).parents[1] / "fixtures"
POISON = ["nan", "NaN", "None"]

# Every family that builds its frames through ``rows_to_frame``: fixture dir -> parsers.
_FAMILIES = {
    "asa": [parse_asa, parse_asa_goals_added],
    "fifa": [parse_fifa],
    "fotmob": [parse_fotmob],
    "uefa": [parse_uefa],
    "mls_api": [parse_mls_api, parse_mls_standings, parse_mls_match],
    "nwsl_api": [parse_nwsl_sdp, parse_nwsl_standings, parse_nwsl_stats, parse_nwsl_lineups],
    "euroleague": [parse_euroleague],
}
_EUROLEAGUE_LIVE = {
    "api__Points": parse_euroleague_points,
    "api__Header": parse_euroleague_header,
    "api__PlayByPlay": parse_euroleague_pbp,
    "api__Boxscore": parse_euroleague_boxscore,
}


def _load(family: str, stem: str) -> Any:
    return json.loads((FIXTURES / family / f"{stem}.json").read_text(encoding="utf-8"))


def _cases() -> List[Tuple[str, str, Callable[..., Any]]]:
    cases = [
        (family, path.stem, parser)
        for family, parsers in _FAMILIES.items()
        for path in sorted((FIXTURES / family).glob("*.json"))
        for parser in parsers
    ]
    cases += [("euroleague", stem, parser) for stem, parser in _EUROLEAGUE_LIVE.items()]
    return cases


def _frames(parsed: Any) -> List[pl.DataFrame]:
    return list(parsed.values()) if isinstance(parsed, dict) else [parsed]


@pytest.mark.parametrize(("family", "stem", "parser"), _cases(), ids=lambda v: getattr(v, "__name__", str(v)))
def test_no_family_emits_a_literal_nan_string(family: str, stem: str, parser: Callable[..., Any]) -> None:
    for df in _frames(parser(_load(family, stem))):
        for col in df.columns:
            if df.schema[col] == pl.String:
                assert not df[col].is_in(POISON).any(), f"{family}/{stem}.{col} carries a literal null string"


def test_euroleague_seasons_nullable_boolean_stays_boolean() -> None:
    df = parse_euroleague(_load("euroleague", "competitions__E__seasons"))
    assert df.schema["winner_is_virtual"] == pl.Boolean
    assert df["winner_is_virtual"].null_count() == 1 and df["winner_is_virtual"].to_list()[1:] == [False, False]
    # A column that is null on every row of the capture is Utf8, not Float64.
    assert df.schema["winner"] == pl.String and df["winner"].null_count() == df.height


def test_euroleague_games_keep_boolean_and_integer_columns() -> None:
    df = parse_euroleague(_load("euroleague", "competitions__E__seasons__E2025__games"))
    assert df.schema["played"] == pl.Boolean
    assert df.schema["local_score"] == pl.Int64 and df.schema["road_score"] == pl.Int64


def test_uefa_matches_nullable_integer_and_boolean_keep_their_types() -> None:
    df = parse_uefa(_load("uefa", "match__v5__matches"))
    assert df.schema["score_penalty_away"] == pl.Int64 and df["score_penalty_away"].null_count() > 0
    assert df.schema["winner_match_team_is_place_holder"] == pl.Boolean
    assert df["winner_match_team_is_place_holder"].null_count() > 0


def test_mixed_scalar_classes_in_one_column_are_stringified() -> None:
    df = rows_to_frame([{"v": 1}, {"v": "x"}, {"v": None}])
    assert df.schema["v"] == pl.String and df["v"].to_list() == ["1", "x", None]
    assert rows_to_frame([{"v": 1}, {"v": 2.5}]).schema["v"] == pl.Float64
