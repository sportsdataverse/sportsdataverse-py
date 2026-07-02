"""Tests for nba_player_positions (listed position -> numeric 1-5)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_player_positions import _position_to_num, nba_player_positions


def test_position_to_num_mapping() -> None:
    assert _position_to_num("PG") == 1.0
    assert _position_to_num("SG") == 2.0
    assert _position_to_num("SF") == 3.0
    assert _position_to_num("PF") == 4.0
    assert _position_to_num("C") == 5.0
    assert _position_to_num("G") == 1.5  # guard midpoint
    assert _position_to_num("F") == 3.5  # forward midpoint
    assert _position_to_num("G-F") == 2.5  # hyphenated midpoint of G(1.5) & F(3.5)
    assert _position_to_num("") == 3.0  # missing -> neutral
    assert _position_to_num("nonsense") == 3.0


def test_nba_player_positions_parses_playerindex() -> None:
    def fake(**kw: object) -> pl.DataFrame:
        return pl.DataFrame({"person_id": [1, 2, 3], "position": ["G", "F-C", ""]})

    df = nba_player_positions("2023-24", fetch=fake)
    assert df.columns == ["player_id", "position_num"]
    assert df["player_id"].to_list() == [1, 2, 3]
    assert df["position_num"].to_list() == [1.5, 4.25, 3.0]  # G=1.5, F-C=(3.5+5)/2=4.25
