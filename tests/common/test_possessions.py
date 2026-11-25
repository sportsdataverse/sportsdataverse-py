"""The per-league free-throw weight of the box possession estimate."""

from __future__ import annotations

import pytest

from sportsdataverse._common.possessions import (
    COLLEGE_FT_POSSESSION_WEIGHT,
    PRO_FT_POSSESSION_WEIGHT,
    TS_FT_WEIGHT,
    ft_possession_weight,
)


@pytest.mark.parametrize(
    ("league", "w"),
    [("nba", 0.44), ("wnba", 0.44), ("gleague", 0.44), ("mbb", 0.475), ("wbb", 0.475), ("mens", 0.475)],
)
def test_weight_by_league(league: str, w: float) -> None:
    assert ft_possession_weight(league) == w


def test_true_shooting_keeps_the_pro_weight() -> None:
    assert TS_FT_WEIGHT == PRO_FT_POSSESSION_WEIGHT == 0.44 < COLLEGE_FT_POSSESSION_WEIGHT


def test_unknown_league_raises() -> None:
    with pytest.raises(ValueError, match="no free-throw possession weight"):
        ft_possession_weight("nfl")
