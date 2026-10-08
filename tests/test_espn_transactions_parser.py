"""espn_<league>_transactions parses its real payload (records live under ``transactions``, not ``items``)."""

from __future__ import annotations

from sportsdataverse import _common_espn_parsers as P
from tests.conftest import load_fixture


def test_transactions_routes_to_its_own_parser():
    """It was routed to parse_items, which reads ``items``/``entries`` and so returned an empty
    frame for every league's real reply."""
    assert P.ENDPOINT_PARSERS["transactions"] is P.parse_transactions


def test_one_row_per_transaction_with_its_team():
    df = P.parse_transactions(load_fixture("espn", "transactions_nba"))
    assert df.height == 25
    for col in ("date", "description", "team_id", "team_abbreviation", "team_display_name"):
        assert col in df.columns


def test_an_empty_or_malformed_payload_is_a_zero_row_frame():
    assert P.parse_transactions({}).height == 0
    assert P.parse_transactions({"transactions": "nope"}).height == 0
