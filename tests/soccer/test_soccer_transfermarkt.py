"""Transfermarkt loaders (served real rows) and the ESPN -> Transfermarkt team crosswalk with its gate."""

from __future__ import annotations

import gzip
import io
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError, SchemaDriftWarning
from sportsdataverse.soccer import (
    load_soccer_transfermarkt_clubs,
    load_soccer_transfermarkt_competitions,
    load_soccer_transfermarkt_player_valuations,
    load_soccer_transfermarkt_players,
    load_soccer_transfermarkt_transfers,
    soccer_espn_transfermarkt_team_crosswalk,
)
from sportsdataverse import _codegen_runtime as rt
from sportsdataverse.soccer import soccer_transfermarkt as tm
from tests.conftest import skip_if_no_live
from tools.build_soccer_espn_reep_crosswalk import norm

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "transfermarkt"

#: Pre-registered 2026-11-26, before the bridge was scored: of the ESPN teams in the seven first-tier leagues
#: (the Transfermarkt dataset has no second tiers), >= 90% must reach a Transfermarkt clubs row, and >= 75% in
#: every league. Measured: 139/144 (0.965); lowest fra.1, ger.1 and mex.1, 17/18 each.
FIRST_TIER = ("eng.1", "esp.1", "ger.1", "ita.1", "fra.1", "usa.1", "mex.1")
GATE = {"min_share": 0.90, "min_league_share": 0.75}


_REAL_FROM_RELEASE = tm._from_release


@pytest.fixture(autouse=True)
def _source_path(monkeypatch):
    """Every test reads the original-source path unless it opts back into the mirror (``mirror``)."""
    monkeypatch.setattr(tm, "_from_release", lambda table: None)


@pytest.fixture
def mirror(monkeypatch):
    """The real release-mirror path, its parquet read served from the typed fixture rows."""
    monkeypatch.setattr(tm, "_from_release", _REAL_FROM_RELEASE)
    calls: list[str] = []

    def fake_release(url):
        calls.append(url)
        table = url.rsplit("/", 1)[1].removesuffix(".parquet")
        return rt._parse_release(url, io.BytesIO(_parquet(table)))  # the runtime's own parquet parse

    monkeypatch.setattr(tm, "_fetch_release_parquet", fake_release)
    return calls


def _parquet(table: str, frame: pl.DataFrame | None = None) -> bytes:
    """The mirror's bytes for one table, written the way the publish script writes them."""
    buf = io.BytesIO()
    (
        frame if frame is not None else tm._typed(pl.read_csv(FIX / f"{table}.csv", infer_schema=False), table)
    ).write_parquet(buf)
    return buf.getvalue()


def _release_bytes(monkeypatch, body: bytes) -> None:
    monkeypatch.setattr(tm, "_fetch_release_parquet", lambda url: rt._parse_release(url, io.BytesIO(body)))


class _Resp:
    def __init__(self, body: bytes, status_code: int = 200):
        self.content = body
        self.status_code = status_code
        self.text = ""


@pytest.fixture
def served(monkeypatch):
    """Serve each fixture table gzipped, as the published files are."""
    calls: list[str] = []

    def fake_download(url, *a, **k):
        calls.append(url)
        table = url.rsplit("/", 1)[1].removesuffix(".csv.gz")
        return _Resp(gzip.compress((FIX / f"{table}.csv").read_bytes()))

    monkeypatch.setattr(tm, "download", fake_download)
    return calls


def test_players_types_and_url(served) -> None:
    df = load_soccer_transfermarkt_players()
    assert served == [tm.TRANSFERMARKT_DATA_URL.format(table="players")]
    assert df.height == pl.read_csv(FIX / "players.csv", infer_schema=False).height
    assert list(df.columns) == list(tm._TABLES["players"])
    assert df.schema["player_id"] == df.schema["current_club_id"] == pl.Int64
    assert df.schema["date_of_birth"] == df.schema["contract_expiration_date"] == pl.Date
    assert df.schema["market_value_in_eur"] == pl.Int64
    assert df["player_id"].n_unique() == df.height
    assert df["date_of_birth"].null_count() >= 2 and df["height_in_cm"].null_count() >= 2


def test_every_table_loads_in_its_dtypes(served) -> None:
    for fn, table in (
        (load_soccer_transfermarkt_clubs, "clubs"),
        (load_soccer_transfermarkt_player_valuations, "player_valuations"),
        (load_soccer_transfermarkt_transfers, "transfers"),
        (load_soccer_transfermarkt_competitions, "competitions"),
    ):
        df = fn()
        assert dict(df.schema) == tm._TABLES[table], table
        assert df.height == pl.read_csv(FIX / f"{table}.csv", infer_schema=False).height
    clubs = load_soccer_transfermarkt_clubs()
    assert clubs.filter(pl.col("club_id") == 131)["name"].to_list() == ["FC Barcelona"]
    moves = load_soccer_transfermarkt_transfers()
    assert moves["transfer_fee"].null_count() >= 3


def test_pandas_keeps_nullable_ids(served) -> None:
    pdf = load_soccer_transfermarkt_players(return_as_pandas=True)
    assert str(pdf["current_national_team_id"].dtype).startswith("int64")


def test_a_non_integer_value_is_refused_with_a_warning(monkeypatch) -> None:
    body = (FIX / "players.csv").read_text(encoding="utf-8").replace(",184,", ",184.5,", 1).encode()
    monkeypatch.setattr(tm, "download", lambda url, *a, **k: _Resp(gzip.compress(body)))
    with pytest.warns(SchemaDriftWarning, match="height_in_cm"):
        df = load_soccer_transfermarkt_players()
    assert df.schema["height_in_cm"] == pl.Utf8 and df.schema["player_id"] == pl.Int64


def test_a_non_gzip_body_or_a_missing_column_raises(monkeypatch) -> None:
    monkeypatch.setattr(tm, "download", lambda url, *a, **k: _Resp(b"<html>maintenance</html>"))
    with pytest.raises(AssetFetchError, match="gzip CSV"):
        load_soccer_transfermarkt_clubs()
    body = (FIX / "clubs.csv").read_text(encoding="utf-8").replace("stadium_seats", "seats", 1).encode()
    monkeypatch.setattr(tm, "download", lambda url, *a, **k: _Resp(gzip.compress(body)))
    with pytest.raises(AssetFetchError, match="stadium_seats"):
        load_soccer_transfermarkt_clubs()


def test_team_crosswalk_shape() -> None:
    xw = soccer_espn_transfermarkt_team_crosswalk()
    assert xw.schema["espn_team_id"] == pl.Utf8 and xw.schema["club_id"] == pl.Int64
    assert xw["espn_team_id"].n_unique() == xw.height
    ids = xw["club_id"].drop_nulls()
    assert ids.n_unique() == ids.len()
    assert set(xw["match_method"].drop_nulls().unique().to_list()) <= {"reep", "review"}
    assert (xw["club_id"].is_null() == xw["match_method"].is_null()).all()
    row = {r["espn_team_id"]: r for r in xw.iter_rows(named=True)}
    assert row["83"]["club_id"] == 131 and row["83"]["match_method"] == "review"  # Barcelona
    assert row["360"]["club_id"] == 985 and row["360"]["match_method"] == "reep"  # Manchester United
    assert soccer_espn_transfermarkt_team_crosswalk(league="ita.1").height == 20
    with pytest.raises(ValueError, match="league must be one of"):
        soccer_espn_transfermarkt_team_crosswalk(league="usa.nwsl")


def test_reviewed_ids_only_fill_teams_reep_has_no_id_for() -> None:
    from sportsdataverse.soccer import soccer_espn_reep_team_crosswalk

    reep = soccer_espn_reep_team_crosswalk()
    for espn_id in tm._TM_REVIEWED:
        assert reep.filter(pl.col("espn_team_id") == espn_id)["key_transfermarkt"].to_list() == [None]


def test_team_crosswalk_passes_the_pre_registered_gate(served) -> None:
    clubs = load_soccer_transfermarkt_clubs().select("club_id", pl.col("name").alias("tm_name"))
    xw = soccer_espn_transfermarkt_team_crosswalk().filter(pl.col("espn_league").is_in(FIRST_TIER))
    assert xw.schema["club_id"] == clubs.schema["club_id"]
    j = xw.join(clubs, on="club_id", how="left")
    share = j["tm_name"].is_not_null().mean()
    per = j.group_by("espn_league").agg(pl.col("tm_name").is_not_null().mean().alias("share"))
    assert share >= GATE["min_share"], share
    assert per["share"].min() >= GATE["min_league_share"], per
    # the reep Transfermarkt ids name the same club: every matched pair shares a name token
    hit = j.filter(pl.col("tm_name").is_not_null())
    unshared = {
        e
        for e, t in hit.select("espn_display_name", "tm_name").rows()
        if not set(norm(e).split()) & set(norm(t).split())
    }
    # the names that share no token are reviewed variants of the same club
    assert unshared <= {"Brest", "FC Cologne", "Hamburg SV", "Internazionale", "LAFC"}, unshared
    # every ESPN league lands in exactly one Transfermarkt league
    leagues = load_soccer_transfermarkt_clubs().select("club_id", "domestic_competition_id")
    per_league = (
        xw.join(leagues, on="club_id").group_by("espn_league").agg(pl.col("domestic_competition_id").n_unique())
    )
    assert (per_league["domestic_competition_id"] == 1).all(), per_league


@pytest.mark.parametrize(("status", "error"), [(404, NoDataError), (422, ValueError), (503, AssetFetchError)])
def test_a_failed_fetch_raises_its_error_never_an_empty_frame(monkeypatch, status, error) -> None:
    def fake_download(url, *a, **k):
        if status == 404:
            raise NoDataError(f"{url} answered 404")  # download() raises a 404 itself
        return _Resp(b"", status)

    monkeypatch.setattr(tm, "download", fake_download)
    with pytest.raises(error):
        load_soccer_transfermarkt_competitions()


def test_a_dropped_connection_or_a_truncated_body_is_a_failed_fetch(monkeypatch) -> None:
    import requests

    def dropped(url, *a, **k):
        raise requests.ConnectionError("reset")

    monkeypatch.setattr(tm, "download", dropped)
    with pytest.raises(AssetFetchError):
        load_soccer_transfermarkt_competitions()
    body = gzip.compress((FIX / "competitions.csv").read_bytes())
    for bad in (body[: len(body) // 2], gzip.compress(b"")):
        monkeypatch.setattr(tm, "download", lambda url, *a, b=bad, **k: _Resp(b))
        with pytest.raises(AssetFetchError, match="not a readable gzip CSV"):
            load_soccer_transfermarkt_competitions()
    header = (FIX / "competitions.csv").read_bytes().splitlines(keepends=True)[0]
    monkeypatch.setattr(tm, "download", lambda url, *a, **k: _Resp(gzip.compress(header)))
    with pytest.raises(AssetFetchError, match="header and no rows"):
        load_soccer_transfermarkt_competitions()


def test_a_date_with_a_time_is_refused_not_truncated(monkeypatch) -> None:
    text = (FIX / "player_valuations.csv").read_text(encoding="utf-8")
    first = pl.read_csv(io.StringIO(text), infer_schema=False)["date"][0]
    body = text.replace(f",{first},", f",{first} 15:30:00,", 1).encode()
    monkeypatch.setattr(tm, "download", lambda url, *a, **k: _Resp(gzip.compress(body)))
    with pytest.warns(SchemaDriftWarning, match="date"):
        df = load_soccer_transfermarkt_player_valuations()
    assert df.schema["date"] == pl.Utf8


def test_bundled_reep_transfermarkt_ids_are_integers() -> None:
    from sportsdataverse.soccer import soccer_espn_reep_team_crosswalk

    ids = soccer_espn_reep_team_crosswalk()["key_transfermarkt"].drop_nulls()
    assert ids.str.contains(r"^[0-9]+$").all()


def test_the_mirror_is_read_first_and_the_source_is_not_touched(mirror, monkeypatch) -> None:
    def no_source(*a, **k):
        raise AssertionError("the original source was read while the mirror answered")

    monkeypatch.setattr(tm, "download", no_source)
    for table in tm._TABLES:
        df = getattr(tm, f"load_soccer_transfermarkt_{table}")()
        assert dict(df.schema) == tm._TABLES[table]
        assert df.height == pl.read_csv(FIX / f"{table}.csv", infer_schema=False).height
    assert mirror == [tm.TRANSFERMARKT_RELEASE_URL.format(table=t) for t in tm._TABLES]


def _clubs() -> pl.DataFrame:
    return tm._typed(pl.read_csv(FIX / "clubs.csv", infer_schema=False), "clubs")


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (NoDataError("absent"), "NoDataError: absent"),
        (AssetFetchError("rate limited"), "AssetFetchError: rate limited"),
        (b"not a parquet file at all", "ArrowInvalid"),
        (b"", "Error"),
        ("corrupt_footer", "OSError"),
        ("dropped_column", "are not the loader's"),
        ("reordered", "are not the loader's"),
        ("int32_id", "are not the loader's"),
        ("empty", "has no rows"),
    ],
    ids=["absent", "failed", "garbage", "empty_body", "corrupt_footer", "dropped", "reordered", "int32", "no_rows"],
)
def test_an_unusable_mirror_warns_naming_why_and_falls_back(served, monkeypatch, failure, reason) -> None:
    monkeypatch.setattr(tm, "_from_release", _REAL_FROM_RELEASE)
    if isinstance(failure, Exception):

        def raise_it(url):
            raise failure

        monkeypatch.setattr(tm, "_fetch_release_parquet", raise_it)
    elif isinstance(failure, bytes):
        _release_bytes(monkeypatch, failure)
    elif failure == "corrupt_footer":
        good = _parquet("clubs")
        _release_bytes(monkeypatch, good[:-8] + b"\0\0\0\0PAR1")
    else:
        frame = {
            "dropped_column": lambda: _clubs().drop("url"),
            "reordered": lambda: _clubs().select(reversed(_clubs().columns)),
            "int32_id": lambda: _clubs().with_columns(pl.col("club_id").cast(pl.Int32)),
            "empty": lambda: _clubs().head(0),
        }[failure]()
        _release_bytes(monkeypatch, _parquet("clubs", frame))
    with pytest.warns(UserWarning, match=f"unavailable \\(.*{reason}.*\\); reading the original source"):
        df = load_soccer_transfermarkt_clubs()
    assert served == [tm.TRANSFERMARKT_DATA_URL.format(table="clubs")]
    assert df.schema == pl.Schema(tm._TABLES["clubs"]) and df.height > 0


def test_an_unexpected_mirror_error_is_not_swallowed(monkeypatch) -> None:
    monkeypatch.setattr(tm, "_from_release", _REAL_FROM_RELEASE)

    def boom(url):
        raise TypeError("a bug, not a fetch failure")

    monkeypatch.setattr(tm, "_fetch_release_parquet", boom)
    with pytest.raises(TypeError):
        load_soccer_transfermarkt_clubs()


def test_mirror_and_pandas(mirror) -> None:
    pdf = load_soccer_transfermarkt_players(return_as_pandas=True)
    assert str(pdf["current_national_team_id"].dtype).startswith("int64")


@skip_if_no_live
@pytest.mark.parametrize("table", list(tm._TABLES))
def test_live_release_mirror(monkeypatch, table) -> None:
    monkeypatch.setattr(tm, "_from_release", _REAL_FROM_RELEASE)

    def no_source(*a, **k):
        raise AssertionError("fell back to the original source")

    monkeypatch.setattr(tm, "download", no_source)
    df = getattr(tm, f"load_soccer_transfermarkt_{table}")()
    assert df.schema == pl.Schema(tm._TABLES[table]) and df.height > 0


@skip_if_no_live
def test_live_competitions_table() -> None:
    comps = load_soccer_transfermarkt_competitions()
    assert dict(comps.schema) == tm._TABLES["competitions"]
    assert "GB1" in comps["competition_id"].to_list()
