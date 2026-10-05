import sqlite3

from sdv_docs.schema import ASSET, MANIFEST, SCHEMA_SQL, SCHEMA_VERSION, SEARCH_SQL


def _db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA_SQL)
    con.execute(
        "INSERT INTO functions VALUES ('load_nhl_pbp','python','sportsdataverse','m','nhl','loader',"
        "NULL,'Load NHL play-by-play.','load_nhl_pbp(seasons)','u')"
    )
    con.execute("INSERT INTO params VALUES ('load_nhl_pbp','seasons','int',1,NULL,'seasons (>= 2010)')")
    con.executescript(SEARCH_SQL)
    return con


def test_fts_splits_identifiers_on_underscores():
    con = _db()
    q = "SELECT name FROM search WHERE search MATCH ?"
    assert con.execute(q, ['"nhl" AND "pbp"']).fetchall() == [("load_nhl_pbp",)]
    assert con.execute(q, ['"load_nhl_pbp"']).fetchall() == [("load_nhl_pbp",)]


def test_function_search_body_includes_param_descriptions():
    con = _db()
    assert con.execute("SELECT name FROM search WHERE search MATCH '\"2010\"'").fetchall() == [("load_nhl_pbp",)]


def test_asset_names_carry_the_schema_version():
    assert ASSET == f"sdv_docs_v{SCHEMA_VERSION}.sqlite"
    assert MANIFEST == f"manifest_v{SCHEMA_VERSION}.json"
