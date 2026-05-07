"""sdv-py NFL static datasets — nflreadpy parity exports.

Three module-level dicts shipped at import time:

- ``team_abbr_mapping``: every historical team abbreviation -> current
  abbreviation (relocations FOLDED). Use when you want to canonicalize
  historical data to today's franchise names — e.g. ``"OAK" -> "LV"``,
  ``"SD" -> "LAC"``, ``"STL" -> "LA"``.
- ``team_abbr_mapping_norelocate``: every historical team abbreviation ->
  itself, with typos / variant codes resolved. Use when you want to PRESERVE
  the historical franchise identity — ``"OAK"`` stays ``"OAK"`` rather than
  becoming ``"LV"``, but variant casings / spellings still resolve.
- ``player_name_mapping``: common name variants -> canonical name. Used to
  cross-reference players across sources where naming conventions differ.

The data is bundled inline (rather than read from a separate JSON file at
import time) so the dicts ship cleanly inside any wheel / sdist without
relying on the project's ``package_data`` declaration. Total payload is
~12 KB across the three dicts, which is small enough that the inline form
costs nothing in import time and removes one class of packaging surprise.

Source of truth: nflreadpy's ``data/`` parquet files
(https://github.com/nflverse/nflreadpy/tree/main/src/nflreadpy/data).
The R-side equivalent is the ``nflreadr`` package, which exposes the same
mappings as polars DataFrames with two columns ``name`` / ``value``.

To refresh from upstream:

    1. Download the three parquets:
       https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/team_abbr_mapping.parquet
       https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/team_abbr_mapping_norelocate.parquet
       https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/player_name_mapping.parquet
    2. For each, ``polars.read_parquet(...).unique(subset=['name'],
       keep='first', maintain_order=True)`` then build a
       ``dict(zip(name, value))``. The ``unique(keep='first')`` step
       deduplicates the small number of full-team-name keys that appear
       twice in the upstream ``team_abbr_mapping_norelocate`` parquet
       (e.g. ``"RAMS"`` -> ``"STL"`` and ``"RAMS"`` -> ``"LA"``); keeping
       the first occurrence preserves the "norelocate" semantics by
       picking the historical (older) abbreviation.
    3. Replace the three literals below with the new dicts (sorted keys
       for diff-friendliness).

The dicts are loaded eagerly at import time so they behave like
nflreadpy's module-level exports.

Example:
    Canonicalize a relocated franchise::

        from sportsdataverse.nfl import team_abbr_mapping
        team_abbr_mapping["OAK"]  # -> "LV"
        team_abbr_mapping["SD"]   # -> "LAC"
        team_abbr_mapping["STL"]  # -> "LA"

    Preserve historical identity (no relocation fold)::

        from sportsdataverse.nfl import team_abbr_mapping_norelocate
        team_abbr_mapping_norelocate["OAK"]  # -> "OAK"

    Resolve a player-name variant::

        from sportsdataverse.nfl import player_name_mapping
        player_name_mapping["Pat Mahomes"]  # -> canonical "Patrick Mahomes"

    Use defensively in a polars pipeline::

        import polars as pl
        from sportsdataverse.nfl import load_nfl_pbp, team_abbr_mapping

        pbp = (
            load_nfl_pbp(seasons=[2024])
            .with_columns(
                home_team=pl.col("home_team").map_elements(
                    lambda t: team_abbr_mapping.get(t, t), return_dtype=pl.Utf8
                )
            )
        )

See Also:
    * `nflverse`_ -- full data ecosystem (R + Python)
    * `nflreadpy`_ -- direct nflverse Python bindings (mirrors these dicts)

.. _nflverse: https://nflverse.nflverse.com
.. _nflreadpy: https://github.com/nflverse/nflreadpy
"""

from __future__ import annotations

from typing import Dict

# ---------------------------------------------------------------------------
# team_abbr_mapping (143 entries)
# ---------------------------------------------------------------------------
# Historical / variant abbreviations FOLDED into current franchise codes.
# Relocated franchises map to their post-relocation abbreviation:
#   OAK -> LV, SD -> LAC, STL -> LA.
team_abbr_mapping: Dict[str, str] = {
    "49ERS": "SF",
    "AFC": "AFC",
    "ARI": "ARI",
    "ARIZONA CARDINALS": "ARI",
    "ARZ": "ARI",
    "ATL": "ATL",
    "ATLANTA FALCONS": "ATL",
    "BAL": "BAL",
    "BALTIMORE RAVENS": "BAL",
    "BEARS": "CHI",
    "BENGALS": "CIN",
    "BILLS": "BUF",
    "BLT": "BAL",
    "BRONCOS": "DEN",
    "BROWNS": "CLE",
    "BUCCANEERS": "TB",
    "BUF": "BUF",
    "BUFFALO BILLS": "BUF",
    "CAR": "CAR",
    "CARDINALS": "ARI",
    "CAROLINA PANTHERS": "CAR",
    "CHARGERS": "LAC",
    "CHI": "CHI",
    "CHICAGO BEARS": "CHI",
    "CHIEFS": "KC",
    "CIN": "CIN",
    "CINCINNATI BENGALS": "CIN",
    "CLE": "CLE",
    "CLEVELAND BROWNS": "CLE",
    "CLT": "IND",
    "CLV": "CLE",
    "COLTS": "IND",
    "COMMANDERS": "WAS",
    "COWBOYS": "DAL",
    "CRD": "ARI",
    "DAL": "DAL",
    "DALLAS COWBOYS": "DAL",
    "DEN": "DEN",
    "DENVER BRONCOS": "DEN",
    "DET": "DET",
    "DETROIT LIONS": "DET",
    "DOLPHINS": "MIA",
    "EAGLES": "PHI",
    "FALCONS": "ATL",
    "FOOTBALL TEAM": "WAS",
    "GB": "GB",
    "GBP": "GB",
    "GIANTS": "NYG",
    "GNB": "GB",
    "GREEN BAY PACKERS": "GB",
    "HOU": "HOU",
    "HOUSTON TEXANS": "HOU",
    "HST": "HOU",
    "HTX": "HOU",
    "IND": "IND",
    "INDIANAPOLIS COLTS": "IND",
    "JAC": "JAX",
    "JACKSONVILLE JAGUARS": "JAX",
    "JAGUARS": "JAX",
    "JAX": "JAX",
    "JETS": "NYJ",
    "KAN": "KC",
    "KANSAS CITY CHIEFS": "KC",
    "KC": "KC",
    "KCC": "KC",
    "LA": "LA",
    "LAC": "LAC",
    "LACH": "LAC",
    "LAR": "LA",
    "LARM": "LA",
    "LAS VEGAS RAIDERS": "LV",
    "LIONS": "DET",
    "LOS ANGELES CHARGERS": "LAC",
    "LOS ANGELES RAMS": "LA",
    "LV": "LV",
    "LVR": "LV",
    "MIA": "MIA",
    "MIAMI DOLPHINS": "MIA",
    "MIN": "MIN",
    "MINNESOTA VIKINGS": "MIN",
    "NE": "NE",
    "NEP": "NE",
    "NEW ENGLAND PATRIOTS": "NE",
    "NEW ORLEANS SAINTS": "NO",
    "NEW YORK GIANTS": "NYG",
    "NEW YORK JETS": "NYJ",
    "NFC": "NFC",
    "NFL": "NFL",
    "NINERS": "SF",
    "NO": "NO",
    "NOR": "NO",
    "NOS": "NO",
    "NWE": "NE",
    "NYG": "NYG",
    "NYJ": "NYJ",
    "OAK": "LV",
    "OAKLAND RAIDERS": "LV",
    "OTI": "TEN",
    "PACKERS": "GB",
    "PANTHERS": "CAR",
    "PATRIOTS": "NE",
    "PHI": "PHI",
    "PHILADELPHIA EAGLES": "PHI",
    "PHO": "ARI",
    "PIT": "PIT",
    "PITTSBURGH STEELERS": "PIT",
    "RAI": "LV",
    "RAIDERS": "LV",
    "RAM": "LA",
    "RAMS": "LA",
    "RAV": "BAL",
    "RAVENS": "BAL",
    "REDSKINS": "WAS",
    "SAINTS": "NO",
    "SAN DIEGO CHARGERS": "LAC",
    "SAN FRANCISCO 49ERS": "SF",
    "SD": "LAC",
    "SDC": "LAC",
    "SDG": "LAC",
    "SEA": "SEA",
    "SEAHAWKS": "SEA",
    "SEATTLE SEAHAWKS": "SEA",
    "SF": "SF",
    "SFO": "SF",
    "SL": "LA",
    "ST LOUIS RAMS": "LA",
    "STEELERS": "PIT",
    "STL": "LA",
    "TAM": "TB",
    "TAMPA BAY BUCCANEERS": "TB",
    "TB": "TB",
    "TBB": "TB",
    "TEN": "TEN",
    "TENNESSEE TITANS": "TEN",
    "TEXANS": "HOU",
    "TITANS": "TEN",
    "VIKINGS": "MIN",
    "WAS": "WAS",
    "WASHINGTON COMMANDERS": "WAS",
    "WASHINGTON FOOTBALL TEAM": "WAS",
    "WASHINGTON REDSKINS": "WAS",
    "WFT": "WAS",
    "WSH": "WAS",
}


# ---------------------------------------------------------------------------
# team_abbr_mapping_norelocate (143 entries)
# ---------------------------------------------------------------------------
# Historical / variant abbreviations resolved to their original-era code.
# Preserves the historical franchise identity:
#   OAK -> OAK (not LV), SD -> SD (not LAC), STL -> STL (not LA).
# Useful when you're working with pre-relocation game data and don't want
# to lose the original franchise context.
team_abbr_mapping_norelocate: Dict[str, str] = {
    "49ERS": "SF",
    "AFC": "AFC",
    "ARI": "ARI",
    "ARIZONA CARDINALS": "ARI",
    "ARZ": "ARI",
    "ATL": "ATL",
    "ATLANTA FALCONS": "ATL",
    "BAL": "BAL",
    "BALTIMORE RAVENS": "BAL",
    "BEARS": "CHI",
    "BENGALS": "CIN",
    "BILLS": "BUF",
    "BLT": "BAL",
    "BRONCOS": "DEN",
    "BROWNS": "CLE",
    "BUCCANEERS": "TB",
    "BUF": "BUF",
    "BUFFALO BILLS": "BUF",
    "CAR": "CAR",
    "CARDINALS": "ARI",
    "CAROLINA PANTHERS": "CAR",
    "CHARGERS": "SD",
    "CHI": "CHI",
    "CHICAGO BEARS": "CHI",
    "CHIEFS": "KC",
    "CIN": "CIN",
    "CINCINNATI BENGALS": "CIN",
    "CLE": "CLE",
    "CLEVELAND BROWNS": "CLE",
    "CLT": "IND",
    "CLV": "CLE",
    "COLTS": "IND",
    "COMMANDERS": "WAS",
    "COWBOYS": "DAL",
    "CRD": "ARI",
    "DAL": "DAL",
    "DALLAS COWBOYS": "DAL",
    "DEN": "DEN",
    "DENVER BRONCOS": "DEN",
    "DET": "DET",
    "DETROIT LIONS": "DET",
    "DOLPHINS": "MIA",
    "EAGLES": "PHI",
    "FALCONS": "ATL",
    "FOOTBALL TEAM": "WAS",
    "GB": "GB",
    "GBP": "GB",
    "GIANTS": "NYG",
    "GNB": "GB",
    "GREEN BAY PACKERS": "GB",
    "HOU": "HOU",
    "HOUSTON TEXANS": "HOU",
    "HST": "HOU",
    "HTX": "HOU",
    "IND": "IND",
    "INDIANAPOLIS COLTS": "IND",
    "JAC": "JAX",
    "JACKSONVILLE JAGUARS": "JAX",
    "JAGUARS": "JAX",
    "JAX": "JAX",
    "JETS": "NYJ",
    "KAN": "KC",
    "KANSAS CITY CHIEFS": "KC",
    "KC": "KC",
    "KCC": "KC",
    "LA": "LA",
    "LAC": "LAC",
    "LACH": "LAC",
    "LAR": "LA",
    "LARM": "LA",
    "LAS VEGAS RAIDERS": "LV",
    "LIONS": "DET",
    "LOS ANGELES CHARGERS": "LAC",
    "LOS ANGELES RAMS": "LA",
    "LV": "LV",
    "LVR": "LV",
    "MIA": "MIA",
    "MIAMI DOLPHINS": "MIA",
    "MIN": "MIN",
    "MINNESOTA VIKINGS": "MIN",
    "NE": "NE",
    "NEP": "NE",
    "NEW ENGLAND PATRIOTS": "NE",
    "NEW ORLEANS SAINTS": "NO",
    "NEW YORK GIANTS": "NYG",
    "NEW YORK JETS": "NYJ",
    "NFC": "NFC",
    "NFL": "NFL",
    "NINERS": "SF",
    "NO": "NO",
    "NOR": "NO",
    "NOS": "NO",
    "NWE": "NE",
    "NYG": "NYG",
    "NYJ": "NYJ",
    "OAK": "OAK",
    "OAKLAND RAIDERS": "OAK",
    "OTI": "TEN",
    "PACKERS": "GB",
    "PANTHERS": "CAR",
    "PATRIOTS": "NE",
    "PHI": "PHI",
    "PHILADELPHIA EAGLES": "PHI",
    "PHO": "ARI",
    "PIT": "PIT",
    "PITTSBURGH STEELERS": "PIT",
    "RAI": "LV",
    "RAIDERS": "OAK",
    "RAM": "LA",
    "RAMS": "STL",
    "RAV": "BAL",
    "RAVENS": "BAL",
    "REDSKINS": "WAS",
    "SAINTS": "NO",
    "SAN DIEGO CHARGERS": "SD",
    "SAN FRANCISCO 49ERS": "SF",
    "SD": "SD",
    "SDC": "SD",
    "SDG": "SD",
    "SEA": "SEA",
    "SEAHAWKS": "SEA",
    "SEATTLE SEAHAWKS": "SEA",
    "SF": "SF",
    "SFO": "SF",
    "SL": "STL",
    "ST LOUIS RAMS": "STL",
    "STEELERS": "PIT",
    "STL": "STL",
    "TAM": "TB",
    "TAMPA BAY BUCCANEERS": "TB",
    "TB": "TB",
    "TBB": "TB",
    "TEN": "TEN",
    "TENNESSEE TITANS": "TEN",
    "TEXANS": "HOU",
    "TITANS": "TEN",
    "VIKINGS": "MIN",
    "WAS": "WAS",
    "WASHINGTON COMMANDERS": "WAS",
    "WASHINGTON FOOTBALL TEAM": "WAS",
    "WASHINGTON REDSKINS": "WAS",
    "WFT": "WAS",
    "WSH": "WAS",
}


# ---------------------------------------------------------------------------
# player_name_mapping (136 entries)
# ---------------------------------------------------------------------------
# Common-name variants -> canonical name. Examples:
#   "Mitch Trubisky" -> "Mitchell Trubisky"
#   "Pat Mahomes"    -> "Patrick Mahomes"
#   "Sauce Gardner"  -> "Ahmad Gardner"
# Useful when joining player data across sources that disagree about
# nicknames vs legal names (or about diacritics / capitalization).
player_name_mapping: Dict[str, str] = {
    "AJ Johnson": "Alexander Johnson",
    "Ade Ogundeji": "Adetokunbo Ogundeji",
    "Alexander Armah": "Alex Armah",
    "AndrewVanGinkel": "Andrew Van Ginkel",
    "Art Maulet": "Arthur Maulet",
    "Bless Austin": "Blessuan Austin",
    "Boogie Basham": "Carlos Basham",
    "Brandon Watson": "Brandon Rusnak",
    "Cam Sample": "Cameron Sample",
    "Cameron Batson": "Cam Batson",
    "Cameron Johnston": "Cam Johnston",
    "Cameron Lewis": "Cam Lewis",
    "Ced Wilson": "Cedrick Wilson",
    "Chatarius Atwell": "Tutu Atwell",
    "Chauncey Gardner-Johnson": "CJ Gardner-Johnson",
    "Chig Okonkwo": "Chigoziem Okonkwo",
    "Chris Beanie Wells": "Beanie Wells",
    "Christion Jones": "Chris Jones",
    "Christopher Herndon": "Chris Herndon",
    "Christopher Smith": "Chris Smith",
    "Crevon LeBlanc": "CreVon LeBlanc",
    "DaVon Hamilton": "Davon Hamilton",
    "Daquan Jones": "DaQuan Jones",
    "Darius Leonard": "Shaquille Leonard",
    "Darney Holmes": "Darnay Holmes",
    "Daron Payne": "DaRon Payne",
    "Dashon Polk": "DaShon Polk",
    "Dax Hill": "Daxton Hill",
    "DeMario Davis": "Demario Davis",
    "DeMario Douglas": "Demario Douglas",
    "Deandra Cobb": "DeAndra Cobb",
    "Deandrew Rubin": "DeAndrew Rubin",
    "Deangelo Hall": "DeAngelo Hall",
    "Deangelo Tyson": "DeAngelo Tyson",
    "Decobie Durant": "Cobie Durant",
    "Dee Eskridge": "DWayne Eskridge",
    "Dejon Gomes": "DeJon Gomes",
    "Dejuan Groce": "DeJuan Groce",
    "Delawrence Grant": "DeLawrence Grant",
    "Demarco Sampson": "DeMarco Sampson",
    "Demarcus Lawrence": "DeMarcus Lawrence",
    "Demarcus Van Dyke": "DeMarcus Van Dyke",
    "Demarcus Ware": "DeMarcus Ware",
    "Deonte Harris": "Deonte Harty",
    "Deshaun Foster": "DeShaun Foster",
    "Devante Parker": "DeVante Parker",
    "Devon Achane": "DeVon Achane",
    "Devonta Smith": "DeVonta Smith",
    "Drew Ogletree": "Andrew Ogletree",
    "Dru Phillips": "Andru Phillips",
    "Elerson G. Smith": "Elerson Smith",
    "Foley Fatukasi": "Folorunso Fatukasi",
    "Foye Oluokun": "Foyesade Oluokun",
    "Gabe Davis": "Gabriel Davis",
    "Gary Jennings Jr": "Gary Jennings",
    "Grant Dubose": "Grant DuBose",
    "Greg Rousseau": "Gregory Rousseau",
    "HaSean Clinton-Dix": "HaHa Clinton-Dix",
    "Hasean Clinton-Dix": "HaHa Clinton-Dix",
    "Hollywood Brown": "Marquise Brown",
    "Jackrabbit Jenkins": "Janoris Jenkins",
    "Jajuan Dawson": "JaJuan Dawson",
    "Jajuan Seider": "JaJuan Seider",
    "Jake Dolegala": "Jacob Dolegala",
    "Jake Martin": "Jacob Martin",
    "Jamycal Hasty": "JaMycal Hasty",
    "Jayson Oweh": "Odafe Oweh",
    "Jeffery Wilson": "Jeff Wilson",
    "Joe Fortson": "Jody Fortson",
    "Joe Tryon": "Joe Tryon-Shoyinka",
    "Johnathan Ford": "Rudy Ford",
    "Jonathan Brown": "Jon Brown",
    "Josh Perkins": "Joshua Perkins",
    "Joshua Jacobs": "Josh Jacobs",
    "Joshua Palmer": "Josh Palmer",
    "Joshua Uche": "Josh Uche",
    "JuJu Brents": "Julius Brents",
    "Ken Walker": "Kenneth Walker",
    "Ken-yon Rambo": "Ken-Yon Rambo",
    "Kerrith Whyte Jr": "Kerrith Whyte",
    "Khadarel Hodge": "KhaDarel Hodge",
    "Labrandon Toefield": "LaBrandon Toefield",
    "Lamical Perine": "LaMical Perine",
    "Lamont Jordan": "LaMont Jordan",
    "Latarence Dunbar": "LaTarence Dunbar",
    "Lavar Arrington": "LaVar Arrington",
    "Leroy Hill": "LeRoy Hill",
    "Levante Bellamy": "LeVante Bellamy",
    "Malaefou Mackenzie": "Malaefou MacKenzie",
    "Mark Legree": "Mark LeGree",
    "Matt Judon": "Matthew Judon",
    "Matt Slater": "Matthew Slater",
    "Matthew Ioannidis": "Matt Ioannidis",
    "Maurice Drew": "Maurice Jones-Drew",
    "Michael Vick": "Mike Vick",
    "Mike Badgley": "Michael Badgley",
    "Mike Danna": "Michael Danna",
    "Mike Hall": "Michael Hall",
    "Mike Jordan": "Michael Jordan",
    "Mitch Trubisky": "Mitchell Trubisky",
    "Nathan Gerry": "Nate Gerry",
    "Nathan Landman": "Nate Landman",
    "Nathaniel Dell": "Tank Dell",
    "Navorro Bowman": "NaVorro Bowman",
    "Nicholas Williams": "Nick Williams",
    "Nick Westbrook": "Nick Westbrook-Ikhine",
    "Nickell Robey": "Nickell Robey-Coleman",
    "Norman Lejeune": "Norman LeJeune",
    "Ogbo Okoronkwo": "Ogbonnia Okoronkwo",
    "Olasunkanmi Adeniyi": "Ola Adeniyi",
    "Pat Jones": "Patrick Jones",
    "Pat Mahomes": "Patrick Mahomes",
    "Pat Surtain": "Patrick Surtain",
    "Phillip Walker": "PJ Walker",
    "Quan Martin": "Jartavius Martin",
    "R jay Soward": "R Jay Soward",
    "Riq Woolen": "Tariq Woolen",
    "Robby Anderson": "Robbie Anderson",
    "Robert Kelley": "Rob Kelley",
    "Sauce Gardner": "Ahmad Gardner",
    "Scotty Miller": "Scott Miller",
    "Sean Bunting": "Sean Murphy-Bunting",
    "Sebastian Joseph": "Sebastian Joseph-Day",
    "Seth Devalve": "Seth DeValve",
    "Shaq Barrett": "Shaquil Barrett",
    "Shaq Griffin": "Shaquill Griffin",
    "Stephen Hauschka": "Steven Hauschka",
    "Steve Mclendon": "Steve McLendon",
    "Travis Carrie": "TJ Carrie",
    "Travis Laboy": "Travis LaBoy",
    "Tron Lafavor": "Tron LaFavor",
    "Ugochukwu Amadi": "Ugo Amadi",
    "Yaya Diaby": "YaYa Diaby",
    "Zach Carter": "Zachary Carter",
    "Zeke Elliott": "Ezekiel Elliott",
    "Zeke Turner": "Ezekiel Turner",
}


__all__ = [
    "team_abbr_mapping",
    "team_abbr_mapping_norelocate",
    "player_name_mapping",
]
