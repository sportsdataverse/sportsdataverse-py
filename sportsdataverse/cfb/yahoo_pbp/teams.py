"""ESPN college-football team id -> Yahoo ``ncaaf.t.N`` number (static snapshot).

The Yahoo CFB **game** id is a pure function of the kickoff date and the *home* team's
Yahoo number (``ncaaf.g.{YYYYMMDD}{yahoo home id:04d}``, 157/157 verified 2014-2026 and
934/934 against the stored 2025 crosswalk), so translating an ESPN event id into a Yahoo
one offline needs exactly this one table -- which is why it is committed rather than
fetched: the failover this adapter exists for is an ESPN outage, and
:func:`sportsdataverse.cfb.cfb_teams_crosswalk` builds its ESPN leg on ESPN.

Built 2026-09-18 with ``cfb_teams_crosswalk(season=2026, week=2, providers=("espn",
"yahoo"))``: 261 teams matched on the normalised name. Yahoo lists no team it has never
covered, which is one reason an FCS-hosted game resolves to no Yahoo id at all.

# ponytail: a static snapshot. A new FBS member (or a rename that breaks the name match)
# is a missing key, which degrades to "no Yahoo id" and hands the game to the next source --
# never to a wrong id. The upgrade path is the id map's ``TEAM_SCHEMA.yahoo_team_id``
# column, which this module is only the offline fallback for.
"""

from __future__ import annotations

from typing import Dict, Optional, Union

_YAHOO_TEAM_ID_BY_ESPN: Dict[str, int] = {
    "2": 75,  # Auburn Tigers
    "5": 207,  # UAB Blazers
    "6": 2549,  # South Alabama Jaguars
    "8": 74,  # Arkansas Razorbacks
    "9": 58,  # Arizona State Sun Devils
    "12": 57,  # Arizona Wildcats
    "13": 108,  # Cal Poly Mustangs
    "16": 110,  # Sacramento State Hornets
    "21": 93,  # San Diego State Aztecs
    "23": 44,  # San José State Spartans
    "25": 59,  # California Golden Bears
    "26": 64,  # UCLA Bruins
    "30": 62,  # USC Trojans
    "36": 89,  # Colorado State Rams
    "38": 18,  # Colorado Buffaloes
    "41": 202,  # UConn Huskies
    "47": 146,  # Howard Bison
    "48": 195,  # Delaware Blue Hens
    "50": 145,  # Florida A&M Rattlers
    "55": 247,  # Jacksonville State Gamecocks
    "56": 2598,  # Stetson Hatters
    "57": 67,  # Florida Gators
    "58": 1019,  # South Florida Bulls
    "59": 4,  # Georgia Tech Yellow Jackets
    "61": 68,  # Georgia Bulldogs
    "62": 91,  # Hawai'i Rainbow Warriors
    "66": 19,  # Iowa State Cyclones
    "68": 112,  # Boise State Broncos
    "70": 114,  # Idaho Vandals
    "79": 124,  # Southern Illinois Salukis
    "84": 27,  # Indiana Hoosiers
    "87": 104,  # Notre Dame Fighting Irish
    "93": 154,  # Murray State Racers
    "96": 69,  # Kentucky Wildcats
    "97": 100,  # Louisville Cardinals
    "98": 224,  # Western Kentucky Hilltoppers
    "99": 76,  # LSU Tigers
    "103": 10,  # Boston College Eagles
    "107": 162,  # Holy Cross Crusaders
    "113": 204,  # Massachusetts Minutemen
    "119": 221,  # Towson Tigers
    "120": 5,  # Maryland Terrapins
    "127": 30,  # Michigan State Spartans
    "128": 298,  # Northern Michigan Wildcats
    "130": 29,  # Michigan Wolverines
    "135": 31,  # Minnesota Golden Gophers
    "142": 22,  # Missouri Tigers
    "145": 77,  # Ole Miss Rebels
    "147": 117,  # Montana State Bobcats
    "149": 116,  # Montana Grizzlies
    "150": 2,  # Duke Blue Devils
    "151": 99,  # East Carolina Pirates
    "152": 7,  # NC State Wolfpack
    "153": 6,  # North Carolina Tar Heels
    "154": 9,  # Wake Forest Demon Deacons
    "155": 308,  # North Dakota Fighting Hawks
    "158": 23,  # Nebraska Cornhuskers
    "160": 205,  # New Hampshire Wildcats
    "164": 13,  # Rutgers Scarlet Knights
    "166": 41,  # New Mexico State Aggies
    "167": 92,  # New Mexico Lobos
    "183": 14,  # Syracuse Orange
    "189": 49,  # Bowling Green Falcons
    "193": 53,  # Miami (OH) RedHawks
    "194": 33,  # Ohio State Buckeyes
    "195": 54,  # Ohio Bobcats
    "197": 25,  # Oklahoma State Cowboys
    "201": 24,  # Oklahoma Sooners
    "202": 107,  # Tulsa Golden Hurricane
    "204": 61,  # Oregon State Beavers
    "213": 34,  # Penn State Nittany Lions
    "218": 15,  # Temple Owls
    "221": 12,  # Pittsburgh Panthers
    "222": 199,  # Villanova Wildcats
    "227": 206,  # Rhode Island Rams
    "228": 1,  # Clemson Tigers
    "231": 174,  # Furman Paladins
    "233": 311,  # South Dakota Coyotes
    "235": 101,  # Memphis Tigers
    "236": 177,  # Chattanooga Mocs
    "238": 72,  # Vanderbilt Commodores
    "239": 79,  # Baylor Bears
    "242": 81,  # Rice Owls
    "245": 84,  # Texas A&M Aggies
    "248": 80,  # Houston Cougars
    "249": 182,  # North Texas Mean Green
    "251": 83,  # Texas Longhorns
    "252": 88,  # BYU Cougars
    "253": 111,  # Southern Utah Thunderbirds
    "254": 94,  # Utah Utes
    "256": 196,  # James Madison Dukes
    "257": 198,  # Richmond Spiders
    "258": 8,  # Virginia Cavaliers
    "259": 16,  # Virginia Tech Hokies
    "264": 65,  # Washington Huskies
    "265": 66,  # Washington State Cougars
    "275": 36,  # Wisconsin Badgers
    "276": 176,  # Marshall Thundering Herd
    "277": 17,  # West Virginia Mountaineers
    "278": 90,  # Fresno State Bulldogs
    "282": 122,  # Indiana State Sycamores
    "284": 243,  # Stonehill Skyhawks
    "290": 175,  # Georgia Southern Eagles
    "295": 2371,  # Old Dominion Monarchs
    "301": 169,  # San Diego Toreros
    "302": 263,  # UC Davis Aggies
    "304": 115,  # Idaho State Bengals
    "309": 45,  # Louisiana Ragin' Cajuns
    "311": 203,  # Maine Black Bears
    "322": 163,  # Lafayette Leopards
    "324": 2316,  # Coastal Carolina Chanticleers
    "326": 185,  # Texas State Bobcats
    "328": 46,  # Utah State Aggies
    "330": 234,  # Virginia State Trojans
    "331": 113,  # Eastern Washington Eagles
    "333": 73,  # Alabama Crimson Tide
    "338": 2610,  # Kennesaw State Owls
    "344": 78,  # Mississippi State Bulldogs
    "349": 97,  # Army Black Knights
    "356": 26,  # Illinois Fighting Illini
    "399": 546,  # UAlbany Great Danes
    "2000": 276,  # Abilene Christian Wildcats
    "2005": 87,  # Air Force Falcons
    "2006": 47,  # Akron Zips
    "2010": 356,  # Alabama A&M Bulldogs
    "2011": 187,  # Alabama State Hornets
    "2016": 188,  # Alcorn State Braves
    "2026": 171,  # App State Mountaineers
    "2029": 589,  # Arkansas-Pine Bluff Golden Lions
    "2032": 37,  # Arkansas State Red Wolves
    "2046": 150,  # Austin Peay Governors
    "2050": 48,  # Ball State Cardinals
    "2065": 143,  # Bethune-Cookman Wildcats
    "2075": 226,  # Bowie State Bulldogs
    "2083": 159,  # Bucknell Bison
    "2084": 208,  # Buffalo Bulls
    "2086": 165,  # Butler Bulldogs
    "2097": 2357,  # Campbell Fighting Camels
    "2110": 268,  # Central Arkansas Bears
    "2115": 209,  # Central Connecticut Blue Devils
    "2116": 210,  # UCF Knights
    "2117": 50,  # Central Michigan Chippewas
    "2122": 278,  # Central Oklahoma Bronchos
    "2127": 211,  # Charleston Southern Buccaneers
    "2130": 2665,  # Chicago State Cougars
    "2132": 98,  # Cincinnati Bearcats
    "2142": 160,  # Colgate Raiders
    "2166": 212,  # Davidson Wildcats
    "2168": 166,  # Dayton Flyers
    "2169": 144,  # Delaware State Hornets
    "2175": 2667,  # Dickinson Red Devils
    "2181": 167,  # Drake Bulldogs
    "2184": 136,  # Duquesne Dukes
    "2193": 173,  # East Tennessee State Buccaneers
    "2197": 120,  # Eastern Illinois Panthers
    "2198": 151,  # Eastern Kentucky Colonels
    "2199": 51,  # Eastern Michigan Eagles
    "2210": 350,  # Elon Phoenix
    "2226": 2294,  # Florida Atlantic Owls
    "2229": 2307,  # Florida International Panthers
    "2230": 161,  # Fordham Rams
    "2233": 2362,  # Franklin Grizzlies
    "2241": 351,  # Gardner-Webb Runnin' Bulldogs
    "2247": 2558,  # Georgia State Panthers
    "2261": 229,  # Hampton Pirates
    "2277": 2601,  # Houston Christian Huskies
    "2287": 121,  # Illinois State Redbirds
    "2294": 28,  # Iowa Hawkeyes
    "2305": 20,  # Kansas Jayhawks
    "2306": 21,  # Kansas State Wildcats
    "2309": 52,  # Kent State Golden Flashes
    "2320": 2559,  # Lamar Cardinals
    "2329": 164,  # Lehigh Mountain Hawks
    "2335": 215,  # Liberty Flames
    "2339": 2467,  # Lincoln (PA) Lions
    "2341": 251,  # Long Island University Sharks
    "2348": 38,  # Louisiana Tech Bulldogs
    "2355": 2591,  # Virginia Lynchburg Dragons
    "2368": 139,  # Marist Red Foxes
    "2377": 180,  # McNeese Cowboys
    "2382": 2597,  # Mercer Bears
    "2385": 253,  # Mercyhurst Lakers
    "2390": 11,  # Miami Hurricanes
    "2393": 152,  # Middle Tennessee Blue Raiders
    "2400": 191,  # Mississippi Valley State Delta Devils
    "2405": 216,  # Monmouth Hawks
    "2413": 153,  # Morehead State Eagles
    "2415": 147,  # Morgan State Bears
    "2426": 102,  # Navy Midshipmen
    "2428": 621,  # North Carolina Central Eagles
    "2429": 2595,  # Charlotte 49ers
    "2433": 103,  # UL Monroe Warhawks
    "2439": 40,  # UNLV Rebels
    "2440": 39,  # Nevada Wolf Pack
    "2441": 254,  # New Haven Chargers
    "2447": 181,  # Nicholls Colonels
    "2448": 148,  # North Carolina A&T Aggies
    "2449": 309,  # North Dakota State Bison
    "2450": 233,  # Norfolk State Spartans
    "2453": 273,  # North Alabama Lions
    "2458": 310,  # Northern Colorado Bears
    "2459": 42,  # Northern Illinois Huskies
    "2460": 123,  # Northern Iowa Panthers
    "2464": 118,  # Northern Arizona Lumberjacks
    "2466": 183,  # Northwestern State Demons
    "2483": 60,  # Oregon Ducks
    "2502": 257,  # Portland State Vikings
    "2504": 192,  # Prairie View A&M Panthers
    "2509": 35,  # Purdue Boilermakers
    "2523": 217,  # Robert Morris Colonials
    "2529": 259,  # Sacred Heart Pioneers
    "2534": 184,  # Sam Houston Bearkats
    "2535": 218,  # Samford Bulldogs
    "2545": 2234,  # SE Louisiana Lions
    "2546": 155,  # Southeast Missouri State Redhawks
    "2567": 82,  # SMU Mustangs
    "2569": 149,  # South Carolina State Bulldogs
    "2571": 312,  # South Dakota State Jackrabbits
    "2572": 105,  # Southern Miss Golden Eagles
    "2579": 70,  # South Carolina Gamecocks
    "2582": 193,  # Southern Jaguars
    "2617": 186,  # Stephen F. Austin Lumberjacks
    "2619": 629,  # Stony Brook Seawolves
    "2623": 125,  # Missouri State Bears
    "2627": 262,  # Tarleton State Texans
    "2628": 85,  # TCU Horned Frogs
    "2630": 156,  # UT Martin Skyhawks
    "2633": 71,  # Tennessee Volunteers
    "2634": 157,  # Tennessee State Tigers
    "2635": 158,  # Tennessee Tech Golden Eagles
    "2636": 2560,  # UTSA Roadrunners
    "2638": 95,  # UTEP Miners
    "2640": 194,  # Texas Southern Tigers
    "2641": 86,  # Texas Tech Red Raiders
    "2643": 172,  # The Citadel Bulldogs
    "2646": 380,  # Thomas More Saints
    "2649": 55,  # Toledo Rockets
    "2653": 222,  # Troy Trojans
    "2655": 106,  # Tulane Green Wave
    "2674": 170,  # Valparaiso Beacons
    "2678": 178,  # VMI Keydets
    "2681": 223,  # Wagner Seahawks
    "2692": 119,  # Weber State Wildcats
    "2698": 275,  # West Georgia Wolves
    "2710": 126,  # Western Illinois Leathernecks
    "2711": 56,  # Western Michigan Broncos
    "2717": 179,  # Western Carolina Catamounts
    "2729": 200,  # William & Mary Tribe
    "2747": 267,  # Wofford Terriers
    "2751": 96,  # Wyoming Cowboys
    "2754": 225,  # Youngstown State Penguins
    "2755": 189,  # Grambling Tigers
    "2771": 2332,  # Merrimack Warriors
    "2803": 1948,  # Bryant Bulldogs
    "2815": 2002,  # Lindenwood Lions
    "2837": 279,  # East Texas A&M Lions
    "2900": 468,  # St. Thomas Tommies
    "2916": 2539,  # Incarnate Word Cardinals
    "3077": 2552,  # Kentucky Christian Knights
    "3101": 2344,  # Utah Tech Trailblazers
    "124386": 2648,  # Arkansas Baptist Buffaloes
}


def _yahoo_team_number(espn_team_id: Union[str, int, None]) -> Optional[int]:
    """The Yahoo ``ncaaf.t.N`` number for an ESPN team id, or None when Yahoo has no team."""
    if espn_team_id is None:
        return None
    return _YAHOO_TEAM_ID_BY_ESPN.get(str(espn_team_id))
