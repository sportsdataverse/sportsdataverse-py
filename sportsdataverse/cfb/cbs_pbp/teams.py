"""ESPN college-football team id -> CBS team id, location and mascot (static snapshot).

CBS publishes its own numeric team id twice in the places this adapter reads -- in the week
scoreboard card's logo URL (``team-logos/alt/{id}.svg``) and on the game's own NAPI
``scoreboard`` block -- so **the id resolution joins on that number, never on a name or an
abbreviation**: CBS spells Texas ``TEX`` in its team directory, ``TEXAS`` in the game
abbreviation and ``UT`` in its short name, and no one of those is the token every surface uses.

The location and mascot are carried for the ESPN header ``CFBPlayProcess`` needs: a CBS play
payload states no team name at all, and an empty ``team.name`` charges every synthesized
timeout to both clubs (the mascot is what ``cfb_pbp._timeout_team_match_len`` matches on).

Built 2026-09-18 from CBS NAPI ``season/teams/50000037`` (348 teams, league 56) matched on the
normalised name to ESPN's own football team directory: 320 teams, no CBS id claimed by two
ESPN ids. The unmatched remainder is CBS's D2/D3 tail, which has no play-by-play in any era.

# ponytail: a static snapshot. A new FBS member or a rename is a missing key, which degrades to
# "no CBS id" and hands the game to the next source -- never to a wrong id. The upgrade path is
# the id map's ``TEAM_SCHEMA.cbs_team_id`` column, which this module is only the fallback for.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

#: ESPN team id -> ``(CBS team id, ESPN abbreviation, location, mascot)``; comment = ESPN display name.
_CBS_TEAM_BY_ESPN: Dict[str, Tuple[int, str, str, str]] = {
    "2": (477, "AUB", "Auburn", "Tigers"),  # Auburn Tigers
    "5": (456, "UAB", "UAB", "Blazers"),  # UAB Blazers
    "6": (815, "USA", "South Alabama", "Jaguars"),  # South Alabama Jaguars
    "8": (471, "ARK", "Arkansas", "Razorbacks"),  # Arkansas Razorbacks
    "9": (468, "ASU", "Arizona State", "Sun Devils"),  # Arizona State Sun Devils
    "12": (467, "ARIZ", "Arizona", "Wildcats"),  # Arizona Wildcats
    "13": (508, "CP", "Cal Poly", "Mustangs"),  # Cal Poly Mustangs
    "16": (548, "SAC", "Sacramento State", "Hornets"),  # Sacramento State Hornets
    "21": (793, "SDSU", "San Diego State", "Aztecs"),  # San Diego State Aztecs
    "23": (795, "SJSU", "San Jose State", "Spartans"),  # San José State Spartans
    "24": (836, "STAN", "Stanford", "Cardinal"),  # Stanford Cardinal
    "25": (509, "CAL", "California", "Golden Bears"),  # California Golden Bears
    "26": (873, "UCLA", "UCLA", "Bruins"),  # UCLA Bruins
    "30": (877, "USC", "USC", "Trojans"),  # USC Trojans
    "36": (541, "CSU", "Colorado State", "Rams"),  # Colorado State Rams
    "38": (540, "COLO", "Colorado", "Buffaloes"),  # Colorado Buffaloes
    "41": (545, "CONN", "Connecticut", "Huskies"),  # UConn Huskies
    "43": (932, "YALE", "Yale", "Bulldogs"),  # Yale Bulldogs
    "46": (597, "GTWN", "Georgetown", "Hoyas"),  # Georgetown Hoyas
    "47": (621, "HOW", "Howard", "Bison"),  # Howard Bison
    "48": (553, "DEL", "Delaware", "Fightin' Blue Hens"),  # Delaware Blue Hens
    "50": (582, "FAMU", "Florida A&M", "Rattlers"),  # Florida A&M Rattlers
    "52": (583, "FSU", "Florida State", "Seminoles"),  # Florida State Seminoles
    "55": (638, "JXST", "Jacksonville State", "Gamecocks"),  # Jacksonville State Gamecocks
    "56": (2065949, "STET", "Stetson", "Hatters"),  # Stetson Hatters
    "57": (581, "FLA", "Florida", "Gators"),  # Florida Gators
    "58": (818, "USF", "South Florida", "Bulls"),  # South Florida Bulls
    "59": (600, "GT", "Georgia Tech", "Yellow Jackets"),  # Georgia Tech Yellow Jackets
    "60": (715, "MRHO", "Morehouse", "Maroon Tigers"),  # Morehouse Maroon Tigers
    "61": (599, "UGA", "Georgia", "Bulldogs"),  # Georgia Bulldogs
    "62": (612, "HAW", "Hawaii", "Rainbow Warriors"),  # Hawai'i Rainbow Warriors
    "66": (635, "ISU", "Iowa State", "Cyclones"),  # Iowa State Cyclones
    "68": (492, "BOIS", "Boise State", "Broncos"),  # Boise State Broncos
    "70": (624, "IDHO", "Idaho", "Vandals"),  # Idaho Vandals
    "77": (751, "NU", "Northwestern", "Wildcats"),  # Northwestern Wildcats
    "79": (821, "SIU", "Southern Illinois", "Salukis"),  # Southern Illinois Salukis
    "84": (629, "IU", "Indiana", "Hoosiers"),  # Indiana Hoosiers
    "87": (753, "ND", "Notre Dame", "Fighting Irish"),  # Notre Dame Fighting Irish
    "93": (720, "MUR", "Murray State", "Racers"),  # Murray State Racers
    "96": (647, "UK", "Kentucky", "Wildcats"),  # Kentucky Wildcats
    "97": (668, "LOU", "Louisville", "Cardinals"),  # Louisville Cardinals
    "98": (909, "WKU", "Western Kentucky", "Hilltoppers"),  # Western Kentucky Hilltoppers
    "99": (669, "LSU", "LSU", "Tigers"),  # LSU Tigers
    "103": (493, "BC", "Boston College", "Eagles"),  # Boston College Eagles
    "107": (618, "HC", "Holy Cross", "Crusaders"),  # Holy Cross Crusaders
    "108": (611, "HARV", "Harvard", "Crimson"),  # Harvard Crimson
    "113": (677, "MASS", "Massachusetts", "Minutemen"),  # Massachusetts Minutemen
    "119": (864, "TOW", "Towson", "Tigers"),  # Towson Tigers
    "120": (675, "MD", "Maryland", "Terrapins"),  # Maryland Terrapins
    "127": (690, "MSU", "Michigan State", "Spartans"),  # Michigan State Spartans
    "128": (749, "NMI", "Northern Michigan", "Wildcats"),  # Northern Michigan Wildcats
    "130": (689, "MICH", "Michigan", "Wolverines"),  # Michigan Wolverines
    "135": (699, "MINN", "Minnesota", "Golden Gophers"),  # Minnesota Golden Gophers
    "142": (704, "MIZ", "Missouri", "Tigers"),  # Missouri Tigers
    "145": (701, "MISS", "Ole Miss", "Rebels"),  # Ole Miss Rebels
    "147": (711, "MTST", "Montana State", "Bobcats"),  # Montana State Bobcats
    "149": (710, "MONT", "Montana", "Grizzlies"),  # Montana Grizzlies
    "150": (559, "DUKE", "Duke", "Blue Devils"),  # Duke Blue Devils
    "151": (565, "ECU", "East Carolina", "Pirates"),  # East Carolina Pirates
    "152": (738, "NCSU", "North Carolina State", "Wolfpack"),  # NC State Wolfpack
    "153": (742, "UNC", "North Carolina", "Tar Heels"),  # North Carolina Tar Heels
    "154": (891, "WAKE", "Wake Forest", "Demon Deacons"),  # Wake Forest Demon Deacons
    "155": (743, "UND", "North Dakota", "Fighting Hawks"),  # North Dakota Fighting Hawks
    "158": (726, "NEB", "Nebraska", "Cornhuskers"),  # Nebraska Cornhuskers
    "159": (550, "DART", "Dartmouth", "Big Green"),  # Dartmouth Big Green
    "160": (730, "UNH", "New Hampshire", "Wildcats"),  # New Hampshire Wildcats
    "163": (772, "PRIN", "Princeton", "Tigers"),  # Princeton Tigers
    "164": (785, "RUTG", "Rutgers", "Scarlet Knights"),  # Rutgers Scarlet Knights
    "166": (733, "NMSU", "New Mexico State", "Aggies"),  # New Mexico State Aggies
    "167": (732, "UNM", "New Mexico", "Lobos"),  # New Mexico Lobos
    "171": (542, "COLU", "Columbia", "Lions"),  # Columbia Lions
    "172": (546, "COR", "Cornell", "Big Red"),  # Cornell Big Red
    "183": (844, "SYR", "Syracuse", "Orange"),  # Syracuse Orange
    "189": (496, "BGSU", "Bowling Green", "Falcons"),  # Bowling Green Falcons
    "193": (688, "M-OH", "Miami (OH)", "RedHawks"),  # Miami (OH) RedHawks
    "194": (758, "OSU", "Ohio State", "Buckeyes"),  # Ohio State Buckeyes
    "195": (757, "OHIO", "Ohio", "Bobcats"),  # Ohio Bobcats
    "196": (13550, "NESU", "Northeastern State", "RiverHawks"),  # Northeastern State RiverHawks
    "197": (760, "OKST", "Oklahoma State", "Cowboys"),  # Oklahoma State Cowboys
    "201": (759, "OU", "Oklahoma", "Sooners"),  # Oklahoma Sooners
    "202": (868, "TLSA", "Tulsa", "Golden Hurricane"),  # Tulsa Golden Hurricane
    "204": (762, "ORST", "Oregon State", "Beavers"),  # Oregon State Beavers
    "213": (765, "PSU", "Penn State", "Nittany Lions"),  # Penn State Nittany Lions
    "218": (847, "TEM", "Temple", "Owls"),  # Temple Owls
    "219": (766, "PENN", "Penn", "Quakers"),  # Pennsylvania Quakers
    "221": (767, "PITT", "Pittsburgh", "Panthers"),  # Pittsburgh Panthers
    "222": (883, "VILL", "Villanova", "Wildcats"),  # Villanova Wildcats
    "225": (499, "BRWN", "Brown", "Bears"),  # Brown Bears
    "227": (778, "URI", "Rhode Island", "Rams"),  # Rhode Island Rams
    "228": (535, "CLEM", "Clemson", "Tigers"),  # Clemson Tigers
    "231": (593, "FUR", "Furman", "Paladins"),  # Furman Paladins
    "233": (817, "SDAK", "South Dakota", "Coyotes"),  # South Dakota Coyotes
    "235": (681, "MEM", "Memphis", "Tigers"),  # Memphis Tigers
    "236": (850, "UTC", "Chattanooga", "Mocs"),  # Chattanooga Mocs
    "238": (882, "VAN", "Vanderbilt", "Commodores"),  # Vanderbilt Commodores
    "239": (484, "BAY", "Baylor", "Bears"),  # Baylor Bears
    "242": (780, "RICE", "Rice", "Owls"),  # Rice Owls
    "245": (854, "TA&M", "Texas A&M", "Aggies"),  # Texas A&M Aggies
    "248": (620, "HOU", "Houston", "Cougars"),  # Houston Cougars
    "249": (744, "UNT", "North Texas", "Mean Green"),  # North Texas Mean Green
    "251": (853, "TEX", "Texas", "Longhorns"),  # Texas Longhorns
    "252": (504, "BYU", "BYU", "Cougars"),  # BYU Cougars
    "253": (822, "SUU", "Southern Utah", "Thunderbirds"),  # Southern Utah Thunderbirds
    "254": (878, "UTAH", "Utah", "Utes"),  # Utah Utes
    "256": (639, "JMU", "James Madison", "Dukes"),  # James Madison Dukes
    "257": (781, "RICH", "Richmond", "Spiders"),  # Richmond Spiders
    "258": (884, "UVA", "Virginia", "Cavaliers"),  # Virginia Cavaliers
    "259": (886, "VT", "Virginia Tech", "Hokies"),  # Virginia Tech Hokies
    "264": (895, "WASH", "Washington", "Huskies"),  # Washington Huskies
    "265": (896, "WSU", "Washington State", "Cougars"),  # Washington State Cougars
    "275": (928, "WIS", "Wisconsin", "Badgers"),  # Wisconsin Badgers
    "276": (674, "MRSH", "Marshall", "Thundering Herd"),  # Marshall Thundering Herd
    "277": (907, "WVU", "West Virginia", "Mountaineers"),  # West Virginia Mountaineers
    "278": (587, "FRES", "Fresno State", "Bulldogs"),  # Fresno State Bulldogs
    "282": (631, "INST", "Indiana State", "Sycamores"),  # Indiana State Sycamores
    "284": (838, "STO", "Stonehill", "Skyhawks"),  # Stonehill Skyhawks
    "290": (596, "GASO", "Georgia Southern", "Eagles"),  # Georgia Southern Eagles
    "295": (1678007, "ODU", "Old Dominion", "Monarchs"),  # Old Dominion Monarchs
    "301": (792, "USD", "San Diego", "Toreros"),  # San Diego Toreros
    "302": (506, "UCD", "UC Davis", "Aggies"),  # UC Davis Aggies
    "304": (625, "IDST", "Idaho State", "Bengals"),  # Idaho State Bengals
    "309": (841, "UL", "Louisiana", "Ragin' Cajuns"),  # Louisiana Ragin' Cajuns
    "311": (670, "ME", "Maine", "Black Bears"),  # Maine Black Bears
    "322": (654, "LAF", "Lafayette", "Leopards"),  # Lafayette Leopards
    "324": (431245, "CCU", "Coastal Carolina", "Chanticleers"),  # Coastal Carolina Chanticleers
    "326": (843, "TXST", "Texas State", "Bobcats"),  # Texas State Bobcats
    "328": (879, "USU", "Utah State", "Aggies"),  # Utah State Aggies
    "330": (885, "VSU", "Virginia State", "Trojans"),  # Virginia State Trojans
    "331": (571, "EWU", "Eastern Washington", "Eagles"),  # Eastern Washington Eagles
    "333": (457, "ALA", "Alabama", "Crimson Tide"),  # Alabama Crimson Tide
    "338": (2169062, "KENN", "Kennesaw State", "Owls"),  # Kennesaw State Owls
    "344": (703, "MSST", "Mississippi State", "Bulldogs"),  # Mississippi State Bulldogs
    "349": (474, "ARMY", "Army West Point", "Black Knights"),  # Army Black Knights
    "356": (627, "ILL", "Illinois", "Fighting Illini"),  # Illinois Fighting Illini
    "389": (13599, "UIU", "Upper Iowa", "Peacocks"),  # Upper Iowa Peacocks
    "599": (2871093, "RSVT", "Roosevelt", "Lakers"),  # Roosevelt Lakers
    "613": (13576, "SDMT", "South Dakota Mines", "Hardrockers"),  # South Dakota Mines Hardrockers
    "2000": (451, "ACU", "Abilene Christian", "Wildcats"),  # Abilene Christian Wildcats
    "2005": (454, "AF", "Air Force", "Falcons"),  # Air Force Falcons
    "2006": (455, "AKR", "Akron", "Zips"),  # Akron Zips
    "2010": (458, "AAMU", "Alabama A&M", "Bulldogs"),  # Alabama A&M Bulldogs
    "2011": (459, "ALST", "Alabama State", "Hornets"),  # Alabama State Hornets
    "2013": (461, "ABSU", "Albany State", "Golden Rams"),  # Albany State Golden Rams
    "2016": (462, "ALCN", "Alcorn State", "Braves"),  # Alcorn State Braves
    "2019": (26672311, "ALNU", "Allen", "Yellow Jackets"),  # Allen Yellow Jackets
    "2022": (464, "AIC", "American International", "Yellow Jackets"),  # American International Yellow Jackets
    "2026": (466, "APP", "Appalachian State", "Mountaineers"),  # App State Mountaineers
    "2029": (470, "UAPB", "Arkansas-Pine Bluff", "Golden Lions"),  # Arkansas-Pine Bluff Golden Lions
    "2032": (472, "ARST", "Arkansas State", "Red Wolves"),  # Arkansas State Red Wolves
    "2046": (481, "APSU", "Austin Peay", "Governors"),  # Austin Peay Governors
    "2050": (483, "BALL", "Ball State", "Cardinals"),  # Ball State Cardinals
    "2060": (485, "BENT", "Bentley", "Falcons"),  # Bentley Falcons
    "2065": (488, "BCU", "Bethune-Cookman", "Wildcats"),  # Bethune-Cookman Wildcats
    "2075": (495, "BOWE", "Bowie State", "Bulldogs"),  # Bowie State Bulldogs
    "2083": (500, "BUCK", "Bucknell", "Bison"),  # Bucknell Bison
    "2084": (501, "BUF", "Buffalo", "Bulls"),  # Buffalo Bulls
    "2086": (503, "BTLR", "Butler", "Bulldogs"),  # Butler Bulldogs
    "2097": (1618606, "CAM", "Campbell", "Fighting Camels"),  # Campbell Fighting Camels
    "2110": (516, "CARK", "Central Arkansas", "Bears"),  # Central Arkansas Bears
    "2115": (518, "CCSU", "Central Connecticut State", "Blue Devils"),  # Central Connecticut Blue Devils
    "2116": (519, "UCF", "UCF", "Knights"),  # UCF Knights
    "2117": (520, "CMU", "Central Michigan", "Chippewas"),  # Central Michigan Chippewas
    "2119": (569988, "CNSU", "Central State", "Marauders"),  # Central State Marauders
    "2120": (522, "CWAU", "Central Washington", "Wildcats"),  # Central Washington Wildcats
    "2122": (524, "UCO", "Central Oklahoma", "Bronchos"),  # Central Oklahoma Bronchos
    "2123": (525, "CHAD", "Chadron State", "Eagles"),  # Chadron State Eagles
    "2127": (527, "CHSO", "Charleston Southern", "Buccaneers"),  # Charleston Southern Buccaneers
    "2130": (50000125, "CHST", "Chicago State", "Cougars"),  # Chicago State Cougars
    "2132": (531, "CIN", "Cincinnati", "Bearcats"),  # Cincinnati Bearcats
    "2142": (537, "COLG", "Colgate", "Raiders"),  # Colgate Raiders
    "2148": (543, "CONC", "Concord", "Mountain Lions"),  # Concord Mountain Lions
    "2166": (551, "DAV", "Davidson", "Wildcats"),  # Davidson Wildcats
    "2168": (552, "DAY", "Dayton", "Flyers"),  # Dayton Flyers
    "2169": (554, "DSU", "Delaware State", "Hornets"),  # Delaware State Hornets
    "2170": (556, "DLST", "Delta State", "Statesmen"),  # Delta State Statesmen
    "2181": (558, "DRKE", "Drake", "Bulldogs"),  # Drake Bulldogs
    "2184": (560, "DUQ", "Duquesne", "Dukes"),  # Duquesne Dukes
    "2193": (566, "ETSU", "ETSU", "Buccaneers"),  # East Tennessee State Buccaneers
    "2197": (568, "EIU", "Eastern Illinois", "Panthers"),  # Eastern Illinois Panthers
    "2198": (569, "EKU", "Eastern Kentucky", "Colonels"),  # Eastern Kentucky Colonels
    "2199": (570, "EMU", "Eastern Michigan", "Eagles"),  # Eastern Michigan Eagles
    "2206": (345138, "EDW", "Edward Waters", "Tigers"),  # Edward Waters Tigers
    "2207": (573, "ECSU", "Elizabeth City State", "Vikings"),  # Elizabeth City State Vikings
    "2210": (574, "ELON", "Elon", "Phoenix"),  # Elon Phoenix
    "2220": (578, "FAYU", "Fayetteville State", "Broncos"),  # Fayetteville State Broncos
    "2226": (237794, "FAU", "Florida Atlantic", "Owls"),  # Florida Atlantic Owls
    "2229": (344993, "FIU", "FIU", "Panthers"),  # Florida International Panthers
    "2230": (584, "FOR", "Fordham", "Rams"),  # Fordham Rams
    "2232": (591, "FVSU", "Fort Valley State", "Wildcats"),  # Fort Valley State Wildcats
    "2233": (585, "FRKL", "Franklin", "Grizzlies"),  # Franklin Grizzlies
    "2241": (595, "GWEB", "Gardner-Webb", "Runnin' Bulldogs"),  # Gardner-Webb Runnin' Bulldogs
    "2245": (598, "GTKY", "Georgetown (KY)", "Tigers"),  # Georgetown (KY) Tigers
    "2247": (1748174, "GAST", "Georgia State", "Panthers"),  # Georgia State Panthers
    "2249": (601, "GVLS", "Glenville State", "Pioneers"),  # Glenville State Pioneers
    "2261": (608, "HAMP", "Hampton", "Pirates"),  # Hampton Pirates
    "2262": (13484, "HNVR", "Hanover", "Panthers"),  # Hanover Panthers
    "2277": (2065937, "HCU", "Houston Christian", "Huskies"),  # Houston Christian Huskies
    "2287": (628, "ILST", "Illinois State", "Redbirds"),  # Illinois State Redbirds
    "2294": (634, "IOWA", "Iowa", "Hawkeyes"),  # Iowa Hawkeyes
    "2296": (637, "JKST", "Jackson State", "Tigers"),  # Jackson State Tigers
    "2305": (644, "KU", "Kansas", "Jayhawks"),  # Kansas Jayhawks
    "2306": (645, "KSU", "Kansas State", "Wildcats"),  # Kansas State Wildcats
    "2309": (646, "KENT", "Kent State", "Golden Flashes"),  # Kent State Golden Flashes
    "2310": (648, "KYSU", "Kentucky State", "Thorobreds"),  # Kentucky State Thorobreds
    "2320": (1748173, "LAM", "Lamar", "Cardinals"),  # Lamar Cardinals
    "2324": (658, "LNGT", "Langston", "Lions"),  # Langston Lions
    "2329": (660, "LEH", "Lehigh", "Mountain Hawks"),  # Lehigh Mountain Hawks
    "2335": (662, "LIB", "Liberty", "Flames"),  # Liberty Flames
    "2341": (505, "LIU", "LIU", "Sharks"),  # Long Island University Sharks
    "2347": (435839, "LCHR", "Louisiana Christian", "Wildcats"),  # Louisiana Christian Wildcats
    "2348": (667, "LT", "Louisiana Tech", "Bulldogs"),  # Louisiana Tech Bulldogs
    "2355": (1848383, "VUL", "Virginia-Lynchburg", "Dragons"),  # Virginia Lynchburg Dragons
    "2368": (672, "MRST", "Marist", "Red Foxes"),  # Marist Red Foxes
    "2377": (679, "MCN", "McNeese", "Cowboys"),  # McNeese Cowboys
    "2382": (2065936, "MER", "Mercer", "Bears"),  # Mercer Bears
    "2385": (684, "MERC", "Mercyhurst", "Lakers"),  # Mercyhurst Lakers
    "2390": (687, "MIA", "Miami (Fla.)", "Hurricanes"),  # Miami Hurricanes
    "2393": (692, "MTSU", "Middle Tennessee", "Blue Raiders"),  # Middle Tennessee Blue Raiders
    "2396": (694, "MILE", "Miles", "Golden Bears"),  # Miles Golden Bears
    "2400": (700, "MVSU", "Mississippi Valley State", "Delta Devils"),  # Mississippi Valley State Delta Devils
    "2405": (709, "MONM", "Monmouth", "Hawks"),  # Monmouth Hawks
    "2413": (714, "MORE", "Morehead State", "Eagles"),  # Morehead State Eagles
    "2415": (716, "MORG", "Morgan State", "Bears"),  # Morgan State Bears
    "2426": (722, "NAVY", "Navy", "Midshipmen"),  # Navy Midshipmen
    "2428": (723, "NCCU", "North Carolina Central", "Eagles"),  # North Carolina Central Eagles
    "2429": (2054618, "CLT", "Charlotte", "49ers"),  # Charlotte 49ers
    "2433": (724, "ULM", "Louisiana-Monroe", "Warhawks"),  # UL Monroe Warhawks
    "2439": (728, "UNLV", "UNLV", "Rebels"),  # UNLV Rebels
    "2440": (729, "NEV", "Nevada", "Wolf Pack"),  # Nevada Wolf Pack
    "2441": (731, "NHVN", "New Haven", "Chargers"),  # New Haven Chargers
    "2447": (736, "NICH", "Nicholls", "Colonels"),  # Nicholls Colonels
    "2448": (737, "NCAT", "North Carolina A&T", "Aggies"),  # North Carolina A&T Aggies
    "2449": (13548, "NDSU", "North Dakota State", "Bison"),  # North Dakota State Bison
    "2450": (739, "NORF", "Norfolk State", "Spartans"),  # Norfolk State Spartans
    "2453": (741, "UNA", "North Alabama", "Lions"),  # North Alabama Lions
    "2458": (746, "UNCO", "Northern Colorado", "Bears"),  # Northern Colorado Bears
    "2459": (747, "NIU", "Northern Illinois", "Huskies"),  # Northern Illinois Huskies
    "2460": (748, "UNI", "UNI", "Panthers"),  # Northern Iowa Panthers
    "2464": (750, "NAU", "Northern Arizona", "Lumberjacks"),  # Northern Arizona Lumberjacks
    "2466": (752, "NWST", "Northwestern State", "Demons"),  # Northwestern State Demons
    "2477": (1678523, "OHDU", "Ohio Dominican", "Panthers"),  # Ohio Dominican Panthers
    "2483": (761, "ORE", "Oregon", "Ducks"),  # Oregon Ducks
    "2487": (763, "PACE", "Pace", "Setters"),  # Pace Setters
    "2502": (769, "PRST", "Portland State", "Vikings"),  # Portland State Vikings
    "2504": (770, "PV", "Prairie View A&M", "Panthers"),  # Prairie View A&M Panthers
    "2506": (771, "PRES", "Presbyterian", "Blue Hose"),  # Presbyterian Blue Hose
    "2509": (774, "PUR", "Purdue", "Boilermakers"),  # Purdue Boilermakers
    "2523": (782, "RMU", "Robert Morris", "Colonials"),  # Robert Morris Colonials
    "2529": (786, "SHU", "Sacred Heart", "Pioneers"),  # Sacred Heart Pioneers
    "2534": (790, "SHSU", "Sam Houston", "Bearkats"),  # Sam Houston Bearkats
    "2535": (791, "SAM", "Samford", "Bulldogs"),  # Samford Bulldogs
    "2542": (797, "SAV", "Savannah State", "Tigers"),  # Savannah State Tigers
    "2545": (798, "SELA", "Southeastern Louisiana", "Lions"),  # SE Louisiana Lions
    "2546": (799, "SEMO", "Southeast Missouri State", "Redhawks"),  # Southeast Missouri State Redhawks
    "2567": (807, "SMU", "SMU", "Mustangs"),  # SMU Mustangs
    "2569": (809, "SCST", "South Carolina State", "Bulldogs"),  # South Carolina State Bulldogs
    "2571": (811, "SDST", "South Dakota State", "Jackrabbits"),  # South Dakota State Jackrabbits
    "2572": (812, "USM", "Southern Miss", "Golden Eagles"),  # Southern Miss Golden Eagles
    "2579": (816, "SC", "South Carolina", "Gamecocks"),  # South Carolina Gamecocks
    "2582": (819, "SOU", "Southern University", "Jaguars"),  # Southern Jaguars
    "2583": (820, "SCTS", "Southern Connecticut State", "Owls"),  # Southern Connecticut State Owls
    "2595": (827, "SFIL", "St. Francis (IL)", "Fighting Saints"),  # St. Francis (IL) Fighting Saints
    "2598": (828, "SFPA", "Saint Francis U", "Red Flash"),  # Saint Francis Red Flash
    "2617": (837, "SFA", "Stephen F. Austin", "Lumberjacks"),  # Stephen F. Austin Lumberjacks
    "2619": (839, "STBK", "Stony Brook", "Seawolves"),  # Stony Brook Seawolves
    "2623": (842, "MOST", "Missouri State", "Bears"),  # Missouri State Bears
    "2627": (845, "TAR", "Tarleton State", "Texans"),  # Tarleton State Texans
    "2628": (846, "TCU", "TCU", "Horned Frogs"),  # TCU Horned Frogs
    "2630": (848, "UTM", "UT Martin", "Skyhawks"),  # UT Martin Skyhawks
    "2633": (849, "TENN", "Tennessee", "Volunteers"),  # Tennessee Volunteers
    "2634": (851, "TNST", "Tennessee State", "Tigers"),  # Tennessee State Tigers
    "2635": (852, "TNTC", "Tennessee Tech", "Golden Eagles"),  # Tennessee Tech Golden Eagles
    "2636": (1827968, "UTSA", "UTSA", "Roadrunners"),  # UTSA Roadrunners
    "2638": (856, "UTEP", "UTEP", "Miners"),  # UTEP Miners
    "2640": (858, "TXSO", "Texas Southern", "Tigers"),  # Texas Southern Tigers
    "2641": (859, "TTU", "Texas Tech", "Red Raiders"),  # Texas Tech Red Raiders
    "2646": (2871115, "TMOR", "Thomas More", "Saints"),  # Thomas More Saints
    "2649": (863, "TOL", "Toledo", "Rockets"),  # Toledo Rockets
    "2653": (865, "TROY", "Troy", "Trojans"),  # Troy Trojans
    "2654": (866, "TRST", "Truman State", "Bulldogs"),  # Truman State Bulldogs
    "2655": (867, "TULN", "Tulane", "Green Wave"),  # Tulane Green Wave
    "2657": (870, "TUSK", "Tuskegee", "Golden Tigers"),  # Tuskegee Golden Tigers
    "2673": (880, "VALD", "Valdosta State", "Blazers"),  # Valdosta State Blazers
    "2674": (881, "VAL", "Valparaiso", "Beacons"),  # Valparaiso Beacons
    "2678": (888, "VMI", "VMI", "Keydets"),  # VMI Keydets
    "2681": (890, "WAG", "Wagner", "Seahawks"),  # Wagner Seahawks
    "2683": (2065938, "WRNR", "Warner", "Royals"),  # Warner Royals
    "2691": (433800, "WINT", "Webber International", "Warriors"),  # Webber International Warriors
    "2692": (899, "WEB", "Weber State", "Wildcats"),  # Weber State Wildcats
    "2695": (13607, "UWA", "West Alabama", "Tigers"),  # West Alabama Tigers
    "2698": (902, "WGA", "West Georgia", "Wolves"),  # West Georgia Wolves
    "2707": (906, "WVSU", "West Virginia State", "Yellow Jackets"),  # West Virginia State Yellow Jackets
    "2710": (908, "WIU", "Western Illinois", "Leathernecks"),  # Western Illinois Leathernecks
    "2711": (910, "WMU", "Western Michigan", "Broncos"),  # Western Michigan Broncos
    "2717": (913, "WCU", "Western Carolina", "Catamounts"),  # Western Carolina Catamounts
    "2729": (918, "W&M", "William & Mary", "Tribe"),  # William & Mary Tribe
    "2736": (922, "WSSU", "Winston-Salem State", "Rams"),  # Winston-Salem State Rams
    "2747": (930, "WOF", "Wofford", "Terriers"),  # Wofford Terriers
    "2751": (931, "WYO", "Wyoming", "Cowboys"),  # Wyoming Cowboys
    "2754": (933, "YSU", "Youngstown State", "Penguins"),  # Youngstown State Penguins
    "2755": (602, "GRAM", "Grambling State", "Tigers"),  # Grambling Tigers
    "2771": (13529, "MRMK", "Merrimack", "Warriors"),  # Merrimack Warriors
    "2803": (13428, "BRY", "Bryant", "Bulldogs"),  # Bryant Bulldogs
    "2805": (533, "CKGA", "Clark Atlanta", "Panthers"),  # Clark Atlanta Panthers
    "2815": (13506, "LIN", "Lindenwood", "Lions"),  # Lindenwood Lions
    "2816": (678, "MCK", "McKendree", "Bearcats"),  # McKendree Bearcats
    "2834": (2871111, "SRST", "Sul Ross State", "Lobos"),  # Sul Ross State Lobos
    "2839": (869, "TUSC", "Tusculum", "Pioneers"),  # Tusculum Pioneers
    "2848": (13612, "WORU", "Western Oregon", "Wolves"),  # Western Oregon Wolves
    "2851": (921, "WNST", "Winona State", "Warriors"),  # Winona State Warriors
    "2900": (13585, "STMN", "St. Thomas (MN)", "Tommies"),  # St. Thomas Tommies
    "2986": (13471, "FMSU", "Fairmont State", "Falcons"),  # Fairmont State Falcons
    "3077": (1827966, "KYCHR", "Kentucky Christian", "Knights"),  # Kentucky Christian Knights
    "3101": (1123361, "UTU", "Utah Tech", "Trailblazers"),  # Utah Tech Trailblazers
    "3179": (2054619, "POINT", "Point University", "Skyhawks"),  # Point University Skyhawks
    "101784": (3159236, "ERSK", "Erskine", "Flying Fleet"),  # Erskine Flying Fleet
    "110242": (2871132, "UWF", "West Florida", "Argonauts"),  # West Florida Argonauts
    "111610": (2932255, "STAU", "St. Andrews", "Knights"),  # St. Andrews Knights
    "112334": (3115531, "FP", "Franklin Pierce", "Ravens"),  # Franklin Pierce Ravens
    "124386": (29191269, "ARBA", "Arkansas Baptist", "Buffaloes"),  # Arkansas Baptist Buffaloes
}

#: CBS team id -> ESPN team id (the reverse of the same snapshot).
_ESPN_TEAM_BY_CBS: Dict[str, str] = {str(v[0]): k for k, v in _CBS_TEAM_BY_ESPN.items()}


def _cbs_team(espn_team_id: object) -> Optional[Tuple[int, str, str, str]]:
    """``(CBS team id, ESPN abbreviation, location, mascot)`` for an ESPN team id, or None."""
    return _CBS_TEAM_BY_ESPN.get(str(espn_team_id))
