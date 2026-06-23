from __future__ import annotations

from sportsdataverse.cfb.cfb_espn_ext import *
from sportsdataverse.cfb.cfb_fox_ext import *
from sportsdataverse.cfb.cfb_game_rosters import *
from sportsdataverse.cfb.cfb_loaders import *
from sportsdataverse.cfb.cfb_loaders_extra import *
from sportsdataverse.cfb.cfb_pbp import *
from sportsdataverse.cfb.cfb_fourth_down import *
from sportsdataverse.cfb.cfb_pbp_fox import *
from sportsdataverse.cfb.cfb_play_participants import *
from sportsdataverse.cfb.cfb_player_stats import *
from sportsdataverse.cfb.cfb_schedule import *
from sportsdataverse.cfb.cfb_teams import *
from sportsdataverse.cfb.cfb_yahoo_ext import *

# Cross-provider crosswalks depend on the provider modules above, so import last.
from sportsdataverse.cfb.cfb_crosswalk import *
