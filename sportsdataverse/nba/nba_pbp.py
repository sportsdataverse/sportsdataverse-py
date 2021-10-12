import pyarrow.parquet as pq
import pandas as pd
import json
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.config import NBA_BASE_URL, NBA_TEAM_BOX_URL, NBA_PLAYER_BOX_URL, NBA_TEAM_SCHEDULE_URL
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download


