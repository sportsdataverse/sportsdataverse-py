import pyarrow.parquet as pq
import pandas as pd
import json
from typing import List, Callable, Iterator, Union, Optional
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.dl_utils import download
