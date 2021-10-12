---
title: Load CFB Rosters
sidebar_label: Load CFB Rosters
sidebar_position: 4
---

### cfbfastR.cfb.load_cfb_rosters(seasons: List[int])
Load roster data

Example:

    cfb_df = cfbfastR.cfb.load_cfb_rosters(seasons=range(2014,2021))

Args:

    seasons (list): Used to define different seasons. 2014 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.

Raises:

    ValueError: If season is less than 2014.


