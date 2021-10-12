---
title: Load CFB Team Info
sidebar_label: Load CFB Team Info
sidebar_position: 3
---

### cfbfastR.cfb.load_cfb_team_info(seasons: List[int])
Load college football team info

Example:

    cfb_df = cfbfastR.cfb.load_cfb_team_info(seasons=range(2002,2021))


Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the team info available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.
