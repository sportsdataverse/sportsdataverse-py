---
title: Load WBB Player Boxscore
sidebar_label: Load WBB Player Boxscore
---

### wehoop.wbb.load_wbb_player_boxscore(seasons: List[int])
Load women’s college basketball player boxscore data

Example:

    wbb_df = wehoop.wbb.load_wbb_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.