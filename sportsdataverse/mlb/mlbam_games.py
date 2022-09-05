## Script: games.py
## Author: Joseph Armstrong (armstjc)

import pandas as pd
from pandas import json_normalize
import json
from sportsdataverse.dl_utils import download
from datetime import datetime



def mlbam_schedule(season:int,gameType="R"):
	"""
	Retrieves the start and end date for games for every league, and the MLB,for a given season.
	This function does not get individual games.

	Args:
		season (int):
			Required parameter. Indicates the season you are trying to find the games for.

		gameType (string) = "R":
			Optional parameter. If there's no input, this function will get the info for the regular season.

			Other parts of the season are indicated as follows in the MLBAM API:

			'S' - Spring Training
			'E' - Exhibition
			'A' - All Star Game
			'D' - Division Series
			'F' - First Round (Wild Card)
			'L' - League Championship
			'W' - World Series

	Returns: 
		A pandas dataframe containing MLB scheduled games.
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.org_game_type_date_info.bam?current_sw='Y'&sport_code='mlb'&"

	if gameType == "R" or gameType == "S" or gameType == "E" or gameType == "A" or gameType == "D" or gameType == "F" or gameType == "L" or gameType == "W":
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'

	now = datetime.now()
	if season < 1860:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\''
	elif int(now.year) < season:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\''
	else:
		searchURL = searchURL  + f'season=\'{season}\''

	resp = download(searchURL)

	resp_str = str(resp, 'UTF-8')

	resp_json = json.loads(resp_str)
	try:
		result_count = int(resp_json['org_game_type_date_info']['queryResults']['totalSize'])
	except:
		result_count = 0

	if result_count > 0:
		main_df = json_normalize(resp_json['org_game_type_date_info']['queryResults']['row'])
	else:
		print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
	return main_df
