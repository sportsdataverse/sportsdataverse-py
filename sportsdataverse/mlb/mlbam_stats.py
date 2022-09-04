## Script: stats.py
## Author: Joseph Armstrong (armstjc)

import pandas as pd
from pandas import json_normalize
import json
from sportsdataverse.dl_utils import download
from datetime import datetime

import os



def mlbam_player_season_hitting_stats(playerID:int,season:int,gameType="R"):
	"""
	Retrieves the hitting stats for an MLB player in a given season, given a proper MLBAM ID.

	Args:
		playerID (int):
			Required parameter. If no playerID is provided, the function wil not work.

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
		A pandas dataframe containing career hitting stats for an MLB player.
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.sport_hitting_tm.bam?league_list_id='mlb'&"

	if len(gameType) >1:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		pass

	if gameType == "R" or gameType == "S" or gameType == "E" or gameType == "A" or gameType == "D" or gameType == "F" or gameType == "L" or gameType == "W":
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'

	now = datetime.now()
	if season < 1860 or season == None:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	elif int(now.year) < season:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	else:
		searchURL = searchURL  + f'season=\'{season}\'&'

	if playerID < 1 or playerID == None or season == None or season < 1860:
		print('You must provide a playerID and a proper season. Function aborted.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_hitting_tm']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			main_df = json_normalize(resp_json['sport_hitting_tm']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')

		return main_df


def mlbam_player_season_pitching_stats(playerID:int,season:int,gameType="R"):
	"""Retrieves the pitching stats for an MLB player in a given season, given a proper MLBAM ID

	Args:
		playerID (int):
			Required parameter. If no playerID is provided, the function wil not work.

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
		A pandas dataframe containing pitching stats for an MLB player in a given season.
	
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.sport_pitching_tm.bam?league_list_id='mlb'&"

	if len(gameType) >1:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"

	if gameType == "R" or gameType == "S" or gameType == "E" or gameType == "A" or gameType == "D" or gameType == "F" or gameType == "L" or gameType == "W":
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'

	now = datetime.now()
	if season < 1860 or season == None:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	elif int(now.year) < season:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	else:
		searchURL = searchURL  + f'season=\'{season}\'&'

	if playerID < 1 or playerID == None:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_pitching_tm']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			main_df = json_normalize(resp_json['sport_pitching_tm']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')

		return main_df

def mlbam_player_career_hitting_stats(playerID:int,gameType="R"):
	"""
	Retrieves the career hitting stats for an MLB player, given a proper MLBAM ID

	Args:
		playerID (int):
			Required parameter. If no playerID is provided, the function wil not work.

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
		A pandas dataframe containing hitting stats for an MLB player in a given season.
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.sport_career_hitting.bam?league_list_id='mlb'&"

	if len(gameType) >1:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		pass

	if gameType == "R" or gameType == "S" or gameType == "E" or gameType == "A" or gameType == "D" or gameType == "F" or gameType == "L" or gameType == "W":
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'

	if playerID < 1 or playerID == None:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_career_hitting']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			main_df = json_normalize(resp_json['sport_career_hitting']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a different search for better results.')

		return main_df

def mlbam_player_career_pitching_stats(playerID:int,gameType="R"):
	"""
	Retrieves the career pitching stats for an MLB player, given a proper MLBAM ID

	Args:
		playerID (int):
			Required parameter. If no playerID is provided, the function wil not work.

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
		A pandas dataframe containing career pitching stats for an MLB player.
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.sport_career_pitching.bam?league_list_id='mlb'&"

	if len(gameType) >1:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		pass

	if gameType == "R" or gameType == "S" or gameType == "E" or gameType == "A" or gameType == "D" or gameType == "F" or gameType == "L" or gameType == "W":
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'
	else:
		print('Check your input for seasonType. Searching for regular season stats instead.')
		gameType = "R"
		searchURL = searchURL  + f'game_type=\'{gameType}\'&'

	if playerID < 1 or playerID == None:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_career_pitching']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			main_df = json_normalize(resp_json['sport_career_pitching']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a different search for better results.')

		return main_df