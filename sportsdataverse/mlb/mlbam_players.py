## Script: players.py
## Author: Joseph Armstrong (armstjc)

import pandas as pd
from pandas import json_normalize
import json
from sportsdataverse.dl_utils import download


def mlbam_search_mlb_players(search:str,isActive=""):
	"""
	Searches for an MLB player in the MLBAM API.

	Args:
		search (string):
			Inputted string of the player(s) the user is intending to search.
			If there is nothing inputted, nothing will be searched.

		isActive (string, optional):
			If called, it will specify if you want active players, or inactive players
			in your search.

			If you want active players, set isActive to "Y" or "Yes".

			If you want inactive players, set isActive to "N" or "No".

	Returns:
		A pandas dataframe containing MLBAM players whose name(s) matches the input string.

	"""
	searchURL = "http://lookup-service-prod.mlb.com/json/named.search_player_all.bam?sport_code='mlb'"

	p_df = pd.DataFrame()
	main_df = pd.DataFrame()

	if len(isActive) == 0:
		print('')
	elif isActive.lower() == "y" or isActive.lower() == "yes":
		searchURL = searchURL + "&active_sw='Y'"
	elif isActive.lower() == "n" or isActive.lower() == "no":
		searchURL = searchURL + "&active_sw='N'"
	else:
		print('Improper input for the isActive input. \nIf you want active players, set isActive to "Y" or "Yes". \nIf you want inactive players, set isActive to "N" or "No".\n\nIn the meantime, your search will search for all players in MLB history.')

	if len(search) > 0:
		print(f"Searching for a player named \"{search}\".")

		searchURL= searchURL + f"&name_part='{search}%25'"

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		result_count = int(resp_json['search_player_all']['queryResults']['totalSize'])
		if result_count > 0:
			print(f'{result_count} players found,\nParsing results into a dataframe.')

			for i in resp_json['search_player_all']['queryResults']['row']:

				p_df = json_normalize(resp_json['search_player_all']['queryResults']['row'])
				main_df = pd.concat([p_df,main_df],ignore_index=True)
		else:
			print(f'No results found for {search}. \nTry a different search for better results.')
		main_df.drop_duplicates(subset="player_id",keep="first",inplace=True)
		return main_df

	else:
		print("To search for MLB players in the MLBAM API, you must include text relating to the player you're searching for.")

def mlbam_player_info(playerID:int):
	"""Retrieves the player info for an MLB player, given a proper MLBAM ID

	Args:
		playerID (int):
			Required parameter. If no playerID is provided, the function wil not work.
	
	Returns:
		A pandas dataframe cointaining player information for the specified MLBAM player ID.
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.player_info.bam?sport_code='mlb'&player_id="

	if playerID < 1:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"\'{playerID}\'%27"

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['player_info']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			main_df = json_normalize(resp_json['player_info']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a different search for better results.')

		return main_df

def mlbam_player_teams(playerID:int,season:int):
	"""
	Retrieves the info regarding which teams that player played for in a given season, or in the player's career.

	Args:
		playerID (int):
			Required parameter. If no playerID is provided, the function wil not work.

		season (int):
			Required parameter. If provided, the search will only look for teams
			that player played for in that season.

	Returns:
		A pandas dataframe containing teams a player played for in that season.
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.player_teams.bam?"

	if season >1 and season < 1860:
		print('Enter a valid season. Baseball wasn\'t really a thing in the year you specified.')
	elif season > 1860:
		searchURL = searchURL + f'season=\'{season}\'&'
	else:
		print('Searching for all the teams this player has played on')

	if playerID < 1:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['player_teams']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:

			print(f'{result_count} players found,\nParsing results into a dataframe.')
			main_df = json_normalize(resp_json['player_teams']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a different search for better results.')
		return main_df
