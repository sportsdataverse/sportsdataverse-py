## Script: teams.py
## Author: Joseph Armstrong (armstjc)

from re import search
import pandas as pd
from pandas import json_normalize
import json
from datetime import datetime
from sportsdataverse.dl_utils import download, underscore

import os


def mlbam_teams(season:int,retriveAllStarRosters=False):
	"""
	Retrieves the player info for an MLB team, given an MLB season.

	Args:
		season (int):
			Required parameter. If no season is provided, the function wil not work.

		retriveAllStarRosters (boolean):
			Optional parameter. If set to 'True', MLB All-Star rosters will be returned when
			running this function.

	Returns:
		A pandas dataframe containing information about MLB teams that played in that season.
	
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&"

	if retriveAllStarRosters == True:
		searchURL = searchURL + 'all_star_sw=\'Y\'&'
	else:
		searchURL = searchURL + 'all_star_sw=\'N\'&'

	now = datetime.now()

	if season < 1860 or season == None:
		print('1_Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'sort_order=\'name_asc\'&season=\'{season}\''
	elif int(now.year) < season:
		print('0_Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'sort_order=\'name_asc\'&season=\'{season}\''
	else:
		searchURL = searchURL  + f'sort_order=\'name_asc\'&season=\'{season}\''

	resp = download(searchURL)

	resp_str = str(resp, 'UTF-8')

	resp_json = json.loads(resp_str)
	try:
		result_count = int(resp_json['team_all_season']['queryResults']['totalSize'])
	except:
		result_count = 0

	if result_count > 0:

		print(f'{result_count} statlines found,\nParsing results into a dataframe.')
		main_df = json_normalize(resp_json['team_all_season']['queryResults']['row'])
		print('Done')
	else:
		print(f'No results found for the provided playerID. \nTry a different search for better results.')

	return main_df

def mlbam_40_man_roster(teamID:int):
	"""
	Retrieves the current 40-man roster for a team, given a proper MLBAM team ID.

	Args:

	teamID (int):
    	Required parameter. This should be the MLBAM team ID for the MLB team you want a 40-man roster from.

	Returns:
		A pandas dataframe containing the current 40-man roster for the given MLBAM team ID.
	"""

	main_df = pd.DataFrame()

	searchURL = 'http://lookup-service-prod.mlb.com/json/named.roster_40.bam?team_id='

	searchURL = searchURL + f'\'{teamID}\''

	resp = download(searchURL)

	resp_str = str(resp, 'UTF-8')

	resp_json = json.loads(resp_str)
	try:
		result_count = int(resp_json['roster_40']['queryResults']['totalSize'])
	except:
		result_count = 0

	if result_count > 0:

		print(f'{result_count} statlines found,\nParsing results into a dataframe.')
		main_df = json_normalize(resp_json['roster_40']['queryResults']['row'])
		print('Done')
	else:
		print(f'No results found for the provided playerID. \nTry a different search for better results.')

	return main_df

def mlbam_team_roster(teamID:int,startSeason:int,endSeason:int):
	"""
	Retrieves the cumulative roster for a MLB team in a specified timeframe.

	Args:
		teamID (int):
			Required parameter. This should be the number MLBAM associates for an MLB team.
			For example, the Cincinnati Reds have an MLBAM team ID of 113.

		startSeason (int):
			Required parameter. This value must be less than endSeason for this function to work.

		endSeason (int):
			Required parameter. This value must be greater than startSeason for this function to work.
	
	Returns:
		A pandas dataframe containg the roster(s) for the MLB team.
	"""
	holding_num = 0
	main_df = pd.DataFrame()

	if endSeason < startSeason:
		holding_num = endSeason
		startSeason = endSeason
		endSeason = holding_num
	else:
		pass

	searchURL = 'http://lookup-service-prod.mlb.com/json/named.roster_team_alltime.bam?'

	## Add the Season ranges
	searchURL = searchURL + f'start_season=\'{startSeason}\'&end_season=\'{endSeason}\'&'
	## Add the TeamID
	searchURL = searchURL + f'team_id=\'{teamID}\''

	resp = download(searchURL)


	resp_str = str(resp, 'latin-1')

	resp_json = json.loads(resp_str)
	try:
		result_count = int(resp_json['roster_team_alltime']['queryResults']['totalSize'])
	except:
		result_count = 0

	if result_count > 0:

		print(f'{result_count} statlines found,\nParsing results into a dataframe.')
		main_df = json_normalize(resp_json['roster_team_alltime']['queryResults']['row'])
		print('Done')
	else:
		print(f'No results found for the provided playerID. \nTry a different search for better results.')

	return main_df