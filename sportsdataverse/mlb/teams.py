## Script: teams.py
## Author: Joseph Armstrong (armstjc)

from re import search
import pandas as pd
from pandas import json_normalize
import json
from datetime import datetime
from sportsdataverse.dl_utils import download

import os


def pullCopyrightInfo(saveFile=False,returnFile=False):
	"""
	Displays the copyright info for the MLBAM API.

	Args:
	saveFile (boolean) = False
		If saveFile is set to True, the copyright file generated is saved.

	returnFile (boolean) = False
		If returnFile is set to True, the copyright file is returned.
	"""
	url = "http://gdx.mlb.com/components/copyright.txt"
	resp = download(url=url)

	l_string = str(resp, 'UTF-8')
	if resp is not None:
		with open("mlbam_copyright.txt","w+" ,encoding = "utf-8") as file:
			file.writelines(str(l_string))

		with open("mlbam_copyright.txt", "r" ,encoding = "utf-8") as file:
			mlbam = file.read()

		if saveFile == False:
			if os.path.exists("mlbam_copyright.txt"):
				os.remove("mlbam_copyright.txt")
			else:
				pass
		else:
			pass
		print(mlbam)

		if returnFile == True:
			return mlbam
		else:
			pass


	else:
		print('Could not connect to the internet. \nPlease fix this issue to be able to use this package.')

def getTeamData(season=0,retriveAllStarRosters=False):
	'''
	Retrives the player info for an MLB player, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.
	'''
	pullCopyrightInfo()
	#p_df = pd.DataFrame()
	main_df = pd.DataFrame()
	
	searchURL = "http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&"

	if retriveAllStarRosters == True:
		searchURL = searchURL + 'all_star_sw=\'Y\'&'
	else:
		searchURL = searchURL + 'all_star_sw=\'N\'&'

	now = datetime.now()

	if season < 1860:
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

	print(searchURL)
	resp_str = str(resp, 'UTF-8')
	print(resp_str)

	resp_json = json.loads(resp_str)
	try:
		result_count = int(resp_json['team_all_season']['queryResults']['totalSize'])
	except:
		result_count = 0

	if result_count > 0:
		#print(resp_json['player_teams']['queryResults']['row'])

		print(f'{result_count} statlines found,\nParsing results into a dataframe.')
		#players = resp_json['search_player_all']['queryResults']['row']
		main_df = json_normalize(resp_json['team_all_season']['queryResults']['row']) 
		print('Done')
	else:
		print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
	return main_df

def get40ManRoster(teamID=120):
	'''
	Retrives the player info for an MLB player, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.
	'''
	pullCopyrightInfo()
	
	main_df = pd.DataFrame()
	
	searchURL = 'http://lookup-service-prod.mlb.com/json/named.roster_40.bam?team_id='

	searchURL = searchURL + f'\'{teamID}\''	

	resp = download(searchURL)

	print(searchURL)
	resp_str = str(resp, 'UTF-8')
	print(resp_str)

	resp_json = json.loads(resp_str)
	try:
		result_count = int(resp_json['roster_40']['queryResults']['totalSize'])
	except:
		result_count = 0

	if result_count > 0:
		#print(resp_json['player_teams']['queryResults']['row'])

		print(f'{result_count} statlines found,\nParsing results into a dataframe.')
		#players = resp_json['search_player_all']['queryResults']['row']
		main_df = json_normalize(resp_json['roster_40']['queryResults']['row']) 
		print('Done')
	else:
		print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
	return main_df

def getAllTimeRoster(teamID=120,startSeason=2020,endSeason=2021):
	'''
	Retrives the player info for an MLB player, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.
	'''
	pullCopyrightInfo()
	
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

	#print(searchURL)
	
	resp_str = str(resp, 'latin-1')
	#print(resp_str)

	resp_json = json.loads(resp_str)
	try:
		result_count = int(resp_json['roster_team_alltime']['queryResults']['totalSize'])
	except:
		result_count = 0

	if result_count > 0:
		#print(resp_json['player_teams']['queryResults']['row'])

		print(f'{result_count} statlines found,\nParsing results into a dataframe.')
		#players = resp_json['search_player_all']['queryResults']['row']
		main_df = json_normalize(resp_json['roster_team_alltime']['queryResults']['row']) 
		print('Done')
	else:
		print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
	return main_df