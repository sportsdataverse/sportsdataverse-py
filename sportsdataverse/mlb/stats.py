## Script: stats.py
## Author: Joseph Armstrong (armstjc)

import pandas as pd
from pandas import json_normalize
import json
from sportsdataverse.dl_utils import download
from datetime import datetime

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


def getSeasonHittingStats(playerID=0,season=0,gameType="R"):
	'''
	Retrives the hitting stats for an MLB player in a given season, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.

	season (int):
		Required paramater. Indicates the season you are trying to find the games for.

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
	'''
	pullCopyrightInfo()
	#p_df = pd.DataFrame()
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
	if season < 1860:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	elif int(now.year) < season:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	else:
		searchURL = searchURL  + f'season=\'{season}\'&'



	if playerID < 1 or season < 1860:
		print('You must provide a playerID and a proper season. Function aborted.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"
		
		
		#searchURL = urllib.parse.quote_plus(str(searchURL))
		resp = download(searchURL)

		#print(searchURL)
		resp_str = str(resp, 'UTF-8')
		#print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_hitting_tm']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['sport_hitting_tm']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df


def getSeasonPitchingStats(playerID=0,gameType="R",season=0):
	'''
	Retrives the pitching stats for an MLB player in a given season, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.

	season (int):
		Required paramater. Indicates the season you are trying to find the games for.

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
	'''
	pullCopyrightInfo()
	#p_df = pd.DataFrame()
	main_df = pd.DataFrame()
	
	searchURL = "http://lookup-service-prod.mlb.com/json/named.sport_career_hitting.bam?league_list_id='mlb'&"

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
	if season < 1860:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	elif int(now.year) < season:
		print('Please input a proper year. The search will continue with the current year instead.')
		season = int(now.year)
		searchURL = searchURL  + f'season=\'{season}\'&'
	else:
		searchURL = searchURL  + f'season=\'{season}\'&'



	if playerID < 1:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"
		
		
		#searchURL = urllib.parse.quote_plus(str(searchURL))
		resp = download(searchURL)

		#print(searchURL)
		resp_str = str(resp, 'UTF-8')
		#print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_hitting_tm']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['sport_hitting_tm']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df


def getCareerHittingStats(playerID=0,gameType="R"):
	'''
	Retrives the career hitting stats for an MLB player, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.

	season (int):
		Required paramater. Indicates the season you are trying to find the games for.

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
	'''
	pullCopyrightInfo()
	#p_df = pd.DataFrame()
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




	if playerID < 1:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"
		
		
		#searchURL = urllib.parse.quote_plus(str(searchURL))
		resp = download(searchURL)

		print(searchURL)
		resp_str = str(resp, 'UTF-8')
		print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_career_hitting']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['sport_career_hitting']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df


def getCareerPitchingStats(playerID=0,gameType="R"):
	'''
	Retrives the career pitching stats for an MLB player, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.

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
	'''
	pullCopyrightInfo()
	#p_df = pd.DataFrame()
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

	if playerID < 1:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"
		
		
		#searchURL = urllib.parse.quote_plus(str(searchURL))
		resp = download(searchURL)

		print(searchURL)
		resp_str = str(resp, 'UTF-8')
		print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['sport_career_pitching']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['sport_career_pitching']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df

def getProjectedPitchingStats(playerID=0,gameType="R"):
	'''
	Retrives the projected pitching stats for an MLB player, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.

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
	'''
	pullCopyrightInfo()
	#p_df = pd.DataFrame()
	main_df = pd.DataFrame()
	
	searchURL = "http://lookup-service-prod.mlb.com/json/named.proj_pecota_pitching.bam?league_list_id='mlb'&"

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

	if playerID < 1:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"
		
		
		#searchURL = urllib.parse.quote_plus(str(searchURL))
		resp = download(searchURL)

		print(searchURL)
		resp_str = str(resp, 'UTF-8')
		print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['proj_pecota_pitching']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['proj_pecota_pitching']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df

def getProjectedHittingStats(playerID=0,gameType="R"):
	'''
	Retrives the projected hitting stats for an MLB player, given a proper MLBAM ID

	Args:
	
	playerID (int):
		Required paramater. If no playerID is provided, the function wil not work.

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
	'''
	pullCopyrightInfo()
	#p_df = pd.DataFrame()
	main_df = pd.DataFrame()
	
	searchURL = "http://lookup-service-prod.mlb.com/json/named.proj_pecota_batting.bam?league_list_id='mlb'&"

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

	if playerID < 1:
		print('You must provide a playerID. Without a proper playerID, this function will not work.')
		return None
	else:
		searchURL= searchURL + f"player_id=\'{playerID}\'"
		
		
		#searchURL = urllib.parse.quote_plus(str(searchURL))
		resp = download(searchURL)

		print(searchURL)
		resp_str = str(resp, 'UTF-8')
		print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['proj_pecota_batting']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['proj_pecota_batting']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df

