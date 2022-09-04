from re import search
import pandas as pd
from pandas import json_normalize
import json
from datetime import datetime
from sportsdataverse.dl_utils import download

import os
def mlbam_copyright_info(saveFile=False,returnFile=False):
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
	try:
		if resp is not None:
			l_string = str(resp, 'UTF-8')
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
	except:
		print('Could not connect to the internet. \nPlease fix this issue to be able to use this package.')