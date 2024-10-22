import json
import os
import tempfile
import zipfile
import hashlib

import requests


major_vers = 1
minor_vers = 3

class PavlovUpdater:
	# need to initialize the class with:
	#	pavlov_mod_dir_path = path to the pavlov mod directory
	#	modio_api_token = Mod.io API token this program will use to read+write data from/to the Mod.io API
	# API tokens can be acquired from Mod.IO on the "https://mod.io/me/access" page
	def __init__(self, pavlov_mod_dir_path, modio_api_token, logging_obj) -> None:
		self.pavlov_mod_dir_path = pavlov_mod_dir_path
		self.pavlov_gameid = '3959'
		# self.settings_path = ''
		self.modio_api_url = 'https://api.mod.io/v1'
		self.modio_api_token = modio_api_token
		self.target_os = 'windows'
		self.logger = logging_obj

	# make a get request to modio
	#	route = address to make request at (ex. games/3959/mods)
	#	ret_json = converts response to json if True, return raw response if False
	#	raw = the function will not add the modio api url, what is supplied to route is the address the request will be made to
	def modio_get(self, route, ret_json=True, raw=False):
		self.logger.info(f'Get request {route}')
		# assemble address and header
		addr = f"{self.modio_api_url}/{route}"
		head = {'Authorization': f'Bearer {self.modio_api_token}', 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
		# send request
		response = requests.get(addr if raw==False else route, params={}, headers=head)
		# convert the response to a json dict
		if ret_json == True:
			d = response.json()
			if 'error' in d.keys():
				self.logger.error(f"response containted error {d['error']['code']}, {d['error']['message']}")
				return f"error{d['error']['code']}"

		if ret_json:
			return d
		else:
			return response
		
	# make a post request to modio
	#	route = address to make request at (ex. games/3959/mods)
	def modio_post(self, route, ret_json=True, params={}):
		self.logger.info(f'Post request {route}')
		# assemble address and header
		addr = f"{self.modio_api_url}/{route}"
		head = {'Authorization': f'Bearer {self.modio_api_token}', 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
		# send request
		response = requests.post(addr, params=params, headers=head)
		# convert the response to a json dict
		if ret_json == True:
			d = response.json()
			if 'error' in d.keys():
				self.logger.error(f"response containted error {d['error']['code']}, {d['error']['message']}")
				return f"error{d['error']['code']}"

		if ret_json:
			return d
		else:
			return response
	
	
	# make a delete request to modio
	#	route = address to make request at (ex. games/3959/mods)
	def modio_delete(self, route):
		self.logger.info(f'Delete request {route}')
		# assemble address and header
		addr = f"{self.modio_api_url}/{route}"
		head = {'Authorization': f'Bearer {self.modio_api_token}', 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
		# send request
		response = requests.delete(addr, params={}, headers=head)

		return response
	
	# get an image from modio
	#	route = image address acquired from a mod message
	def get_modio_image(self, route):
		self.logger.info(f'Get modio image {route}')
		# assemble address and header
		# addr = f"{self.modio_api_url}/{route}"
		head = {'Authorization': f'Bearer {self.modio_api_token}', 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
		# send request
		response = requests.get(route, params={}, headers=head)

		if response.status_code != 200:
			return None
		return response.content
	
	# rate a mod on modio
	def modio_rate_mod(self, ugc, like=False, dislike=False):
		resp = None
		if like:
			resp = self.modio_post(f'games/{self.pavlov_gameid}/mods/{ugc}/ratings', params={'rating':1}, ret_json=False)
		elif dislike:
			resp = self.modio_post(f'games/{self.pavlov_gameid}/mods/{ugc}/ratings', params={'rating':-1}, ret_json=False)
		else:
			resp = self.modio_post(f'games/{self.pavlov_gameid}/mods/{ugc}/ratings', params={'rating':0}, ret_json=False)

		if resp:
			if resp.status_code == 404:
				return False
			return True
		return False
		
	# get the user ratings from modio and strip it of non-pavlov games
	def get_modio_user_ratings(self):
		resp = self.modio_get(f'me/ratings', ret_json=False)
		if resp.status_code != 200:
			return {}
		else:
			decoded_cont = resp.json()
			rating_dict = {}
			for item in decoded_cont["data"]:
				if item['game_id'] == int(self.pavlov_gameid):
					rating_dict[item['mod_id']] = item['rating']
			return rating_dict

	# get the full modlist for Pavlov
	def get_pavlov_modlist(self):
		self.logger.info(f'Getting Pavlov modlist')
		mods = []
		init_resp = self.modio_get(f'games/{self.pavlov_gameid}/mods?_limit=100')
		if 'error' in init_resp:
			return init_resp
		
		resp_result_tot = init_resp['result_total']
		resp_result_cnt = init_resp['result_count']

		if resp_result_tot == 0 or resp_result_cnt == 0:
			self.logger.info(f'No result: total = {resp_result_tot}, count = {resp_result_cnt}')
			return f'errorno mods found'
		
		# create a dict to enter into the mods folder
		def make_entry(m):
			modfile_live_win = None
			for p in m['platforms']:
				# only use downloads for the target operating system
				if p['platform'] == self.target_os:
					modfile_live_win = p['modfile_live']
			
			if modfile_live_win == None:
				return None

			# print(m)
			# attributes in a subscribed modlist entry
			return m

		# go through response and make/add entrys to the mods arr
		for m in init_resp['data']:
			try:
				entry = make_entry(m)
			except:
				self.logger.exception('Error making mod')
				self.logger.error(f'Mod {m}')
				continue
			if entry != None:
				mods.append(entry)
			

		# calculate number of pages to get all users subscribed mods
		total_pages = int(resp_result_tot/resp_result_cnt)+1

		# iter through pages calculated
		for i in range(1,total_pages):
			# get new response (but paginated)
			resp = self.modio_get(f'games/{self.pavlov_gameid}/mods?_offset={int(i*100)}&_limit=100')
			if 'error' in resp:
				return resp
			# go through response and make/add entrys to the mods arr
			for m in resp['data']:
				entry = make_entry(m)
				if entry != None:
					mods.append(entry)
				
		return mods
	

	# get the full subscription list of the user 
	# returns a dictionary entry for each subscribed mod with attributes listed below
	def get_subscribed_modlist(self):
		self.logger.info(f'Getting subscribed modlist')
		mods = []
		# get initial subscribed mods for pavlov (first 100 entrys)
		init_resp = self.modio_get(f'me/subscribed?game_id={self.pavlov_gameid}&_limit=100')
		if 'error' in init_resp:
			return init_resp

		resp_result_tot = init_resp['result_total']
		resp_result_cnt = init_resp['result_count']

		if resp_result_tot == 0 or resp_result_cnt == 0:
			self.logger.info(f'No result: total = {resp_result_tot}, count = {resp_result_cnt}')
			return f'errorno subscribed mods found'

		# create a dict to enter into the mods folder
		def make_entry(m):
			modfile_live_win = None
			for p in m['platforms']:
				# only use downloads for the target operating system
				if p['platform'] == self.target_os:
					modfile_live_win = p['modfile_live']
			
			if modfile_live_win == None:
				return None

			# attributes in a subscribed modlist entry
			return m

		# go through response and make/add entrys to the mods arr
		for m in init_resp['data']:
			entry = make_entry(m)
			if entry != None:
				mods.append(entry)

		# calculate number of pages to get all users subscribed mods
		total_pages = int(resp_result_tot/resp_result_cnt)+1

		# iter through pages calculated
		for i in range(1,total_pages):
			# get new response (but paginated)
			resp = self.modio_get(f'me/subscribed?game_id={self.pavlov_gameid}&_offset={int(i*100)}&_limit=100')
			if 'error' in resp:
				return resp
			# go through response and make/add entrys to the mods arr
			for m in resp['data']:
				entry = make_entry(m)
				if entry != None:
					mods.append(entry)
				
		return mods
	
	
	
if __name__ == "__main__":
	import logging
	import sys

	logging.basicConfig(filename="pypavlovupdater.log",
						format='%(asctime)s %(message)s',
						filemode='w+')
	logger = logging.getLogger()
	logger.addHandler(logging.StreamHandler(sys.stdout))

	logger.setLevel(logging.INFO)
	# logger.setLevel(logging.ERROR)

	logger.info(f'PyPavlovUpdater Version {major_vers}.{minor_vers}\n')

	# use the configuration manager to load configuration variables from the .conf file
	import settings_manager

	conf_dict = None
	cm = settings_manager.Conf_Manager('PPU.conf', logger)
	if os.path.exists('PPU.conf'):
		conf_dict = cm.get_file_conts_as_dict()

	# file doesnt exist so make a new file
	else:
		logger.info('PPU.conf does not exist, creating file')
		cm.make_new_conf_file()

	# create variables to hold state of api, directory vars and whether the conf should be updated
	api_ok = False
	dir_ok = False
	update = False

	# if the dict is none, add keys to conf_dict (will trigger input)
	if conf_dict == None:
		conf_dict = {'modio_api_token':'', 'pavlov_mod_dir_path':''}

	# check if 'modio_api_token' str in dict is empty, get the user to input a valid API token (until ctrl+c)
	if conf_dict['modio_api_token'] == "":
		try:
			modio_api_token_input = None
			while modio_api_token_input == None:
				modio_api_token_input = input('Paste the Mod.io API token: ')
				if modio_api_token_input != "" and len(modio_api_token_input) > 64:
					api_ok = True
					update = True
					conf_dict['modio_api_token'] = modio_api_token_input
				else:
					logger.info(f'Invalid API token input')
					modio_api_token_input = None
		except:
			logger.exception(f'Exception when attempting to get token')
			logger.info(f'Canceled attempt to enter API token')
	else:
		api_ok = True
	
	# check if 'pavlov_mod_dir_path' str in dict is empty, get the user to input a valid API token (until ctrl+c)
	if conf_dict['pavlov_mod_dir_path'] == "":
		try:
			pavlov_mod_dir_path_input = None
			while pavlov_mod_dir_path_input == None:
				pavlov_mod_dir_path_input = input('Paste the path to the Pavlov mod directory: ')
				if pavlov_mod_dir_path_input != "":
					dir_ok = True
					update = True
					conf_dict['pavlov_mod_dir_path'] = pavlov_mod_dir_path_input
				else:
					logger.info(f'Invalid Pavlov directory input')
					pavlov_mod_dir_path_input = None
		except:
			logger.exception(f'Exception when attempting to get mod path')
			logger.info(f'Canceled attempt to enter mod path')
	else:
		dir_ok = True

	# update the configuration if either the API or mod dir have changed
	if update:
		os.remove('PPU.conf')
		cm.make_new_conf_file(conf_dict['modio_api_token'], conf_dict['pavlov_mod_dir_path'])
	
	# check if there is an API string and mod directory path
	if api_ok and dir_ok:
		# create pavlov updater object
		pu = PavlovUpdater(pavlov_mod_dir_path=conf_dict['pavlov_mod_dir_path'], modio_api_token=conf_dict['modio_api_token'], logging_obj=logger)
		# get all subscribed modes
		logger.info(f'Updating subscribed mods')
		pu.update_subscribed_mods()
	
		logger.info('=== Finished Updating ===')