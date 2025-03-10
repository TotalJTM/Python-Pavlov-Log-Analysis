# !/usr/bin/python

from pav_la.pavlov_server_db_manager import Pavlov_Server_Database
import re
from datetime import datetime, timedelta
import logging


def get_datetime_from_line(text):
	patterns = [
		r'\[\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3}\]',
		r'\[\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\]'
	]
	try:
		for pattern in patterns:
			result = re.search(pattern, text)
			if result:
				date_time_str = result.group().strip('[').strip(']')
				date_format = "%Y.%m.%d-%H.%M.%S:%f"
				date_time_obj = datetime.strptime(date_time_str, date_format)
				date_time_obj = date_time_obj-timedelta(hours=5)
				return date_time_obj.timestamp()
			else:
				logging.info('Error when getting datetime in line "{text}"')
				return None
	except:
		return None

def parse_server_log_into_database(target_path, database_obj):
	full_text = None
	# try:
	logging.info(f' Reading in {target_path} '.center(64,'='))
	with open(target_path, 'r', encoding="utf-8") as f:
		full_text = f.read()
		lines = full_text.split('\n')
		main_ind = 0
		continue_looping = True
		second_round = False
		while continue_looping:
			# check if the iterator is longer than lines length, prevents array overruns
			if main_ind >= len(lines):
				continue_looping = False
				continue

			if '' == lines[main_ind]:
				main_ind += 1
				# continue_looping = False
				continue

			# parse a 'StatManagerLog' item (is a complicated dict parser that may not work for other dict representations in strings)
			if 'StatManagerLog' in lines[main_ind] and '{' in lines[main_ind]:
				logging.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
				start_ind = main_ind
				end_ind = None

				main_ind += 1
				iter_ind = start_ind + 1
				stat_looping = True

				dict_stack = [{}]
				dict_key_stack = []
				arr_stack = []
				dict_level = 1
				is_dict = [True]
				while iter_ind < len(lines) and stat_looping:
					raw_line = lines[iter_ind]
					line = raw_line.replace('\t', '')
					# logging.info(line)

					iter_ind += 1 # increase iter_ind since we wont use it again

					# handle the killdata line with improperly formatted dict {
					if '"KillData":' in line:
						iter_ind += 1 # increase iter again to cover the next line 
						dict_level += 1

						is_dict.append(True)
						dict_stack.append({})
						dict_key_stack.append('KillData')
						continue

					# handle the RoundState line with improperly formatted dict {
					if '"RoundState":' in line:
						iter_ind += 1 # increase iter again to cover the next line 
						dict_level += 1

						is_dict.append(True)
						dict_stack.append({})
						dict_key_stack.append('RoundState')
						continue

					# handle the killdata line with improperly formatted dict {
					if '"SwitchTeam":' in line:
						iter_ind += 1 # increase iter again to cover the next line 
						dict_level += 1

						is_dict.append(True)
						dict_stack.append({})
						dict_key_stack.append('SwitchTeam')
						continue

					# handle a normal key, value pair (this will be most key,value pairs)
					if ':' in line and line.endswith('",'):
						logging.info(line)
						key_value = line.split(':')
						key = key_value[0].strip(' ').strip('"')
						value = ''.join(key_value[1:]).strip(' ').strip(',').strip('"')

						dict_stack[-1][key] = value
						continue

					if '{' in line:
						dict_stack.append({})
						dict_level += 1

						if is_dict[-1]:
							split_line = line.split(':')
							key = split_line[0].strip(' ').strip('"')
							dict_key_stack.append(key)

						is_dict.append(True)

					elif '}' in line:
						dict_level -= 1
						is_dict.pop()
						if is_dict == []:
							stat_looping = False
							end_ind = iter_ind
							continue

						popped_dict = dict_stack.pop()

						if is_dict[-1]:
							popped_key = dict_key_stack.pop()
							dict_stack[-1][popped_key] = popped_dict
						else:
							arr_stack[-1].append(popped_dict)

						if dict_level <= 1:
							stat_looping = False
							end_ind = iter_ind
							continue

					elif '[' in line:
						dict_level += 1
						# is_dict = False
						is_dict.append(False)
						split_line = line.split(':')
						key = split_line[0].strip(' ').strip('"')

						arr_stack.append([])
						dict_key_stack.append(key)
						# logging.info(dict_key_stack)

					elif ']' in line:
						# logging.info(dict_stack)
						# logging.info(arr_stack)
						# is_dict = True
						is_dict.pop()
						dict_level -= 1

						popped_key = dict_key_stack.pop()
						popped_arr = arr_stack.pop()
						dict_stack[-1][popped_key] = popped_arr

					# handle a key value pair (same code as above, is the fallback to prevent loose []/{} characters in strings from screwing dict parsing)
					elif ':' in line:
						logging.info(line)
						key_value = line.split(':')
						key = key_value[0].strip(' ').strip('"')
						value = ''.join(key_value[1:]).strip(' ').strip(',').strip('"')

						dict_stack[-1][key] = value

				reconstructed = dict_stack[0]
				logging.info(reconstructed)

				log_datetime = get_datetime_from_line(lines[start_ind])
				logging.info(log_datetime)

				if data:=reconstructed.get('KillData', None):
					logging.info('KillData')
					database_obj.add_kill_table_entry(
						killer_id = data.get('Killer', 0),
						killed_id = data.get('Killed', 0),
						killer_teamid = data.get('KillerTeamID', -1),
						killed_teamid = data.get('KilledTeamID', -1),
						killed_by = data.get('KilledBy', ''),
						is_headshot = 1 if data.get('Headshot', -1) == 'true' else 0,
						entry_timecode = log_datetime,
					)

				elif data:=reconstructed.get('allStats', None):
					logging.info('allStats')
					for player in data:

						stat_type_dict = {}
						for stat in player.get('stats', {}):
							stat_type_dict[stat['statType']] = stat['amount']

						logging.info(stat_type_dict)

						# no stat entry should have a steam id of ''
						if player.get('uniqueId', '') == '':
							continue

						database_obj.add_stats_table_entry(
							steam_id = player.get('uniqueId', ''), 
							steam_name = player.get('playerName', ''), 
							kill_count = stat_type_dict.get('Kill', 0), 
							death_count = stat_type_dict.get('Death', 0), 
							assist_count = stat_type_dict.get('Assist', 0), 
							teamkill_count = stat_type_dict.get('TeamKill', 0), 
							headshot_count = stat_type_dict.get('Headshot', 0), 
							experience_count = stat_type_dict.get('Experience', 0), 
							map_label = reconstructed.get('MapLabel', '').strip('UGC'), 
							mode = reconstructed.get('GameMode', ''), 
							team_id = player.get('teamId', ''),
							entry_timecode = log_datetime, 
						)

				elif data:=reconstructed.get('RoundState', None):

					state = -1
					state_str = data.get('State', "")
					if state_str == "Starting":
						state = 0
					elif state_str == "StandBy":
						state = 1
					elif state_str == "Started":
						state = 2
					elif state_str == "Ended":
						state = 3
					else:
						pass


					reason = 0
					if state == 3:
						t_ind = start_ind-1
						while start_ind-t_ind < 20:
							line_text = lines[t_ind]
							if 'Reason:' in line_text:
								logging.info(line_text)
								# reason_str = lines[t_ind].split('Reason:')[1]
								if 'SitesDestroyed' in line_text:
									reason = 1
								elif 'OutOfTicketsAttackers' in line_text:
									reason = 2
								elif 'OutOfTicketsDefenders' in line_text:
									reason = 3
								else:
									pass

								break
							else:
								t_ind -= 1


					logging.info(f'RoundState {data}, Reason = {reason}, State = {state}')
					database_obj.add_roundstate_table_entry(
						state = state, 
						reason_int = reason,
						entry_timecode = log_datetime,
					)

				elif data:=reconstructed.get('SwitchTeam', None):
					logging.info('SwitchTeam')
					database_obj.add_teamswitch_table_entry(
						steam_id = data.get('PlayerID', ''), 
						team_id = data.get('NewTeamID', -1),
						reason_int = 1,
						entry_timecode = log_datetime,
					)

				main_ind = end_ind + 1

			if 'Rcon: SwitchTeam' in lines[main_ind]:
				line = lines[main_ind]
				log_datetime = get_datetime_from_line(line)
				split_line = line.split('Rcon: SwitchTeam ')
				steamid, teamid = split_line[1].replace('TeamID:','').split(' ')
				database_obj.add_teamswitch_table_entry(
						steam_id = steamid, 
						team_id = teamid,
						reason_int = 0,
						entry_timecode = log_datetime,
					)

			if 'LogTemp: Bomb Armed' in lines[main_ind]:
				line = lines[main_ind]
				log_datetime = get_datetime_from_line(line)
				database_obj.add_event_table_entry(
						event_int = 0, 
						player = -1,
						entry_timecode = log_datetime,
					)

			if 'LogLoad: LoadMap:' in lines[main_ind] and '??listen?game=' in lines[main_ind]:
				line = lines[main_ind]
				log_datetime = get_datetime_from_line(line)
				# print(line)

				ugc_name = line.split('LogLoad: LoadMap:')[1].split('??listen?game=')[0]
				ugc_name = ugc_name.split('/')
				# print(ugc_name)
				mapid = ugc_name[1].replace('UGC','')
				mapname = '_'.join(ugc_name[2:])
				# print(mapname)
				database_obj.add_maps_table_entry(
					map_id = mapid, 
					map_name = mapname, 
					entry_timecode=log_datetime
				)

			#
			if 'LogNet: Login request' in lines[main_ind]:
				try:
					line = lines[main_ind]
					logging.info(line)

					log_datetime = get_datetime_from_line(line)

					# Define a regex pattern to match the specified variables
					# pattern = r'\?Name=(?P<Name>[^?]+)\?playerHeight=(?P<playerHeight>[^?]+)\?rightHanded=(?P<rightHanded>[^?]+)\?vstock=(?P<vstock>[^?]+)\?pid=(?P<pid>[^?]+)\?name=(?P<name>[^?]+) userId: (?P<userId>[^?]+) platform: (?P<platform>[^?]+)'

					# match = re.search(pattern, line)
					variables_dict = {}
					# [2024.11.05-04.28.10:119][162]LogNet: Login request: ?Name=Mint?playerHeight=160.000000?rightHanded=1?vstock=1?platform=steam?pid=76561198137550699?name=Mint userId: NULL:0002f5697d014c168b0ce2d6ae5c8796 platform: NULL
					split_line = line.split('?Name=')
					split_line = split_line[1].split('?playerHeight=')
					variables_dict['Name'] = split_line[0]
					split_line = split_line[1].split('?rightHanded=')
					variables_dict['playerHeight'] = split_line[0]
					split_line = split_line[1].split('?vstock=')
					variables_dict['rightHanded'] = split_line[0]
					split_line = split_line[1].split('?platform=')
					variables_dict['vstock'] = split_line[0]
					split_line = split_line[1].split('?pid=')
					variables_dict['platform'] = split_line[0]
					split_line = split_line[1].split('?name=')
					variables_dict['pid'] = split_line[0]
					# split_line = split_line[1].split('?name=')

					# split_line = split_line[1].split(' userId: ')

					logging.info(variables_dict)

					if variables_dict:
						looper=0
						login_success = 0
						while looper < 400:
							looper += 1

							# if f'Join succeeded: {variables_dict.get("Name", "   ")}' in lines[main_ind+looper]:
							if f'Join succeeded:' in lines[main_ind+looper]:
								logging.info('Login Succeeded')
								if variables_dict['Name']:
									login_success = 1
							
						if login_success == 0:
							logging.info(f"Login failed for {variables_dict.get('pid', '')}")

							# import time
							# time.sleep(10)
						database_obj.add_login_table_entry(
							steam_id = variables_dict.get('pid', ''), 
							hardware_id = variables_dict.get('userId', '').strip('NULL:'),  
							steam_name = variables_dict.get('Name', ''),  
							player_height = variables_dict.get('playerHeight', ''), 
							player_vstock = variables_dict.get('vstock', ''), 
							player_left_hand = 0 if variables_dict.get('rightHanded', '') == '1' else 1, 
							login_success = login_success,
							entry_timecode = log_datetime, 
						)
						database_obj.add_player_table_entry(
							steam_id = variables_dict.get('pid', ''), 
							steam_name = variables_dict.get('Name', ''), 
							vac_ban_status = 0, 
							steam_logo = '', 
							entry_timecode = log_datetime,
							)
				except:
					logging.exception(f'line {main_ind}')

				main_ind += 1

			else:
				main_ind += 1

			# logging.info(f'{main_ind}/{len(lines)}')

				



	# except Exception as e:
	# 	logging.info(f'Encountered exception: {e}')


if __name__ == "__main__":
	import argparse 
	import os
	import sys

	logging.basicConfig(filename="pavlov_server_log_parser.log",
					format='%(asctime)s %(message)s',
					filemode='w+')
	logger = logging.getLogger()
	logger.addHandler(logging.StreamHandler(sys.stdout))

	logger.setLevel(logging.INFO)

	parser = argparse.ArgumentParser(description='Program to parse a Pavlov log file and add the contents to a database')
	parser.add_argument('-lf','--log-file', type=str, default='', help='Path to the target pavlov server log file')
	parser.add_argument('-ld','--log-directory', type=str, default='',help='Path to the target pavlov server log file directory')
	parser.add_argument('-db','--database-file', type=str, default='',help='Path to database this data should be added to')

	# parse the resulting commandline args
	args = parser.parse_args()
	arg_opts = vars(args)
	logging.info(arg_opts)
	
	if arg_opts['database_file'] == '':
		logging.error(f'Could not proceed, no database file path')
		os._exit(1)

	psd = Pavlov_Server_Database(arg_opts['database_file'])
	
	try:
		if arg_opts['log_file'] != '':
			parse_server_log_into_database(arg_opts['log_file'], psd)

		if arg_opts['log_directory'] != '':
			for root, dirs, files in os.walk(arg_opts['log_directory']):
				for file in files:
					if file.endswith('.log'):
						parse_server_log_into_database(os.path.join(root,file), psd)
				break

		logging.info(f'Committing {psd.uncommitted_entries} changes')
		psd.commit_changes()

	except Exception as e:
		logger.exception(f'Encountered exception {e}')
		yes_no = input(f'Would you like to commit {psd.uncommitted_entries} entries generated during session? (y)es or (n)o')
		if 'yes' in yes_no.lower() or 'y' == yes_no.lower():
			logger.info(f'Committing {psd.uncommitted_entries} changes')
			psd.commit_changes()