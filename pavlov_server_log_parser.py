# !/usr/bin/python

from pavlov_server_db_manager import Pavlov_Server_Database
import json
import re
from datetime import datetime
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
	with open(target_path, 'r') as f:
		full_text = f.read()
		lines = full_text.split('\n')
		main_ind = 0
		continue_looping = True
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
						killer_id = data.get('Killer', ''), 
						killed_id = data.get('Killed', ''), 
						killed_by = data.get('KilledBy', ''), 
						is_headshot = 1 if data.get('Headshot', '') == 'true' else 0,
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
					logging.info('RoundState')

				main_ind = end_ind + 1

			#
			if 'LogNet: Login request' in lines[main_ind]:
				line = lines[main_ind]
				logging.info(line)

				log_datetime = get_datetime_from_line(line)

				# Define a regex pattern to match the specified variables
				pattern = r'\?Name=(?P<Name>[^?]+)\?playerHeight=(?P<playerHeight>[^?]+)\?rightHanded=(?P<rightHanded>[^?]+)\?vstock=(?P<vstock>[^?]+)\?pid=(?P<pid>[^?]+)\?name=(?P<name>[^?]+) userId: (?P<userId>[^?]+) platform: (?P<platform>[^?]+)'

				match = re.search(pattern, line)
				logging.info(match)

				if match:
					variables_dict = match.groupdict()
					logging.info(variables_dict)
					looper=0
					while looper < 100:
						looper += 1

						if f'Join succeeded: {variables_dict.get("Name", "   ")}' in lines[main_ind+looper]:
							logging.info('Login Succeeded')
							looper = 1500

					if looper == 1500:
						database_obj.add_login_table_entry(
							steam_id = variables_dict.get('pid', ''), 
							hardware_id = variables_dict.get('userId', '').strip('NULL:'),  
							steam_name = variables_dict.get('Name', ''),  
							player_height = variables_dict.get('playerHeight', ''), 
							player_vstock = variables_dict.get('vstock', ''), 
							player_left_hand = 0 if variables_dict.get('rightHanded', '') == '1' else 1, 
							entry_timecode = log_datetime, 
						)
					else:
						logging.info('Login failed')
						# import time
						# time.sleep(10)

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

		psd.commit_changes()

	except Exception as e:
		logger.exception('Encountered exception {e}')
		yes_no = input(f'Would you like to commit {psd.uncommitted_entries} entries generated during session? (y)es or (n)o')
		if 'yes' in yes_no.lower() or 'y' == yes_no.lower():
			logger.info('Committing changes')
			psd.commit_changes()