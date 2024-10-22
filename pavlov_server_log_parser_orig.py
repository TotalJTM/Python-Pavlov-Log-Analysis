# !/usr/bin/python

from pavlov_server_db_manager import Pavlov_Server_Database
import json


def parse_server_log_into_database(target_path, database_obj):
	full_text = None
	# try:
	print(f' Reading in {target_path} '.center(64,'='))
	with open(target_path, 'r') as f:
		full_text = f.read()
		lines = full_text.split('\n')
		main_ind = 0
		continue_looping = True
		while continue_looping:
			if '' == lines[main_ind]:
				break

			if 'StatManagerLog' in lines[main_ind] and '{' in lines[main_ind]:
				start_ind = main_ind
				end_ind = None

				stack = [{}]
				list_stack = []
				keys_for_level = []
				continue_loop = True
				index = main_ind
				is_dict = False
				while index < len(lines) and continue_loop:
					line = lines[index].replace('\t', '')#.replace('   ', '')
					# print(lines[index])
					print(line)

					# If it's a curly brace, update the stack accordingly
					if '{' in line:
						is_dict = True
						stack.append({})
					elif '}' in line:
						if len(stack) == 1:
							continue_loop = False
							continue

						current_dict = stack.pop()
						if is_dict:
							
							stack[-1][key] = current_dict
						else:
							list_stack.append(current_dict)

					# If it's a square bracket, update the list stack accordingly
					elif '[' in line:
						is_dict = False
						if line.endswith(': ['):
							list_stack.append([])
							line_stripped = line.strip(': [').strip('"')
							print(f'Line stripped: {line_stripped}')
							keys_for_level.append(line_stripped)

					elif ']' in line:
						current_list = list_stack.pop()

						# If the list stack is not empty, add the current_list to the top of the stack
						if list_stack:
							list_stack[-1][key] = current_list
						else:
							stack[-1][key] = current_list

					# If it's a colon, the preceding text is the key
					elif ':' in line:
						key, val = line.split(':')
						val = val.strip(',').strip(' ').strip('"')
						key = key.strip('"')
						# if is_dict:
						stack[-1][key] = val
						# else:
						# 	list_stack.append()

					# If it's a comma and we are not inside a string, move to the next key-value pair
					# elif char == "," and '"' not in data_str[:index]:
					# 	value = data_str[:index].strip()

					# 	if value:
					# 		# If the value is a list, add it to the list stack
					# 		if "[" in value:
					# 			list_stack[-1].append(ast.literal_eval(value))
					# 		else:
					# 			stack[-1][key] = ast.literal_eval(value)

					# 	index += 1

					# Move to the next character
					index += 1

				reconstructed = stack.pop()

				kill_data = reconstructed.get("KillData")
				all_data = reconstructed.get("allStats")
				print(kill_data)
				print(all_data)

				if kill_data != None:
					print(kill_data)

				if all_data != None:
					print(all_data)


				main_ind = index

			else:
				main_ind += 1

				



	# except Exception as e:
	# 	print(f'Encountered exception: {e}')


if __name__ == "__main__":
	import argparse 
	import os

	parser = argparse.ArgumentParser(description='Program to parse a Pavlov log file and add the contents to a database')
	parser.add_argument('-lf','--log-file', type=str, default='', help='Path to the target pavlov server log file')
	parser.add_argument('-ld','--log-directory', type=str, default='',help='Path to the target pavlov server log file directory')
	parser.add_argument('-db','--database-file', type=str, default='',help='Path to database this data should be added to')

	# parse the resulting commandline args
	args = parser.parse_args()
	arg_opts = vars(args)
	
	if arg_opts['database_file'] == '':
		print(f'Could not proceed, no database file path')
		os._exit(1)

	psd = Pavlov_Server_Database(arg_opts['database_file'])

	if arg_opts['log_file'] != '':
		parse_server_log_into_database(arg_opts['log_file'], psd)

	if arg_opts['log_directory'] != '':
		for root, dirs, files in os.walk(arg_opts['log_directory']):
			for file in files:
				if file.endswith('.log'):
					parse_server_log_into_database(os.path.join([root,file]), psd)