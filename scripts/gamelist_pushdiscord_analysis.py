# !/usr/bin/python3
# Script written by TotalJTM 2024
# Version 1, 11/8/24

from pav_la import game_list_database_handler
import pandas as pd
import matplotlib.pyplot as plt

pd.set_option('display.precision', 2)  # Set decimal precision to 2 decimals

version = 1

# path to database files
gamelist_database_path = r'Y:/database_data/gamelist_records.db'
pushserver_name = '[PUSH Discord] High player PUSH maps'

import datetime
import logging

# generate a log timestamp str using datetime
def generate_log_timestamp_str():
	return datetime.datetime.now().strftime("%m_%d_%y__%H_%M_%S")

def decode_epoch_to_UTC(val):
	return datetime.datetime.fromtimestamp(int(val)).strftime('%H:%M:%S UTC on %m-%d-%Y')

# handle json files
def load_json_file(file_path):
	with open(file_path,'r') as f:
		return json.load(f)
	
def write_json_file(file_path, d):
	with open(file_path,'w+') as f:
		json.dump(d,f)

# call the necessary functions to start the logging module, returns a logging object
def generate_new_log(logpath, debuglevel=logging.INFO, print_to_console=True):
	import sys
	logging.basicConfig(filename=logpath,
						format='%(asctime)s : %(message)s',
						filemode='w+',
						)
	logger = logging.getLogger()
	logger.setLevel(debuglevel)
	if print_to_console:
		logger.addHandler(logging.StreamHandler(sys.stdout))	
	return logger


# program main
if __name__ == "__main__":
	import os
	import argparse
	import json
	parser = argparse.ArgumentParser(description='_____')
	parser.add_argument('-st','--start-time', type=int, help='Start time for analysis (use epoch utc time)')
	parser.add_argument('-et','--end-time', type=int, help='End time for analysis (use epoch utc time)')
	parser.add_argument('-ft','--fix-time', required=False, type=int, help='Correct the time by X hours')
	# parser.add_argument('-cp','--combined-plots', required=False, action='store_true', help='Make a combined plot for the servers in time period')
	# parser.add_argument('-pc','--player-count-plots', required=False, action='store_true', help='Make a plot for the player count in time period')
	# parser.add_argument('-cppc','--combined-plots-player-count', required=False, action='store_true', help='Make a combined plot for the servers in time period with player count')
	# parser.add_argument('-a','--opt-a', type=int, help='Help_Text')
	# parser.add_argument('-c','--opt-c', required=False, action='store_true', help='Help_Text')

	# parse the resulting commandline args
	args = parser.parse_args()
	arg_opts = vars(args)
	# print(arg_opts)

	# do option validation
	single_plots = True if 'single_plots' in arg_opts else False
	combined_plots = True if 'combined_plots' in arg_opts else False
	player_count_plots = True if 'player_count_plots' in arg_opts else False
	combined_player_count_plots = True if 'combined_plots_player_count' in arg_opts else False
	start_time = int(arg_opts['start_time'])
	end_time = int(arg_opts['end_time']) if int(arg_opts['end_time']) != -1 else game_list_database_handler.epoch_timestamp()



	# log file vars
	path_to_log_dir = f"log_outputs"
	log_file_prefix = f"gamelist_pushdisc_analysis"
	# log timestamp goes between prefix and suffix
	log_file_suffix = f".txt"


	# if not os.path.exists(arg_opts['file_path']):
	# 	print(f"File does not exist, stopping program")
	# 	os._exit(0)

	# gamelist_head, gamelist_filename = os.path.split(arg_opts['file_path'])

	# generate new logfile with current timestamp
	# timestamp_str = generate_log_timestamp_str()
	# logger = generate_new_log(f"{log_outputs_dir}\\{log_file_prefix}{timestamp_str}{log_file_suffix}")
	log_outputs_dir = f"{path_to_log_dir}\\pav_server_charts_{start_time}_to_{end_time}"
			# print(log_outputs_dir)

	if not os.path.isdir(log_outputs_dir):
		os.mkdir(log_outputs_dir)
	logger = generate_new_log(f"{log_outputs_dir}\\{log_file_prefix}{log_file_suffix}")
			

################################################################################################


	def get_playercount_for_week(server_df):
		start_time_datetime = datetime.datetime.fromtimestamp(int(start_time))

		end_time_datetime = datetime.datetime.fromtimestamp(int(end_time))
		end_time_DoW = datetime.datetime.fromtimestamp(int(end_time)).weekday()

		adjusted_datetime = end_time_datetime - datetime.timedelta(days=end_time_DoW)
		adjusted_datetime = adjusted_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
		
		week_num = 0
		probe_end_time = end_time_datetime.timestamp()

		split_dataframes = []

		while start_time_datetime < (adjusted_datetime-datetime.timedelta(week_num*7)):
			probe_start_time = (adjusted_datetime-datetime.timedelta(week_num*7)).timestamp()
			# print(probe_start_time)

			split_frame = server_df[(server_df['entry_timecode'] >= probe_start_time) & (server_df['entry_timecode'] < probe_end_time)].reset_index()
			split_dataframes.append(split_frame)

			week_num += 1
			probe_end_time = probe_start_time

		return split_dataframes
	

	def get_average_playercount_for_week(server_df):
		start_time_datetime = datetime.datetime.fromtimestamp(int(start_time))

		end_time_datetime = datetime.datetime.fromtimestamp(int(end_time))
		end_time_DoW = datetime.datetime.fromtimestamp(int(end_time)).weekday()

		adjusted_datetime = end_time_datetime - datetime.timedelta(days=end_time_DoW)
		adjusted_datetime = adjusted_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
		
		week_num = 0
		probe_end_time = end_time_datetime.timestamp()

		split_dataframes = []


		timeseries = [i/(24*60) for i in range(1,(7*24*60))]
		vals = [[] for i in timeseries]

		while start_time_datetime < (adjusted_datetime-datetime.timedelta(week_num*7)):
			probe_start_time = (adjusted_datetime-datetime.timedelta(week_num*7)).timestamp()
			# print(probe_start_time)

			split_frame = server_df[(server_df['entry_timecode'] >= probe_start_time) & (server_df['entry_timecode'] < probe_end_time)].reset_index()
			split_frame['entry_timecode'] = (split_frame['entry_timecode']-split_frame['entry_timecode'].iloc[0])
			# print(split_frame)
			week_num += 1
			probe_end_time = probe_start_time

			# print(split_frame)

			for i in range(0,(7*24*60)-1):
				# print((i*60),((i+1)*60))
				subset = split_frame[(split_frame['entry_timecode'] >= (i*60)) & (split_frame['entry_timecode'] < ((i+1)*60))]
				# print(subset)

				if len(subset) > 0:
					avg = subset['slots'].mean()
					vals[i].append(avg)
				else:
					vals[i].append(0)

		# print(vals)

		vals_normalized = []
		for i in range(0,len(timeseries)):
			summed_vals = 0
			for val in vals[i]:
				summed_vals += val
			vals_normalized.append(summed_vals/week_num)

		return pd.DataFrame({'entry_timecode':timeseries, 'slots':vals_normalized})
	
		# 	num_weeks_in_set = len(corrected_dfs)
		# avg_timestamps = []
		# avg_values = []
		# resolution = 4
		# for i in range(0,24*7*resolution):
		# 	subframe = corrected_dfs[(corrected_dfs['entry_timecode'] >= (i/(24*resolution))) & (corrected_dfs['entry_timecode'] < ((i+1)/(24*resolution)))].reset_index()
		# 	avg_timestamps.append(i/(24*resolution))
		# 	avg_values.append(subframe['slots'].mean())


	def plot_average_playercount_over_week(server_dataframe_week_arr, avg_df):
		fig, ax = plt.subplots(figsize=(36,12))
		plt.subplots_adjust(left=0.03, right=0.97, top=0.93, bottom=0.07)
		# plt.style.use('_mpl-gallery')
		# make data

		# corrected_dfs = []

		for dataframe in server_dataframe_week_arr:
			copy_df = dataframe.copy()
			copy_df['entry_timecode'] = (copy_df['entry_timecode']-copy_df['entry_timecode'].iloc[0])/60/60/24
			plt.plot(copy_df['entry_timecode'], copy_df['slots'], alpha=0.2, marker='.', linestyle='', color='r')
			# corrected_dfs.append(copy_df)
	
		# print(corrected_dfs)

		# concat_df = pd.concat(corrected_dfs)
		# concat_df = concat_df.sort_values('entry_timecode', ascending=True)
		# print(concat_df)

		# time_freq = pd.crosstab(index=concat_df['entry_timecode'], columns=['slots'], normalize=False)
		# print(time_freq.columns)
		# time_freq.plot(kind='line')
		# plt.plot(concat_df['entry_timecode'], concat_df['slots'].ewm(span=20).mean(), alpha=1, marker='', linestyle='-', color='r')
		# plt.plot(time_freq, time_freq['slots'].ewm(span=20).mean(), alpha=1, marker='', linestyle='-', color='r')
		plt.plot(avg_df['entry_timecode'], avg_df['slots'], alpha=1, marker='', linestyle='-', color='r')

		plt.axvline(0, 0, 40, **{'linestyle':'-', 'color':'black'})

		intervals = 8
		for i in range(0,7*intervals):
			if i%intervals == 0:
				plt.axvline(i/intervals, 0, 40, **{'linestyle':'-', 'color':'black'})
			else:
				plt.axvline(i/intervals, 0, 40, **{'linestyle':':', 'color':'black', 'alpha':0.3})
		plt.axvline(7, 0, 40, **{'linestyle':'-', 'color':'black'})

		plt.text(0.02, 40, f"Monday", rotation=90, verticalalignment='top')
		plt.text(1.02, 40, f"Tuesday", rotation=90, verticalalignment='top')
		plt.text(2.02, 40, f"Wednesday", rotation=90, verticalalignment='top')
		plt.text(3.02, 40, f"Thurday", rotation=90, verticalalignment='top')
		plt.text(4.02, 40, f"Friday", rotation=90, verticalalignment='top')
		plt.text(5.02, 40, f"Saturday", rotation=90, verticalalignment='top')
		plt.text(6.02, 40, f"Sunday", rotation=90, verticalalignment='top')




		# locator = mdates.AutoDateLocator()
		# formatter = mdates.ConciseDateFormatter(locator)
		# ax.xaxis.set_major_formatter(formatter)


		plt.xlabel(f'Days (UTC Timezone)') 
		plt.ylabel('Players') 
		plt.title('')

		# plt.show()
		fig.savefig(f'{log_outputs_dir}\highpush_playercount.png')
		plt.clf()
		# plt.cla()
		plt.close()


	# Rank maps by the delta in ave/min/max playercount vs same for map that came before
		

	try:
		# start sqlite connection to store/get data
		# zero timestamp servers before 
		db_handler = game_list_database_handler.Pavlov_Gamelist_Database(gamelist_database_path)
		server_table_data_raw = db_handler.get_all_table_data_within_range('servers', 'entry_timecode', start_time, end_time).fetchall()
		count_table_data_raw = db_handler.get_all_table_data_within_range('playercount', 'entry_timecode', start_time, end_time).fetchall()
		db_handler.close()

		# print(stats_table_data_raw)

		server_table_data = pd.DataFrame(data=server_table_data_raw, columns=['name', 'ip', 'slots', 'max_slots', 'map_id', 'map_label', 'gamemode', 'pass_en', 'entry_timecode'])
		# print(server_table_data)
		server_table_data = server_table_data[server_table_data['name'] == pushserver_name].reset_index()
		count_table_data = pd.DataFrame(data=count_table_data_raw, columns=['count', 'source', 'entry_timecode'])

		# print(server_table_data)

		mapdict = load_json_file('modio_mapdict.json')
		

		# if single_plots or combined_plots or player_count_plots or combined_player_count_plots:
			
		# def make_map_plots(map_summary_arr, maplist_dict):
		# 	sorted_dict = {}
		# 	for map in map_summary_arr:
		# 		n = map['name']
		# 		if n not in sorted_dict:
		# 			sorted_dict[n] = []
		# 			sorted_dict[n].append(map)
		# 		else:
		# 			sorted_dict[n].append(map)



			
			
		logger.info(f"================================================================================================================")
		
		
		logger.info(f"================================================================================================================")
		# server_dataframe_week_arr = get_playercount_for_week(server_table_data)
		# avg_server_dataframe_week_arr = get_average_playercount_for_week(server_table_data)
		# print(avg_server_dataframe_week_arr)
		# plot_average_playercount_over_week(server_dataframe_week_arr, avg_server_dataframe_week_arr)
		logger.info(f"================================================================================================================")

		
	except:
		logger.exception('exception')

