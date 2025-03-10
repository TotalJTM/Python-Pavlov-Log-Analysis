# !/usr/bin/python3
# Script written by TotalJTM 2024
# Version 2, 3/7/25

from pav_la import pavlov_server_db_manager
from pav_la import game_list_database_handler
import pandas as pd
from elommr import EloMMR, Player
import math
import matplotlib.pyplot as plt


pd.set_option('display.precision', 2)  # Set decimal precision to 2 decimals

version = 2

# path to database files
gamelist_database_path = r'Y:/database_data/gamelist_records.db'
pushserver_name = '[PUSH Discord] High player PUSH maps'
# server_database_path = r'gamelist_test.db'
server_database_path = r'pushdiscord_servstats.db'
# server_database_path = r'midpushdiscord_servstats.db'

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
	parser.add_argument('-cp','--combined-plots', required=False, action='store_true', help='Make a combined plot for the servers in time period')

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
	end_time = int(arg_opts['end_time']) if int(arg_opts['end_time']) != -1 else pavlov_server_db_manager.epoch_timestamp()



	# log file vars
	path_to_log_dir = f"log_outputs"
	log_file_prefix = f"game_db_analysis"
	# log timestamp goes between prefix and suffix
	log_file_suffix = f".txt"

	log_outputs_dir = f"{path_to_log_dir}\\pav_server_charts_{start_time}_to_{end_time}"

	if not os.path.isdir(log_outputs_dir):
		os.mkdir(log_outputs_dir)
	logger = generate_new_log(f"{log_outputs_dir}\\{log_file_prefix}{log_file_suffix}")
			

	######################################################################################################
	##################                          Data Functions                          ##################
	######################################################################################################

	# summarize all games in the given time period, return a list of dict entries that represent the match
	def make_server_map_summary(stats_df):
		server_maps_played = []
		unique_entry_timecodes = stats_df['entry_timecode'].unique()

		for i, unique_entry_timecode_data in enumerate(unique_entry_timecodes):
			# print(unique_entry_timecodes[i+1])
			entry_timecode_df = stats_df[stats_df['entry_timecode'] == unique_entry_timecode_data]
			sorted_df = entry_timecode_df.sort_values('experience_count', ascending=False)

			d = {}
			d['map'] = entry_timecode_df['map_label'].unique()[0]
			d['entry_timecode'] = unique_entry_timecode_data
			print(unique_entry_timecodes[i], unique_entry_timecodes[i-1], unique_entry_timecodes[i]-unique_entry_timecodes[i-1])
			d['duration'] = (unique_entry_timecodes[i]-unique_entry_timecodes[i-1]) if i-1 != -1 else -1
			d['player_list'] = []
			total_exp = 0


			# for i in range(0,len(sorted_df)):
			for index, row in sorted_df.iterrows():
				total_exp += row['experience_count']
				d['player_list'].append({
					'steam_id':row['steam_id'],
					'steam_name':row['steam_name'],
					'kill_count':row['kill_count'],
					'death_count':row['death_count'],
					'assist_count':row['assist_count'],
					'headshot_count':row['headshot_count'],
					'experience_count':row['experience_count'],
					'teamkill_count':row['teamkill_count'],
					'team_id':row['team_id'],
				})
			
			round_avg_experience = total_exp/len(sorted_df)
			for i in range(0, len(d['player_list'])):
				d['player_list'][i]['round_avg_exp'] = round_avg_experience

			server_maps_played.append(d)
			# print(d)

		return server_maps_played
		
	# record the top num_to_show entries per match, uses the output of make_server_map_summary() and imported mapdict json data
	def record_top_players_per_match(server_maps_played, mapdict, num_to_show):

		for match_num, server_map in enumerate(server_maps_played):
			team0_exp = 0
			team1_exp = 0
			team0_players = 0
			team1_players = 0

			for player in server_map['player_list']:
				if player['team_id'] == 1:
					team1_exp += player['experience_count']
					team1_players += 1
				else:
					team0_exp += player['experience_count']
					team0_players += 1


			mapname = mapdict.get(str(server_map['map']), None)
			if mapname == None:
				mapname = f"UGC{server_map['map']}"
			else:
				mapname = mapname['name']

			str_accum = "\n"
			str_accum += f"Match {match_num+1} : Played at {decode_epoch_to_UTC(server_map['entry_timecode'])} : Map {mapname} : {int(server_map['duration']/60.0)} minutes\n"
			str_accum += f"Blue Team (0): {team0_exp} exp with {team0_players} players | Red Team (1): {team1_exp} exp with {team1_players} players\n"
			str_accum += f"{'Player Name':45} | {'Exp':5} | {'K/D/A':13} | Team\n"
			# print(server_map['player_list'])
			for i in range(0,min(num_to_show, len(server_map['player_list']))):
				player_data = server_map['player_list'][i]
				player_name = f"{i+1}. {player_data['steam_name']}"
				# attack first is team_id = 0
				str_accum += f"{player_name:45} | {int(player_data['experience_count']):5} | ({int(player_data['kill_count']):3}/{int(player_data['death_count']):3}/{int(player_data['assist_count']):3}) | {'Red' if player_data['team_id'] == 1 else 'Blue'}\n"

			logger.info(str_accum)

	# summarize player stats across the given time period, returns a dataframe of player data 
	def make_player_stats_summary(stats_df):
		unique_steam_ids = stats_df['steam_id'].unique()
		player_totals_df = pd.DataFrame({
			'steam_id':pd.Series(dtype='str'), 
			'steam_name':pd.Series(dtype='str'), 
			'kill_count':pd.Series(dtype='int'), 
			'death_count':pd.Series(dtype='int'), 
			'assist_count':pd.Series(dtype='int'), 
			'headshot_count':pd.Series(dtype='int'), 
			'experience_count':pd.Series(dtype='int'), 
			'teamkill_count':pd.Series(dtype='int'), 
			'num_games':pd.Series(dtype='int')
		})

		for steam_id in unique_steam_ids:
			steam_id_df = stats_df[stats_df['steam_id'] == steam_id].reset_index()
			# print(steam_id_df)

			new_entry = {
				'steam_id':steam_id_df['steam_id'].values[0],
				'steam_name':steam_id_df['steam_name'].values[0],
				'kill_count':steam_id_df['kill_count'].sum(),
				'death_count':steam_id_df['death_count'].sum(),
				'assist_count':steam_id_df['assist_count'].sum(),
				'headshot_count':steam_id_df['headshot_count'].sum(),
				'experience_count':steam_id_df['experience_count'].sum(),
				'teamkill_count':steam_id_df['teamkill_count'].sum(),
				'num_games':len(steam_id_df)
			}
			# print(new_entry)
			player_totals_df = player_totals_df._append(new_entry, ignore_index=True)

		return player_totals_df

	# record the output of make_player_stats_summary() with raw totals
	def record_player_totals_unweighted(player_totals_df, max_num=None, min_games=5):
		str_accum = "\n"
		str_accum += f"Player Totals for Period\n"
		player_totals_df_s = player_totals_df[player_totals_df['num_games'] >= min_games].reset_index()
		sorted_df = player_totals_df_s.sort_values('experience_count', ascending=False)
		
		if max_num:
			max_num = len(sorted_df) if len(sorted_df) < max_num else max_num

		str_accum += f"{'Player Name':45} | {'Exp':6} | {'K/D/A':16}\n"
		for i in range(0, max_num if max_num else len(sorted_df)):
			player_data = sorted_df.iloc[i]
			player_name = f"{i+1}. {player_data['steam_name']}"
			str_accum += f"{player_name:45} | {int(player_data['experience_count']):6} | ({int(player_data['kill_count']):4}/{int(player_data['death_count']):4}/{int(player_data['assist_count']):4}) in {int(player_data['num_games'])} games\n"

		logger.info(str_accum)

	# record the output of make_player_stats_summary() with averaged totals (divides all values by number of games played)
	def record_player_totals_weighted(player_totals_df, max_num=None, min_games=5):
		str_accum = "\n"
		str_accum += f"Player Total Averages for Period\n"

		player_totals_df_s = player_totals_df[player_totals_df['num_games'] >= min_games].reset_index()
		player_totals_df_s['experience_count'] = player_totals_df_s['experience_count'] / player_totals_df_s['num_games']
		player_totals_df_s['kill_count'] = player_totals_df_s['kill_count'] / player_totals_df_s['num_games']
		player_totals_df_s['death_count'] = player_totals_df_s['death_count'] / player_totals_df_s['num_games']
		player_totals_df_s['assist_count'] = player_totals_df_s['assist_count'] / player_totals_df_s['num_games']
		sorted_df = player_totals_df_s.sort_values('experience_count', ascending=False)
		
		if max_num:
			max_num = len(sorted_df) if len(sorted_df) < max_num else max_num

		str_accum += f"{'Player Name':45} | {'AvgExp':6} | {'Avg K/D/A':16}\n"
		for i in range(0, max_num if max_num else len(sorted_df)):
			player_data = sorted_df.iloc[i]
			player_name = f"{i+1}. {player_data['steam_name']}"
			str_accum += f"{player_name:45} | {int(player_data['experience_count']):6} | ({int(player_data['kill_count']):4}/{int(player_data['death_count']):4}/{int(player_data['assist_count']):4}) average across {int(player_data['num_games'])} games\n"

		logger.info(str_accum)

	# record the output of make_player_stats_summary() with raw totals, dont list player KDA
	def record_player_totals_unweighted_no_KDA(player_totals_df, max_num=None, min_games=5):
		str_accum = "\n"
		str_accum += f"Player Totals for Period\n"
		player_totals_df_s = player_totals_df[player_totals_df['num_games'] >= min_games].reset_index()
		sorted_df = player_totals_df_s.sort_values('experience_count', ascending=False)
		
		if max_num:
			max_num = len(sorted_df) if len(sorted_df) < max_num else max_num

		str_accum += f"{'Player Name':45}\n"
		for i in range(0, max_num if max_num else len(sorted_df)):
			player_data = sorted_df.iloc[i]
			player_name = f"{i+1}. {player_data['steam_name']}"
			str_accum += f"{player_name:45} with {int(player_data['num_games'])} games played (steamid: {player_data['steam_id']})\n"

		logger.info(str_accum)

	# record the output of make_player_stats_summary() with averaged totals, dont list player KDA
	def record_player_totals_weighted_no_KDA(player_totals_df, max_num=None, min_games=5):
		str_accum = "\n"
		str_accum += f"Player Total Averages for Period\n"

		player_totals_df_s = player_totals_df[player_totals_df['num_games'] >= min_games].reset_index()
		player_totals_df_s['experience_count'] = player_totals_df_s['experience_count'] / player_totals_df_s['num_games']
		player_totals_df_s['kill_count'] = player_totals_df_s['kill_count'] / player_totals_df_s['num_games']
		player_totals_df_s['death_count'] = player_totals_df_s['death_count'] / player_totals_df_s['num_games']
		player_totals_df_s['assist_count'] = player_totals_df_s['assist_count'] / player_totals_df_s['num_games']
		sorted_df = player_totals_df_s.sort_values('experience_count', ascending=False)
		
		if max_num:
			max_num = len(sorted_df) if len(sorted_df) < max_num else max_num

		str_accum += f"{'Player Name':45}\n"
		for i in range(0, max_num if max_num else len(sorted_df)):
			player_data = sorted_df.iloc[i]
			player_name = f"{i+1}. {player_data['steam_name']}"
			str_accum += f"{player_name:45} with {int(player_data['num_games'])} games played (steamid: {player_data['steam_id']})\n"

		logger.info(str_accum)

	# record the number of player teamkills using the output of make_player_stats_summary()
	def record_player_tks(player_totals_df, max_num=None):
		str_accum = "\n"
		str_accum += f"Teamkiller Totals for Period\n"
		sorted_df = player_totals_df.sort_values('teamkill_count', ascending=False)

		if max_num:
			max_num = len(sorted_df) if len(sorted_df) < max_num else max_num

		str_accum += f"{'Player Name':45} | {'TKs':6} | {'Exp':6} | {'K/D/A':16}\n"
		for i in range(0, max_num if max_num else len(sorted_df)):
			print(i)
			player_data = sorted_df.iloc[i]
			player_name = f"{i+1}. {player_data['steam_name']}"
			str_accum += f"{player_name:45} | {int(player_data['teamkill_count']):6} | {int(player_data['experience_count']):6} | ({int(player_data['kill_count']):4}/{int(player_data['death_count']):4}/{int(player_data['assist_count']):4}) in {int(player_data['num_games'])} games\n"

		logger.info(str_accum)

	# calculate player ELO using ELOMMR, use the output of make_server_map_summary() and stats table data to generate an ELO score after every match
	# returns player ELO after the last played map
	def calculate_player_elos(server_maps_played, stats_df):
		elo_mmr = EloMMR()

		unique_steam_ids = stats_df['steam_id'].unique()
		elos_df = pd.DataFrame({
			'steam_id':pd.Series(dtype='int'), 
			'steam_name':pd.Series(dtype='str'), 
			'kd':pd.Series(dtype='float'), 
			'experience_count':pd.Series(dtype='float'), 
			'elo':pd.Series(dtype='float'), 
			'num_games':pd.Series(dtype='int')
		})
		player_dict = {}

			
		for match in server_maps_played:
			round_standing = []
			top_exp = 0
			top_ka_score = 0
			top_death = 0
			average_death = 0
			team0_exp = 0
			team1_exp = 0

			for player_entry in match['player_list']:
				if top_exp < player_entry['experience_count']:
					top_exp = player_entry['experience_count']
				if top_death < player_entry['death_count']:
					top_death = player_entry['death_count']
				if top_ka_score < ((player_entry['kill_count']*2)+player_entry['kill_count']):
					top_ka_score = ((player_entry['kill_count']*2)+player_entry['kill_count'])
				average_death += player_entry['death_count']

				if player_entry['team_id'] == 0:
					team0_exp += player_entry['experience_count']
				else:
					team1_exp += player_entry['experience_count']
			
			average_death = average_death/len(match['player_list'])


			for i, player_entry in enumerate(match['player_list']):
				if player_entry['steam_id'] not in player_dict:
					player_dict[player_entry['steam_id']] = Player()
				# exp_rank = int(top_exp/player_entry['experience_count']) if player_entry['experience_count'] > 0 else (10+(5*player_entry['death_count']))
				# exp_rank = int(math.log10(top_exp/player_entry['experience_count'])*25) if player_entry['experience_count'] > 0 else (len(match['player_list'])+(5*player_entry['death_count']))
				
				# death_weight = (top_death/player_entry['death_count']) if player_entry['death_count'] != 0 else 0
				# exp_weight = (top_exp/player_entry['experience_count']) if player_entry['experience_count'] != 0 else 0
				# kill_weight = (top_ka_score/((player_entry['kill_count']*2)+player_entry['kill_count'])) if player_entry['kill_count'] != 0 else 0

				death_weight = math.sqrt(player_entry['death_count']/top_death) if player_entry['death_count'] != 0 else 0
				ka_weight = (top_ka_score/((player_entry['kill_count']*2)+player_entry['assist_count'])) if (player_entry['kill_count'] != 0 or player_entry['assist_count'] != 0) else 0


				exp_rank = int(death_weight+ka_weight)
				# exp_rank = int((death_weight+ka_weight)) if player_entry['experience_count'] > 0 else (len(match['player_list'])+(5*player_entry['death_count']))
				if exp_rank > 50:
					exp_rank = 50
				else:
					exp_rank = exp_rank-1
				# print(f"{i:3}, {exp_rank:4}, {player_entry['kill_count']:3}, {player_entry['death_count']:3}, {player_entry['assist_count']:3}, {player_entry['experience_count']:4}")
				round_standing.append((player_dict[player_entry['steam_id']], i, exp_rank))

			max_weight = 1.1
			min_weight = 0.5
			# max_weight - abs((t0 experience / (experience total)) - normalize to 0 if equal match) = weight_calc
			# 10 10 0.5		1.1 - abs(0.5 - 0.5) = 1.1
			#2  18 0.1		1.1 - abs(0.1 - 0.5) = 0.7
			#18 2  0.9		1.1 - abs(0.9 - 0.5) = 0.7
			if team0_exp == 0 or team1_exp == 0:
				weight_out = min_weight
			else:
				weight_calc = max_weight - abs((team0_exp/(team0_exp+team1_exp)) - 0.5)
				weight_out = min_weight if weight_calc < min_weight else weight_calc
			
			elo_mmr.round_update(round_standing, match['entry_timecode'], weight=weight_out)

		for player_id in player_dict:
			# player_elo = player_dict[player_id][-1].display_rating()
			player_elo = player_dict[player_id].event_history[-1].rating_mu
			steam_id_df = stats_df[stats_df['steam_id'] == player_id].reset_index()
			# 
				
			# print(steam_id_df)

			new_entry = {
				'steam_id':player_id,
				'steam_name':steam_id_df['steam_name'].values[0],
				'kd':steam_id_df['kill_count'].sum()/steam_id_df['death_count'].sum(),
				'avg_experience_count':steam_id_df['experience_count'].sum()/len(steam_id_df),
				'elo':player_elo,
				'num_games':len(steam_id_df)
			}
			# print(new_entry)
			elos_df = elos_df._append(new_entry, ignore_index=True)

		return elos_df

	# record the player ELO output from calculate_player_elos()
	def record_player_elo(elo_df, max_num=None):
		str_accum = "\n"
		str_accum += f"Player ELO for Period\n"
		sorted_df = elo_df.sort_values('elo', ascending=False)

		str_accum += f"{'Player Name':45} | {'Elo':6} | {'AvgExp':6} | {'AvgKD':5}\n"
		for i in range(0, max_num if max_num else len(sorted_df)):
			player_data = sorted_df.iloc[i]
			player_name = f"{i+1}. {player_data['steam_name']}"
			str_accum += f"{player_name:45} | {player_data['elo']:6} | {int(player_data['avg_experience_count']):6} | ({round(player_data['kd'],2):4}) average across {int(player_data['num_games'])} games\n"

		logger.info(str_accum)

	# generate a summary of teamswitchers (RCON and manual switches) and the number of times the player logged into the server
	def analyze_teamswitchers(login_df, teamswitch_df):
		teamswitch_summary_df = pd.DataFrame({
			'steam_id':pd.Series(dtype='str'), 
			'steam_name':pd.Series(dtype='str'), 
			'rcon':pd.Series(dtype='int'), 
			'manual':pd.Series(dtype='int'), 
			'logins':pd.Series(dtype='int')
		})
		unique_steam_ids = teamswitch_df['steam_id'].unique()
		for unique_id in unique_steam_ids:
			steam_name_df = login_df[login_df['steam_id'] == unique_id].reset_index()['steam_name']
			# print(steam_name_df)
			steam_name = 'unknown'
			if len(steam_name_df)>0:
				steam_name = steam_name_df.values[-1]
				
			steam_id_df = teamswitch_df[teamswitch_df['steam_id'] == unique_id].reset_index()

			new_entry = {
				'steam_id':unique_id,
				'steam_name':steam_name,
				'rcon':len(steam_id_df[steam_id_df['reason_int'] == 0]),
				'manual':len(steam_id_df[steam_id_df['reason_int'] == 1]),
				'logins':len(steam_name_df),
			}
			teamswitch_summary_df = teamswitch_summary_df._append(new_entry, ignore_index=True)

		return teamswitch_summary_df

	# record the output of analyze_teamswitchers()
	def record_teamswitcher_summary(teamswitch_summary_df, max_num=None):
		str_accum = "\n"
		str_accum += f"Teamswitchers for Period\n"
		sorted_df = teamswitch_summary_df.sort_values('manual', ascending=False)

		str_accum += f"\n{'Steam Name':40} ({'Steam Id':17}): Logins,  RCON, Manual\n"
		for i in range(0, max_num if max_num else len(sorted_df)):
			player_data = sorted_df.iloc[i]
			str_accum += f"{player_data['steam_name']:40} ({player_data['steam_id']:17}): {player_data['logins']:5}, {player_data['rcon']:5}, {player_data['manual']:5}\n"

		logger.info(str_accum)

	# reduce the mapdict down to just the maps played in the mapid_list (array of map ids generated from unique() dataframe func on stats table 'map_label' key)
	def filter_maps_from_mapdict(mapid_list, mapdict):
		mapid_strings = []
		for map in mapid_list:
			mapid_strings.append(str(map))

		new_dict = {}
		for entry in mapdict:
			if entry in mapid_strings:
				new_dict[entry] = mapdict[entry]

		return new_dict

	# make a number of scatter plots from passed in arrays for x/y axis, strength and color
	max_subplot_x = 4
	max_subplot_y = 3
	max_per_subplot = 12
	def make_scatter_plot(x_arr, y_arr, s_arr, c_arr, x_label, y_label, titles_arr, dur_arr, fig_title, filepath):
		num_plots = len(x_arr)
		graph_max_y = 0
		for i in range(0,num_plots):
			if max(y_arr[i]) > graph_max_y:
				graph_max_y = max(y_arr[i])

		if num_plots == 1:
			fig, ax = plt.subplots(1, sharex=True, sharey=True, layout="constrained", figsize=(18,12))
			ax.scatter(x=x_arr[0], y=y_arr[0], s=s_arr[0], c=c_arr[0], alpha=0.5)
			ax.set_title(titles_arr[0])
			ax.text(0, graph_max_y-(0.1*graph_max_y), f'Players: {len(x_arr[0])}\nDuration: {round(dur_arr[0]/60,1)} mins')

		elif num_plots <= max_subplot_x:
			fig, axs = plt.subplots(num_plots, sharex=True, sharey=True, layout="constrained", figsize=(18,12))
			for i,ax in enumerate(axs):
				ax.scatter(x=x_arr[i], y=y_arr[i], s=s_arr[i], c=c_arr[i], alpha=0.5)
				ax.set_title(titles_arr[i])
				ax.text(0, graph_max_y-(0.1*graph_max_y), f'Players: {len(x_arr[i])}\nDuration: {round(dur_arr[i]/60,1)} mins')

		else:
			sub_x_num = math.ceil(num_plots/max_subplot_y)
			sub_y_num = min(num_plots,max_subplot_y)
			# print(sub_x_num, sub_y_num)

			fig, axs = plt.subplots(sub_x_num, sub_y_num, sharex=True, sharey=True, layout="constrained", figsize=(18,12))
			# plt.subplots_adjust(left=0.03, right=0.97, top=0.93, bottom=0.07)

			count = 0
			# print(num_plots)
			for i in range(0,num_plots):
				# print(i)
				sub_ind_x = math.floor(i/max_subplot_y)
				sub_ind_y = i%max_subplot_y
				# print(sub_ind_x,sub_ind_y)
				
				axs[sub_ind_x, sub_ind_y].scatter(x=x_arr[i], y=y_arr[i], s=s_arr[i], c=c_arr[i], alpha=0.5)
				axs[sub_ind_x, sub_ind_y].set_title(titles_arr[i])
				axs[sub_ind_x, sub_ind_y].text(0, graph_max_y-(0.1*graph_max_y), f'Players: {len(x_arr[i])}\nDuration: {round(dur_arr[i]/60,1)} mins')
				# print('end')

		fig.suptitle(fig_title)
		fig.supxlabel(x_label)
		fig.supylabel(y_label)

		# plt.show()
		fig.savefig(filepath)
		plt.clf()
		plt.cla()
		plt.close()

	# make a line plot with overlapping datasets (x_arrs and y_arrs are arrays of dataframes or lists)
	def make_overlapping_line_plot(x_arrs, y_arrs, x_label, y_label, legend_arr, fig_title, filepath):
		fig = plt.figure(figsize=(26,12))
		plt.subplots_adjust(left=0.03, right=0.97, top=0.93, bottom=0.07)
		# plt.style.use('_mpl-gallery')

		# make data
		largest_time = 0
		count = 0

		for i in range(0,len(x_arrs)):
			plt.plot(x_arrs[i], y_arrs[i])

			count+=1
	
		plt.xlabel(x_label) 
		plt.ylabel(y_label) 
		plt.title(fig_title)
		plt.legend(legend_arr)

		plt.show()
		fig.savefig(filepath)
		plt.clf()
		# plt.cla()
		plt.close()

	# make line plots from passed in arrays for x/y axis data **not used**
	def make_grid_line_plot(x_arr, y_arr, x_label, y_label, titles_arr, dur_arr, fig_title, filepath):
		num_plots = len(x_arr)
		graph_max_y = 0
		for i in range(0,num_plots):
			if max(y_arr[i]) > graph_max_y:
				graph_max_y = max(y_arr[i])

		if num_plots == 1:
			fig, ax = plt.subplots(1, sharex=True, sharey=True, layout="constrained", figsize=(18,12))
			ax.plot(x=x_arr[0], y=y_arr[0])
			ax.set_title(titles_arr[0])
			ax.text(0, graph_max_y-(0.1*graph_max_y), f'Players: {len(x_arr[0])}\nDuration: {round(dur_arr[0]/60,1)} mins')

		elif num_plots <= max_subplot_x:
			fig, axs = plt.subplots(num_plots, sharex=True, sharey=True, layout="constrained", figsize=(18,12))
			for i,ax in enumerate(axs):
				ax.plot(x=x_arr[i], y=y_arr[i])
				ax.set_title(titles_arr[i])
				ax.text(0, graph_max_y-(0.1*graph_max_y), f'Players: {len(x_arr[i])}\nDuration: {round(dur_arr[i]/60,1)} mins')

		else:
			sub_x_num = math.ceil(num_plots/max_subplot_y)
			sub_y_num = min(num_plots,max_subplot_y)
			# print(sub_x_num, sub_y_num)

			fig, axs = plt.subplots(sub_x_num, sub_y_num, sharex=True, sharey=True, layout="constrained", figsize=(18,12))
			# plt.subplots_adjust(left=0.03, right=0.97, top=0.93, bottom=0.07)

			count = 0
			# print(num_plots)
			for i in range(0,num_plots):
				# print(i)
				sub_ind_x = math.floor(i/max_subplot_y)
				sub_ind_y = i%max_subplot_y
				# print(sub_ind_x,sub_ind_y)
				
				axs[sub_ind_x, sub_ind_y].plot(x=x_arr[i], y=y_arr[i])
				axs[sub_ind_x, sub_ind_y].set_title(titles_arr[i])
				axs[sub_ind_x, sub_ind_y].text(0, graph_max_y-(0.1*graph_max_y), f'Players: {len(x_arr[i])}\nDuration: {round(dur_arr[i]/60,1)} mins')
				# print('end')

		fig.suptitle(fig_title)
		fig.supxlabel(x_label)
		fig.supylabel(y_label)

		# plt.show()
		fig.savefig(filepath)
		plt.clf()
		plt.cla()
		plt.close()

	# make scatter plots of KD for each map (across all matches) played in the given time period
	def make_map_KD_scatter_plots(server_maps_played, mapdict):
		file_dir_path = f'{log_outputs_dir}/KDA_scatter_plots'
		if not os.path.isdir(file_dir_path):
			os.mkdir(file_dir_path)

		num_maps = len(server_maps_played)

		sorted_maps = {}

		for i in range(0, num_maps):
			ind_dict = server_maps_played[i]

			if ind_dict['map'] not in sorted_maps:
				sorted_maps[ind_dict['map']] = []

			sorted_maps[ind_dict['map']].append({'mapdata':server_maps_played[i],'ind':i})

		# print(sorted_maps.keys())
		for map in sorted_maps:
			x_vals = []
			y_vals = []
			s_vals = []
			c_vals = []
			dur_vals = []
			titles = []
			mapcount = len(sorted_maps[map])

			for i in range(0, mapcount):
				x_accum = []
				y_accum = []
				s_accum = []
				c_accum = []
				
				# print(sorted_maps[map][i])
				for player_data in sorted_maps[map][i]['mapdata']['player_list']:
					# print(player_data)
					# player_data = sorted_maps[map]['player_list'][player]
					x_accum.append(player_data['death_count'])
					y_accum.append(player_data['kill_count'])
					s_accum.append(player_data['assist_count']*2)
					c_accum.append('red' if player_data['team_id'] == 1 else 'blue')

				x_vals.append(x_accum)
				y_vals.append(y_accum)
				s_vals.append(s_accum)
				c_vals.append(c_accum)
				titles.append(f"Game {sorted_maps[map][i]['ind']+1} : {sorted_maps[map][i]['mapdata']['entry_timecode']}")
				dur_vals.append(sorted_maps[map][i]['mapdata']['duration'])


			mapname = 'Not Found'
			if str(map) in mapdict:
				mapname = mapdict[str(map)]['name']

			plots_to_generate = math.ceil(mapcount/max_per_subplot)
			print(f'{plots_to_generate} plots from {mapcount}')
			for i in range(0,plots_to_generate):
				range_min = i*max_per_subplot
				range_max = max_per_subplot*(i+1) if mapcount-((i+1)*max_per_subplot) > 0 else ((mapcount%max_per_subplot)+(i*max_per_subplot))
				make_scatter_plot(
					x_arr=x_vals[range_min:range_max], 
					y_arr=y_vals[range_min:range_max],  
					s_arr=s_vals[range_min:range_max], 
					c_arr=c_vals[range_min:range_max], 
					dur_arr=dur_vals[range_min:range_max],
					x_label='Deaths', 
					y_label='Kills', 
					titles_arr=titles[range_min:range_max], 
					fig_title=f"Map {mapname} (UGC{map}) game(s) {range_min+1}-{range_max} of {mapcount}", 
					filepath=f"{file_dir_path}/UGC{map}_games_{range_min+1}_through_{range_max}.png",
				)
			
	# get a list of dict entries that record match start/stop time 
	def get_map_start_stop_timestamps(roundstate_df, map_df):
		map_arr = []

		last_timecode = None
		print(map_df)
		# for i, map in map_df.iterrows():

		list_len = len(map_df)
		for i in range(0, list_len):
			mapid = map_df.iloc[i]['map_id']
			start_entrytime = map_df.iloc[i]['entry_timecode']

			if i == 0:
				map_arr.append({'map_id':'_IGNORED', 'start':start_time, 'end':start_entrytime})
				end_entrytime = map_df.iloc[i+1]['entry_timecode']
				map_arr.append({'map_id':mapid, 'start':start_entrytime, 'end':end_entrytime})
			
			elif i == list_len-1:
				map_arr.append({'map_id':mapid, 'start':start_entrytime, 'end':end_time})

			else:
				end_entrytime = map_df.iloc[i+1]['entry_timecode']
				map_arr.append({'map_id':mapid, 'start':start_entrytime, 'end':end_entrytime})

		return map_arr

	# make a plot of KDA for a map across all matches in a given time period **not working**
	def make_map_KDA_line_plots(kill_df, roundstate_df, event_df, mapdict):
		file_dir_path = f'{log_outputs_dir}/KDA_line_plots'
		if not os.path.isdir(file_dir_path):
			os.mkdir(file_dir_path)

		num_maps = len(server_maps_played)

		sorted_maps = {}

		for i in range(0, num_maps):
			ind_dict = server_maps_played[i]

			if ind_dict['map'] not in sorted_maps:
				sorted_maps[ind_dict['map']] = []

			sorted_maps[ind_dict['map']].append({'mapdata':server_maps_played[i],'ind':i})

		# print(sorted_maps.keys())
		for map in sorted_maps:
			x_arrs = []
			team0_y_arrs = []
			team1_y_arrs = []
			dur_vals = []
			event_val_arrs = []
			titles = []
			mapcount = len(sorted_maps[map])

			for i in range(0, mapcount):
				team0_y_arr = []
				team1_y_arr = []
				x_arr = []
				
				# print(sorted_maps[map][i])
				for player_data in sorted_maps[map][i]['mapdata']['player_list']:
					# print(player_data)
					# player_data = sorted_maps[map]['player_list'][player]
					x_accum.append(player_data['death_count'])
					y_accum.append(player_data['kill_count'])
					s_accum.append(player_data['assist_count']*2)
					c_accum.append('red' if player_data['team_id'] == 1 else 'blue')

				x_vals.append(x_accum)
				y_vals.append(y_accum)
				s_vals.append(s_accum)
				c_vals.append(c_accum)
				titles.append(f"Game {sorted_maps[map][i]['ind']+1} : {sorted_maps[map][i]['mapdata']['entry_timecode']}")
				dur_vals.append(sorted_maps[map][i]['mapdata']['duration'])


			mapname = 'Not Found'
			if str(map) in mapdict:
				mapname = mapdict[str(map)]['name']

			plots_to_generate = math.ceil(mapcount/max_per_subplot)
			print(f'{plots_to_generate} plots from {mapcount}')
			for i in range(0,plots_to_generate):
				range_min = i*max_per_subplot
				range_max = max_per_subplot*(i+1) if mapcount-((i+1)*max_per_subplot) > 0 else ((mapcount%max_per_subplot)+(i*max_per_subplot))
				make_scatter_plot(
					x_arr=x_vals[range_min:range_max], 
					y_arr=y_vals[range_min:range_max],  
					s_arr=s_vals[range_min:range_max], 
					c_arr=c_vals[range_min:range_max], 
					dur_arr=dur_vals[range_min:range_max],
					x_label='Deaths', 
					y_label='Kills', 
					titles_arr=titles[range_min:range_max], 
					fig_title=f"Map {mapname} (UGC{map}) game(s) {range_min+1}-{range_max} of {mapcount}", 
					filepath=f"{file_dir_path}/UGC{map}_games_{range_min+1}_through_{range_max}.png",
				)

	# make a dict with lists of KDA across matches in the given time period
	def get_one_min_averages_from_kill_df(kill_df, avg_period_s=60):
		team0_kills = []
		team1_kills = []
		team0_deaths = []
		team1_deaths = []
		team0_tks = []
		team1_tks = []
		team0_suicide = []
		team1_suicide = []
		time_arr = []

		for i in range(start_time,end_time,avg_period_s):
			print(i)
			# mask = (kill_df['entry_timecode'] >= i) & (kill_df['entry_timecode'] <= i+avg_period_s)
			# print(mask)
			# subset = kill_df[kill_df['entry_timecode'] >= i and kill_df['entry_timecode'] <= i+avg_period_s].reset_index()
			subset = kill_df[(kill_df['entry_timecode'] >= i) & (kill_df['entry_timecode'] <= i+avg_period_s)].reset_index()
			time_arr.append(i)

			sum_t0_kills = len(subset[(subset['killer_teamid'] == 0) & (subset['killed_teamid'] == 1)])
			sum_t1_kills = len(subset[(subset['killer_teamid'] == 1) & (subset['killed_teamid'] == 0)])
			sum_t0_deaths = len(subset[(subset['killer_teamid'] == 1) & (subset['killed_teamid'] == 0)])
			sum_t1_deaths = len(subset[(subset['killer_teamid'] == 0) & (subset['killed_teamid'] == 1)])
			sum_t0_suicide = len(subset[(subset['killer_id'] == subset['killed_id']) & (subset['killed_teamid'] == 0)])
			sum_t1_suicide = len(subset[(subset['killer_id'] == subset['killed_id']) & (subset['killed_teamid'] == 1)])
			sum_t0_tks = len(subset[(subset['killer_teamid'] == 0) & (subset['killed_teamid'] == 0)]) - sum_t0_suicide
			sum_t1_tks = len(subset[(subset['killer_teamid'] == 1) & (subset['killed_teamid'] == 1)]) - sum_t1_suicide
			
			team0_kills.append(sum_t0_kills)
			team1_kills.append(sum_t1_kills)
			team0_deaths.append(sum_t0_deaths)
			team1_deaths.append(sum_t1_deaths)
			team0_tks.append(sum_t0_tks)
			team1_tks.append(sum_t1_tks)
			team0_suicide.append(sum_t0_suicide)
			team1_suicide.append(sum_t1_suicide)

		return {'team0_kills':team0_kills,'team1_kills':team1_kills,'team0_deaths':team0_deaths,'team1_deaths':team1_deaths,'team0_tks':team0_tks,'team1_tks':team1_tks,'team0_suicide':team0_suicide,'team1_suicide':team1_suicide,'times':time_arr}

	# make plots of KDA averages across the given time period, also plot the server population numbers
	def make_period_KDA_line_plots_with_serverpop(kill_df, roundstate_df, event_df, maps_df, server_table_data, mapdict):
		file_dir_path = f'{log_outputs_dir}/period_KDA_line_plots'
		if not os.path.isdir(file_dir_path):
			os.mkdir(file_dir_path)

		num_maps = len(maps_df)

		split_after_x_mins = 180

		avgs = get_one_min_averages_from_kill_df(kill_df)
		print(avgs)

		import math
		for i in range(0,math.ceil((len(avgs['times'])/split_after_x_mins))):
			legend = [
				'T0:Deaths','T1:Deaths',
				'T0:Kills','T1:Kills',
				'T0:Teamkills','T1:Teamkills',
				'T0:Suicides','T1:Suicides',
				'PlayersInLobby'
				]


			fig = plt.figure(figsize=(26,12))
			plt.subplots_adjust(left=0.03, right=0.97, top=0.93, bottom=0.07)
			# plt.style.use('_mpl-gallery')

			ind_0 = i*split_after_x_mins
			ind_1 = (i+1)*split_after_x_mins
			if ind_1 > len(avgs['times']):
				ind_1 = len(avgs['times'])-1

			plt.plot(avgs['times'][ind_0:ind_1], avgs['team0_deaths'][ind_0:ind_1], color='darkred', linestyle=':')
			plt.plot(avgs['times'][ind_0:ind_1], avgs['team1_deaths'][ind_0:ind_1], color='darkblue', linestyle=':')
			plt.plot(avgs['times'][ind_0:ind_1], avgs['team0_kills'][ind_0:ind_1], color='red', linestyle='-')
			plt.plot(avgs['times'][ind_0:ind_1], avgs['team1_kills'][ind_0:ind_1], color='blue', linestyle='-')
			plt.plot(avgs['times'][ind_0:ind_1], avgs['team0_tks'][ind_0:ind_1], color='firebrick', linestyle='-.')
			plt.plot(avgs['times'][ind_0:ind_1], avgs['team1_tks'][ind_0:ind_1], color='mediumblue', linestyle='-.')
			plt.plot(avgs['times'][ind_0:ind_1], avgs['team0_suicide'][ind_0:ind_1], color='lightcoral', linestyle='--')
			plt.plot(avgs['times'][ind_0:ind_1], avgs['team1_suicide'][ind_0:ind_1], color='slateblue', linestyle='--')
			plt.plot(server_table_data['entry_timecode'],server_table_data['slots'], color='grey', linestyle='-')

			for map in maps_df.iterrows():
				conts = map[1]
				# mapname = mapdict[conts['map_id']] if str(conts['map_id']) in mapdict else f"UGC{conts['map_id']}"
				mapname = mapdict.get(str(conts['map_id']), f"UGC{conts['map_id']}")
				if type(mapname) == dict:
					mapname = mapname['name']
				plt.axvline(conts['entry_timecode'], 0, 40, **{'linestyle':'-', 'color':'black'})
				plt.text(conts['entry_timecode'], 39, f"{mapname}", rotation=90, verticalalignment='top')

			# state : 0=starting, 1=standby, 2=started, 3=end, -1=unknown
			# reason_int : 0=no reason (including time expiration), 1=SitesDestroyed, 2=OutOfTicketsAttackers, 3=OutOfTicketsDefenders
			for roundstate in roundstate_df.iterrows():
				conts = roundstate[1]
				round_state = conts['state']
				if round_state == 0 or round_state == 1:
					pass
				state_trans = 'UK'
				if round_state == 2:
					# state_trans = 'Start'
					plt.axvline(conts['entry_timecode'], 0, 10, **{'linestyle':'--', 'color':'green'})
				if round_state == 3:
					# state_trans = 'End'
					plt.axvline(conts['entry_timecode'], 0, 10, **{'linestyle':'--', 'color':'orange'})

					reason = int(conts['reason_int'])
					print(reason)

					reason_text = 'TimeExpired'
					if reason == 1:
						reason_text = 'SitesDestroyed'
					if reason == 2:
						reason_text = 'OutofTicketsAttack'
					if reason == 3:
						reason_text = 'OutofTicketsDefend'

					plt.text(conts['entry_timecode'], 20, f"{reason_text}", rotation=90, verticalalignment='top', horizontalalignment='right')
				# plt.axvline(map['entry_timecode'], 0, 40, **{'linestyle':':', 'color':'red'})
				# plt.text(map['entry_timecode'], 40, f"{state_trans}", rotation=90, verticalalignment='top')

			# 0: bomb armed, 1: bomb defused, 2: capture point attack, 3: capture point defend
			for event in event_df.iterrows():
				conts = event[1]
				if conts['event_int'] == 0:
					plt.axvline(conts['entry_timecode'], 0, 40, **{'linestyle':':', 'color':'gold'})

			st = start_time+(i*split_after_x_mins*60)
			et = start_time+((i+1)*split_after_x_mins*60)
			et = end_time if et > end_time else et


			plt.xlim(st, et)
			plt.ylim(-1,40)

			plt.xlabel('Timestamp') 
			plt.ylabel('Value') 
			plt.title(f'Server Matches UTC:{st} through UTC:{et}')
			plt.legend(legend)

			plt.show()
			fig.savefig(f"{file_dir_path}/server_matches_{start_time}_through_{end_time}_image{i}.png")
			plt.clf()
			# plt.cla()
			plt.close()

	# make plots of KDA averages across the given time period
	def make_period_KDA_line_plots(kill_df, roundstate_df, event_df, maps_df, mapdict):
		file_dir_path = f'{log_outputs_dir}/period_KDA_line_plots'
		if not os.path.isdir(file_dir_path):
			os.mkdir(file_dir_path)

		num_maps = len(maps_df)

		split_after_x_mins = 180

		avgs = get_one_min_averages_from_kill_df(kill_df)
		print(avgs)

		legend = [
			'T0:Kills','T1:Kills',
			'T0:Deaths','T1:Deaths',
			'T0:Teamkills','T1:Teamkills',
			'T0:Suicides','T1:Suicides',
			]


		fig = plt.figure(figsize=(26,12))
		plt.subplots_adjust(left=0.03, right=0.97, top=0.93, bottom=0.07)
		# plt.style.use('_mpl-gallery')

		plt.plot(avgs['times'], avgs['team0_deaths'], color='darkred', linestyle=':')
		plt.plot(avgs['times'], avgs['team1_deaths'], color='darkblue', linestyle=':')
		plt.plot(avgs['times'], avgs['team0_kills'], color='red', linestyle='-')
		plt.plot(avgs['times'], avgs['team1_kills'], color='blue', linestyle='-')
		plt.plot(avgs['times'], avgs['team0_tks'], color='firebrick', linestyle='-.')
		plt.plot(avgs['times'], avgs['team1_tks'], color='mediumblue', linestyle='-.')
		plt.plot(avgs['times'], avgs['team0_suicide'], color='lightcoral', linestyle='--')
		plt.plot(avgs['times'], avgs['team1_suicide'], color='slateblue', linestyle='--')

		for map in maps_df.iterrows():
			conts = map[1]
			# mapname = mapdict[conts['map_id']] if str(conts['map_id']) in mapdict else f"UGC{conts['map_id']}"
			mapname = mapdict.get(str(conts['map_id']), f"UGC{conts['map_id']}")
			if type(mapname) == dict:
				mapname = mapname['name']
			plt.axvline(conts['entry_timecode'], 0, 40, **{'linestyle':'-', 'color':'black'})
			plt.text(conts['entry_timecode'], 39, f"{mapname}", rotation=90, verticalalignment='top')

		# state : 0=starting, 1=standby, 2=started, 3=end, -1=unknown
		# reason_int : 0=no reason (including time expiration), 1=SitesDestroyed, 2=OutOfTicketsAttackers, 3=OutOfTicketsDefenders
		for roundstate in roundstate_df.iterrows():
			conts = roundstate[1]
			round_state = conts['state']
			if round_state == 0 or round_state == 1:
				pass
			state_trans = 'UK'
			if round_state == 2:
				# state_trans = 'Start'
				plt.axvline(conts['entry_timecode'], 0, 10, **{'linestyle':'--', 'color':'green'})
			if round_state == 3:
				# state_trans = 'End'
				plt.axvline(conts['entry_timecode'], 0, 10, **{'linestyle':'--', 'color':'orange'})

				reason = int(conts['reason_int'])
				print(reason)

				reason_text = 'TimeExpired'
				if reason == 1:
					reason_text = 'SitesDestroyed'
				if reason == 2:
					reason_text = 'OutofTicketsAttack'
				if reason == 3:
					reason_text = 'OutofTicketsDefend'

				plt.text(conts['entry_timecode'], 20, f"{reason_text}", rotation=90, verticalalignment='top', horizontalalignment='right')
			# plt.axvline(map['entry_timecode'], 0, 40, **{'linestyle':':', 'color':'red'})
			# plt.text(map['entry_timecode'], 40, f"{state_trans}", rotation=90, verticalalignment='top')

		# 0: bomb armed, 1: bomb defused, 2: capture point attack, 3: capture point defend
		# for event in event_df.iterrows():
		# 	conts = event[1]
		# 	if conts['event_int'] == 0:
		# 		plt.axvline(conts['entry_timecode'], 0, 40, **{'linestyle':':', 'color':'blue'})


		plt.xlim(start_time-10, end_time+10)
		plt.ylim(-1,40)

		plt.xlabel('Timestamp') 
		plt.ylabel('Value') 
		plt.title(f'Server Matches UTC:{start_time} through UTC:{end_time}')
		plt.legend(legend)

		plt.show()
		fig.savefig(f"{file_dir_path}/server_matches_{start_time}_through_{end_time}.png")
		plt.clf()
		# plt.cla()
		plt.close()

	# make plots of the average ELO across games **not used**
	def make_server_elo_game_plots(elo_df, mapdict):
		pass

	# generate match statistics like min, max, avg, match len, number of logins
	def get_server_match_statistics(login_df, roundstate_df, map_df, server_table_data, mapdict):
		start_stop_dict = get_map_start_stop_timestamps(roundstate_df, map_df)

		dict_arr = []

		for map_times in start_stop_dict:
			new_entry = {}

			new_entry['start'] = map_times['start']

			new_entry['map_id'] = map_times['map_id']
			new_entry['map_name'] = mapdict.get(str(map_times['map_id']), f"UGC{map_times['map_id']}")
			if type(new_entry['map_name']) == dict:
				new_entry['map_name'] = new_entry['map_name']['name']

			new_entry['duration'] = map_times['end'] - map_times['start']
			if new_entry['duration'] < 2.5*60:
				continue

			login_df_subset = login_df[(login_df['entry_timecode'] >= map_times['start']) & (login_df['entry_timecode'] <= map_times['end'])].reset_index()
			new_entry['logins'] = len(login_df_subset)

			server_df_subset = server_table_data[(server_table_data['entry_timecode'] >= map_times['start']) & (server_table_data['entry_timecode'] <= map_times['end'])].reset_index()
			if len(server_df_subset) < 5:
				continue

			new_entry['min'] = server_df_subset[4:]['slots'].min()
			new_entry['max'] = server_df_subset[4:]['slots'].max()
			new_entry['avg'] = server_df_subset[4:]['slots'].mean()

			dict_arr.append(new_entry)

		return dict_arr

	# record the output of the get_server_match_statistics() function
	def record_match_statistics(match_stat_arr):
		str_accum = "\n"
		str_accum += f"Match Statistic Summary Raw\n"
		str_accum += f"{'#':3} | {'Timestamp':12} | {'Logins':6} | {'Min':3}, {'Avg':4}, {'Max':3} | {'Duration (mins)':5} | {'Map Name':45}\n"

		for i, stat in enumerate(match_stat_arr):
			round_dur = round(stat['duration']/60,1)
			round_dur = round_dur if round_dur <= 200.0 else '  N/A '
			str_accum += f"{i+1:3} | {int(stat['start']):12} | {int(stat['logins']):6} | {int(stat['min']):3}, {round(stat['avg'],1):4}, {int(stat['max']):3} | {round_dur:5} | {stat['map_name']:45}\n"

		logger.info(str_accum)

	# do an analysis of the get_server_match_statistics() output (summarize data into a dict)
	def analyze_match_statistics_by_map(match_stats_arr):
		accum_dict = {}
		last_match = None
		for match in match_stats_arr:
			mapid_key = match['map_id']
			if mapid_key not in accum_dict:
				accum_dict[mapid_key] = {'map_name':match['map_name'], 'data':pd.DataFrame({
					'min_gain':pd.Series(dtype='int'), 
					'max_gain':pd.Series(dtype='int'), 
					'avg_gain':pd.Series(dtype='float'), 
					'login_gain':pd.Series(dtype='int'), 
					'dur_diff_s':pd.Series(dtype='int'), 
					'start':pd.Series(dtype='int'), 
					'duration':pd.Series(dtype='int')
				})}

			if last_match == None:
				last_match = match
				continue
			
			entry = {}
			entry['min_gain'] = match['min'] - last_match['min']
			entry['max_gain'] = match['max'] - last_match['max']
			entry['avg_gain'] = match['avg'] - last_match['avg']
			entry['login_gain'] = match['logins'] - last_match['logins']
			entry['dur_diff_s'] = match['duration'] - last_match['duration']
			entry['start'] = match['start']
			entry['duration'] = match['duration']

			if match['duration']/60 <= 200:
				accum_dict[mapid_key]['data'] = accum_dict[mapid_key]['data']._append(entry, ignore_index=True)

			last_match = match

		return accum_dict

	# record the output of the analyze_match_statistics_by_map() function
	def record_match_analysis(match_anal_dict):
			str_accum = "\n"
			str_accum += "Match Statistic Summary\n"
			str_accum += f"{'Map Name':45} | Avg Gain: {'Logins':6}, {'Min':3}, {'Avg':4}, {'Max':3} | {'Num Games':9} |{'Avg Duration (mins)':19}\n"

			for map_id in match_anal_dict:
				name = match_anal_dict[map_id]['map_name']
				data = match_anal_dict[map_id]['data']
				print(match_anal_dict[map_id])
				if data.empty:
					login_gain = 0
					min_gain = 0
					avg_gain = 0
					max_gain = 0
					min_gain = 0
					duration = 0
				
				else:
					login_gain = int(data['login_gain'].mean())
					min_gain = int(data['min_gain'].mean())
					avg_gain = round(data['avg_gain'].mean(),1)
					max_gain = int(data['max_gain'].mean())
					min_gain = int(len(data['min_gain']))
					duration = round(int(data['duration'].mean())/60,1)

				str_accum += f"{name:45} |           {login_gain:6}, {min_gain:3}, {avg_gain:4}, {max_gain:3} | {min_gain:9} | {duration}\n" #:19


			logger.info(str_accum) 
				# avg_min_gain = 0
				# avg_max_gain = 0
				# avg_avg_gain = 0
				# login_gain = 0
				# map_count = len(data)

				# for entry in data:
				# 	avg_min_gain += entry['min_gain']
				# 	avg_max_gain += entry['max_gain']
				# 	avg_avg_gain += entry['avg_gain']
				# 	avg_dur_accum += match['duration']

	# make plots from the analyze_match_statistics_by_map() function output
	def make_match_statistics_plots(match_anal_dict):
		file_dir_path = f'{log_outputs_dir}/match_statistics_plots'
		if not os.path.isdir(file_dir_path):
			os.mkdir(file_dir_path)

		num_maps = int(len(match_anal_dict))
		num_plots = int(num_maps/8)+1
		division = int(num_maps/num_plots)
		dict_keys = list(match_anal_dict.keys())

		def plot_stats(title, fig_name, y_key):
			for i in range (0,num_plots):
				fig, ax = plt.subplots(1, layout="constrained", figsize=(18,12))
				legend = []
				ind_upper = (i+1)*division
				ind_lower = i*division
				if ind_upper > len(match_anal_dict):
					ind_upper = len(match_anal_dict)

				counter = 8
				if (counter+(i*8) > num_maps):
					counter = num_maps - (i*8)
				# chunk = match_anal_dict[ind_upper:ind_lower]
				# for map_id in chunk:
				for y in range(0,counter):
					map_id = dict_keys[(i*8)+y]
					name = match_anal_dict[map_id]['map_name']
					data = match_anal_dict[map_id]['data']
					# print(map_id)
					ax.scatter(x=data['duration']/60, y=data[y_key], alpha=0.7) # s=s_arr[0], c=c_arr[0], 
					legend.append(name)

				ax.set_title(f'{title} ({i})')
				ax.set_xlabel('Match Duration (mins)')
				ax.set_ylabel('Players Gained')
				fig.legend(legend)
				# fig.show()
				fig.savefig(f'{fig_name}_{i}.png')
				plt.clf()
				# plt.cla()
				plt.close()
		
		plot_stats('Minimum Gain Comparison', f'{file_dir_path}/minimum_gain_scatter', 'min_gain')
		plot_stats('Maximum Gain Comparison', f'{file_dir_path}/maximum_gain_scatter', 'max_gain')
		plot_stats('Average Gain Comparison', f'{file_dir_path}/average_gain_scatter', 'avg_gain')
		plot_stats('Logins Gain Comparison', f'{file_dir_path}/login_gain_scatter', 'login_gain')
					
	# generate a leaderboard of players during the 3/8/25 Push discord event
	def generate_competitive_leaderboard_3_8_25(kill_df, stats_df, roundstate_df, map_df, mapdict):
		special_weapon_weights = {
			'grenade_ger':2,
			'grenade_us':2,
			'Grenade':2,
			'fraggrenade_svt':2,
			'fraggrenade_ger':2,
			'grenade_svt':2,
			'Knife':4,
			'ww2knife':4,
			'bayonet_held':4,
			'bayonet_charge':4,
		}

		number_matches_to_consider = 3

		event_player_ids = {
			76561198040665846:{'name': 'Ushanka'},
			76561199079428245:{'name': 'Keyboardperson'},
			76561198066299492:{'name': 'TotalJTM'},
			76561198142540651:{'name': 'ConConCotter'},
			76561198445634612:{'name': 'Wabungus'},
			76561198133756203:{'name': 'Hammurabi'},
			76561198101042388:{'name': 'Wrestlerninja'},
			76561197977856199:{'name': 'gnabiator'},
			76561198017751181:{'name': 'Miltaneix'},
			76561197971431908:{'name': 'ScarletWahoo'},
			76561198018892304:{'name': 'Donny Demon'},
			76561199059360770:{'name': 'LEUGIM/SCUDIZZL'},
			76561198123954890:{'name': 'Kiritowerty/Youre Next'},
			76561198452363499:{'name': 'k.yle'},
			76561198063848274:{'name': 'data2hd'},
			76561197981053744:{'name': 'VRAVE HARD'},
			76561199274363687:{'name': 'Killacamcam727'},
			76561198866778629:{'name': 'Rumrobot'},
			76561198806722099:{'name': 'Hiroxcz'},
		}
		
		##### step 1: get list of maps and times #####
		match_start_stop_times = get_map_start_stop_timestamps(roundstate_df, map_df)

		##### step 2: make dict of player data and match data #####
		accum_match_data = []
		for ind, match in enumerate(match_start_stop_times[1:]):
			## get data subset from data tables
			player_data_subset = kill_df[(kill_df['entry_timecode'] >= match['start']) & (kill_df['entry_timecode'] <= match['end'])].reset_index()
			stat_data_subset = stats_df[(stats_df['entry_timecode'] >= match['start']) & (stats_df['entry_timecode'] <= match['end'])].reset_index()
			player_ids = list(set(list(player_data_subset.copy()['killer_id'].unique()) + list(player_data_subset.copy()['killed_id'].unique())))

			## put together initial match dict for populating in next part
			curr_match = {
				'map': int(match['map_id']),
				'start': float(match['start']),
				'stop': float(match['end']),
				'match_number': ind+1,
				'player_data':[]
			}
			
			## iterate through players in this match
			team_0_score = 0
			team_0_players = 0
			team_1_score = 0
			team_1_players = 0
			for playerid in player_ids:
				player_dict = {
					'steam_id': int(playerid)
				}

				## get data from dataframes
				player_stat_entry = stat_data_subset[(stat_data_subset['steam_id'] == playerid)].reset_index(drop=True)
				player_kills = player_data_subset[(player_data_subset['killer_id'] == playerid) & (player_data_subset['killed_id'] != playerid)].reset_index(drop=True)
				player_deaths = player_data_subset[(player_data_subset['killer_id'] != playerid) & (player_data_subset['killed_id'] == playerid)].reset_index(drop=True)
				player_suicides = player_data_subset[(player_data_subset['killer_id'] == playerid) & (player_data_subset['killed_id'] == playerid)].reset_index(drop=True)
				player_teamkills = player_kills[(player_kills['killer_teamid'] == player_kills['killed_teamid'])].reset_index(drop=True)
				player_teamkills = player_teamkills[(player_teamkills['killed_by'] != 'OutOfBounds')].reset_index(drop=True)
				player_assists = 0 if len(player_stat_entry) == 0 else player_stat_entry['assist_count'].values[0]
				player_stat_score = 0 if len(player_stat_entry) == 0 else player_stat_entry['experience_count'].values[0]
				player_stat_team = 0 if len(player_stat_entry) == 0 else player_stat_entry['team_id'].values[0]

				## score is calculated as : kill += 2, assist += 1, death += 0, teamkill/suicide (not out of bounds) += -4
				kills = (len(player_kills)-len(player_teamkills))
				calced_score = ((kills if kills > 0 else 0)*2) + player_assists + (len(player_teamkills)*-4)

				## get weapon kills from special_weapon_weights dict
				weapon_score = 0
				accum_weapon_kills = {}
				for weapon in special_weapon_weights:
					weapon_kills = player_kills[(player_kills['killed_by'] == weapon) & (player_kills['killer_teamid'] != player_kills['killed_teamid'])].reset_index()
					accum_weapon_kills[weapon] = len(weapon_kills)
					weapon_score += len(weapon_kills) * special_weapon_weights[weapon]
				
				## record data in player dict
				player_dict['kills'] = int(kills)
				player_dict['deaths'] = int(len(player_deaths))
				player_dict['assists'] = int(player_assists)
				player_dict['teamkills'] = int(len(player_teamkills))
				player_dict['suicides'] = int(len(player_suicides))
				player_dict['calced_score'] = int(calced_score)
				player_dict['stat_score'] = int(player_stat_score)
				player_dict['addtl_score'] = int(calced_score + weapon_score)
				player_dict['team'] = int(player_stat_team)
				player_dict['special_weapon_kills'] = accum_weapon_kills
				curr_match['player_data'].append(player_dict)

				## accum team data
				if player_stat_team == 0:
					team_0_score += calced_score
					team_0_players += 1
				else:
					team_1_score += calced_score
					team_1_players += 1

			## determine how much weight should be applied to the match, should scale between 0.5 and 1.5
			match_balance = (team_0_score/(team_0_score+team_1_score))+0.5 if (team_0_score+team_1_score) > 0 else 1.0
			## determine a 0-1 weight that can be multiplied with each score to reduce score imbalances when team scores are too varied
			match_weight = 1 - (abs(match_balance-1)*2)

			## record team score data
			curr_match['team_0_score'] = int(team_0_score)
			curr_match['team_1_score'] = int(team_1_score)
			curr_match['team_0_players'] = int(team_0_players)
			curr_match['team_1_players'] = int(team_1_players)
			curr_match['match_balance'] = float(match_balance)
			curr_match['match_weight'] = float(match_weight)

			## add match to accumulated list
			accum_match_data.append(curr_match)

		# logger.info(accum_match_data)


		##### step 3: seperate player data into best matches, record results #####
		## update event_player_ids dict with new match score key to hold match data
		event_player_score_data = dict(event_player_ids)
		for player in event_player_score_data:
			event_player_score_data[player]['match_scores'] = []
			event_player_score_data[player]['best_scores'] = []

		## go through each match and make a new dataset with only participants
		event_player_match_data = []
		for match_data_orig in accum_match_data:
			match_data = match_data_orig.copy()
			new_player_data = []
			## iter through players and save data to new array and event_player_score_data
			for player_data in match_data['player_data']:
				if player_data['steam_id'] in event_player_score_data:
					new_player_data.append(player_data)
					event_player_score_data[player_data['steam_id']]['match_scores'].append({
						'match':match_data['match_number'], 
						'raw_score':player_data['calced_score'], 
						'addtl_score':player_data['addtl_score'], 
						'weight_raw_score':player_data['calced_score']*match_data['match_weight'],
						'weight_addtl_score':player_data['addtl_score']*match_data['match_weight']
					})
			match_data['player_data'] = new_player_data
			event_player_match_data.append(match_data)

		# logger.info(event_player_match_data)
		# logger.info(event_player_score_data)

		## get best X scores (according to the score_key that matches the score type we want to use) and determine a calculate score
		score_key = 'weight_addtl_score'
		for player in event_player_score_data:
			temp_storage = event_player_score_data[player]['match_scores'].copy()
			accum_score = 0
			for best_of_x in range(0,min(number_matches_to_consider,len(event_player_score_data[player]['match_scores']))):
				best_score = -999
				best_score_ind = -999
				best_score_match = -999
				for i, match in enumerate(temp_storage):
					if match[score_key] > best_score:
						best_score = match[score_key]
						best_score_ind = i
						best_score_match = match['match']
				event_player_score_data[player]['best_scores'].append({'match':best_score_match, 'score':best_score})
				accum_score += best_score
				del temp_storage[best_score_ind]
			
			event_player_score_data[player]['total_score'] = accum_score
				
		logger.info(event_player_score_data)


		## sort cumulative scores from highest to lowest
		event_player_sorted_ids = []
		event_player_sorted_names = []
		event_player_sorted_data = []
		event_player_sorted_score = []
		for leaderboard_ind in range(0, len(event_player_score_data)):
			top_score = -999
			top_score_ind = -999
			best_scores = None
			player_name = ""
			player_id = -999
			for i, player in enumerate(event_player_score_data):
				if player not in event_player_sorted_ids:
					if event_player_score_data[player]['total_score'] > top_score:
						top_score = event_player_score_data[player]['total_score']
						top_score_ind = i
						best_scores = event_player_score_data[player]['best_scores']
						player_name = event_player_score_data[player]['name']
						player_id = player
	
			event_player_sorted_ids.append(player_id)
			event_player_sorted_data.append(best_scores)
			event_player_sorted_score.append(top_score)
			event_player_sorted_names.append(player_name)
		
		## record player data into logfile
		log_str = "\nEvent Leaderboard:\n"
		log_str += f"{'Num':3} | {'Name':30} | {'Score':7} | {'#1 Match: Score':15} | {'#2 Match: Score':15} | {'#3 Match: Score':15}\n"
		for leaderboard_ind in range(0,len(event_player_sorted_ids)):
			lb_scores = event_player_sorted_data[leaderboard_ind]
			log_str += f"{leaderboard_ind+1:3} | {event_player_sorted_names[leaderboard_ind]:30} | {round(event_player_sorted_score[leaderboard_ind],1):<7} "
			lb_score_len = len(lb_scores)
			for i in range(0,lb_score_len):
				log_str += f"| {lb_scores[i]['match']:>5} : {round(lb_scores[i]['score'],1):<8}"
			for i in range(0,number_matches_to_consider-lb_score_len):
				log_str += f"| {0:>5} : {round(0,1):<8}"
			log_str += "\n"

		logger.info(log_str)








	#####################################################################################################
	##################               Get table data and perform analysis               ##################
	#####################################################################################################
	try:
		# start sqlite connection to store/get data
		# zero timestamp servers before 1731025992

		### get all data during time period from the server-data database
		db_handler = pavlov_server_db_manager.Pavlov_Server_Database(server_database_path)
		stats_table_data_raw = db_handler.get_all_table_data_within_range('stats', 'entry_timecode', start_time, end_time).fetchall()
		kill_table_data_raw = db_handler.get_all_table_data_within_range('kill', 'entry_timecode', start_time, end_time).fetchall()
		login_table_data_raw = db_handler.get_all_table_data_within_range('login', 'entry_timecode', start_time, end_time).fetchall()
		player_table_data_raw = db_handler.get_all_table_data_within_range('player', 'entry_timecode', start_time, end_time).fetchall()
		teamswitch_table_data_raw = db_handler.get_all_table_data_within_range('teamswitch', 'entry_timecode', start_time, end_time).fetchall()
		roundstate_table_data_raw = db_handler.get_all_table_data_within_range('roundstate', 'entry_timecode', start_time, end_time).fetchall()
		event_table_data_raw = db_handler.get_all_table_data_within_range('event', 'entry_timecode', start_time, end_time).fetchall()
		maps_table_data_raw = db_handler.get_all_table_data_within_range('maps', 'entry_timecode', start_time, end_time).fetchall()
		db_handler.close()

		### get all data during time period from the gamelist database
		db_handler = game_list_database_handler.Pavlov_Gamelist_Database(gamelist_database_path)
		server_table_data_raw = db_handler.get_all_table_data_within_range('servers', 'entry_timecode', start_time, end_time).fetchall()
		count_table_data_raw = db_handler.get_all_table_data_within_range('playercount', 'entry_timecode', start_time, end_time).fetchall()
		db_handler.close()

		# print(stats_table_data_raw)

		### reformat table data as dataframes
		stats_table_data = pd.DataFrame(data=stats_table_data_raw, columns={'steam_id':int, 'steam_name':str, 'kill_count':int, 'death_count':int, 'assist_count':int, 'teamkill_count':int, 'headshot_count':int, 'experience_count':int, 'map_label':int, 'mode':str, 'team_id':int, 'entry_timecode':float})
		kill_table_data = pd.DataFrame(data=kill_table_data_raw, columns={'killer_id':int, 'killed_id':int, 'killer_teamid':int, 'killed_teamid':int, 'killed_by':str, 'is_headshot':int, 'entry_timecode':float})
		login_table_data = pd.DataFrame(data=login_table_data_raw, columns={'steam_id':int, 'hardware_id':str,'steam_name':str,'player_height':float,'player_vstock':int, 'player_left_hand':int, 'login_success':int,'entry_timecode':float})
		player_table_data = pd.DataFrame(data=player_table_data_raw, columns={'steam_id':int, 'steam_name':str,'vac_ban_status':int, 'steam_logo':int,'entry_timecode':float})
		teamswitch_table_data = pd.DataFrame(data=teamswitch_table_data_raw, columns={'steam_id':int, 'team_id':int, 'reason_int':int, 'entry_timecode':float})
		roundstate_table_data = pd.DataFrame(data=roundstate_table_data_raw, columns={'state':int, 'reason_int':int, 'entry_timecode':float})
		event_table_data = pd.DataFrame(data=event_table_data_raw, columns={'event_int':int, 'player':int, 'entry_timecode':float})
		maps_table_data = pd.DataFrame(data=maps_table_data_raw, columns={'map_id':int, 'map_name':int, 'entry_timecode':float})

		server_table_data = pd.DataFrame(data=server_table_data_raw, columns=['name', 'ip', 'slots', 'max_slots', 'map_id', 'map_label', 'gamemode', 'pass_en', 'entry_timecode'])
		server_table_data = server_table_data[server_table_data['name'] == pushserver_name].reset_index()
		count_table_data = pd.DataFrame(data=count_table_data_raw, columns=['count', 'source', 'entry_timecode'])

		# print(stats_table_data)

		### load the mapdict json file generated by map_list_generator.py
		mapdict = load_json_file('modio_mapdict.json')
		

		### generate data and log data to log file
		logger.info(f"================================================================================================================")
		map_summary = make_server_map_summary(stats_table_data)
		record_top_players_per_match(map_summary, mapdict, 10)
		logger.info(f"================================================================================================================")
		player_totals = make_player_stats_summary(stats_table_data)
		record_player_totals_unweighted(player_totals.copy())
		logger.info(f"================================================================================================================")
		record_player_totals_weighted(player_totals.copy(), min_games=3)
		logger.info(f"================================================================================================================")

		elo = calculate_player_elos(map_summary, stats_table_data)
		record_player_elo(elo)
		logger.info(f"================================================================================================================")

		teamswitch_summary_df = analyze_teamswitchers(login_table_data,teamswitch_table_data)
		record_teamswitcher_summary(teamswitch_summary_df)
		logger.info(f"================================================================================================================")

		record_player_tks(player_totals.copy(), 100)
		logger.info(f"================================================================================================================")

		record_player_totals_unweighted(player_totals.copy(), 50, min_games=3)
		record_player_totals_weighted(player_totals.copy(), 50, min_games=3)
		logger.info(f"================================================================================================================")


		# def show_percent_kills_deaths(player_totals):
		logger.info('Server Totals:')
		logger.info(f"\tKills: {player_totals['kill_count'].sum()}")
		logger.info(f"\tDeaths: {player_totals['death_count'].sum()}")
		logger.info(f"\tAssists: {player_totals['assist_count'].sum()}")
		logger.info(f"\tTeamkills: {player_totals['teamkill_count'].sum()}")
		logger.info(f"\tHeadshots: {player_totals['headshot_count'].sum()}")
		logger.info(f"\tGames: {player_totals['num_games'].sum()}")
		logger.info(f"================================================================================================================")

		mapids = stats_table_data.copy()['map_label'].unique()
		# maplist_dict = get_map_names_from_id_list(mapids)
		played_maplist = filter_maps_from_mapdict(mapids, mapdict)
		logger.info(played_maplist)

		logger.info(f"================================================================================================================")

		match_statistics_df = get_server_match_statistics(login_table_data, roundstate_table_data, maps_table_data, server_table_data, mapdict)
		record_match_statistics(match_statistics_df)
		match_analysis_dict = analyze_match_statistics_by_map(match_statistics_df)
		record_match_analysis(match_analysis_dict)

		logger.info(f"================================================================================================================")
		if single_plots or combined_plots or player_count_plots or combined_player_count_plots:
			logger.info(f"Making Plots")
			# Plot Notes:
			# Red team attacks first, defends second
			# bubble size changes depending on number of assists
			# includes times when the server failed to start (low pop + long duration) or when the server was not being actively played (really long duration)
			# timestamps are UTC Epoch, you can use an Epoch calculator to reference the datetime
			# make_map_KD_scatter_plots(map_summary, mapdict)
			# make_period_KDA_line_plots_with_serverpop(kill_table_data, roundstate_table_data, event_table_data, maps_table_data, server_table_data, mapdict)
			# make_match_statistics_plots(match_analysis_dict)

		else:
			logger.info("Skipping plot generation")

		logger.info(f"================================================================================================================")
		# record_player_totals_unweighted_no_KDA(player_totals.copy(), 250, min_games=10)
		logger.info(f"================================================================================================================")
		record_player_totals_weighted_no_KDA(player_totals.copy(), 250, min_games=1)

		logger.info(f"================================================================================================================")
		unique_weapons = kill_table_data.copy()['killed_by'].unique()
		logger.info(f"Unique weapons across matches:\n{unique_weapons}")
		logger.info(f"================================================================================================================")
		generate_competitive_leaderboard_3_8_25(kill_table_data, stats_table_data, roundstate_table_data, maps_table_data, mapdict)
		logger.info(f"================================================================================================================")
		
	except:
		logger.exception('exception')

