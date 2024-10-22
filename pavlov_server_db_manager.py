# !/usr/bin/python

import sqlite3
import time, datetime
import os
import logging

class Pavlov_Server_Database:
	def __init__(self, database_path) -> None:
		# start sqlite connection to store/get data
		self.connection = sqlite3.connect(database_path)
		self.uncommitted_entries = 0
		# cursor = connection.cursor()

	def create_player_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE player (\
steam_id INTEGER NOT NULL, \
steam_name TEXT, \
vac_ban_status INTEGER, \
steam_logo INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the player table '.center(64,'='))

	def create_login_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE login (\
steam_id INTEGER NOT NULL, \
hardware_id TEXT, \
steam_name TEXT, \
player_height FLOAT, \
player_vstock INTEGER, \
player_left_hand INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the login table '.center(64,'='))

	def create_kill_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE kill (\
killer_id INTEGER NOT NULL, \
killed_id INTEGER, \
killed_by TEXT, \
is_headshot INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the kill table '.center(64,'='))

	def create_stats_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE stats (\
steam_id INTEGER NOT NULL, \
steam_name TEXT, \
kill_count INTEGER, \
death_count INTEGER, \
assist_count INTEGER, \
teamkill_count INTEGER, \
headshot_count INTEGER, \
experience_count INTEGER, \
map_label INTEGER, \
mode TEXT, \
team_id INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the stats table '.center(64,'='))


	def commit_changes(self):
		self.connection.commit()
		self.uncommitted_entries = 0


	def add_player_table_entry(self, steam_id, steam_name, vac_ban_status, steam_logo, entry_timecode):
		cursor = self.connection.cursor()
		entry_keys = (
			'steam_id',
			'steam_name',
			'vac_ban_status',
			'steam_logo',
			'entry_timecode',
		)
		entry_values = (
			str(steam_id),
			str(steam_name),
			str(vac_ban_status),
			str(steam_logo),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO player {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1


	def add_login_table_entry(self, steam_id, hardware_id, steam_name, player_height, player_vstock, 
							player_left_hand, entry_timecode):
		cursor = self.connection.cursor()
		entry_keys = (
			'steam_id',
			'hardware_id',
			'steam_name',
			'player_height',
			'player_vstock',
			'player_left_hand',
			'entry_timecode',
		)
		entry_values = (
			str(steam_id),
			str(hardware_id),
			str(steam_name),
			str(player_height),
			str(player_vstock),
			str(player_left_hand),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO login {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1

	def add_kill_table_entry(self, killer_id, killed_id, killed_by, is_headshot, entry_timecode):
		cursor = self.connection.cursor()
		entry_keys = (
			'killer_id',
			'killed_id',
			'killed_by',
			'is_headshot',
			'entry_timecode',
		)
		entry_values = (
			str(killer_id),
			str(killed_id),
			str(killed_by),
			str(is_headshot),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO kill {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1

	
	def add_stats_table_entry(self, steam_id, steam_name, kill_count, death_count, assist_count, teamkill_count, 
							headshot_count, experience_count, map_label, mode, team_id, entry_timecode):
		cursor = self.connection.cursor()
		entry_keys = (
			'steam_id',
			'steam_name',
			'kill_count',
			'death_count',
			'assist_count',
			'teamkill_count',
			'headshot_count',
			'experience_count',
			'map_label',
			'mode',
			'team_id',
			'entry_timecode',
		)
		entry_values = (
			str(steam_id),
			str(steam_name),
			str(kill_count),
			str(death_count),
			str(assist_count),
			str(teamkill_count),
			str(headshot_count),
			str(experience_count),
			str(map_label),
			str(mode),
			str(team_id),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO stats {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1

	def get_table_estimate(self, table, addtl=None):
		statement = f'''SELECT COUNT() FROM {table}'''
		cursor = self.connection.cursor()
		if addtl:
			statement += f''' {addtl}'''
		n_estimate = cursor.execute(statement).fetchone()[0]
		return n_estimate

	def get_table_data(self, table, sel_arg, addtl=None):
		statement = f'''SELECT {sel_arg} FROM {table}'''
		if addtl:
			statement += f''' {addtl}'''
		cursor = self.connection.cursor()
		ret = cursor.execute(statement)
		if ret:
			logging.info(f'get table data command executed: {statement}')
			return ret
		return None
	
	def get_table_data_with_count(self, table, sel_arg, addtl=None):
		estimate = self.get_table_estimate(table, addtl)

		if estimate == 0:
			return None, 0

		statement = f'''SELECT {sel_arg} FROM {table}'''
		if addtl:
			statement += f''' {addtl}'''
		cursor = self.connection.cursor()
		ret = cursor.execute(statement)
		if ret:
			logging.info(f'get table data/count command executed: {statement}')
			return ret, estimate
		return None, estimate
	
	# def get_kill_entries(self, killer_id=None, killed_id=None):
	# 	table = 'kill'
	# 	addtl = 'WHERE '

	# 	if killer_id:
	# 		addtl += 'killer_id={killer_id}'
	# 	if killer_id:
			
			
	# 	if killer_id:
	# 		estimate = self.get_table_estimate('kill', '*', f"WHERE killer_id={killer_id} OR killed_id={killed_id};")
	# 		entries = self.get_table_data('kill', '*', f"WHERE killer_id={killer_id} OR killed_id={killed_id};")
	# 		if entries:
	# 			logging.info(f'Got ~{estimate} table entries: {entries}')
	# 			return entries

	def display_table_counts(self):
		kills = self.get_table_estimate('kill')
		logins = self.get_table_estimate('login')
		players = self.get_table_estimate('player')
		stats = self.get_table_estimate('stats')

		text = f'''Server database entries:
Kills: {kills},
Logins: {logins},
Players: {players},
Stats: {stats},
Total entries: {kills+logins+players+stats}'''
		print(text)
	


if __name__ == "__main__":
	import argparse
	import sys

	logging.basicConfig(filename="pavlov_server_database.log",
					format='%(asctime)s %(message)s',
					filemode='w+')
	logger = logging.getLogger()
	logger.addHandler(logging.StreamHandler(sys.stdout))

	logger.setLevel(logging.INFO)

	parser = argparse.ArgumentParser(description='Program to create and manage the Pavlov Server Database file.')
	parser.add_argument('-c','--create-database', required=False, action='store_true', help='Path to the target pavlov server log file')
	parser.add_argument('-db','--database-file', type=str, default='',help='Path to database this data should be added to')
	parser.add_argument('-ps','--print-server-stats', required=False, action='store_true', help='Display statistics about the database')

	# parse the resulting commandline args
	args = parser.parse_args()
	arg_opts = vars(args)

	if arg_opts['database_file'] == '':
		logger.info(f'Could not proceed, no database file path')
		os._exit(1)

	# start sqlite connection to store/get data
	psd = Pavlov_Server_Database(arg_opts['database_file'])

	if arg_opts.get('create_database', False) == True:
		psd.create_player_database()
		psd.create_login_database()
		psd.create_kill_database()
		psd.create_stats_database()
		logger.info('Create database command finished')

	if arg_opts.get('print_server_stats', False) == True:
		psd.display_table_counts()

	# player1_entries, player1_count = psd.get_table_data_with_count('kill', '*', f"WHERE killer_id=76561198017751181 OR killed_id=76561198017751181;")
	# logging.info(f'Got {player1_count} entries:\n{player1_entries.fetchall()}')

	# player2_entries, player2_count = psd.get_table_data_with_count('kill', '*', f"WHERE killer_id=76561199018318986 OR killed_id=76561199018318986;")
	# logging.info(f'Got {player2_count} entries:\n{player2_entries.fetchall()}')