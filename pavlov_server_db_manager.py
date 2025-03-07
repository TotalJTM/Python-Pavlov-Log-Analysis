# !/usr/bin/python

import sqlite3
import time
from datetime import datetime, timezone
import os
import logging
# import ftfy
# from regex import Regex

def epoch_timestamp():
	return int(datetime.now().timestamp())

def sanitize_name(name):
	# step = ftfy.fixes.remove_terminal_escapes(ftfy.fixes.unescape_html(name))
	# fixed = ftfy.fix_text(step)
	# step = name.replace('\\xc2', r'\\')
	# Regex.Replace(name, "[^\x20-\xaf]+", "")
	# return name
	# logging.info(f'namefix : {step}, {fixed}')
	# return fixed
	name = list(name)
	i = 0
	while i < len(name):
		# Finding the character whose
		# ASCII value fall under this
		# range
		if (ord(name[i]) < ord('A') or
			ord(name[i]) > ord('Z') and
			ord(name[i]) < ord('a') or
			ord(name[i]) > ord('z')):
				 
			# erase function to erase
			# the character
			del name[i]
			i -= 1
		i += 1
 
	ret = "".join(name)
	return ret

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
login_success INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the login table '.center(64,'='))

	def create_kill_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE kill (\
killer_id INTEGER NOT NULL, \
killed_id INTEGER, \
killer_teamid INTEGER, \
killed_teamid INTEGER, \
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

	def create_teamswitch_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE teamswitch (\
steam_id INTEGER NOT NULL, \
team_id INTEGER, \
reason_int INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the teamswitch table '.center(64,'='))


	# state : 0=starting, 1=standby, 2=started, 3=end, -1=unknown
	# reason_int : 0=no reason (including time expiration), 1=SitesDestroyed, 2=OutOfTicketsAttackers, 3=OutOfTicketsDefenders
	def create_roundstate_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE roundstate (\
state INTEGER NOT NULL, \
reason_int INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the roundstate table '.center(64,'='))

	def create_maps_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE maps (\
map_id INTEGER NOT NULL, \
map_name INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the maps table '.center(64,'='))

	# 0: bomb armed, 1: bomb defused, 2: capture point attack, 3: capture point defend
	def create_event_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE event (\
event_int INTEGER NOT NULL, \
player INTEGER, \
entry_timecode FLOAT\
);"""
		cursor.execute(table)
		logging.info(f' Made the event table '.center(64,'='))


	def commit_changes(self):
		self.connection.commit()
		self.uncommitted_entries = 0


	def close(self):
		self.connection.close()


	def add_player_table_entry(self, steam_id, steam_name, vac_ban_status, steam_logo, entry_timecode):
		# fixed_name = steam_name.decode('utf-8')
		fixed_name = sanitize_name(steam_name)
		d = self.get_table_data('player', '*', addtl=f'WHERE steam_id={steam_id}')
		d = d.fetchall()
		# logging.info(d)
		if len(d) > 0:
			for entry in d:
				# (steamid, name, vac, steamlogo, timestamp)
				logging.info(f"{type(entry[1])}, {type(fixed_name)}, {str(entry[1]) == str(fixed_name)}, {bytes(entry[1], 'utf-8')}, {bytes(fixed_name, 'utf-8')}")
				if str(entry[1]) == str(fixed_name):# or entry[4] == entry_timecode:
					return

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
			str(fixed_name),
			str(vac_ban_status),
			str(steam_logo),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO player {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1

	
	def add_maps_table_entry(self, map_id, map_name, entry_timecode):
		d = self.get_table_data('maps', '*', addtl=f'WHERE entry_timecode={entry_timecode}')
		d = d.fetchall()
		if len(d) > 0:
			return

		cursor = self.connection.cursor()
		entry_keys = (
			'map_id',
			'map_name',
			'entry_timecode',
		)
		entry_values = (
			str(map_id),
			str(map_name),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO maps {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1

	
	def add_teamswitch_table_entry(self, steam_id, team_id, reason_int, entry_timecode):
		d = self.get_table_data('teamswitch', '*', addtl=f'WHERE entry_timecode={entry_timecode}')
		d = d.fetchall()
		if len(d) > 0:
			for entry in d:
				if entry[0] == int(steam_id):
					return

		cursor = self.connection.cursor()
		entry_keys = (
			'steam_id',
			'team_id',
			'reason_int',
			'entry_timecode',
		)
		entry_values = (
			str(steam_id),
			str(team_id),
			str(reason_int),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO teamswitch {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1


	def add_roundstate_table_entry(self, state, reason_int, entry_timecode):
		d = self.get_table_data('roundstate', '*', addtl=f'WHERE entry_timecode={entry_timecode}')
		d = d.fetchall()
		if len(d) > 0:
			return

		cursor = self.connection.cursor()
		entry_keys = (
			'state',
			'reason_int',
			'entry_timecode',
		)
		entry_values = (
			str(state),
			str(reason_int),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO roundstate {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1


	def add_login_table_entry(self, steam_id, hardware_id, steam_name, player_height, player_vstock, 
							player_left_hand, login_success, entry_timecode):
		fixed_name = sanitize_name(steam_name)
		d = self.get_table_data('login', '*', addtl=f'WHERE entry_timecode={entry_timecode}')
		d = d.fetchall()
		if len(d) > 0:
			for entry in d:
				if entry[0] == int(steam_id):
					return

		cursor = self.connection.cursor()
		entry_keys = (
			'steam_id',
			'hardware_id',
			'steam_name',
			'player_height',
			'player_vstock',
			'player_left_hand',
			'login_success',
			'entry_timecode',
		)
		entry_values = (
			str(steam_id),
			str(hardware_id),
			str(fixed_name),
			str(player_height),
			str(player_vstock),
			str(player_left_hand),
			str(login_success),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO login {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1

	def add_kill_table_entry(self, killer_id, killed_id, killer_teamid, killed_teamid, killed_by, is_headshot, entry_timecode):
		d = self.get_table_data('kill', '*', addtl=f'WHERE entry_timecode={entry_timecode}')
		d = d.fetchall()
		if len(d) > 0:
			for entry in d:
				if entry[0] == int(killer_id) and entry[1] == int(killed_id):
					return

		cursor = self.connection.cursor()
		entry_keys = (
			'killer_id',
			'killed_id',
			'killer_teamid',
			'killed_teamid',
			'killed_by',
			'is_headshot',
			'entry_timecode',
		)
		entry_values = (
			str(killer_id),
			str(killed_id),
			str(killer_teamid),
			str(killed_teamid),
			str(killed_by),
			str(is_headshot),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO kill {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to database: {entry_values}')
		self.uncommitted_entries += 1

	
	def add_stats_table_entry(self, steam_id, steam_name, kill_count, death_count, assist_count, teamkill_count, 
							headshot_count, experience_count, map_label, mode, team_id, entry_timecode):
		fixed_name = sanitize_name(steam_name)
		d = self.get_table_data('stats', '*', addtl=f'WHERE entry_timecode={entry_timecode}')
		d = d.fetchall()
		if len(d) > 0:
			for entry in d:
				if entry[0] == int(steam_id):
					return

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
			str(fixed_name),
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

	def add_event_table_entry(self, event_int, player, entry_timecode):
		d = self.get_table_data('event', '*', addtl=f'WHERE entry_timecode={entry_timecode}')
		d = d.fetchall()
		if len(d) > 0:
			return

		cursor = self.connection.cursor()
		entry_keys = (
			'event_int',
			'player',
			'entry_timecode',
		)
		entry_values = (
			str(event_int),
			str(player),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO event {entry_keys} VALUES {entry_values}''')
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
	
	def get_all_table_data_within_range(self, table, param, start, end):
		statement = f'''SELECT * FROM {table} WHERE {param} BETWEEN {start} AND {end}'''

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

	# python pavlov_server_db_manager.py -c -db pushdiscord_servstats.db
	# python pavlov_server_db_manager.py -c -db midpushdiscord_servstats.db
	if arg_opts.get('create_database', False) == True:
		psd.create_player_database()
		psd.create_login_database()
		psd.create_kill_database()
		psd.create_stats_database()
		psd.create_teamswitch_database()
		psd.create_maps_database()
		psd.create_roundstate_database()
		psd.create_event_database()
		logger.info('Create database command finished')

	if arg_opts.get('print_server_stats', False) == True:
		psd.display_table_counts()

	# player1_entries, player1_count = psd.get_table_data_with_count('kill', '*', f"WHERE killer_id=76561198017751181 OR killed_id=76561198017751181;")
	# logging.info(f'Got {player1_count} entries:\n{player1_entries.fetchall()}')

	# player2_entries, player2_count = psd.get_table_data_with_count('kill', '*', f"WHERE killer_id=76561199018318986 OR killed_id=76561199018318986;")
	# logging.info(f'Got {player2_count} entries:\n{player2_entries.fetchall()}')