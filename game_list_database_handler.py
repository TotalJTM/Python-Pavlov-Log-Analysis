# !/usr/bin/python3
# Script written by TotalJTM 2024
# Version 2, 11/2/24

import sqlite3
import time
from datetime import datetime, timezone
import os
import logging

def epoch_timestamp():
	return int(datetime.now().timestamp())

class Pavlov_Gamelist_Database:
	def __init__(self, database_path) -> None:
		# start sqlite connection to store/get data
		self.connection = sqlite3.connect(database_path)
		self.uncommitted_entries = 0
		# cursor = connection.cursor()


	def create_servers_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE servers (\
server_name TEXT, \
ip TEXT, \
slots INTEGER, \
max_slots INTEGER, \
map_id INTEGER, \
map_label TEXT, \
gamemode TEXT, \
pass_en BOOLEAN, \
entry_timecode INTEGER\
);"""
		cursor.execute(table)
		logging.info(f' Made the servers table '.center(64,'='))

	def create_playercount_database(self):
		cursor = self.connection.cursor()
		table = """CREATE TABLE playercount (\
count INTEGER, \
source TEXT, \
entry_timecode INTEGER\
);"""
		cursor.execute(table)
		logging.info(f' Made the servers table '.center(64,'='))

	def commit_changes(self):
		self.connection.commit()
		self.uncommitted_entries = 0

	def close(self):
		self.connection.close()
		
	def make_timestamp(self):
		return epoch_timestamp()

	def add_player_table_entry(self, server_name, ip, slots, max_slots, map_id, map_label, gamemode, pass_en, entry_timecode):
		cursor = self.connection.cursor()
		entry_keys = (
			'server_name',
			'ip',
			'slots',
			'max_slots',
			'map_id',
			'map_label',
			'gamemode',
			'pass_en',
			'entry_timecode'
		)
		entry_values = (
			str(server_name),
			str(ip),
			str(slots),
			str(max_slots),
			str(map_id),
			str(map_label),
			str(gamemode),
			str(pass_en),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO servers {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to servers database: {entry_values}')
		self.uncommitted_entries += 1

	def add_pavlov_playercount_table_entry(self, count, source, entry_timecode):
		cursor = self.connection.cursor()
		entry_keys = (
			'count',
			'source',
			'entry_timecode'
		)
		entry_values = (
			str(count),
			str(source),
			str(entry_timecode),
		)
		cursor.execute(f'''INSERT INTO playercount {entry_keys} VALUES {entry_values}''')
		logging.info(f'Added to servers database: {entry_values}')
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

	def display_table_counts(self):
		servers = self.get_table_estimate('servers')
		playercount = self.get_table_estimate('playercount')

		text = f'''Server database entries:
servers: {servers},
playercount: {playercount},
Total entries: {servers+playercount}'''
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
	psd = Pavlov_Gamelist_Database(arg_opts['database_file'])

	if arg_opts.get('create_database', False) == True:
		psd.create_servers_database()
		psd.create_playercount_database()
		logger.info('Create database command finished')

	if arg_opts.get('print_database_stats', False) == True:
		psd.display_table_counts()

	# player1_entries, player1_count = psd.get_table_data_with_count('kill', '*', f"WHERE killer_id=76561198017751181 OR killed_id=76561198017751181;")
	# logging.info(f'Got {player1_count} entries:\n{player1_entries.fetchall()}')

	# player2_entries, player2_count = psd.get_table_data_with_count('kill', '*', f"WHERE killer_id=76561199018318986 OR killed_id=76561199018318986;")
	# logging.info(f'Got {player2_count} entries:\n{player2_entries.fetchall()}')