from pav_la.pavlovpdater import PavlovUpdater
import sqlite3
import time, datetime


def get_entry_from_mod_id(cursor, id):
    data = cursor.execute(f'''SELECT * FROM stats WHERE mod_id == {id}''')
    return data

def get_entry_from_date_range(cursor, start_date, end_date):
    data = cursor.execute(f'''SELECT * FROM stats WHERE entry_timecode >= {start_date.timestamp()} AND entry_timecode <= {end_date.timestamp()}''')
    return data

def extract_stats_from_mods(modlist):
	stats_list = []
	for mod in modlist:
		if 'stats' in mod:
			stats_list.append(mod['stats'])
	return stats_list


def create_stats_database(cursor):
	# {'mod_id': 2771448, 'popularity_rank_position': 169, 'popularity_rank_total_mods': 2268, 'downloads_today': 72, 'downloads_total': 35303, 'subscribers_total': 414, 'ratings_total': 24, 'ratings_positive': 16, 
# 'ratings_negative': 8, 'ratings_percentage_positive': 67, 'ratings_weighted_aggregate': 0.47, 'ratings_display_text': 'Mixed', 'date_expires': 1701097294}
	table = """CREATE TABLE stats (\
mod_id INTEGER NOT NULL, \
popularity_rank_position INTEGER, \
popularity_rank_total_mods INTEGER, \
downloads_total INTEGER, \
subscribers_total INTEGER, \
ratings_total INTEGER, \
ratings_positive INTEGER, \
ratings_negative INTEGER, \
ratings_weighted_aggregate REAL, \
date_expires INTEGER, \
entry_timecode INTEGER\
);"""
	cursor.execute(table)
	logger.info(f' Made the table '.center(center_dist,'='))

def add_mod_to_database(cursor, mod):
	entry_keys = (
		'mod_id',
		'popularity_rank_position',
		'popularity_rank_total_mods',
		'downloads_total',
		'subscribers_total',
		'ratings_total',
		'ratings_positive',
		'ratings_negative',
		'ratings_weighted_aggregate',
		'date_expires',
		'entry_timecode'
	)
	entry_values = (
		str(mod['mod_id']),
		str(mod['popularity_rank_position']),
		str(mod['popularity_rank_total_mods']),
		str(mod['downloads_total']),
		str(mod['subscribers_total']),
		str(mod['ratings_total']),
		str(mod['ratings_positive']),
		str(mod['ratings_negative']),
		str(mod['ratings_weighted_aggregate']),
		str(mod['date_expires']),
		str(int(time.time()))
	)
	cursor.execute(f'''INSERT INTO stats {entry_keys} VALUES {entry_values}''')
	logger.info(f'Added to database: {entry_values}')



center_dist = 100
if __name__ == "__main__":
	import logging
	import sys

	logging.basicConfig(filename="mod_analysis.log",
						format='%(asctime)s %(message)s',
						filemode='w+')
	logger = logging.getLogger()
	logger.addHandler(logging.StreamHandler(sys.stdout))

	logger.setLevel(logging.INFO)
	# logger.setLevel(logging.ERROR)

	logger.info(f'Mod analysis')

	# use the configuration manager to load configuration variables from the .conf file
	from pav_la import settings_manager

	conf_dict = None
	cm = settings_manager.Conf_Manager('PPU.conf', logger)
	conf_dict = cm.get_file_conts_as_dict()

	# create pavlov updater object
	pu = PavlovUpdater(pavlov_mod_dir_path=conf_dict['pavlov_mod_dir_path'], modio_api_token=conf_dict['modio_api_token'], logging_obj=logger)

	# full_modlist = pu.get_pavlov_modlist()
	logger.info(f' Got full modlist '.center(center_dist,'='))
	# logger.info(full_modlist)

	# sub_modlist = pu.get_subscribed_modlist()
	logger.info(f' Got subbed modlist '.center(center_dist,'='))
	# logger.info(sub_modlist)

	# all_mod_stats = extract_stats_from_mods(full_modlist)
	logger.info(f' All mod stats isolated '.center(center_dist,'='))
	# logger.info(all_mod_stats)


	# start sqlite connection to store/get data
	connection = sqlite3.connect("mod_stats.db")
	cursor = connection.cursor()

	#### create database ####
	# create_stats_database(cursor)
	#########################

	#### make entries ####
	# for mod in all_mod_stats:
	# 	add_mod_to_database(cursor, mod)
	#########################

	# logger.info(f' Added new stats to table '.center(center_dist,'='))

	# connection.commit()
	# logger.info(f' Committed to table '.center(center_dist,'='))

	# data = cursor.execute('''SELECT * FROM stats''') # get all stats

	data = get_entry_from_date_range(cursor, datetime.datetime(2023,11,1), datetime.datetime(2023,11,30))
	logger.info(f' Printing back contents '.center(center_dist,'='))
	for row in data:
		logger.info(row)




	# logger.info(f' ___ '.center(center_dist,'='))