from pav_la.pavlovpdater import PavlovUpdater
import json


# handle json files
def load_json_file(file_path):
	with open(file_path,'r') as f:
		return json.load(f)
	
def write_json_file(file_path, d):
	with open(file_path,'w+') as f:
		json.dump(d,f)


def extract_stats_from_mods(modlist):
	stats_list = []
	for mod in modlist:
		if 'stats' in mod:
			stats_list.append(mod['stats'])
	return stats_list


center_dist = 100
if __name__ == "__main__":
	import logging
	import sys

	logging.basicConfig(filename="mod_gen.log",
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

	full_modlist = pu.get_pavlov_modlist()
	logger.info(f' Got full modlist '.center(center_dist,'='))
	# logger.info(full_modlist)

	# sub_modlist = pu.get_subscribed_modlist()
	# logger.info(f' Got subbed modlist '.center(center_dist,'='))
	# logger.info(sub_modlist)

	# all_mod_stats = extract_stats_from_mods(full_modlist)
	# logger.info(f' All mod stats isolated '.center(center_dist,'='))
	# logger.info(all_mod_stats)
	
	logger.info(f' Printing back contents '.center(center_dist,'='))
	mod_dict = {}
	for mod in full_modlist:
		logger.info(mod)
		id = mod['id']
		mod_dict[id] = {
			'name': mod['name'],
			'author': mod['submitted_by']['username'],
		}
	
	write_json_file('modio_mapdict.json', mod_dict)

	# logger.info(f' ___ '.center(center_dist,'='))