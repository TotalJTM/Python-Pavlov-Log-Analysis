# import requests module highpush_pavlov_db_file
import requests
from bs4 import BeautifulSoup
import os
from pav_la import pavlov_server_db_manager as psdb, pavlov_server_log_parser as pslp
import http_auth_credentials as credentials

# path to log directory: ex. r'http://ser.ver.add.ress:port/Logs'
highpush_discord_log_dir = credentials.highpush_discord_log_dir
# HTTPBasicAuth class with login information: ex. HTTPBasicAuth('username', 'password')
highpush_discord_http_auth = credentials.highpush_discord_http_auth
# local storage location for log files
highpush_raw_log_storage_path = credentials.highpush_raw_log_storage_path
# database file path to store data in
highpush_pavlov_db_file = credentials.highpush_pavlov_db_file


# path to log directory: ex. r'http://ser.ver.add.ress:port/Logs'
midpush_discord_log_dir = credentials.midpush_discord_log_dir
# HTTPBasicAuth class with login information: ex. HTTPBasicAuth('username', 'password')
midpush_discord_http_auth = credentials.midpush_discord_http_auth
# local storage location for log files
midpush_raw_log_storage_path = credentials.midpush_raw_log_storage_path
# database file path to store data in
midpush_pavlov_db_file = credentials.midpush_pavlov_db_file


def download_file_with_requests(url, filepath, httpbasicauth):
	resp = requests.get(url, auth=httpbasicauth)
	if resp.status_code == 200:
		with open(filepath, 'w', encoding='utf-8') as f:
			# for chunk in r.iter_content():
				# f.write(chunk)
				f.write(resp.text)


ignore_list = [
	'..',
]

def get_filelist_from_server(url, httpbasicauth):
	# Making a get request 
	resp = requests.get(url, auth=httpbasicauth)
	
	# print request object 
	if resp.status_code == 200:
		# print(resp.text)
		soup = BeautifulSoup(resp.content, "html.parser")
		pre_soup = soup.find("pre")
		# print(pre_soup.text)
		filenames_href = pre_soup.find_all("a")

		filenames = []
		for filename in filenames_href:
			# print(filename)
			ignore_list_bools = [i==filename.text for i in ignore_list]
			if any(ignore_list_bools):
				continue
			filenames.append(filename.text)
		return filenames
	else:
		print(f'Error: resp code {resp.status_code}')
		return None


def get_list_of_files_from_dir(dir):
	for path, folders, files in os.walk(dir):
		return files


def download_latest_pavlov_server_files(log_addr, http_auth, storage_path):
	server_filelist = get_filelist_from_server(log_addr, http_auth)
	# print(server_filelist)

	raw_dir_filelist = get_list_of_files_from_dir(storage_path)

	filepaths = []

	for file in server_filelist:
		file_overlap_bools = [i==file for i in raw_dir_filelist]
		# print(file_overlap_bools)
		if any(file_overlap_bools):
			print(f'File "{file}" already exists')
			continue

		print(f'Downloading {file}')
		target_filepath = f'{storage_path}/{file}'
		download_file_with_requests(f'{log_addr}/{file}', target_filepath, http_auth)
		print(f'Downloaded {file}')
		filepaths.append(target_filepath)

	print(f'Downloading Pavlov.log')
	target_filepath = f'{storage_path}/Pavlov.log'
	download_file_with_requests(f'{log_addr}/Pavlov.log', target_filepath, http_auth)
	filepaths.append(target_filepath)
	print(f'Downloaded new Pavlov.log')

	return filepaths


if __name__ == "__main__":
	download_list = download_latest_pavlov_server_files(highpush_discord_log_dir, highpush_discord_http_auth, highpush_raw_log_storage_path)
	# download_list = [f'{highpush_raw_log_storage_path}/{i}' for i in get_list_of_files_from_dir(highpush_raw_log_storage_path)]

	print(f'\nDownloads from Highpush:\n{download_list}')

	psd_high = psdb.Pavlov_Server_Database(highpush_pavlov_db_file)
	
	try:
		for file in download_list:
			print(f'Updating database with file "{file}"')
			pslp.parse_server_log_into_database(file, psd_high)

		print(f'Committing {psd_high.uncommitted_entries} changes')
		psd_high.commit_changes()

	except Exception as e:
		print(f'Encountered exception {e}')
		yes_no = input(f'Would you like to commit {psd_high.uncommitted_entries} entries generated during session? (y)es or (n)o')
		if 'yes' in yes_no.lower() or 'y' == yes_no.lower():
			print(f'Committing {psd_high.uncommitted_entries} changes')
			psd_high.commit_changes()

	print(f'\n')

	download_list = download_latest_pavlov_server_files(midpush_discord_log_dir, midpush_discord_http_auth, midpush_raw_log_storage_path)
	# download_list = [f'{midpush_raw_log_storage_path}/{i}' for i in get_list_of_files_from_dir(midpush_raw_log_storage_path)]

	print(f'\nDownloads from Midpush:\n{download_list}')

	psd_mid = psdb.Pavlov_Server_Database(midpush_pavlov_db_file)
	
	try:
		for file in download_list:
			print(f'Updating database with file "{file}"')
			pslp.parse_server_log_into_database(file, psd_mid)

		print(f'Committing {psd_mid.uncommitted_entries} changes')
		psd_mid.commit_changes()

	except Exception as e:
		print(f'Encountered exception {e}')
		yes_no = input(f'Would you like to commit {psd_mid.uncommitted_entries} entries generated during session? (y)es or (n)o')
		if 'yes' in yes_no.lower() or 'y' == yes_no.lower():
			print(f'Committing {psd_mid.uncommitted_entries} changes')
			psd_mid.commit_changes()