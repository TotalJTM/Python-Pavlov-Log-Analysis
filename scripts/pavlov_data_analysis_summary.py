import json
import datetime
import os
import requests
from bs4 import BeautifulSoup
import logging
import sys

logging.basicConfig(filename="pavlov_data_analysis_summary.log",
                    format='%(asctime)s %(message)s',
                    filemode='w+')
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))

logger.setLevel(logging.INFO)

###########################################################
### File Selection and Decleration
###########################################################
summary_2 = r'message (2).txt'
summary_1 = r'message (1).txt'



###########################################################
### Program Variables
###########################################################
outputs_directory = 'outputs'

target_file = summary_2

leader_count = 100

###########################################################
### Get file imported
###########################################################

# load old steamid data
loaded_steamids = None
with open('steamids.json', 'r') as f:
    loaded_steamids = json.loads(f.read())
if loaded_steamids == None:
    loaded_steamids = {}


def order_list_by_attribute(player_dict, attribute, count, high_to_low=True):
    if count > len(player_dict):
        count = len(player_dict)

    # make kill leader list
    ordered_arr = []
    player_dict_copy = player_dict.copy()

    steamid_dict = {}

    for i in range(0,count):
        highest_val = 0
        leader = None
        # print(f'new iter, {leader}')
        for steamid in player_dict_copy:
            if leader == None:
                leader = steamid
                highest_val = player_dict_copy[steamid][attribute]

            # if str(steamid) in kill_leader_excl:
            #     print(f'excluded {steamid}')
                # continue
            if highest_val < player_dict_copy[steamid][attribute]:
                leader = steamid
                highest_val = player_dict_copy[steamid][attribute]
                # print(f'Updating leader {leader}')
        
        del player_dict_copy[leader]

        # kill_leader_excl.append(steamid)
        if high_to_low:
            ordered_arr.append((leader, highest_val))
        else:
            ordered_arr.insert(0,(leader, highest_val))
        
    return ordered_arr

def make_ordered_text(ordered_arr):
    out_text = ""
    index = 0
    for leader, value in ordered_arr:
        index+=1
        print(f'Num {index}: {leader}')
        if leader in loaded_steamids:
            out_text += f'{index:4} - {loaded_steamids[leader]}: {value}\n'
        else:
            try:
                steam_url = f'https://steamcommunity.com/profiles/{leader}'
                # print(steam_url)
                resp = requests.get(steam_url)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    steam_name = soup.find("span", class_="actual_persona_name").text
                    # steam_name = steam_name.encode('utf-8').decode('utf-8','ignore')
                    # print(steam_name)
                    out_text += f"{index:4} - {steam_name}: {value}\n"
                    loaded_steamids[leader] = steam_name
                    # print(out_text)
                else:
                    out_text += f'{index:4} - {leader}: {value}\n'
            except Exception as e:
                out_text += f'{index:4} - {leader}: {value}\n'
        
    return out_text+'\n\n'

# Summary 1 message
#   {"allStats":[{"uniqueId":"76561199176267456","productId":"0002c72eb6d34f00bd491f2953bb738b","playerName":"Pasta-******","teamId":0,"stats":[{"statType":"Death","amount":1}]}],"MapLabel":"UGC2849391","GameMode":"PUSH","PlayerCount":2,"bTeams":true,"Team0Score":1,"Team1Score":1}

# dict elements that hold match data including map, score, players, player k-d-exp
def process_summary_1(full_text):
    full_text_rows = full_text.split('\n')
    # dict where 'players' will be stored (player name is key, stores an arr of 'games')
    player_game_dict = {}
    name_dict = {}
    unknown_ids = []

    for line in full_text_rows:
        if line == '':
            continue

        linejson = json.loads(line)
        
        for player in linejson['allStats']:
            print(player)
            if player['uniqueId'] not in player_game_dict:

                if str(player['uniqueId']) in loaded_steamids:
                    name_dict[player['uniqueId']] = loaded_steamids[str(player['uniqueId'])]
                    logger.info(f'New name {name_dict[player["uniqueId"]]} with id {player["uniqueId"]}')
                else:
                    unknown_ids.append(player['uniqueId'])
                    logger.info(f'New unknown id {player["uniqueId"]}')
                
                player_game_dict[player['uniqueId']] = []
                

            kills = 0
            deaths = 0
            exp = 0
            
            for stat in player['stats']:
                if stat['statType'] == 'Kill':
                    kills = stat['amount']
                elif stat['statType'] == 'Death':
                    deaths = stat['amount']
                elif stat['statType'] == 'Experience':
                    exp = stat['amount']

            game = {
                'map':linejson['MapLabel'],
                'mode':linejson['GameMode'],
                'kills':kills,
                'deaths':deaths,
                'experience':exp,
            }

            player_game_dict[player['uniqueId']].append(game)

    logger.info(f'Process summary 2 complete, player game dict below')
    logger.info(player_game_dict)
    logger.info(f'Unknown IDs: {unknown_ids}')



    player_dict = {}
    map_kill_dict = {}
    map_deaths_dict = {}
    map_exp_dict = {}


    for player in player_game_dict:
        kills = 0
        deaths = 0
        exp = 0
        # assists = 0
        # headshots = 0
        # teamkills = 0
        
        for game in player_game_dict[player]:
            # handle normal player_dict stuff
            kills += game['kills']
            deaths += game['deaths']
            exp += game['experience']
            # add kills/deaths/exp to map arrs
            if game['map'] not in map_kill_dict:
                map_kill_dict[game['map']] = {}
                map_deaths_dict[game['map']] = {}
                map_exp_dict[game['map']] = {}

            if player not in map_kill_dict[game['map']]:
                map_kill_dict[game['map']][player] = []
                map_deaths_dict[game['map']][player] = []
                map_exp_dict[game['map']][player] = []

            map_kill_dict[game['map']][player].append(game['kills'])
            map_deaths_dict[game['map']][player].append(game['deaths'])
            map_exp_dict[game['map']][player].append(game['experience'])


        player_dict[player] = {
            'kills':kills,
            'deaths':deaths,
            'experience':exp,
            'assists':0,
            'headshots':0,
            'teamkills':0,
        }

    return player_game_dict, player_dict, name_dict, map_kill_dict, map_deaths_dict, map_exp_dict


# Summary 2 message
#   76561199214357182
#   alienboy11
#   [{"statType":"Death","amount":1}]
#   76561199176267456
#   Pasta-******
#   [{"statType":"Death","amount":1}]

# seems to be a summary of the players activity over a period of time, not sure when exactly but around 11/22/23-11/26/23.
def process_summary_2(full_text):
    full_text_rows = full_text.split('\n')
    # dict where 'players' will be stored (player name is key, stores an arr of 'games')
    player_dict = {}
    name_dict = {}

    keys = []
    new_ids = {}

    for i in range(0, int(len(full_text_rows)/3)):
        index = i*3

        if len(full_text_rows) < index+2:
            break

        id = full_text_rows[index]
        name = full_text_rows[index+1]
        stats = json.loads(full_text_rows[index+2])

        if id not in loaded_steamids:
            new_ids[id] = name


        kills = 0
        deaths = 0
        exp = 0
        assists = 0
        headshots = 0
        teamkills = 0
        
        for stat in stats:
            if stat['statType'] == 'Kill':
                kills = stat['amount']
            elif stat['statType'] == 'Death':
                deaths = stat['amount']
            elif stat['statType'] == 'Experience':
                exp = stat['amount']
            elif stat['statType'] == 'TeamKill':
                teamkills = stat['amount']
            elif stat['statType'] == 'Headshot':
                headshots = stat['amount']
            elif stat['statType'] == 'Assist':
                assists = stat['amount']
            else:
                keys.append(stat['statType'])

        game = {
            'kills':kills,
            'deaths':deaths,
            'experience':exp,
            'assists':assists,
            'headshots':headshots,
            'teamkills':teamkills,
        }

        player_dict[id] = game

    logger.info(f'Process summary 2 complete, player dict below')
    logger.info(player_dict)
    logger.info(f'Player stat keys not included in entries:\n{keys}')
    logger.info(f'Players not in steam database:\n{new_ids}')

    return player_dict, name_dict


def get_steam_name_from_id(steamid):
    try:
        steam_url = f'https://steamcommunity.com/profiles/{steamid}'
        # print(steam_url)
        resp = requests.get(steam_url)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            return soup.find("span", class_="actual_persona_name").text
    except Exception as e:
        logger.exception()
        return None


if __name__ == "__main__":
    ### standard file read-in
    full_text = None
    logger.info(f'Reading in {target_file}')
    with open(target_file, 'r') as f:
        full_text = f.read()


    logger.info(f'Finished reading file')
    #
    # full_text_rows = full_text.split('\n')

    player_game_dict = None
    player_dict = None
    name_dict = None
    map_kill_dict = None
    map_deaths_dict = None
    map_exp_dict = None

    if target_file == summary_1:
        player_game_dict, player_dict, name_dict, map_kill_dict, map_deaths_dict, map_exp_dict = process_summary_1(full_text)
    elif target_file == summary_2:
        player_dict, name_dict = process_summary_2(full_text)


    ### save the pavlov analysis data
    # format a filename for the new file
    pavlov_data_analysis_path = f'{outputs_directory}/pavlov_data_analysis_summary_{datetime.datetime.now().strftime("%m_%d_%y__%H_%M_%S")}.txt'
    # save the data
    print('Writing output data to file')
    with open(pavlov_data_analysis_path, 'w+', encoding="utf-8") as f:
        # record source file information
        f.write(f'Sourcefile: {target_file}\n')

        if player_game_dict != None:
            # record game dictionary (compilation of games) as formatted json
            # f.write('\n========== Raw Player-Game JSON ==========\n\n')
            # f.write(json.dumps(player_game_dict, indent=4))
            logger.info('\n========== Raw Player-Game JSON ==========\n\n')
            logger.info(json.dumps(player_game_dict, indent=4))
        
        if player_dict != None:
            # record kill dictionary (compilation of kills) as formatted json
            # f.write('\n========== Raw Player JSON ==========\n\n')
            # f.write(json.dumps(player_dict, indent=4))
            logger.info('\n========== Raw Player JSON ==========\n\n')
            logger.info(json.dumps(player_dict, indent=4))
            kill_leader_arr = order_list_by_attribute(player_dict, 'kills', leader_count, high_to_low=True)
            death_leader_arr = order_list_by_attribute(player_dict, 'deaths', leader_count, high_to_low=True)
            experience_leader_arr = order_list_by_attribute(player_dict, 'experience', leader_count, high_to_low=True)
            assist_leader_arr = order_list_by_attribute(player_dict, 'assists', leader_count, high_to_low=True)
            headshot_leader_arr = order_list_by_attribute(player_dict, 'headshots', leader_count, high_to_low=True)
            teamkill_leader_arr = order_list_by_attribute(player_dict, 'teamkills', leader_count, high_to_low=True)


            kill_leader_text = make_ordered_text(kill_leader_arr)
            death_leader_text = make_ordered_text(death_leader_arr)
            experience_leader_text = make_ordered_text(experience_leader_arr)
            assist_leader_text = make_ordered_text(assist_leader_arr)
            headshot_leader_text = make_ordered_text(headshot_leader_arr)
            teamkill_leader_text = make_ordered_text(teamkill_leader_arr)

            # record kill leader compiled text
            f.write(f'========== Top {leader_count} Kill Leaders ==========\n\n')
            f.write(kill_leader_text)

            f.write(f'========== Top {leader_count} Death Leaders ==========\n\n')
            f.write(death_leader_text)

            f.write(f'========== Top {leader_count} Experience Leaders ==========\n\n')
            f.write(experience_leader_text)

            f.write(f'========== Top {leader_count} Assist Leaders ==========\n\n')
            f.write(assist_leader_text)

            f.write(f'========== Top {leader_count} Headshot Leaders ==========\n\n')
            f.write(headshot_leader_text)

            f.write(f'========== Top {leader_count} Teamkill Leaders ==========\n\n')
            f.write(teamkill_leader_text)
            

        if name_dict != None:
            # record name dict
            # f.write('\n========== Raw Name JSON ==========\n\n')
            # f.write(json.dumps(name_dict, indent=4))
            logger.info('\n========== Raw Name JSON ==========\n\n')
            logger.info(json.dumps(name_dict, indent=4))

        if map_kill_dict != None:
            # record map dictionary (compilation of map stats) as formatted json
            # f.write('\n========== Raw Map-Kill JSON ==========\n\n')
            # f.write(json.dumps(map_kill_dict, indent=4))
            logger.info('\n========== Raw Map-Kill JSON ==========\n\n')
            logger.info(json.dumps(map_kill_dict, indent=4))

        if map_deaths_dict != None:
            # record map dictionary (compilation of map stats) as formatted json
            # f.write('\n========== Raw Map-Death JSON ==========\n\n')
            # f.write(json.dumps(map_deaths_dict, indent=4))
            logger.info('\n========== Raw Map-Death JSON ==========\n\n')
            logger.info(json.dumps(map_deaths_dict, indent=4))

        if map_exp_dict != None:
            # record map dictionary (compilation of map stats) as formatted json
            # f.write('\n========== Raw Map-Exp JSON ==========\n\n')
            # f.write(json.dumps(map_exp_dict, indent=4))
            logger.info('\n========== Raw Map-Exp JSON ==========\n\n')
            logger.info(json.dumps(map_exp_dict, indent=4))