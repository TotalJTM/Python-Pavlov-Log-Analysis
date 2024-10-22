import json
import datetime
import os
import requests
from bs4 import BeautifulSoup

###########################################################
### File Selection and Decleration
###########################################################
pushkills_file = r'push_kills_10_15_23.txt'
allkills_file = r'allkills.json'



###########################################################
### Program Variables
###########################################################
outputs_directory = 'outputs'

target_file = allkills_file

leader_count = 10000

###########################################################
### Get file imported
###########################################################

### standard file read-in
allkills_text = None
print(f'Reading in {target_file}')
with open(target_file, 'r') as f:
    allkills_text = f.read()

print(f'Finished reading file\n')
# split file by lines (next part expects lines with format ``{"Killer":"76561198821696829","KillerTeamID":1,"Killed":"76561198821696829","KilledTeamID":1,"KilledBy":"None","Headshot":true}``)
allkills_text = allkills_text.split('\n')


### tried to make a way to import files that are indented, could not process them
# if allkills_text[0] == '{':
#     replacement_arr = []
#     for i in range(0, (int(len(allkills_text)/8))):
#         if (int(len(allkills_text)/8) - i) < 0:
#             print(i)
#             continue
#         try:
#             n = '{' + allkills_text[i+1] + allkills_text[i+2] + allkills_text[i+3] + allkills_text[i+4] + allkills_text[i+5] + allkills_text[i+6] + '}'
#             replacement_arr.append(n)
#         except:
#             print(f'Could not handle line {i+8}')

#     allkills_text = replacement_arr
#     print(replacement_arr)


print(f'Split file text, starting data processing')
###########################################################
### Data Processing
###########################################################
kill_dict = {}
allkills_dict_arr = []


# Example data:
# {"Killer":"76561198821696829","KillerTeamID":1,"Killed":"76561198821696829","KilledTeamID":1,"KilledBy":"None","Headshot":true}
# {"Killer":"76561198821696829","KillerTeamID":1,"Killed":"76561198821696829","KilledTeamID":1,"KilledBy":"Grenade","Headshot":false}
# {"Killer":"76561198171505589","KillerTeamID":0,"Killed":"76561198171505589","KilledTeamID":0,"KilledBy":"None","Headshot":true}
### split the lines into a formatted dict item, stored in allkills_dict_arr and kill_dict
for count, line in enumerate(allkills_text):
    # handle last line from file condition
    if line == '':
        continue

    # remove file endings
    line_items = line.strip('{').strip('}').split(',')
    
    line_dict = {}
    for line_item in line_items:
        splt = line_item.split(':')
        try:
            line_dict[splt[0].strip('"')] = splt[1].strip('"')
        except:
            print(f'Could not split item {line_item} on line {count}')
            # print(splt)
            # print(line)
            continue

    allkills_dict_arr.append(line_dict)
    # print(line_dict)

    if line_dict['Killer'] in kill_dict:
        kill_dict[line_dict['Killer']] += 1
    else:
        kill_dict[line_dict['Killer']] = 1


print('Parsed file data, now starting to make leaderboard')
### make leaderboard

# load old steamid data
loaded_steamids = None
with open('steamids.json', 'r') as f:
    loaded_steamids = json.loads(f.read())
if loaded_steamids == None:
    loaded_steamids = {}

if leader_count > len(kill_dict):
    leader_count = len(kill_dict)

# make kill leader list
kill_leader_arr = []
kill_dict_copy = kill_dict.copy()
kill_leader_text = ''

steamid_dict = {}

for i in range(0,leader_count):
    highest_kills = 0
    leader = None
    # print(f'new iter, {leader}')
    for steamid in kill_dict_copy:
        if leader == None:
            leader = steamid
            highest_kills = kill_dict_copy[steamid]

        # if str(steamid) in kill_leader_excl:
        #     print(f'excluded {steamid}')
            # continue
        if highest_kills < kill_dict_copy[steamid]:
            leader = steamid
            highest_kills = kill_dict_copy[steamid]
            # print(f'Updating leader {leader}')
    
    del kill_dict_copy[leader]

    if leader in loaded_steamids:
        kill_leader_text += f'{i+1:4} - {loaded_steamids[leader]}: {highest_kills}\n'
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
                kill_leader_text += f"{i+1:4} - {steam_name}: {highest_kills}\n"
                steamid_dict[leader] = steam_name
                # print(kill_leader_text)
            else:
                kill_leader_text += f'{i+1:4} - {leader}: {highest_kills}\n'
        except Exception as e:
            kill_leader_text += f'{i+1:4} - {leader}: {highest_kills}\n'
            print(e)
    # kill_leader_excl.append(steamid)
    # print(kill_leader_excl)
    kill_leader_arr.append((leader, highest_kills))
# print(steamid_dict)
# print(kill_leader_excl)
# sorted_kill_dict = sorted(kill_dict.items(), key=lambda x:x[1], reverse=True)
# print(sorted_kill_dict)

print('Finished data processing! Starting file saves')
###########################################################
### File Saving
###########################################################
### update imported steamid list with new steamids not yet in list
# generate output file/update file with new steamids acquired during this run
not_included_count = 0
for id in steamid_dict:
    # print(id)
    if id not in loaded_steamids:
        not_included_count += 1
        loaded_steamids[id] = steamid_dict[id]

# print(loaded_steamids)
# print(steamid_dict)

### update steamids file
if not_included_count > 0:
    print('Writing new steamids')
    with open('steamids.json', 'w+', encoding="utf-8") as f:
        # steam_dict = {
        #     'data': loaded_steamids,
        # }
        f.write(json.dumps(loaded_steamids, indent=4))

    print(f'Finished writing steamids file\n')


### save the pavlov analysis data
# format a filename for the new file
pavlov_data_analysis_path = f'{outputs_directory}/pavlov_data_analysis_{datetime.datetime.now().strftime("%m_%d_%y__%H_%M_%S")}.txt'
# save the data
print('Writing output data to file')
with open(pavlov_data_analysis_path, 'w+', encoding="utf-8") as f:
    # record source file information
    f.write(f'Sourcefile: {target_file}\n')
    # record kill leader compiled text
    f.write(f'========== Top {leader_count} Kill Leaders ==========\n\n')
    f.write(kill_leader_text)
    # record kill dictionary (compilation of kills) as formatted json
    f.write('\n========== Raw JSON ==========\n\n')
    f.write(json.dumps(kill_dict, indent=4))

print(f'Finished writing {pavlov_data_analysis_path} file\n')


### update of the allkills.json file (properly formatted json)
allkills_path = f'{outputs_directory}/allkills_actual.json'
if not os.path.exists(allkills_path):
    # writing JSON allkills dir
    allkills_dict = {
        'data': allkills_dict_arr,
    }

    print('Writing formatted JSON representation to allkills_actual.json file')
    with open(allkills_path, 'w+') as f:
        f.write(json.dumps(allkills_dict, indent=4))

    print('Finished writing JSON Allkills file\n')