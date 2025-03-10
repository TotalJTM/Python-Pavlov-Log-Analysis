
log_path = r"D:\Projects\Coding\PavlovDataAnalysis\raw\"
filename = r"pushbot_kills_midnov_2023_midjan_2024.csv"


def parse_file(filepath, skip=0):
    ftext_raw = None

    with open(filepath, 'r') as f:
        ftext_raw = f.read()

    if not ftext_raw:
        print('Could not get file contents')
        return None
    
    ftext_lines = ftext_raw.split('\n')

    last_entry_conts = None
    last_map_id = None
    last_time_str = None

    for ftext_line in ftext_lines[skip:]:
        line = ftext_line.split(',')

        if line[1] != 'pavlov-rcon-bot-push':
            continue

        if 'Playing PUSH on map' not in line[12]:
            continue

        map_id = line[13]
        time_str = line[7] if line[7] != "" else line[3].strip('Today at ').strip('Yesterday at ')

        if last_map_id == None:
            last_map_id = map_id
        
        if last_map_id == map_id:
            last_entry_conts = line
            last_time_str = line

        if map_id != last_map_id:

        blue_team = {}
        red_team = {}


if __name__ == "__main__":
    parse_file(log_path+filename, skip=1)