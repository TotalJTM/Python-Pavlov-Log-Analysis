

data = None

with open(r'raw/push_server_maplist_highpush.csv', 'r') as f:
    data = f.read()



map_pool = {} # each key is a map name
reconstructed_maps = {} # each key is the entry date
freq_count = {} # each key is a map name
ugcs = {} # each key is a map name


if data:
    lines = data.split('\n')
    print(lines)
    lines.reverse()
    for line in lines:
        ls = line.split(',')
        date = ls[0].strip('ï»¿')
        print(date)
        reconstructed_maps[date] = []
        for val in ls[1:]:
            map_name, ugc = val.split(' <PUSH> ')
            map_pool[map_name] = date
            reconstructed_maps[date].append(map_name)
            ugcs[map_name] = ugc
            if map_name in freq_count:
                freq_count[map_name] += 1
            else:
                freq_count[map_name] = 0
            

    # print(map_pool)

    for map_name in map_pool:
        print(f'{map_pool[map_name]}')

    for map_name in map_pool:
        print(f'{map_name}')

    for map_name in map_pool:
        print(f'{freq_count[map_name]}')

    for map_name in map_pool:
        print(f'{ugcs[map_name]}')
    
    

for map_name in map_pool:
        print(f'{map_pool[map_name]}, {freq_count[map_name]}, {map_name}')

