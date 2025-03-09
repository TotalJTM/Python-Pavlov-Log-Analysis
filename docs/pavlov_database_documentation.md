






### 'player' Table
The player table records a record of the players that join the server. This can be updated periodically to keep the latest player entries fresh. This is planned to be used in the front end display. This table should be added to whenever a player is seen for the first time. After that the table should be indexed and each player should be updated with fresh data. 
    'steam_id' : (integer) player steam id
    'steam_name' : (integer) player steam name (when player record was recorded)
    'vac_ban_status' : (integer) 1 if vac banned, 0 if not
    'steam_logo' : (integer) url to steam player image
    'entry_timecode' : (integer) log timestamp as a unix timestamp


### 'login' Table
The login table records a LogNet "Login request" event. The request address needs to be parsed.
    'steam_id' : (integer) player steam id
    'hardware_id' : (text) UE5 assigned hardware ID, needs to be converted from hex to integer
    'steam_name' : (text) player steam name (when log entry was recorded)
    'player_height' : (float) player height
    'player_vstock' : (integer) 1 is vstock, 0 is none
    'player_left_hand' : (integer) 1 is left hand, 0 is right hand
    'entry_timecode' : (integer) log timestamp as a unix timestamp


### 'kill' Table
The kill table records a StatManagerLog "KillData" event.
    'killer_id' : (integer) player steam id
    'killed_id' : (integer) player steam id
    'killed_by' : (text) weapon string
    'is_headshot' : (integer) 0 is no headshot, 1 is headshot
    'entry_timecode' : (integer) log timestamp as a unix timestamp


### 'stats' Table
The stats table records a StatManagerLog "allStats" event.
    'steam_id' : (integer) player steam id
    'steam_name' : (text) player steam name (when log entry was recorded)
    'kill_count' : (integer) kills in a match
    'death_count' : (integer) deaths in a match
    'assist_count' : (integer) assists in a match
    'teamkill_count' : (integer) teamkills in a match
    'headshot_count' : (integer) headshots in a match
    'experience_count' : (integer) experience gained in a match
    'map_label' : (int) UGC for map (no UGC prefix, just the integer representation)
    'mode' : (text) game mode text
    'team_id' : (int) team number player was on
    'entry_timecode' : (integer) log timestamp as a unix timestamp