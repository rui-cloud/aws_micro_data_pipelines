import json
from collections import defaultdict

# Opening JSON file
with open("raw_crypto_data.json") as json_file: #this is where we fetch the data from S3 at cloud level
    data = json.load(json_file)

#Creates an empty dictionary 
new_dict= defaultdict(dict)

#The loop below allows reshapes the data structure 
for k in data['data']: #Allows to acces the object "data"
    del k['tags'] #it deletes tags 

    date = k['quote']['GBP']['last_updated']
    ref = k['id']
    pos= k['cmc_rank']
    name = k['name']
    sbl = k['symbol']
    pri = k['quote']['GBP']['price']
    vol24 = k['quote']['GBP']['volume_24h']
    ch24h = k['quote']['GBP']['percent_change_24h']
    ch7d = k['quote']['GBP']['percent_change_7d']
    mkc = k['quote']['GBP']['market_cap']
    mkd = k['quote']['GBP']['market_cap_dominance']

    new_dict[sbl]['date'] = date
    new_dict[sbl]['id'] = ref
    new_dict[sbl]['cmc_rank_position'] = pos
    new_dict[sbl]['name'] = name
    new_dict[sbl]['GBP_price'] = pri
    new_dict[sbl]['volume_24h'] = vol24
    new_dict[sbl]['change_24h'] = ch24h
    new_dict[sbl]['change_7d'] = ch7d
    new_dict[sbl]['market_cap'] = mkc
    new_dict[sbl]['market_dominance'] = mkd 

# print(new_dict)
 
    
with open('clean_crypto_data.json', 'w') as json_file:
     json.dump(new_dict, json_file, indent=2)

    


