import boto3
import json
from collections import defaultdict

s3 = boto3.client('s3')
ssm = boto3.client('ssm', 'eu-west-2')
bucket_name_r = ssm.get_parameter( Name='raw_data_bucket_name',WithDecryption=False)['Parameter']['Value'] # this allows to have easy management of secrets/configuration-parameters in a centralised and secure manner
file_name_r = ssm.get_parameter( Name='d1_raw_data_file_name',WithDecryption=False)['Parameter']['Value']
bucket_name_c = ssm.get_parameter( Name='clean_data_bucket_name',WithDecryption=False)['Parameter']['Value'] # this allows to have easy management of secrets/configuration-parameters in a centralised and secure manner
file_name_c = ssm.get_parameter( Name='d1_clean_data_file_name',WithDecryption=False)['Parameter']['Value']


def transform_data(event, context):
    
    #It gets the raw data object 
    resp=s3.get_object(Bucket=bucket_name_r, Key=file_name_r) #it dowloads the data (object)
    s3_data_raw=resp.get('Body').read()  #reads the body of the object (data)
    json_data_raw= json.loads(s3_data_raw) #loads data into a json object    

    #Creates a Dict
    new_dict= defaultdict(dict)

    #The loop below allows reshapes the data structure 
    for k in json_data_raw[1]['data']: #Allows to acces the object "data"
        
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

        new_clean_data= new_dict

    #It gets the clean data object 
    resp=s3.get_object(Bucket=bucket_name_c, Key=file_name_c) #it dowloads the data (object)
    s3_data_clean=resp.get('Body').read()  #reads the body of the object (data)
    json_data_clean = json.loads(s3_data_clean, ) #loads data into a json object
    json_data= [json_data_clean, new_clean_data] 
    s3.put_object(Bucket=bucket_name_c, Key=file_name_c, Body=json.dumps(json_data, indent=4).encode())


    return {
        "statusCode": 200,
        }
        


