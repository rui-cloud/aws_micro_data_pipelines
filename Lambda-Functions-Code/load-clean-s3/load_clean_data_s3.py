import boto3
import json
from collections import defaultdict

s3 = boto3.client('s3')
ssm = boto3.client('ssm', 'eu-west-2')
bucket_name_c = ssm.get_parameter( Name='clean_data_bucket_name',WithDecryption=False)['Parameter']['Value'] # this allows to have easy management of secrets/configuration-parameters in a centralised and secure manner
file_name_c = ssm.get_parameter( Name='d1_clean_data_file_name',WithDecryption=False)['Parameter']['Value']

def load_clean_data_s3 (event, context):
    
    data= event['body']
    
    resp=s3.get_object(Bucket=bucket_name_c, Key=file_name_c) #it dowloads the data (object)
    s3_data_clean=resp.get('Body').read()  #reads the body of the object (data)
    json_data = json.loads(s3_data_clean, ) #loads data into a json object
    json_data= [json_data, data] 
    s3.put_object(Bucket=bucket_name_c, Key=file_name_c, Body=json.dumps(json_data, indent=4).encode())

    return {
        'status': 200,
        'body': data
            }
        