#This runs using Python 3.8.3 64-bit ('base':conda)

import boto3
from requests import Request, Session
import json
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

s3 = boto3.client('s3')
ssm = boto3.client('ssm', 'eu-west-2')

api_key = ssm.get_parameter( Name='api_key',WithDecryption=False)['Parameter']['Value']
bucket_name_r = ssm.get_parameter( Name='raw_data_bucket_name',WithDecryption=False)['Parameter']['Value'] # this allows to have easy management of secrets/configuration-parameters in a centralised and secure manner
file_name_r = ssm.get_parameter( Name='d1_raw_data_file_name',WithDecryption=False)['Parameter']['Value']

def extract_data(event, context):

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest' #the Coin-Market-Cap API url 
    parameters = {
    'limit':'100', #it limits the query to the first 100 rows 
    'convert':'GBP',
    'tag':'all'
    }
    headers = {
    'Accepts': 'application/json', #its a way of informing the api that i want the data in json format 
    'X-CMC_PRO_API_KEY':api_key, #This bit of information should hold the api-key in the machine enviroment, but because it will be tested by a third party it needs to hard coded
    }

    session = Session()
    session.headers.update(headers) #this allows that every time a request is made everything works smoothly with the right credentials 

    #The code bellows initiates the session and requests 
    try:
        response = session.get(url, params=parameters) #it innitiates the session and uses the parameters to filter the data 
        raw_data = json.loads(response.text) #it transfroms the retrieved data into json format 
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
    

    resp=s3.get_object(Bucket=bucket_name_r, Key=file_name_r) #it dowloads the data (object)
    s3_data_raw=resp.get('Body').read()  #reads the body of the object (data)
    json_data= json.loads(s3_data_raw) #loads data into a json object
    json_data= [json_data, raw_data] 
    s3.put_object(Bucket=bucket_name_r, Key=file_name_r, Body=json.dumps(json_data, indent=4).encode())
    
    return {
        "statusCode": 200,
        "body": raw_data
        }


