#This runs using Python 3.9.7 64-bit ('base':conda)
import boto3
from airflow import DAG  
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.S3_hook import S3Hook 

import json
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from botocore.exceptions import ClientError
import logging
from collections import defaultdict



s3 = boto3.client('s3')
ssm = boto3.client('ssm', 'eu-west-2')
sns_client = boto3.client('sns', 'eu-west-2')
logger = logging.getLogger(__name__)

bucket_name_r = ssm.get_parameter( Name='raw_data_bucket_name',WithDecryption=False)['Parameter']['Value'] # this allows to have easy management of secrets/configuration-parameters in a centralised and secure manner
file_name_r = ssm.get_parameter( Name='raw_data_file_name',WithDecryption=False)['Parameter']['Value']
bucket_name_c = ssm.get_parameter( Name='clean_data_bucket_name',WithDecryption=False)['Parameter']['Value'] # this allows to have easy management of secrets/configuration-parameters in a centralised and secure manner
file_name_c = ssm.get_parameter( Name='clean_data_file_name',WithDecryption=False)['Parameter']['Value']
api_key = ssm.get_parameter( Name='api_key',WithDecryption=False)['Parameter']['Value']




def raw_data_extract(ti):

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest' #the Coin-Market-Cap API url 
    parameters = {
        'limit':'350', #it limits the query to the first 350 rows 
        'convert':'GBP',
        'tag':'all'
        }
    headers = {
        'Accepts': 'application/json', #its a way of informing the api that i want the data in json format 
        'X-CMC_PRO_API_KEY':api_key, #This line of code protects the API-key as it holds it in a centralised parameter store
        }

    session = Session()
    session.headers.update(headers) #this allows that every time a request is made everything works smoothly with the right credentials 
    
    #The code bellows initiates the session and requests 
    try:
        response = session.get(url, params=parameters) #it innitiates the session and uses the parameters to filter the data 
        raw_data = json.loads(response.text) #it transfroms the retrieved data into json format 
        #pprint.pprint(data)
        #print(data)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)

    raw_data= json.dumps(raw_data,indent=2)

    ti.xcom_push(key='raw_data', value= raw_data) #loads data into Meta database with a specific key

def raw_data_s3(ti):

    raw_data_2= json.loads(ti.xcom_pull(key='raw_data', task_ids='raw_data_extract_task' ))
    
    hook= S3Hook()
    s3c= hook.get_conn()
    resp=s3c.get_object(Bucket=bucket_name_r, Key=file_name_r) #it dowloads the data (object)
    s3_data_raw=resp.get('Body').read()  #reads the body of the object (data)
    json_data= json.loads(s3_data_raw) #loads data into a json object
    json_data= [json_data, raw_data_2] 
    s3.put_object(Bucket=bucket_name_r, Key=file_name_r, Body=json.dumps(json_data, indent=4).encode())
    
def transform_raw_data(ti):
    # Fetching raw_data JSON file
    data= json.loads(ti.xcom_pull(key='raw_data', task_ids='raw_data_extract_task'))

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

    clean_data= json.dumps(new_dict, indent=2) #sets data back into JSON
    return clean_data

def load_clean_data_to_s3(ti):

    clean_data= json.loads(ti.xcom_pull(task_ids='transform_raw_data_task'))

    hook= S3Hook()
    s3c= hook.get_conn()
    resp=s3c.get_object(Bucket=bucket_name_c, Key=file_name_c) #it dowloads the data (object)
    s3_data_clean=resp.get('Body').read()  #reads the body of the object (data)
    json_data= json.loads(s3_data_clean) #loads data into a json object
    json_data= [json_data, clean_data] 
    s3.put_object(Bucket=bucket_name_c, Key=file_name_c, Body=json.dumps(json_data, indent=2).encode())


def publish_text_message():

    try:
        sns_client.publish(
            TopicArn= "arn:aws:sns:eu-west-2:534799599157:send-text-D2",
            Message="Hi Rui, today's ETL-D2 was a success!"
            )

    except ClientError:
        logger.exception("Couldn't publish message")
        raise
    else:
        return "Something went wrong"


#Creation of the DAG Object
with DAG(
 
    'etl_s3', #unique id
    start_date= datetime(year=2022, month=3, day=17),
    schedule_interval= '15 20 * 03 *', #how often the job needs to run
    catchup= False #no need to catch up from start date till now, so it gets set to false 

) as dag:
    #1. Sets the parameters and access details for API data collection
    task_raw_data_extract= PythonOperator( 
        task_id= 'raw_data_extract_task',
        python_callable= raw_data_extract
    )
    #2. Loads raw data into S3 
    task_raw_data_s3= PythonOperator( 
        task_id= 'raw_data_s3_task',
        python_callable= raw_data_s3
    )
    #3. Transforms the raw data
    task_transform_raw_data = PythonOperator( 
        task_id= 'transform_raw_data_task',
        python_callable= transform_raw_data
    )
    #4. Load clean data into S3
    task_load_clean_data_to_s3 = PythonOperator( 
        task_id= 'load_clean_data_to_s3_task',
        python_callable= load_clean_data_to_s3
    )
    #5. Notify admin
    task_sms_admin = PythonOperator( 
        task_id= 'sms_task',
        python_callable= publish_text_message
    )

    task_raw_data_extract >> task_raw_data_s3 >> task_transform_raw_data >> task_load_clean_data_to_s3 >> task_sms_admin

