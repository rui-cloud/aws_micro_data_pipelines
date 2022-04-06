#This runs using Python 3.8.3 64-bit ('base':conda)

from requests import Request, Session
import json
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
#import pprint
from datetime import datetime


url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest' #the Coin-Market-Cap API url 
parameters = {
  'limit':'1', #it limits the query to the first 100 rows 
  'convert':'GBP',
  'tag':'all'
}
headers = {
  'Accepts': 'application/json', #its a way of informing the api that i want the data in json format 
  'X-CMC_PRO_API_KEY':'2af2396a-ecfd-44cf-8df5-762b8ea648c9', #This bit of information should hold the api-key in the machine enviroment, but because it will be tested by a third party it needs to hard coded
}

session = Session()
session.headers.update(headers) #this allows that every time a request is made everything works smoothly with the right credentials 


#The code bellows initiates the session and requests 
try:
  response = session.get(url, params=parameters) #it innitiates the session and uses the parameters to filter the data 
  data = json.loads(response.text) #it transfroms the retrieved data into json format 
  #pprint.pprint(data)
  #print(data)
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)


#The block of code below saves the data into a json-file with the current Date & time the task was concluded as a tittle 
# file_name = datetime.strftime(datetime.now(), '%d-%m-%YT%H:%M:%S')
# with open('crypto_'+file_name+'.json', 'w') as json_file: #this will save the file in your default directory 
#     json.dump(data, json_file)

file_name = datetime.strftime(datetime.now(), '%d-%m-%YT%H:%M:%S')
with open('raw_crypto_data.json', 'w') as json_file: #this will save the file in your default directory 
    json.dump(data, json_file)