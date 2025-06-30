from dotenv import load_dotenv
from tools.pipeline import Pipeline
from tools.auth import Auth
import pandas as pd
import requests
import numpy
import os

"""
    The purpose of this file is to extract game data since 2024 with releases in 2025 and up. This raw data 
    dumps into a MongoDB collection (bronze layer) and then flattens out into a PostgreSQL staging table (silver layer),
    finally the data will be modeled and ready for analytics using dbt into PostgreSQL production table (gold layer).
    The data will be displayed in a React dashboard on a locally hosted site. 

    There is a rate limit of 4 requests per second.
    If you go over this limit you will receive a response with status code 429 Too Many Requests.
    
    You are able to have up to 8 open requests at any moment in time. (semaphore of 8)
    This can occur if requests take longer than 1 second to respond when multiple requests are being made.
"""
load_dotenv()
base_url = 'https://api.igdb.com/v4/'
token_url = 'https://id.twitch.tv/oauth2/token'
client_id = os.getenv('TWITCH_CLIENT_ID')
client_secret = os.getenv('TWITCH_CLIENT_SECRET')

pipeline = Pipeline(base_url)

token_info = Auth.get_token(
    token_url=token_url,
    client_id = client_id,
    client_secret = client_secret
)

access_token = token_info[0]

# extract for every endpoint you'll need.
# make a list of endpoints, for loop through it. Any way to do this better?
# USe asyncio for datadumping??
#
data = pipeline.extract(
    endpoint='games',
    client_id = client_id,
    token=access_token,
    body_query='fields *;'
)

pipeline.load(database_platform='mongodb',
         database_name='igbm',
         table_name='test_table',
         data=data)

# extract the raw json returns from IGDB API. https://api-docs.igdb.com
