from dotenv import load_dotenv
from tools.extract_load import Pipeline
import pandas as pd
import requests
import numpy
import os

load_dotenv()
base_url = 'https://id.twitch.tv'
pipeline = Pipeline(base_url)

token = pipeline.auth(
    token_endpoint='/oauth2/token',
    client_id = os.getenv('TWITCH_CLIENT_ID'),
    client_secret = os.getenv('TWITCH_CLIENT_SECRET')
)

print(token)



# extract the raw json returns from IGDB API. https://api-docs.igdb.com
