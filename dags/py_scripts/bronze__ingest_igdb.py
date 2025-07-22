from py_scripts.tools import env_loader as env
from py_scripts.tools.pipeline import Pipeline
from py_scripts.tools.auth import Auth
from py_scripts.tools.query import Query
from py_scripts.tools.observe import Logger, Tracer, Meter
import time
from time import sleep
import pandas as pd
import requests
import numpy

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
def main():
    base_url = 'https://api.igdb.com/v4/'
    token_url = 'https://id.twitch.tv/oauth2/token'
    client_id = env.get_env('TWITCH_CLIENT_ID')
    client_secret = env.get_env('TWITCH_CLIENT_SECRET')
    pipeline = Pipeline(base_url=base_url, service_name='igdb')
    token_info = Auth.get_token(
        token_url = token_url,
        client_id = client_id,
        client_secret = client_secret
    )

    access_token = token_info[0]

    # theme 42 = erotic, not interested
    # game types 3 = bundles, 5 = mods, 12 = forks
    base_body_query = """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """

    endpoints = [
        'games',
        'popularity_types',
        'popularity_primitives',
        'companies',
        # 'company_logos',
        # 'covers',
        'game_engines',
        'game_modes',
        'game_statuses',
        'game_time_to_beats',
        'game_types',
        'genres',
        'keywords',
        'platforms'
    ]


    # fix covers and company logos

    body_queries = [
    """
        fields 
            name, 
            id, 
            cover.image_id, 
            created_at, 
            game_engines, 
            game_modes, 
            genres, 
            themes,
            involved_companies, 
            parent_game, 
            platforms, 
            player_perspectives, 
            release_dates.date,
            release_dates.region,
            release_dates.platform, 
            slug, 
            summary,
            themes, 
            updated_at, 
            url, 
            game_type, 
            similar_games,
            franchise, 
            multiplayer_modes, 
            total_rating, 
            rating, 
            rating_count, 
            game_status, 
            age_ratings; 
        where 
            release_dates.date >= 1735689600 &
            updated_at > {last_updated} & 
            game_type != 3 &
            game_type != 5 &
            game_type != 12 &
            themes != 42; 
        limit 
            {batch_size};
        offset 
            {offset};
        sort id asc;""",
    # """ 
    # fields 
    #     *;
    # where 
    #     updated_at > {last_updated};
    # limit 
    #     {batch_size};
    # offset 
    #     {offset};
    #     """,
    # """ 
    # fields 
    #     *;
    # where 
    #     updated_at > {last_updated};
    # limit 
    #     {batch_size};
    # offset 
    #     {offset};
    #     """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """,
    """ 
    fields 
        *;
    where 
        updated_at > {last_updated};
    limit 
        {batch_size};
    offset 
        {offset};
        """
    ]

    paired = zip(endpoints, body_queries)


    batch_size = 500
    for endpoint, body_query_template in paired:
        all_results = []
        offset = 0
        # last_updated = 0 
        q = Query(
        database_platform='mongodb',
        database_name='igdb',
        data_store=f'{endpoint}_tracking'
        )
        last_updated_doc = q.collection.find_one(sort=[("last_updated", -1)])
        last_updated = last_updated_doc['last_updated'] if last_updated_doc else 0
        
        while True:
            body_query = body_query_template.format(batch_size=batch_size, offset=offset, last_updated=last_updated)
            data = pipeline.extract(
                endpoint=f'{endpoint}',
                client_id=client_id,
                token=access_token,
                body_query=f'{body_query}'
            )
            # No more results, stop loop
            if not data:
                break
            all_results.extend(data)  # Flatten into single list
            offset += batch_size  # Move to next page
            sleep(0.1) # Temporary until asyncio incorporation
    if all_results != []:
        pipeline.load(
            database_platform='mongodb',
            database_name='igdb',
            data_store=f'{endpoint}',
            data=all_results
        )
        pipeline.load(
            database_platform='mongodb', 
            database_name='igdb',
            data_store=f'{endpoint}_tracking',
            data={'last_updated': int(time.time())}
        )
if __name__ == '__main__':
    main()