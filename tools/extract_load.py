import requests
from typing import Any

class Pipeline:
    def __init__(self, base_url: str):
        self.base_url = base_url

    # Expecting a JSON object return.
    def extract(
        self,
        endpoint: str,
        client_id: str,
        token: str,
        data: str,
        # token_type: str
    ) -> dict[str, Any]:

        headers = {
            'Client-ID': client_id,
            'Authorization': f'Bearer {token}',
        }

        r = requests.post(
            url=self.base_url+endpoint,
            headers=headers,
            data=data
        )

        if r.json():
            return r.json()
        else:
            raise Exception('No data from that endpoint.')

# To do
    def ingest(self, database: str, table: str, user: str, password: str) -> bool:
        if database == 'postgres':
            # do some work
            return True
        elif database == 'mongodb':
            # do some work
            return True
        else:
            raise Exception('Correct database not entered.')