import requests
from typing import Any

class Auth:
    @staticmethod
    def get_token(token_url: str, client_id: str, client_secret: str) -> tuple:
        url = token_url
        params = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials'
        }
        r = requests.post(
            url=url,
            params=params
        )
        if r.status_code == 200:
            data = r.json()
            access_token = data.get('access_token')
            expires_in = str(data.get('expires_in') / 60 / 60) + ' hours'  # Converting seconds to minutes
            token_type = data.get('token_type')
            return access_token, expires_in, token_type
        else:
            raise Exception('Did not receive authentication token.')