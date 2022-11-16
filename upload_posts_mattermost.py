import requests
import json
from pprint import pprint


class MattermostApiClient:
    def get_user_token(self):
        url = 'http://localhost:8065/api/v4/users/login'
        body = {
            "login_id": "pravar.sharma",
            "password": "Demo@1234"
        }

        r = requests.post(url, json=body)
        if r.status_code in range(200, 299):
            res = r.headers
            return res['Token']
        else:
            return {'error': r.json(), 'detail': "Something Went Wrong.", }

    def insert_chat_data():
        url = 'http://localhost:8065/api/v4/posts'
        body = {
            "channel_id": "sir51xkh4tri9rpsu8mh848d8h",
            "message": "new msg",
            "root_id": "",
            "file_ids": [
                ""
            ],
            "props": {}
        }

        r = requests.post(url, json=body)


if __name__ == '__main__':
    client = MattermostApiClient()
    print(client.get_user_token())
