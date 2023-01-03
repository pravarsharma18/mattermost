from flask import Flask, request
from decouple import config
import requests
from typing import Tuple
from sql import ZohoSqlClient
from utils import remove_punctions
from datetime import datetime
import time
import json


app = Flask(__name__)


@app.route("/")
def index():
    code = request.args.get("code")
    state = request.args.get("state")
    print(state, code)
    zoho_client = ZohoApiClient(state, code)
    return "<p>Syncing in progress</p>"


class ZohoApiClient:
    zoho_chat_base_url = "https://cliq.zoho.in/"

    def __init__(self, state, code) -> None:
        self.state = state
        self.code = code
        self.token_data = None

        self.get_auth_token()
        self.get_personal_chats()
    
    def get_timestamp_from_date(self, date) -> int:
        if date != 'NaN':
            date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
            tuple = date.timetuple()
            return int(time.mktime(tuple) * 1000)
        else:
            return int(time.time() * 1000)

    def get_auth_token(self) -> None:
        base_url = "https://accounts.zoho.in/oauth/v2/token"
        grant_type = "authorization_code"
        scope = config('SCOPE')
        client_id = config('CLIENT_ID')
        client_secret = config('CLIENT_SECRET')
        redirect_url = config('REDIRECT_URL')
        url = f"{base_url}?grant_type={grant_type}&client_id={client_id}&client_secret={client_secret}&redirect_uri={redirect_url}&code={self.code}"
        
        new_request = requests.post(url)
        self.token_data = new_request.json()

        ZohoSqlClient.sql_post(table_name="tokens", attrs=["token"], values=[json.dumps(self.token_data)])
        print(self.token_data)

        access_token = self.token_data.get('access_token')
        print(f"Access token is ... {access_token}")
    
    def get_chat_api(self, path) -> Tuple[int, dict]:
        url = f"{self.zoho_chat_base_url}{path}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {self.token_data.get('access_token')}",
            "Content-Type": 'application/json'
        }
        r = requests.get(url, headers=headers)
        return r.status_code, r

    def get_personal_chats(self) -> None:
        a = True
        modified_before=""
        chat_count = 1
        
        while a:
            url = "api/v2/chats?limit=100"
            if modified_before:
                url += f"&modified_before={modified_before}"
            
            s, response_chats = self.get_chat_api(url)
            chats = response_chats.json().get("chats", [])

            if not chats or (modified_before and len(chats) <= 1) or s != 200:
                a = False
                continue

            for chat in chats:
                modified_before = self.get_timestamp_from_date(chat["last_modified_time"])
                try:
                    user_email = ZohoSqlClient.sql_get("cliq_users", "email_id", f"id='{chat['creator_id']}'")
                    data = {
                        "recipients_summary": "[]",
                        "title": remove_punctions(chat["name"]),
                        "chat_id": chat["chat_id"],
                        "participant_count": chat["participant_count"] or 0,
                        "total_message_count": 1,
                        "creator_id": user_email[0].get("email_id", ""),
                        "creation_time": self.get_timestamp_from_date(chat["creation_time"]),
                        "last_modified_time": self.get_timestamp_from_date(chat["last_modified_time"]),
                    }

                    recipient_emails = []
                    for recipient in chat["recipients_summary"]:
                        try:
                            user_email_id = ZohoSqlClient.sql_get("cliq_users", "email_id", f"id='{recipient['user_id']}'")
                            recipient_emails.append(user_email_id[0].get("email_id", ""))
                        except Exception as e:
                            print(f"User {recipient['user_id']}:{recipient['name']} not found for chat {chat['chat_id']}")
                    
                    if len(recipient_emails) <= 1:
                        continue

                    data.update({"recipients_summary": json.dumps(list(set(recipient_emails)))})

                    db_cliq_chats = ZohoSqlClient.sql_get("cliq_chats", "chat_id", f"chat_id='{chat['chat_id']}'")
                    if db_cliq_chats:
                        if db_cliq_chats[0]['chat_id'] != chat['chat_id']:
                            ZohoSqlClient.sql_post(
                                table_name="cliq_chats", attrs=list(data.keys()), values=list(data.values()))
                        else:
                            set_value = []
                            for key, value in data.items():
                                set_value.append(f"{key} = '{value}'")
                            set_value = ",".join(set_value)
                            ZohoSqlClient.sql_update(table_name="cliq_chats", set=set_value, where=f"chat_id = '{chat['chat_id']}'")
                    else:
                        ZohoSqlClient.sql_post(
                            table_name="cliq_chats", attrs=list(data.keys()), values=list(data.values()))

                except Exception as e:
                    print(f"Exception in getting chat Members from api : {chat['chat_id']}")
                    print(e)


