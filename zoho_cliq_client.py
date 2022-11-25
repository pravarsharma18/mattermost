import requests
from pprint import pprint
import json
from colorama import Fore
from sql import ZohoSqlClient
from typing import Tuple
import time
import pandas as pd
import csv
import sys
from utils import create_new_column, remove_punctions
from decouple import config


class ZohoClient:
    zoho_chat_base_url = "https://cliq.zoho.in/"
    access_token = config('ZOHO_CLIQ_API_KEY')

    def get_chat_api(self, path, header={}) -> Tuple[int, dict]:
        url = f"{self.zoho_chat_base_url}{path}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": 'application/json'
        }
        headers.update(header)
        r = requests.get(url, headers=headers)
        # print("******************", r)
        if headers.get("Content-Type") == 'text/csv':
            return r.status_code, r.text
        else:
            return r.status_code, r.json()

    def get_all_users(self):
        keys = ZohoSqlClient.get_columns("cliq_users")
        s, data = self.get_chat_api('api/v2/users')
        users = []
        users.extend(data['data'])
        db_users = ZohoSqlClient.sql_get("cliq_users", "email_id")
        try:
            db_users = [user['email_id'] for user in db_users]
        except:
            db_users = None
        if data['has_more']:
            while data['has_more']:
                s, data = self.get_chat_api(
                    f'api/v2/users?next_token={data["next_token"]}')
                users.extend(data['data'])

                if not data['has_more']:
                    break
        # to remove spaces and add '.' as mattermost username doesnot support spaces for username
        df = pd.DataFrame(users)
        df['name'] = df['name'].apply(lambda x: remove_punctions(x))
        df['display_name'] = df['display_name'].apply(
            lambda x: remove_punctions(x))
        users = df.to_dict('records')
        for user in users:
            values = user.values()
            li = [i for i in values]
            if db_users:
                if user['email_id'] not in db_users:
                    ZohoSqlClient.sql_post(
                        table_name="cliq_users", attrs=keys, values=li)
            else:
                ZohoSqlClient.sql_post(
                        table_name="cliq_users", attrs=keys, values=li)
        print(Fore.GREEN + "Users Inserted")

    def bulk_channels(self):
        db_channels = ZohoSqlClient.sql_get("cliq_channels", "channel_id")
        try:
            db_channels = [channel['channel_id'] for channel in db_channels]
        except:
            db_channels = None
        keys = ZohoSqlClient.get_columns("cliq_channels")
        s, data = self.get_chat_api(
            'maintenanceapi/v2/channels?fields=name,channel_id,total_message_count,participant_count,creation_time,description,creator_id', header={"Content-Type": 'text/csv'})
        # print(s)
        with open("channels.csv", 'w') as file:
            file.writelines(data)

        df = pd.DataFrame(pd.read_csv("channels.csv"))
        channels = df.to_dict("records")
        for channel in channels:
            row = list(channel.values())
            if db_channels:
                if channel['channel_id'] not in db_channels:
                    ZohoSqlClient.sql_post(
                        table_name="cliq_channels", attrs=keys, values=row)
            else:
                ZohoSqlClient.sql_post(
                    table_name="cliq_channels", attrs=keys, values=row)

        print(Fore.GREEN + "Channels Inserted")

    def get_channel_members(self):
        keys = ZohoSqlClient.get_columns('cliq_channel_members')
        channels = ZohoSqlClient.sql_get('cliq_channels', 'channel_id')
        db_channel_members = ZohoSqlClient.sql_get("cliq_channel_members", "email,channel_id")
        try:
            db_channel_members = [f"{mem['email']}__{mem['channel_id']}" for mem in db_channel_members]
        except:
            db_channel_members = None
        for channel in channels:
            s, data = self.get_chat_api(
                f"api/v2/channels/{channel['channel_id']}/members")
    
            members = data['members']
            for member in members:
                values = list(member.values())
                values.append(channel['channel_id'])
                if db_channel_members:
                    if (f"{member['email_id']}__{channel['channel_id']}") not in db_channel_members:
                        ZohoSqlClient.sql_post(
                            table_name='cliq_channel_members', attrs=keys, values=values)
                else:
                    ZohoSqlClient.sql_post(
                            table_name='cliq_channel_members', attrs=keys, values=values)
        print(Fore.GREEN + "Channel Members Saved")

    def bulk_conversation(self):
        keys = ZohoSqlClient.get_columns("cliq_chats")
        db_cliq_chats = ZohoSqlClient.sql_get("cliq_chats", "chat_id")
        try:
            db_cliq_chats = [chat['chat_id'] for chat in db_cliq_chats]
        except:
            db_cliq_chats = None
        s, data = self.get_chat_api(
            'maintenanceapi/v2/chats?fields=title,chat_id,participant_count,total_message_count,creator_id,creation_time,last_modified_time', header={"Content-Type": 'text/csv'})
        # s, data = self.get_chat_api(
        #     'api/v2/chats')

        with open('chats.csv', 'w') as f:
            f.writelines(data)

        chats = pd.read_csv('chats.csv')
        # to remove spaces and add '.' as mattermost username doesnot support spaces for username
        df = pd.DataFrame(chats)
        df['title'] = df['title'].apply(lambda x: remove_punctions(x))
        df['last_modified_time'] = df['last_modified_time'].astype('object')
        df['last_modified_time'] = df['last_modified_time'].fillna(0)
        df['last_modified_time'] = df['last_modified_time'].astype('int')
        chats = df.to_dict('records')
        for chat in chats:
            s, chat_members = self.get_chat_api(
                f'maintenanceapi/v2/chats/{chat["chat_id"]}/members?fields=email_id', header={"Content-Type": 'text/csv'})
            v = [i for i in chat.values()]
            values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                v)]
            values.append(json.dumps(chat_members.split()[1:]))
            if db_cliq_chats:
                if chat['chat_id'] not in db_cliq_chats:
                    ZohoSqlClient.sql_post(
                        table_name="cliq_chats", attrs=keys, values=values)
            else:
                ZohoSqlClient.sql_post(
                    table_name="cliq_chats", attrs=keys, values=values)
        print(Fore.GREEN + "Conversations Inserted")

    def bulk_messages(self):
        # keys = ZohoSqlClient.get_columns("cliq_messages")
        chat_ids = ZohoSqlClient.sql_get('cliq_chats', 'chat_id')
        db_cliq_messages = ZohoSqlClient.sql_get("cliq_messages", "id")
        try:
            db_cliq_messages = [chat['id'] for chat in db_cliq_messages]
        except:
            db_cliq_messages = None
        for chat_id in chat_ids:
            s, messages = self.get_chat_api(
                f'maintenanceapi/v2/chats/{chat_id["chat_id"]}/messages')  # maintenance
            try:  # some data has no chats, to eliminate that error
                for data in messages:
                    keys = list(data.keys())
                    columns = ZohoSqlClient.get_columns("cliq_messages")
                    # Alter table columns as per field from api.
                    create_new_column(keys, columns, 'cliq_messages')
                    values = list(data.values())
                    data_values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                        values)]
                    data_values.append(chat_id["chat_id"])
                    keys.append('chat_id')
                    if db_cliq_messages:
                        if data['id'] not in db_cliq_messages:
                            ZohoSqlClient.sql_post(
                                table_name='cliq_messages', attrs=keys, values=data_values)
                    else:
                        ZohoSqlClient.sql_post(
                                table_name='cliq_messages', attrs=keys, values=data_values)
            except:
                pass
        print(Fore.GREEN + "Messages Inserted")

    def main(self):
        """
        portals and projects must be saved before others
        """
        self.get_all_users()
        # time.sleep(1)
        self.bulk_channels()
        # time.sleep(1)
        self.get_channel_members()
        # time.sleep(1)
        self.bulk_conversation()
        # time.sleep(1)
        self.bulk_messages()


if __name__ == '__main__':
    print(Fore.YELLOW + "\n<========Saving Zoho Data in DB=========>\n")
    ZohoClient().main()
    # ZohoClient().save_users_data()
    # ZohoClient().remove_duplicate_entries_from_user_data()
