import requests
from pprint import pprint
import json
from colorama import Fore
from sql import ZohoSqlClient
import sys
from typing import Tuple
import time
import pandas as pd
import csv

from utils import create_new_column, remove_punctions


class ZohoClient:
    zoho_chat_base_url = "https://cliq.zoho.in/"
    access_token = "1000.484ef6b6012e622f139edbf7bbae66dc.d5af0a29c8daff768f959935a9d77b8b"

    def get_chat_api(self, path, header={}) -> Tuple[int, dict]:
        url = f"{self.zoho_chat_base_url}{path}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": 'application/json'
        }
        headers.update(header)
        r = requests.get(url, headers=headers)
        # print("******************", r)
        return r.status_code, r.text  # .json()

    def get_all_users(self):
        keys = ZohoSqlClient.get_columns("cliq_users")
        s, data = self.get_chat_api('api/v2/users')
        data = json.loads(data)
        users = []
        users.extend(data['data'])
        if data['has_more']:
            while data['has_more']:
                s, data = self.get_chat_api(
                    f'api/v2/users?next_token={data["next_token"]}')
                data = json.loads(data)
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
            ZohoSqlClient.sql_post(
                table_name="cliq_users", attrs=keys, values=li)
        print(Fore.GREEN + "Users Inserted")

    def bulk_channels(self):
        keys = ZohoSqlClient.get_columns("cliq_channels")
        s, data = self.get_chat_api(
            'maintenanceapi/v2/channels?fields=name,channel_id,total_message_count,participant_count,creation_time,description,creator_id', header={"Content-Type": 'text/csv'})
        # print(s)
        with open("channels.csv", 'w') as file:
            file.writelines(data)

        with open('channels.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                ZohoSqlClient.sql_post(
                    table_name="cliq_channels", attrs=keys, values=row)
        print(Fore.GREEN + "Channels Inserted")

    def get_channel_members(self):
        keys = ZohoSqlClient.get_columns('cliq_channel_members')
        channels = ZohoSqlClient.sql_get('cliq_channels', 'channel_id')
        for channel in channels:
            s, data = self.get_chat_api(
                f"api/v2/channels/{channel['channel_id']}/members")
            members = json.loads(data)['members']
            for member in members:
                values = list(member.values())
                values.append(channel['channel_id'])
                ZohoSqlClient.sql_post(
                    table_name='cliq_channel_members', attrs=keys, values=values)
        print(Fore.GREEN + "Channel Members Saved")

    def bulk_conversation(self):
        keys = ZohoSqlClient.get_columns("cliq_chats")
        # s, data = self.get_chat_api(
        #     'maintenanceapi/v2/chats?fields=title,chat_id,participant_count,total_message_count,creator_id,creation_time,last_modified_time', header={"Content-Type": 'text/csv'})
        s, data = self.get_chat_api('api/v2/chats')

        chats = json.loads(data)['chats']
        # to remove spaces and add '.' as mattermost username doesnot support spaces for username
        df = pd.DataFrame(chats)
        df['name'] = df['name'].apply(lambda x: remove_punctions(x))
        chats = df.to_dict('records')

        for chat in chats:
            keys = list(chat.keys())
            v = [i for i in chat.values()]
            values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                v)]
            ZohoSqlClient.sql_post(
                table_name="cliq_chats", attrs=keys, values=values)
        print(Fore.GREEN + "Conversations Inserted")

    def bulk_messages(self):
        # keys = ZohoSqlClient.get_columns("cliq_messages")
        chat_ids = ZohoSqlClient.sql_get('cliq_chats', 'chat_id')
        for chat_id in chat_ids:
            s, data = self.get_chat_api(
                f'api/v2/chats/{chat_id["chat_id"]}/messages')  # maintenance
            messages = json.loads(data)
            # pprint(messages['data'])
            # print()
            for data in messages['data']:
                keys = list(data.keys())
                columns = ZohoSqlClient.get_columns("cliq_messages")
                # Alter table columns as per field from api.
                create_new_column(keys, columns, 'cliq_messages')
                values = list(data.values())
                data_values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                    values)]
                data_values.append(chat_id["chat_id"])
                keys.append('chat_id')
                ZohoSqlClient.sql_post(
                    table_name='cliq_messages', attrs=keys, values=data_values)
        print(Fore.GREEN + "Messages Inserted")

    def get_files(self):
        s, data = self.get_chat_api(
            f'api/v2/files/ad4ea84b256b25fcb1777ef77fb61e620567162441817a5937c8f4837954ed346a01e229abdc62ead60fc12c966d2b762de2f4292e96abce103fde1d94a9eeff')
        print(data)

    def main(self):
        """
        portals and projects must be saved before others
        """
        time.sleep(1)
        self.get_all_users()
        time.sleep(1)
        self.bulk_channels()
        time.sleep(1)
        self.get_channel_members()
        time.sleep(1)
        self.bulk_conversation()
        time.sleep(1)
        self.bulk_messages()


if __name__ == '__main__':
    print(Fore.YELLOW + "\n<========Saving Zoho Data in DB=========>\n")
    ZohoClient().main()
    # ZohoClient().save_users_data()
    # ZohoClient().remove_duplicate_entries_from_user_data()
