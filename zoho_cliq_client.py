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


class ZohoClient:
    zoho_chat_base_url = "https://cliq.zoho.in/"
    access_token = "1000.8223d49d62fe11f6809e2adeafa0314f.e8d0a79395cc14f8b4df3222f0bf9abe"

    def get_chat_api(self, path, header={}) -> Tuple[int, dict]:
        url = f"{self.zoho_chat_base_url}{path}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": 'application/json'
        }
        headers.update(header)
        r = requests.get(url, headers=headers)
        print("******************", r)
        return r.status_code, r.text  # .json()

    def get_all_users(self):
        keys = ZohoSqlClient.get_columns("cliq_users")
        s, data = self.get_chat_api('api/v2/users')
        data = json.loads(data)
        print(type(data))
        users = []
        users.extend(data['data'])
        if data['has_more']:
            while data['has_more']:
                s, data = self.get_chat_api(
                    f'api/v2/users?next_token={data["next_token"]}')
                data = json.load(data)
                users.extend(data['data'])

                if not data['has_more']:
                    break
        for user in users:
            values = user.values()
            li = [i for i in values]
            ZohoSqlClient.sql_post(
                table_name="cliq_users", attrs=keys, values=li)
        print(Fore.GREEN + "Users Inserted")

    def bulk_channels(self):
        time.sleep(1)
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
        time.sleep(1)
        keys = ZohoSqlClient.get_columns("cliq_chats")
        s, data = self.get_chat_api(
            'maintenanceapi/v2/chats?fields=title,chat_id,participant_count,total_message_count,creator_id,creation_time,last_modified_time', header={"Content-Type": 'text/csv'})
        # print(s)
        # print(data)
        with open("chats.csv", 'w') as file:
            file.writelines(data)

        with open('chats.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                ZohoSqlClient.sql_post(
                    table_name="cliq_chats", attrs=keys, values=row)
        print(Fore.GREEN + "Conversations Inserted")

    def bulk_messages(self):
        time.sleep(1)
        # keys = ZohoSqlClient.get_columns("cliq_messages")
        chat_ids = ZohoSqlClient.sql_get('cliq_chats', 'chat_id')
        for chat_id in chat_ids:
            s, data = self.get_chat_api(
                f'maintenanceapi/v2/chats/{chat_id["chat_id"]}/messages')
            datas = json.loads(data)
            for data in datas:
                keys = list(data.keys())
                columns = ZohoSqlClient.get_columns("cliq_messages")
                new_columns = [i for i in keys if i not in columns]
                if new_columns:  # Alter table columns as per field from api.
                    for column in new_columns:
                        ZohoSqlClient.update_table_column(
                            "cliq_messages", f"{column}")
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
        self.get_all_users()
        self.bulk_channels()
        self.bulk_conversation()
        self.bulk_messages()


if __name__ == '__main__':
    print(Fore.YELLOW + "\n<========Saving Zoho Data in DB=========>\n")
    ZohoClient().get_channel_members()
    # ZohoClient().save_users_data()
    # ZohoClient().remove_duplicate_entries_from_user_data()
