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
from utils import create_new_column, remove_punctions, save_logs
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
        try:
            s, data = self.get_chat_api('api/v2/users')
            users = []
            users.extend(data['data'])
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
                keys = list(user.keys())
                columns = ZohoSqlClient.get_columns("cliq_users")
                # Alter table columns as per field from api.
                create_new_column(keys, columns, "cliq_users")
                db_users = ZohoSqlClient.sql_get("cliq_users", "email_id", f"email_id='{user['email_id']}'")
                values = user.values()
                li = [i for i in values]
                if db_users:
                    if db_users[0]['email_id'] != user['email_id']:
                        ZohoSqlClient.sql_post(
                            table_name="cliq_users", attrs=keys, values=li)
                else:
                    ZohoSqlClient.sql_post(
                            table_name="cliq_users", attrs=keys, values=li)
            print(Fore.GREEN + "Users Inserted")
        except Exception as e:
            save_logs()

    def bulk_channels(self):
        try:
            db_channels = ZohoSqlClient.sql_get("cliq_channels", "channel_id")
            try:
                db_channels = [channel['channel_id'] for channel in db_channels]
            except:
                db_channels = None
            s, data = self.get_chat_api(
                'maintenanceapi/v2/channels?fields=name,channel_id,total_message_count,participant_count,creation_time,description,creator_id', header={"Content-Type": 'text/csv'})
            # print(s)
            with open("channels.csv", 'w') as file:
                file.writelines(data)

            df = pd.DataFrame(pd.read_csv("channels.csv"))
            channels = df.to_dict("records")
            for channel in channels:
                keys = list(channel.keys())
                columns = ZohoSqlClient.get_columns("cliq_channels")
                # Alter table columns as per field from api.
                create_new_column(keys, columns, "cliq_channels")

                db_channels = ZohoSqlClient.sql_get("cliq_channels", "channel_id", f"channel_id='{channel['channel_id']}'")
                row = list(channel.values())
                if db_channels:
                    if db_channels[0]['channel_id'] != channel['channel_id']:
                        ZohoSqlClient.sql_post(
                            table_name="cliq_channels", attrs=keys, values=row)
                else:
                    ZohoSqlClient.sql_post(
                        table_name="cliq_channels", attrs=keys, values=row)

            print(Fore.GREEN + "Channels Inserted")
        except Exception as e:
            save_logs()

    def get_channel_members(self):
        try:
            keys = ZohoSqlClient.get_columns('cliq_channel_members')
            channels = ZohoSqlClient.sql_get('cliq_channels', 'channel_id')
            for channel in channels:
                s, data = self.get_chat_api(
                    f"api/v2/channels/{channel['channel_id']}/members")
        
                members = data['members']
                for member in members:
                    db_channel_members = ZohoSqlClient.sql_get("cliq_channel_members", "email,channel_id", f"email='{member['email_id']}' and channel_id='{channel['channel_id']}'")
                    values = list(member.values())

                    keys = list(member.keys())
                    columns = ZohoSqlClient.get_columns("cliq_channel_members")
                    # Alter table columns as per field from api.
                    create_new_column(keys, columns, "cliq_channel_members")
                    
                    li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                            values)]
                    li.insert(0,channel['channel_id'])
                    keys.insert(0, "channel_id")
                    if db_channel_members:
                        if db_channel_members[0]['email'] != member['email_id'] and db_channel_members[0]['channel_id'] != channel['channel_id']:
                            ZohoSqlClient.sql_post(
                                table_name='cliq_channel_members', attrs=keys, values=li)
                    else:
                        ZohoSqlClient.sql_post(
                                table_name='cliq_channel_members', attrs=keys, values=li)
            print(Fore.GREEN + "Channel Members Saved")
        except Exception as e:
            save_logs()

    def bulk_conversation(self):
        try:
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
                keys = list(chat.keys())
                columns = ZohoSqlClient.get_columns("cliq_chats")
                # Alter table columns as per field from api.
                create_new_column(keys, columns, "cliq_chats")
                db_cliq_chats = ZohoSqlClient.sql_get("cliq_chats", "chat_id", f"chat_id='{chat['chat_id']}'")
                s, chat_members = self.get_chat_api(
                    f'maintenanceapi/v2/chats/{chat["chat_id"]}/members?fields=email_id', header={"Content-Type": 'text/csv'})
                v = [i for i in chat.values()]
                values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                    v)]
                values.insert(0, json.dumps(chat_members.split()[1:]))
                keys.insert(0, "recipients_summary")
                if db_cliq_chats:
                    if db_cliq_chats[0]['chat_id'] != chat['chat_id']:
                        ZohoSqlClient.sql_post(
                            table_name="cliq_chats", attrs=keys, values=values)
                else:
                    ZohoSqlClient.sql_post(
                        table_name="cliq_chats", attrs=keys, values=values)
            print(Fore.GREEN + "Conversations Inserted")
        except Exception as e:
            save_logs()

    def bulk_messages(self):
        try:
            # keys = ZohoSqlClient.get_columns("cliq_messages")
            chat_ids = ZohoSqlClient.sql_get('cliq_chats', 'chat_id')
            for chat_id in chat_ids:
                s, messages = self.get_chat_api(
                    f'maintenanceapi/v2/chats/{chat_id["chat_id"]}/messages')  # maintenance
                try:  # some data has no chats, to eliminate that error
                    for data in messages:
                        db_cliq_messages = ZohoSqlClient.sql_get("cliq_messages", "id", f"id='{data['id']}'")
                        keys = list(data.keys())
                        columns = ZohoSqlClient.get_columns("cliq_messages")
                        # Alter table columns as per field from api.
                        create_new_column(keys, columns, 'cliq_messages')
                        

                        values = list(data.values())
                        data_values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                            values)]
                        data_values.insert(0, chat_id["chat_id"])
                        keys.insert(0, "chat_id")
                        
                        if db_cliq_messages:
                            if db_cliq_messages[0]['id'] != data['id']:
                                ZohoSqlClient.sql_post(
                                    table_name='cliq_messages', attrs=keys, values=data_values)
                        else:
                            ZohoSqlClient.sql_post(
                                    table_name='cliq_messages', attrs=keys, values=data_values)
                except:
                    pass
            print(Fore.GREEN + "Messages Inserted")
        except Exception as e:
            save_logs()

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
