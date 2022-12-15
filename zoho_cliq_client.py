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
from utils import check_token_revoke_cliq, create_new_column, remove_punctions, save_logs
from decouple import config


class ZohoClient:
    zoho_chat_base_url = "https://cliq.zoho.in/"
    access_token = config('ZOHO_CLIQ_API_KEY')
    token_field = "next_token"

    def get_chat_api(self, path, header={}) -> Tuple[int, dict]:
        url = f"{self.zoho_chat_base_url}{path}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": 'application/json'
        }
        headers.update(header)
        r = requests.get(url, headers=headers)
        # print("******************", url)
        if r.status_code == 401:
            new_access_token = check_token_revoke_cliq(r)
            self.access_token = new_access_token
            
            r.status_code, r  = self.get_chat_api(path)
            return r.status_code, r
        else:
            return r.status_code, r


    def get_all_users(self):
        s, cliq_users = self.get_chat_api('api/v2/users?fields=all')
        data = cliq_users.json()
        users = []
        users.extend(data['data'])
        if data['has_more']:
            while data['has_more']:
                s, cliq_users = self.get_chat_api(
                    f'api/v2/users?fields=all&next_token={data["next_token"]}')
                data = cliq_users.json()
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
            try:
                keys = list(user.keys())
                columns = ZohoSqlClient.get_columns("cliq_users")
                # Alter table columns as per field from api.
                create_new_column(keys, columns, "cliq_users")
                db_users = ZohoSqlClient.sql_get("cliq_users", "email_id", f"email_id='{user['email_id']}'")
                # li = [i for i in values]
                values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                        list(user.values()))]
                if db_users:
                    if db_users[0]['email_id'] != user['email_id']:
                        ZohoSqlClient.sql_post(
                            table_name="cliq_users", attrs=keys, values=values)
                else:
                    ZohoSqlClient.sql_post(
                            table_name="cliq_users", attrs=keys, values=values)
            except Exception as e:
                print(f"Exception in saving cliq_users: {user}")
                save_logs(e)
        print(Fore.GREEN + "Users Inserted")

    def bulk_channels(self):
        a = True
        while a:
            token = ""
            if not token:
                url = 'maintenanceapi/v2/channels?fields=name,channel_id,total_message_count,participant_count,creation_time,description,creator_id'
            else:
                url = f'maintenanceapi/v2/channels?fields=name,channel_id,total_message_count,participant_count,creation_time,description,creator_id?next_token={token}'
            s, channels = self.get_chat_api(url)
            
            data = channels.json()
            # print(channels)
            if data.get('channels'):
                for channel in data.get('channels'):
                    try:
                        keys = list(channel.keys())
                        columns = ZohoSqlClient.get_columns("cliq_channels")
                        # Alter table columns as per field from api.
                        create_new_column(keys, columns, "cliq_channels")

                        db_channels = ZohoSqlClient.sql_get("cliq_channels", "channel_id", f"channel_id='{channel['channel_id']}'")
                        row = list(channel.values())
                        values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                        row)]
                        if db_channels:
                            if db_channels[0]['channel_id'] != channel['channel_id']:
                                ZohoSqlClient.sql_post(
                                    table_name="cliq_channels", attrs=keys, values=values)
                        else:
                            ZohoSqlClient.sql_post(
                                table_name="cliq_channels", attrs=keys, values=values)
                    except Exception as e:
                        print(f"Exception in saving cliq_channel: {channel['name']}")
                        save_logs(e)
            if data.get('has_more'):
                token = data.get("next_token")
            else:
                a = False
                break
        print(Fore.GREEN + "Channels Inserted")

    def get_channel_members(self):
        keys = ZohoSqlClient.get_columns('cliq_channel_members')
        channels = ZohoSqlClient.sql_get('cliq_channels', 'channel_id')
        for channel in channels:
            try:
                s, member = self.get_chat_api(
                    f"api/v2/channels/{channel['channel_id']}/members")
                data = member.json()
                members = data.get('members')
                if members:
                    for member in members:
                        try:
                            values = list(member.values())

                            keys = list(member.keys())
                            columns = ZohoSqlClient.get_columns("cliq_channel_members")
                            # Alter table columns as per field from api.
                            create_new_column(keys, columns, "cliq_channel_members")
                            
                            li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                                    values)]
                            li.insert(0,channel['channel_id'])
                            keys.insert(0, "channel_id")
                            db_channel_members = ZohoSqlClient.sql_get("cliq_channel_members", "email_id,channel_id", f"email_id='{member['email_id']}' and channel_id='{channel['channel_id']}'")
                            if db_channel_members:
                                if db_channel_members[0]['email_id'] != member['email_id'] and db_channel_members[0]['channel_id'] != channel['channel_id']:
                                    ZohoSqlClient.sql_post(
                                        table_name='cliq_channel_members', attrs=keys, values=li)
                            else:
                                ZohoSqlClient.sql_post(
                                        table_name='cliq_channel_members', attrs=keys, values=li)
                        except Exception as e:
                            print(f"Exception in saving members: {member['name']}")
            except Exception as e:
                print(f"Exception in getting channel Members from api : {channel['channel_id']}")
                save_logs(e)
        print(Fore.GREEN + "Channel Members Saved")

    def bulk_conversation(self):
        a = True
        next_token=""
        while a:
            if next_token:
                chat_url = f'maintenanceapi/v2/chats?fields=title,chat_id,participant_count,total_message_count,creator_id,creation_time,last_modified_time&next_token={next_token}'
            else:
                chat_url = f'maintenanceapi/v2/chats?fields=title,chat_id,participant_count,total_message_count,creator_id,creation_time,last_modified_time'
            s, response_chats = self.get_chat_api(
                chat_url, header={"Content-Type": 'text/csv'})
            
            data = response_chats.text
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
            df['chat_id'] = df['chat_id'].fillna(0)
            df = df.fillna("")
            df = df.where(df['chat_id'] != 0)
            df = df.dropna(how='all')

            chats = df.to_dict('records')
            count = 1
            for chat in chats:
                try:
                    keys = list(chat.keys())
                    columns = ZohoSqlClient.get_columns("cliq_chats")
                    # Alter table columns as per field from api.
                    create_new_column(keys, columns, "cliq_chats")
                    db_cliq_chats = ZohoSqlClient.sql_get("cliq_chats", "chat_id", f"chat_id='{chat['chat_id']}'")
                    if count == 12:
                        count = 1
                        print("Members's Api is throttled, wait for 30 seconds....")
                        time.sleep(30)
                    s, response_chat_members = self.get_chat_api(
                        f'maintenanceapi/v2/chats/{chat["chat_id"]}/members?fields=email_id', header={"Content-Type": 'text/csv'})
                    
                    chat_members = response_chat_members.text
                    count += 1
                    v = [i for i in chat.values()]
                    values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                        v)]
                    if "html" in chat_members: 
                        values.insert(0, json.dumps([]))
                    else:
                        values.insert(0, json.dumps(chat_members.split()[1:]))
                    keys.insert(0, "recipients_summary")
                    if db_cliq_chats:
                        if db_cliq_chats[0]['chat_id'] != chat['chat_id']:
                            ZohoSqlClient.sql_post(
                                table_name="cliq_chats", attrs=keys, values=values)
                        else:
                            set_value = ""
                            for i in list(zip(keys, values)):
                                set_value += f"{i[0]} = '{i[1]}' ,"
                            set_value = set_value[:-1] # to remove last ','
                            ZohoSqlClient.sql_update(table_name="cliq_chats", set=set_value, where=f"chat_id = '{chat['chat_id']}'")
                    else:
                        ZohoSqlClient.sql_post(
                            table_name="cliq_chats", attrs=keys, values=values)
                except Exception as e:
                    print(f"Exception in getting channel Members from api : {chat['chat_id']}")
                    save_logs(e)

            with open("chats.csv", 'r') as file:
                tokn = file.readlines()[-1]
                if self.token_field in tokn:
                    next_token = tokn.split("=")[1]
                else:
                    print("chat inelse")
                    a = False
                    break
        print(Fore.GREEN + "Conversations Inserted")

    def bulk_messages(self):
            chat_ids = ZohoSqlClient.sql_get('cliq_chats', 'chat_id')
            count = 1
            a = True
            for chat_id in chat_ids:
                # message_url = f'maintenanceapi/v2/chats/{chat_id["chat_id"]}/messages'
                last_msg_time = ""
                a = True
                while a:
                    if last_msg_time:
                        message_url = f'maintenanceapi/v2/chats/{chat_id["chat_id"]}/messages?fromtime={last_msg_time}'
                    else:
                        message_url = f'maintenanceapi/v2/chats/{chat_id["chat_id"]}/messages'
                    print(message_url)
                    if count == 12:
                        count = 1
                        print("Maintenance Chat Api is throttled, wait for 30 seconds....")
                        time.sleep(30)
                    print("*****",count)
                    s, response_messages = self.get_chat_api(message_url)  # maintenance
                    messages = response_messages.json()
                    count += 1
                    try:  # some data has no chats, to eliminate that error
                        if not messages:
                            a = False
                            continue
                        if (last_msg_time and len(messages) <= 1) or s !=200:
                            a = False
                            continue
                        last_msg_time = messages[-1].get('time')
                        for data in messages:
                            try:
                                keys = list(data.keys())
                                columns = ZohoSqlClient.get_columns("cliq_messages")
                                # Alter table columns as per field from api.
                                create_new_column(keys, columns, 'cliq_messages')
                            
                                values = list(data.values())
                                data_values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                                    values)]
                                data_values.insert(0, chat_id["chat_id"])
                                keys.insert(0, "chat_id")
                                
                                db_cliq_messages = ZohoSqlClient.sql_get("cliq_messages", "id", f"id='{data['id']}'")
                                if db_cliq_messages:
                                    if db_cliq_messages[0]['id'] != data['id']:
                                        ZohoSqlClient.sql_post(
                                            table_name='cliq_messages', attrs=keys, values=data_values)
                                else:
                                    ZohoSqlClient.sql_post(
                                            table_name='cliq_messages', attrs=keys, values=data_values)
                            except Exception as e:
                                print(f"Exception in saving cliq_messages: {data['id']}")
                                save_logs(e)
                        print(f"Saved chats for chat_id: {chat_id}")
                    except Exception as e:
                        print(e)
                        print("Exception while saving chat data: ", chat_id)
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
