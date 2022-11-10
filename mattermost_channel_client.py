from pprint import pprint
import psycopg2
from datetime import datetime
import json
import time
import random
import sys
import string
from sql import ZohoSqlClient, MatterSqlClient
from models import CardProperties
from bs4 import BeautifulSoup
from colorama import Fore
import pandas as pd
from utils import card_propeties_values_id, card_propety_id, get_owners_id


class MattermostClient:
    def get_timestamp_from_date(self, date) -> int:
        date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
        tuple = date.timetuple()
        return int(time.mktime(tuple) * 1000)

    def get_timestamp(self) -> int:
        return int(time.time() * 1000)

    def generate_id(self, size_of_id) -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size_of_id))

    def insert_channels(self):
        keys = ['id', 'createat', 'updateat', 'deleteat', 'teamid', 'type', 'displayname', 'name',
                'header', 'purpose', 'lastpostat', 'totalmsgcount', 'extraupdateat', 'creatorid', 'totalmsgcountroot', 'lastrootpostat']
        team = MatterSqlClient.sql_get("teams", "id")
        channels = ZohoSqlClient.sql_get("cliq_channels")
        for channel in channels:
            creator_id = MatterSqlClient.sql_get(
                "users", "id", f"email='{channel['creator_id']}'")
            values = [self.generate_id(26), channel['creation_time'], channel['creation_time'], 0, team[0]['id'],
                      "O", channel['name'], channel['name'].split('#')[1], "", "", channel['creation_time'], channel['total_message_count'], 0, creator_id[0]['id'], 0, self.get_timestamp()]
            MatterSqlClient.sql_post(
                table_name='channels', attrs=keys, values=values)
        print(Fore.GREEN + "Channel Inserted")

    def insert_channel_members(self):
        # users = MatterSqlClient.sql_get(
        #     'users', 'id,roles,username', "username not in ('channelexport','system-bot','boards','playbooks','appsbot','feedbackbot')")
        keys = MatterSqlClient.get_columns("channelmembers")
        notify_props = {
            "push": "default",
            "email": "default",
            "desktop": "default",
            "mark_unread": "all",
            "ignore_channel_mentions": "default"
        }
        zoho_channels = ZohoSqlClient.sql_get(
            "cliq_channels", "name, channel_id")
        for zoho_channel in zoho_channels:
            users = ZohoSqlClient.sql_get(
                'cliq_channel_members', 'email,user_role', f"channel_id='{zoho_channel['channel_id']}'")  # only channel members
            channel = MatterSqlClient.sql_get(
                'channels', "id", f"displayname='{zoho_channel['name']}'")  # getting the mattermost channels id. this is to prevent duplicate values
            for user in users:
                user_id = MatterSqlClient.sql_get(
                    'users', 'id', f"email='{user['email']}'")
                if "super_admin" in user['user_role']:
                    schemeadmin = json.dumps(True)
                else:
                    schemeadmin = json.dumps(False)
                values = [channel[0]['id'], user_id[0]['id'], "", 0, 0, 0, json.dumps(
                    notify_props), self.get_timestamp(), json.dumps(True), schemeadmin, json.dumps(True), 0, 0]
                MatterSqlClient.sql_post(
                    table_name="channelmembers", attrs=keys, values=values)
        print(Fore.GREEN + "Channle Members Inserted")

    def insert_chats(self):
        keys = ['id', 'createat', 'updateat', 'deleteat', 'teamid', 'type', 'displayname', 'name',
                'header', 'purpose', 'lastpostat', 'totalmsgcount', 'extraupdateat', 'creatorid', 'shared', 'totalmsgcountroot', 'lastrootpostat', 'chat_id']
        team = MatterSqlClient.sql_get("teams", "id")
        chats = ZohoSqlClient.sql_get("cliq_chats")
        try:
            # To get the channel id in the next method
            # Deleted in the next method.
            MatterSqlClient.add_column(
                'channels', 'chat_id', 'varchar(255)')
        except:
            pass
        for chat in chats:
            if chat['chat_type'] == "dm":
                recipient_summaries = json.loads(chat['recipients_summary'])
                rec_ids = []
                for recipient_summary in recipient_summaries:
                    user_id = MatterSqlClient.sql_get(
                        'users', 'id', f"username like '%{recipient_summary['name']}%'")
                    rec_ids.append(user_id[0]['id'])
                rec_ids = "__".join(rec_ids)

                user_email = ZohoSqlClient.sql_get(
                    'cliq_users', 'email_id', f"id='{chat['creator_id']}'")

                creator_id = MatterSqlClient.sql_get(
                    "users", "id", f"email='{user_email[0]['email_id']}'")
                values = [self.generate_id(26), self.get_timestamp_from_date(chat['creation_time']), self.get_timestamp_from_date(chat['last_modified_time']), 0, "",
                          "D", "", rec_ids, "", "", 0, 0, 0, creator_id[0]['id'], json.dumps(False), 0, self.get_timestamp(), chat['chat_id']]
                MatterSqlClient.sql_post(
                    table_name='channels', attrs=keys, values=values)
        print(Fore.GREEN + "Chats Inserted")

    def insert_posts(self):
        keys = MatterSqlClient.get_columns("posts")
        zoho_cliq_messages = ZohoSqlClient.sql_get('cliq_messages')
        zoho_cliq_chats = ZohoSqlClient.sql_get('cliq_chats')
        for zoho_cliq_chat in zoho_cliq_chats:
            channel_id = MatterSqlClient.sql_get(
                'channels', 'id', f"chat_id='{zoho_cliq_chat['chat_id']}'")
            for zoho_cliq_message in zoho_cliq_messages:
                if zoho_cliq_chat['chat_type'] == 'dm' and zoho_cliq_chat['chat_id'] == zoho_cliq_message['chat_id']:
                    if zoho_cliq_message['type'] == 'text':
                        user_id = MatterSqlClient.sql_get(
                            'users', 'id', f"username='{json.loads(zoho_cliq_message['sender'])['name']}'")
                        values = [self.generate_id(
                            26), zoho_cliq_message['time'], zoho_cliq_message['time'], 0, user_id[0]['id'], channel_id[0]['id'], "", "", json.loads(zoho_cliq_message['content'])['text'], "", json.dumps({"disable_group_highlight": True}), "", json.dumps([]), json.dumps([]), json.dumps(False), 0, json.dumps(False), json.dumps(None)]
                        MatterSqlClient.sql_post(
                            table_name='posts', attrs=keys, values=values)
        MatterSqlClient.delete_column('channels', 'chat_id')
        print("Post Added.")

    def main(self):
        self.insert_channels()
        self.insert_channel_members()
        self.insert_chats()
        self.insert_posts()


if __name__ == "__main__":
    c = MattermostClient().main()
