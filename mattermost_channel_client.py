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
        date = datetime.strptime(date, "%m-%d-%Y")
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
            # print(channel['name'])
            creator_id = MatterSqlClient.sql_get(
                "users", "id", f"email='{channel['creator_id']}'")  # to be implemented
            values = [self.generate_id(26), channel['creation_time'], channel['creation_time'], 0, team[0]['id'],
                      "O", channel['name'].split('#')[1], channel['name'].split('#')[1], "", "", channel['creation_time'], channel['total_message_count'], 0, "", 0, self.get_timestamp()]

            MatterSqlClient.sql_post(
                table_name='channels', attrs=keys, values=values)
        print("Channel Inserted")

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
                'channels', "id", f"name='{zoho_channel['name'].split('#')[1]}'")  # getting the mattermost channels id. this is to prevent duplicate values

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

    def main(self):
        self.insert_channels()
        self.insert_channel_members()


if __name__ == "__main__":
    c = MattermostClient().main()
