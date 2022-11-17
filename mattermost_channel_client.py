from pprint import pprint
import psycopg2
from datetime import datetime
import json
import time
import random
import sys
import string
import pathlib
import requests
from zoho_client import ZohoClient
from sql import ZohoSqlClient, MatterSqlClient
from models import CardProperties
from bs4 import BeautifulSoup
from colorama import Fore
from PIL import Image
from utils import remove_punctions


class MattermostClient:
    def get_timestamp_from_date(self, date) -> int:
        if date != 'NaN':
            date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
            tuple = date.timetuple()
            return int(time.mktime(tuple) * 1000)
        else:
            return int(time.time() * 1000)

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

    def insert_chats(self):
        channel_keys = ['id', 'createat', 'updateat', 'deleteat', 'teamid', 'type', 'displayname', 'name',
                        'header', 'purpose', 'lastpostat', 'totalmsgcount', 'extraupdateat', 'creatorid', 'shared', 'totalmsgcountroot', 'lastrootpostat', 'chat_id']
        prefrence_keys = MatterSqlClient.get_columns('preferences')
        # team = MatterSqlClient.sql_get("teams", "id")
        chats = ZohoSqlClient.sql_get("cliq_chats")
        channel_members = MatterSqlClient.get_columns('channelmembers')
        try:
            # To get the channel id in the next method
            # Deleted in the next method.
            MatterSqlClient.add_column(
                'channels', 'chat_id', 'varchar(255)')
        except:
            pass
        for chat in chats:
            # if chat['chat_type'] == "dm":
            recipient_summaries = json.loads(chat['recipients_summary'])
            rec_ids = []
            for recipient_summary in recipient_summaries:
                user_id = MatterSqlClient.sql_get(
                    'users', 'id, username', f"email='{recipient_summary}'")

                rec_ids.append(user_id[0]['id'])
            if len(rec_ids) == 2:
                rec_ids = rec_ids[::-1]
                rec_ids_str = "__".join(rec_ids)
                creator_id = MatterSqlClient.sql_get(
                    "users", "id", f"email='{chat['creator_id']}'")
                channel_values = [self.generate_id(26), chat['creation_time'], chat['last_modified_time'], 0, "",
                                  "D", "", rec_ids_str, "", "", chat['last_modified_time'], 0, 0, creator_id[0]['id'], json.dumps(False), 0, chat['last_modified_time'], chat['chat_id']]
                channel_id = MatterSqlClient.sql_post(
                    table_name='channels', attrs=channel_keys, values=channel_values, returning='id')

                notify_props = {
                    "push": "default",
                    "email": "default",
                    "desktop": "default",
                    "mark_unread": "all",
                    "ignore_channel_mentions": "default"
                }

                member_1 = [channel_id, rec_ids[0], "",
                            0, 0, 0, json.dumps(notify_props), self.get_timestamp(), json.dumps(True), json.dumps(False), json.dumps(True), 0, 0]
                MatterSqlClient.sql_post(
                    table_name='channelmembers', attrs=channel_members, values=member_1)

                member_2 = [channel_id, rec_ids[1], "",
                            0, 0, 0, json.dumps(notify_props), self.get_timestamp(), json.dumps(True), json.dumps(False), json.dumps(True), 0, 0]
                MatterSqlClient.sql_post(
                    table_name='channelmembers', attrs=channel_members, values=member_2)

                # insert in prefrence Table
                prefrence_values_channel_show1 = [rec_ids[0],
                                                  "direct_channel_show", rec_ids[1], "true"]
                prefrence_values_open_time1 = [rec_ids[0],
                                               "channel_open_time", channel_id, self.get_timestamp()]
                MatterSqlClient.sql_post(
                    table_name='preferences', attrs=prefrence_keys, values=prefrence_values_channel_show1)
                MatterSqlClient.sql_post(
                    table_name='preferences', attrs=prefrence_keys, values=prefrence_values_open_time1)

                prefrence_values_channel_show2 = [rec_ids[1],
                                                  "direct_channel_show", rec_ids[0], "true"]
                prefrence_values_open_time2 = [rec_ids[1],
                                               "channel_open_time", channel_id, self.get_timestamp()]

                MatterSqlClient.sql_post(
                    table_name='preferences', attrs=prefrence_keys, values=prefrence_values_channel_show2)
                MatterSqlClient.sql_post(
                    table_name='preferences', attrs=prefrence_keys, values=prefrence_values_open_time2)

        print(Fore.GREEN + "Chats Inserted")

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

    def insert_posts(self):
        posts_keys = MatterSqlClient.get_columns("posts")
        fileinfo_keys = MatterSqlClient.get_columns('fileinfo')
        zoho_cliq_messages = ZohoSqlClient.sql_get('cliq_messages')
        zoho_cliq_chats = ZohoSqlClient.sql_get('cliq_chats')
        for zoho_cliq_chat in zoho_cliq_chats:
            channel_id = MatterSqlClient.sql_get(
                'channels', 'name, id', f"chat_id='{zoho_cliq_chat['chat_id']}'")
            for zoho_cliq_message in zoho_cliq_messages:
                if zoho_cliq_chat['chat_id'] == zoho_cliq_message['chat_id']:
                    if zoho_cliq_message['type'] == 'text':
                        user_id = MatterSqlClient.sql_get(
                            'users', 'id', f"username='{remove_punctions(json.loads(zoho_cliq_message['sender'])['name'])}'")
                        try:  # bot users are not in mattermost users table
                            type_text_values = [self.generate_id(
                                26), zoho_cliq_message['time'], zoho_cliq_message['time'], 0, user_id[0]['id'], channel_id[0]['id'], "", "", json.loads(zoho_cliq_message['content'])['text'], "", json.dumps({"disable_group_highlight": True}), "", json.dumps([]), json.dumps([]), json.dumps(False), 0, json.dumps(False), json.dumps(None)]
                            # MatterSqlClient.sql_post(
                            #     table_name='posts', attrs=posts_keys, values=type_text_values)
                        except:
                            pass
                    elif zoho_cliq_message['type'] == 'file':
                        content = json.loads(zoho_cliq_message['content'])
                        file = content['file']
                        if file['type'] in ['image/jpeg', 'image/jpg', 'image/png']:
                            timestamp = int(zoho_cliq_message['time'])
                            date_folder = datetime.strftime(
                                datetime.fromtimestamp(timestamp / 1000), '%Y%m%d')
                            try:
                                id_channel = channel_id[0]['id']
                                sender_name = remove_punctions(
                                    json.loads(zoho_cliq_message['sender'])['name'])
                                sender_id = MatterSqlClient.sql_get(
                                    "users", 'id', f"username='{sender_name}'")

                                image_fileinfo_id = self.generate_id(26)
                                posts_id = self.generate_id(26)
                                path = f"{date_folder}/teams/noteam/channels/{id_channel}/users/{sender_id[0]['id']}/{image_fileinfo_id}/"
                                type_images_fileinfo_values = [
                                    image_fileinfo_id, sender_id[0]['id'], posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, path, "", "", file['name'], 'jpeg', file['dimensions']['size'], 'image/jpeg', 0, 0, json.dumps(False), json.dumps(None), "", "", json.dumps(False)]
                                image_posts_values = [posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, sender_id[0]['id'], id_channel, "", "", "", "", json.dumps(
                                    {"disable_group_highlight": True}), "", json.dumps([]), json.dumps([image_fileinfo_id]), json.dumps(False), 0, json.dumps(False), json.dumps(None)]
                                print(type_images_fileinfo_values)
                                print(image_posts_values)
                                print()
                                pathlib.Path(path).mkdir(
                                    parents=True, exist_ok=True)

                                r = requests.get(
                                    f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                                        "Authorization": f"Zoho-oauthtoken {ZohoClient().access_token}",
                                        "Content-Type": 'application/json'
                                    }).content
                                with open(f"{path}{file['name']}", 'wb') as handler:
                                    handler.write(r)
                                im = Image.open(f"{path}{file['name']}")

                                im.save(path + '_preview.'.join(file['name'].split('.')),
                                        optimize=True)
                                newsize = (120, 120)
                                im1 = im.resize(newsize)
                                im1.save(path + '_thumb.'.join(
                                    file['name'].split('.')), optimize=True)
                                # MatterSqlClient.sql_post(
                                #     table_name="posts", attrs=posts_keys, values=image_posts_values)

                                # MatterSqlClient.sql_post(
                                #     table_name="fileinfo", attrs=fileinfo_keys, values=type_images_fileinfo_values)
                            except:
                                pass
                        elif file['type'] == 'application/x-ooxml':
                            timestamp = int(zoho_cliq_message['time'])
                            date_folder = datetime.strftime(
                                datetime.fromtimestamp(timestamp / 1000), '%Y%m%d')
                            try:
                                id_channel = channel_id[0]['id']
                                sender_name = remove_punctions(
                                    json.loads(zoho_cliq_message['sender'])['name'])
                                sender_id = MatterSqlClient.sql_get(
                                    "users", 'id', f"username='{sender_name}'")

                                xml_fileinfo_id = self.generate_id(26)
                                posts_id = self.generate_id(26)
                                path = f"{date_folder}/teams/noteam/channels/{id_channel}/users/{sender_id[0]['id']}/{xml_fileinfo_id}/"

                                type_xml_fileinfo_values = [
                                    xml_fileinfo_id, sender_id[0]['id'], posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, path, "", "", file['name'], 'xlsx', file['dimensions']['size'], 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 0, 0, json.dumps(False), json.dumps(None), "", "", json.dumps(False)]
                                xml_posts_values = [posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, sender_id[0]['id'], id_channel, "", "", "", "", json.dumps(
                                    {"disable_group_highlight": True}), "", json.dumps([]), json.dumps([xml_fileinfo_id]), json.dumps(False), 0, json.dumps(False), json.dumps(None)]

                                pathlib.Path(path).mkdir(
                                    parents=True, exist_ok=True)
                                print(type_xml_fileinfo_values)
                                print(xml_posts_values)
                                print()
                                r = requests.get(
                                    f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                                        "Authorization": f"Zoho-oauthtoken {ZohoClient().access_token}",
                                        "Content-Type": 'application/json'
                                    }).content
                                with open(f"{path}{file['name']}", 'wb') as f:
                                    f.write(r.content)
                                # MatterSqlClient.sql_post(
                                #     table_name="posts", attrs=posts_keys, values=xml_posts_values)

                                # MatterSqlClient.sql_post(
                                #     table_name="fileinfo", attrs=fileinfo_keys, values=type_xml_fileinfo_values)
                            except:
                                pass

        # MatterSqlClient.delete_column('channels', 'chat_id')
        print("Post Added.")

    def main(self):
        self.insert_channels()
        self.insert_chats()
        self.insert_channel_members()
        # self.insert_posts()


if __name__ == "__main__":
    print(Fore.YELLOW + "\n<========Saving Mattermost channels Data in DB=========>\n")
    c = MattermostClient().insert_posts()
