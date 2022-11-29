from pprint import pprint
from datetime import datetime
import json
import time
import random
import string
from data_insert import channel_extras, image_data, xlsx_data
from sql import ZohoSqlClient, MatterSqlClient
from colorama import Fore
from utils import remove_punctions
from decouple import config



class MattermostClient:
    mattermost_base_path = config('MATTERMOST_PATH')

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
                'header', 'purpose', 'lastpostat', 'totalmsgcount', 'extraupdateat', 'creatorid', 'totalmsgcountroot', 'lastrootpostat', 'chat_id']
        teams = MatterSqlClient.sql_get("teams", "id")
        channels = ZohoSqlClient.sql_get("cliq_channels")
        try:
            # To get the channel id in the next method
            # Deleted in the next method.
            MatterSqlClient.add_column(
                'channels', 'chat_id', 'varchar(255)')
        except:
            pass
        for team in teams:
            for channel in channels:
                chat_id = ZohoSqlClient.sql_get('cliq_chats', 'chat_id', f"title like '%{channel['name'][1:]}%'" )
                
                channels = MatterSqlClient.sql_get("channels", "teamid,name", f"teamid='{team['id']}' and name='{channel['name'].split('#')[1]}'")
                creator_id = MatterSqlClient.sql_get(
                    "users", "id", f"email='{channel['creator_id']}'")
                values = [self.generate_id(26), channel['creation_time'], channel['creation_time'], 0, team['id'],"O", channel['name'], channel['name'].split('#')[1], "", "", channel['creation_time'], channel['total_message_count'], 0, creator_id[0]['id'], 0, 0, chat_id[0]['chat_id']]
                if channels:
                    if channels[0]['teamid'] != team['id'] and channels[0]['name'] != channel['name'].split('#')[1]:
                        MatterSqlClient.sql_post(
                            table_name='channels', attrs=keys, values=values)
                else:
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
        
        for chat in chats:
            recipient_summaries = json.loads(chat['recipients_summary'])
            rec_ids = []
            for recipient_summary in recipient_summaries:
                user_id = MatterSqlClient.sql_get(
                    'users', 'id', f"email='{recipient_summary}'")
                rec_ids.append(user_id[0]['id'])
            if len(rec_ids) == 2:
                rec_ids = rec_ids
                rec_ids_reverse = rec_ids[::-1]
                rec_ids_str = "__".join(rec_ids)
                rec_ids_str_reverse = "__".join(rec_ids_reverse)
                creator_id = MatterSqlClient.sql_get(
                    "users", "id", f"email='{chat['creator_id']}'")
                channel_values = [self.generate_id(26), chat['creation_time'], chat['last_modified_time'], 0, "",
                                  "D", "", rec_ids_str, "", "", chat['last_modified_time'], 0, 0, creator_id[0]['id'], json.dumps(False), 0, chat['last_modified_time'], chat['chat_id']]
                channel_values_reverse = [self.generate_id(26), chat['creation_time'], chat['last_modified_time'], 0, "",
                                  "D", "", rec_ids_str_reverse, "", "", chat['last_modified_time'], 0, 0, creator_id[0]['id'], json.dumps(False), 0, chat['last_modified_time'], chat['chat_id']]

                channels_db = MatterSqlClient.sql_get("channels", "name,chat_id", f"name='{rec_ids_str}'")
                channel_id = ""
                if channels_db:
                    if channels_db[0]['name'] != rec_ids_str:
                        channel_id = MatterSqlClient.sql_post(
                            table_name='channels', attrs=channel_keys, values=channel_values, returning='id')
                else:
                    channel_id = MatterSqlClient.sql_post(
                            table_name='channels', attrs=channel_keys, values=channel_values, returning='id')
                # channels_db_reverse = MatterSqlClient.sql_get("channels", "name,chat_id", f"name='{rec_ids_str_reverse}' and chat_id='{chat['chat_id']}'")
                # if channels_db_reverse:
                #     if channels_db_reverse[0]['name'] != rec_ids_str and channels_db_reverse[0]['chat_id'] != chat['chat_id']:
                #         channel_id_reverse = MatterSqlClient.sql_post(
                #             table_name='channels', attrs=channel_keys, values=channel_values_reverse, returning='id')
                # else:
                #     channel_id_reverse = MatterSqlClient.sql_post(
                #             table_name='channels', attrs=channel_keys, values=channel_values_reverse, returning='id')

                channel_extras(channel_id,rec_ids, channel_members, rec_ids_reverse, prefrence_keys)
                # channel_extras(channel_id,rec_ids, channel_members, channel_id_reverse, rec_ids_reverse, prefrence_keys)

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
                channel_members = MatterSqlClient.sql_get("channelmembers", "channelid,userid", f"channelid='{channel[0]['id']}' and userid='{user_id[0]['id']}'")
                if "super_admin" in user['user_role']:
                    schemeadmin = json.dumps(True)
                else:
                    schemeadmin = json.dumps(False)
                values = [channel[0]['id'], user_id[0]['id'], "", 0, 0, 0, json.dumps(
                    notify_props), self.get_timestamp(), json.dumps(True), schemeadmin, json.dumps(True), 0, 0]

                if channel_members:
                    if channel_members[0].get("channelid") != channel[0]['id'] and channel_members[0].get("userid") != user_id[0]['id']:
                        MatterSqlClient.sql_post(
                            table_name="channelmembers", attrs=keys, values=values)
                else:
                    MatterSqlClient.sql_post(
                            table_name="channelmembers", attrs=keys, values=values)
        print(Fore.GREEN + "Channle Members Inserted")

    def insert_posts(self):
        posts_keys = MatterSqlClient.get_columns("posts")
        fileinfo_keys = MatterSqlClient.get_columns('fileinfo')
        zoho_cliq_messages = ZohoSqlClient.sql_get('cliq_messages')
        zoho_cliq_chats = ZohoSqlClient.sql_get('cliq_chats')
        mm_channels = MatterSqlClient.sql_get('channels')
        text = 0
        xl = 0
        img = 0
        for mm_channel in mm_channels:
            # channel_id = MatterSqlClient.sql_get(
            #     'channels', 'name, id', f"chat_id='{zoho_cliq_chat['chat_id']}'")
            for zoho_cliq_message in zoho_cliq_messages:
                if mm_channel.get('chat_id') == zoho_cliq_message.get('chat_id'):
                    if zoho_cliq_message['type'] == 'text':
                        # try:  # bot users are not in mattermost users table
                            user_id = MatterSqlClient.sql_get('users', 'id', f"username like '%{remove_punctions(json.loads(zoho_cliq_message['sender'])['name'])}%'")
                            posts_db = MatterSqlClient.sql_get("posts", "channelid,userid", f"channelid='{mm_channel['id']}' and userid='{user_id[0]['id']}'")
                            type_text_values = [self.generate_id(
                                26), zoho_cliq_message['time'], zoho_cliq_message['time'], 0, user_id[0]['id'], mm_channel['id'], "", "", json.loads(zoho_cliq_message['content'])['text'], "", json.dumps({"disable_group_highlight": True}), "", json.dumps([]), json.dumps([]), json.dumps(False), 0, json.dumps(False), json.dumps(None)]
                            if posts_db:
                                if posts_db[0]['channelid'] != mm_channel['id'] and posts_db[0]['userid'] != user_id[0]['id']:
                                    text +=1
                                    MatterSqlClient.sql_post(
                                        table_name='posts', attrs=posts_keys, values=type_text_values)
                            else:
                                text +=1
                                MatterSqlClient.sql_post(
                                        table_name='posts', attrs=posts_keys, values=type_text_values)
                        # except:
                        #     pass
                    if zoho_cliq_message['type'] == 'file':
                        content = json.loads(zoho_cliq_message['content'])
                        file = content['file']
                        if file['type'] in ['image/jpeg', 'image/jpg', 'image/png']:
                            img +=1
                            image_data(mm_channel, zoho_cliq_message, file, posts_keys, fileinfo_keys)
                        elif file['type'] in ['application/x-ooxml', 'application/pdf', 'application/zip', 'text/plain']:
                            xl +=1
                            xlsx_data(mm_channel, zoho_cliq_message, file, posts_keys, fileinfo_keys, file['type'])
             
        MatterSqlClient.delete_column('channels', 'chat_id')
        # print("text", text)
        # print("img", img)
        # print("xl", xl)
        print("Post Added.")
    
    def delete_extrachannel(self):
        query = '''
            delete from channels where type='D' and id not in (select c.id from channels as c join posts as p on p.channelid = c.id group by 1)
        '''
        MatterSqlClient.raw_query(query)
        print("Deleted extra created channels")

    def main(self):
        self.insert_channels()
        time.sleep(1)
        self.insert_chats()
        time.sleep(1)
        self.insert_channel_members()
        time.sleep(1)
        self.insert_posts()
        time.sleep(1)
        self.delete_extrachannel()


if __name__ == "__main__":
    print(Fore.YELLOW + "\n<========Saving Mattermost channels Data in DB=========>\n")
    c = MattermostClient().main()
