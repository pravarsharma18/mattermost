from pprint import pprint
from datetime import datetime
import json
import time
import random
import string
from data_insert import channel_extras, image_data, xlsx_data
from sql import ZohoSqlClient, MatterSqlClient
from colorama import Fore
from utils import remove_punctions, replace_escape_characters, save_logs
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
                    try:
                        display_channel_name = channel.get('name')
                        channel_name = remove_punctions(display_channel_name).replace(".", "-")
                        chat_id = []
                        if channel_name:
                            chat_id = ZohoSqlClient.sql_get('cliq_channels', 'chat_id', f"name like '%{display_channel_name}%'" )
                        if chat_id:                        
                            creator_id = MatterSqlClient.sql_get(
                                "users", "id", f"username='{remove_punctions(channel['creator_name'])}'")
                            if creator_id:
                                values = [
                                    self.generate_id(26), self.get_timestamp_from_date(channel.get('creation_time')), 
                                    self.get_timestamp_from_date(channel.get('creation_time')), 0, team['id'],"O", display_channel_name, 
                                    channel_name, "", "", self.get_timestamp_from_date(channel.get('creation_time')), 
                                    channel.get('total_message_count'), 0, creator_id[0]['id'], 0, 0, chat_id[0]['chat_id']]
                                channels = MatterSqlClient.sql_get("channels", "teamid,name", f"teamid='{team['id']}' and name='{channel_name}'")
                                if channels:
                                    if channels[0]['teamid'] != team['id'] and channels[0]['name'] != channel_name:
                                        MatterSqlClient.sql_post(
                                            table_name='channels', attrs=keys, values=values)
                                else:
                                    MatterSqlClient.sql_post(
                                        table_name='channels', attrs=keys, values=values)
                            else:
                                print(f"channel['creator_id'] {channel['creator_id']} not found in mattermost db users")
                                print(f"channel['creator_id'] {remove_punctions(channel['creator_name'])} not found in mattermost db users")
                        else:
                            print(f"channel['name'] {channel_name} not found in cliq_chats")
                    except Exception as e:
                        print(f"Exception in saving channels")
                        save_logs(e)
            print(Fore.GREEN + "Channel Inserted")

    def insert_chats(self):
            channel_keys = ['id', 'createat', 'updateat', 'deleteat', 'teamid', 'type', 'displayname', 'name',
                            'header', 'purpose', 'lastpostat', 'totalmsgcount', 'extraupdateat', 'creatorid', 'shared', 'totalmsgcountroot', 'lastrootpostat', 'chat_id']
            prefrence_keys = MatterSqlClient.get_columns('preferences')
            # team = MatterSqlClient.sql_get("teams", "id")
            chats = ZohoSqlClient.sql_get("cliq_chats")
            channel_members = MatterSqlClient.get_columns('channelmembers')
            
            for chat in chats:
                try:
                    recipient_summaries = json.loads(chat['recipients_summary'])
                    rec_ids = []
                    for recipient_summary in recipient_summaries:
                        user_id = MatterSqlClient.sql_get('users', 'id', f"email='{recipient_summary}'")
                        if user_id:
                            rec_ids.append(user_id[0]['id'])
                        else:
                            print(f"{recipient_summary} is not found in users db mattermost")
                    if len(rec_ids) == 2 and not chat['chat_id'].startswith('CT'):
                        # rec_ids = rec_ids
                        rec_ids_reverse = rec_ids[::-1]
                        rec_ids_str = "__".join(rec_ids)
                        rec_ids_str_reverse = "__".join(rec_ids_reverse)
                        creator_id = MatterSqlClient.sql_get(
                            "users", "id", f"email='{chat['creator_id']}'")
                        if creator_id:
                            channel_values = [
                                self.generate_id(26), int(float(chat['creation_time'])), int(float(chat['last_modified_time'])), 0, "",
                                "D", "", rec_ids_str, "", "", int(float(chat['last_modified_time'])), 0, 0, creator_id[0]['id'], 
                                json.dumps(False), 0, int(float(chat['last_modified_time'])), chat['chat_id']]
                            channel_values_reverse = [
                                self.generate_id(26), int(float(chat['creation_time'])), int(float(chat['last_modified_time'])), 0, "",
                                "D", "", rec_ids_str_reverse, "", "", int(float(chat['last_modified_time'])), 0, 0, creator_id[0]['id'], 
                                json.dumps(False), 0, int(float(chat['last_modified_time'])), chat['chat_id']]

                            channels_db = MatterSqlClient.sql_get("channels", "name,chat_id", f"name='{rec_ids_str}'")
                            channel_id = ""
                            if channels_db:
                                if channels_db[0]['name'] != rec_ids_str:
                                    channel_id = MatterSqlClient.sql_post(
                                        table_name='channels', attrs=channel_keys, values=channel_values, returning='id')
                            else:
                                channel_id = MatterSqlClient.sql_post(
                                        table_name='channels', attrs=channel_keys, values=channel_values, returning='id')
                            channels_db_reverse = MatterSqlClient.sql_get("channels", "name,chat_id", f"name='{rec_ids_str_reverse}' and chat_id='{chat['chat_id']}'")
                            if channels_db_reverse:
                                if channels_db_reverse[0]['name'] != rec_ids_str and channels_db_reverse[0]['chat_id'] != chat['chat_id']:
                                    channel_id_reverse = MatterSqlClient.sql_post(
                                        table_name='channels', attrs=channel_keys, values=channel_values_reverse, returning='id')
                            else:
                                channel_id_reverse = MatterSqlClient.sql_post(
                                        table_name='channels', attrs=channel_keys, values=channel_values_reverse, returning='id')
                            try:
                                channel_extras(channel_id,rec_ids, channel_members, channel_id_reverse, rec_ids_reverse, prefrence_keys)
                            except:
                                pass
                        else:
                            print(f"chat['creator_id'] {chat['creator_id']} not found in users db mattermost")
                except Exception as e:
                    print("Exception in inserting chat")
                    save_logs(e)
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
                    'cliq_channel_members', 'email_id,user_role', f"channel_id='{zoho_channel['channel_id']}'")  # only channel members
                channel = MatterSqlClient.sql_get(
                    'channels', "id", f"name='{remove_punctions(zoho_channel['name']).replace('.', '-')}'")  # getting the mattermost channels id. this is to prevent duplicate values
                if channel:
                    for user in users:
                        try:
                            user_id = MatterSqlClient.sql_get(
                                'users', 'id', f"email='{user['email_id']}'")
                            if user_id:
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
                            else:
                                print(f"user['email_id'] {user['email_id']} not found in users db mattermost")
                        except Exception as e:
                            print(f"Exception in saving channelmembers: {user['email_id']}")
                            save_logs(e)
                else:
                    print(f"{zoho_channel['name']} is not found in channels db mattermost, no chat is there for this channel.")
            print(Fore.GREEN + "Channle Members Inserted")

    def insert_posts(self):
            posts_keys = MatterSqlClient.get_columns("posts")
            fileinfo_keys = MatterSqlClient.get_columns('fileinfo')
            # zoho_cliq_messages = ZohoSqlClient.sql_get('cliq_messages')
            mm_channels = MatterSqlClient.sql_get('channels')
            text = 1
            file_counter = 1
            for mm_channel in mm_channels:
                zoho_cliq_messages = ZohoSqlClient.sql_get('cliq_messages', params=f"chat_id='{mm_channel.get('chat_id')}'")
                for zoho_cliq_message in zoho_cliq_messages:
                    try:
                        # if mm_channel.get('chat_id') == zoho_cliq_message.get('chat_id'):
                        if zoho_cliq_message['type'] == 'text':
                            user_email = ZohoSqlClient.sql_get("cliq_users", "email_id", f"id='{json.loads(zoho_cliq_message['sender'])['id']}'")
                            if user_email:
                                user_email_id = user_email[0]["email_id"]
                                user_id = MatterSqlClient.sql_get('users', 'id', f"email='{user_email_id}'")
                                
                                if user_id:
                                    posts_db = MatterSqlClient.sql_get("posts", "channelid,userid", f"channelid='{mm_channel['id']}' and userid='{user_id[0]['id']}' and message='{replace_escape_characters(json.loads(zoho_cliq_message['content'])['text'])}' and createat={zoho_cliq_message['time']}")
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
                                else:
                                    print(f"zoho_cliq_message['sender'])['name'] {remove_punctions(json.loads(zoho_cliq_message['sender'])['name'])} not found in users db")
                            else:
                                print(f"zoho_cliq_message['sender'])['name'] {remove_punctions(json.loads(zoho_cliq_message['sender'])['name'])} not found in users db")
                        elif zoho_cliq_message['type'] == 'file':
                            content = json.loads(zoho_cliq_message['content'])
                            file = content['file']
                            file_counter += 1
                            if "image" in file['type']:
                                image_data(mm_channel, zoho_cliq_message, file, posts_keys, fileinfo_keys, file_counter)
                            else:
                                xlsx_data(mm_channel, zoho_cliq_message, file, posts_keys, fileinfo_keys, file['type'], file_counter)
                    except Exception as e:
                        print(f"Exception in inserting posts:  {zoho_cliq_message['chat_id']}")
                        save_logs(e)
                
            MatterSqlClient.delete_column('channels', 'chat_id')
            # print("text", text)
            # print("img", img)
            # print("xl", xl)
            print("Post Added.")
    
    def delete_extrachannel(self):
        try:
            query = '''
                delete from channels where type='D' and id not in (select c.id from channels as c join posts as p on p.channelid = c.id group by 1)
            '''
            MatterSqlClient.raw_query(query)
            print("Deleted extra created channels")
        except Exception as e:
            save_logs(e)

    def delete_dup(self):
        channels = MatterSqlClient.sql_get('channels', 'name,id')
        names = []
        for channel in channels:
            name = channel['name'].split("__")
            if len(name)>=2:
                names.append(name)
        data = {tuple(sorted(item)) for item in names}
        for d in data:
            query = f'''
                select id from channels where name like '%{d[0]}%' and name like '%{d[1]}%'
            '''
            res = MatterSqlClient.raw_query(query, type="get")
            del_query = f"""
            delete from channels where id = (
                select a.chid
                from 
                (
                select count(id) as id  , channelid as chid from posts where channelid in ('{res[0][0]}','{res[1][0]}') group by 2 order by count(id) asc limit 1
                )a
                )
            """
            del_q = f"""
                delete from channelmembers where channelid = (
                select a.chid
                from 
                (
                select count(id) as id  , channelid as chid from posts where channelid in ('{res[0][0]}','{res[1][0]}') group by 2 order by count(id) asc limit 1
                )a
                )
            """
            MatterSqlClient.raw_query(del_query)
            MatterSqlClient.raw_query(del_q)
        # print(data)

                

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
        # self.delete_dup()


if __name__ == "__main__":
    print(Fore.YELLOW + "\n<========Saving Mattermost channels Data in DB=========>\n")
    c = MattermostClient().insert_chats()
