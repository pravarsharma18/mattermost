import json
import pathlib
import requests
from PIL import Image
from decouple import config
from datetime import datetime
import time
from sql import MatterSqlClient
from utils import check_token_revoke_cliq, remove_punctions, generate_id, save_logs
from zoho_cliq_client import ZohoClient
import os
from dotenv import load_dotenv

load_dotenv()
mattermost_base_path = config('MATTERMOST_PATH')
# {mattermost_base_path}
def image_data(channel_id, zoho_cliq_message, file, posts_keys, fileinfo_keys, img_counter):
    access_token = config['ZOHO_CLIQ_API_KEY']
    try:
        timestamp = int(zoho_cliq_message['time'])
        date_folder = datetime.strftime(
            datetime.fromtimestamp(timestamp / 1000), '%Y%m%d')
        id_channel = channel_id['id']
        sender_name = remove_punctions(
            json.loads(zoho_cliq_message['sender'])['name'])
        sender_id = MatterSqlClient.sql_get(
            "users", 'id', f"username like '%{sender_name}%'")
        if sender_id:
            image_fileinfo_id = generate_id(26)
            posts_id = generate_id(26)

            path = f"{date_folder}/teams/noteam/channels/{id_channel}/users/{sender_id[0]['id']}/{image_fileinfo_id}/"
            file_path = f"{mattermost_base_path}{path}" 
            type_images_fileinfo_values = [
                image_fileinfo_id, sender_id[0]['id'], posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, f'{path}{file["name"]}', f"{path}{'_thumb.'.join(file['name'].split('.'))}", f"{path}{'_preview.'.join(file['name'].split('.'))}", file['name'], 'jpeg', file['dimensions']['size'], file['type'], 0, 0, json.dumps(False), json.dumps(None), "", "", json.dumps(False)]
            image_posts_values = [posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, sender_id[0]['id'], id_channel, "", "", "", "", json.dumps(
                {"disable_group_highlight": True}), "", json.dumps([]), json.dumps([image_fileinfo_id]), json.dumps(False), 0, json.dumps(False), json.dumps(None)]
            pathlib.Path(file_path).mkdir(
                parents=True, exist_ok=True)
            
            if img_counter == 30:
                img_counter = 1
                print("Files api for images throttling api please wait for 120 seconds.")
                time.sleep(120)
           
            r = requests.get(
                f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                    "Authorization": f"Zoho-oauthtoken {access_token}",
                    "Content-Type": 'application/json'
                })
            
            if r.status_code == 401:
                if r.json()['code'] == "oauthtoken_invalid":
                    new_access_token = check_token_revoke_cliq(r)
                    # os.environ.pop('ZOHO_CLIQ_API_KEY')
                    # os.environ['ZOHO_CLIQ_API_KEY'] = new_access_token
                    r = requests.get(
                        f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                            "Authorization": f"Zoho-oauthtoken {new_access_token}",
                            "Content-Type": 'application/json'
                        })
            
            r = r.content

            with open(f"{file_path}{file['name']}", 'wb') as handler:
                handler.write(r)
            im = Image.open(f"{file_path}{file['name']}")

            im.save(file_path + '_preview.'.join(file['name'].split('.')),
                    optimize=True)
            newsize = (120, 120)
            im1 = im.resize(newsize)
            im1.save(file_path + '_thumb.'.join(file['name'].split('.')), optimize=True)

            fileinfo_db = MatterSqlClient.sql_get("fileinfo", "name,createat,creatorid", f"name='{file['name']}' and createat='{zoho_cliq_message['time']}' and creatorid='{sender_id[0]['id']}'")

            if fileinfo_db:
                if fileinfo_db[0]['name'] != file['name'] and fileinfo_db[0]['createat'] != zoho_cliq_message['time'] and fileinfo_db[0]['creatorid'] != sender_id[0]['id']:
                    MatterSqlClient.sql_post(
                        table_name="posts", attrs=posts_keys, values=image_posts_values)
                    MatterSqlClient.sql_post(
                        table_name="fileinfo", attrs=fileinfo_keys, values=type_images_fileinfo_values)
            else:
                MatterSqlClient.sql_post(
                        table_name="posts", attrs=posts_keys, values=image_posts_values)
                MatterSqlClient.sql_post(
                        table_name="fileinfo", attrs=fileinfo_keys, values=type_images_fileinfo_values)
        else:
            print(f"sender_name {sender_name} not found in users db mattermost")

    except Exception as e:
        save_logs(e)


def xlsx_data(channel_id, zoho_cliq_message, file, posts_keys, fileinfo_keys, type, xlsx_counter):
    access_token = config['ZOHO_CLIQ_API_KEY']
    try:
        timestamp = int(zoho_cliq_message['time'])
        date_folder = datetime.strftime(
        datetime.fromtimestamp(timestamp / 1000), '%Y%m%d')
        id_channel = channel_id['id']
        sender_name = remove_punctions(
            json.loads(zoho_cliq_message['sender'])['name'])
        # try:
        sender_id = MatterSqlClient.sql_get("users", "id", f"username like '%{sender_name}%'")
        if sender_id:
            xml_fileinfo_id = generate_id(26)
            posts_id = generate_id(26)
            path = f"{date_folder}/teams/noteam/channels/{id_channel}/users/{sender_id[0]['id']}/{xml_fileinfo_id}/"
            file_path = f"{mattermost_base_path}{path}" 

            if type == "application/x-ooxml":
                extension = file['name'].split('.')[-1]
                minetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif type == "application/pdf":
                extension = file['name'].split('.')[-1]
                minetype = type
            elif type == "application/zip":
                extension = file['name'].split('.')[-1]
                minetype = type
            elif type == "text/plain":
                extension = file['name'].split('.')[-1]
                minetype = type

            type_xml_fileinfo_values = [
                xml_fileinfo_id, sender_id[0]['id'], posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, f'{path}{file["name"]}', "", "", file['name'], extension, file['dimensions']['size'], minetype, 0, 0, json.dumps(False), json.dumps(None), "", "", json.dumps(False)]
            xml_posts_values = [posts_id, zoho_cliq_message['time'], zoho_cliq_message['time'], 0, sender_id[0]['id'], id_channel, "", "", "", "", json.dumps(
                {"disable_group_highlight": True}), "", json.dumps([]), json.dumps([xml_fileinfo_id]), json.dumps(False), 0, json.dumps(False), json.dumps(None)]


            pathlib.Path(file_path).mkdir(
                parents=True, exist_ok=True)
            
            if xlsx_counter == 30:
                xlsx_counter = 1
                print("Files api for xlsx throttling api please wait for 120 seconds.")
                time.sleep(120)

            r = requests.get(
                f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                    "Authorization": f"Zoho-oauthtoken {access_token}",
                    "Content-Type": 'application/json'
                })
            
            if r.status_code == 401:
                if r.json()['code'] == "oauthtoken_invalid":
                    new_access_token = check_token_revoke_cliq(r)
                    # os.environ.pop('ZOHO_CLIQ_API_KEY')
                    # os.environ['ZOHO_CLIQ_API_KEY'] = new_access_token
                    r = requests.get(
                        f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                            "Authorization": f"Zoho-oauthtoken {new_access_token}",
                            "Content-Type": 'application/json'
                        })
            
            r = r.content

            with open(f"{file_path}{file['name']}", 'wb') as f:
                f.write(r)
            fileinfo_db = MatterSqlClient.sql_get("fileinfo", "name,createat,creatorid", f"name='{file['name']}' and createat='{zoho_cliq_message['time']}' and creatorid='{sender_id[0]['id']}'")

            if fileinfo_db:
                if fileinfo_db[0]['name'] != file['name'] and fileinfo_db[0]['createat'] != zoho_cliq_message['time'] and fileinfo_db[0]['creatorid'] != sender_id[0]['id']:
                    MatterSqlClient.sql_post(
                        table_name="posts", attrs=posts_keys, values=xml_posts_values)
                    MatterSqlClient.sql_post(
                        table_name="fileinfo", attrs=fileinfo_keys, values=type_xml_fileinfo_values)
            else:
                MatterSqlClient.sql_post(
                        table_name="posts", attrs=posts_keys, values=xml_posts_values)
                MatterSqlClient.sql_post(
                    table_name="fileinfo", attrs=fileinfo_keys, values=type_xml_fileinfo_values)
        else:
            print(f"sender_name {sender_name} not found in users db mattermost")

    except Exception as e:
        save_logs(e)

def get_timestamp() -> int:
        return int(time.time() * 1000)

def channel_extras(channel_id,rec_ids, channel_members, channel_id_reverse, rec_ids_reverse, prefrence_keys):
    notify_props = {
            "push": "default",
            "email": "default",
            "desktop": "default",
            "mark_unread": "all",
            "ignore_channel_mentions": "default"
        }
    member_1 = [channel_id, rec_ids[0], "",
                            0, 0, 0, json.dumps(notify_props), get_timestamp(), json.dumps(True), json.dumps(False), json.dumps(True), 0, 0]
    if channel_id:
        channel_member1 = MatterSqlClient.sql_get("channelmembers", "channelid,userid", f"channelid='{channel_id}' and userid='{rec_ids[0]}'")
        if channel_member1:
            if channel_member1[0]['channelid'] != channel_id and channel_member1[0]['userid'] != rec_ids[0]:
                MatterSqlClient.sql_post(
                    table_name='channelmembers', attrs=channel_members, values=member_1)
        else:
            MatterSqlClient.sql_post(
                    table_name='channelmembers', attrs=channel_members, values=member_1)
    
    channel_member1_reverse = MatterSqlClient.sql_get("channelmembers", "channelid,userid", f"channelid='{channel_id_reverse}' and userid='{rec_ids_reverse[0]}'")
    member_1_reverse = [channel_id_reverse, rec_ids_reverse[0], "",
                0, 0, 0, json.dumps(notify_props), get_timestamp(), json.dumps(True), json.dumps(False), json.dumps(True), 0, 0]
    if channel_member1_reverse:
        if channel_member1_reverse[0]['channelid'] != channel_id_reverse and channel_member1_reverse[0]['userid'] != rec_ids_reverse[0]:
            MatterSqlClient.sql_post(
                table_name='channelmembers', attrs=channel_members, values=member_1_reverse)
    else:
        MatterSqlClient.sql_post(
            table_name='channelmembers', attrs=channel_members, values=member_1_reverse)

    channel_member2 = MatterSqlClient.sql_get("channelmembers", "channelid,userid", f"channelid='{channel_id}' and userid='{rec_ids[1]}'")
    member_2 = [channel_id, rec_ids[1], "",
                0, 0, 0, json.dumps(notify_props), get_timestamp(), json.dumps(True), json.dumps(False), json.dumps(True), 0, 0]
    if channel_member2:
        if channel_member2[0]['channelid'] != channel_id and channel_member2[0]['userid'] != rec_ids[1]:
            MatterSqlClient.sql_post(
                table_name='channelmembers', attrs=channel_members, values=member_2)
    else:
        MatterSqlClient.sql_post(
                table_name='channelmembers', attrs=channel_members, values=member_2)

    # chdelete from channels where type='D' and id not inname='channelmembers', attrs=channel_members, values=member_2_reverse)

    # insert in prefrence Table
    prefrence_values_channel_show1 = [rec_ids[0],
                                        "direct_channel_show", rec_ids[1], "true"]
    MatterSqlClient.sql_post(
        table_name='preferences', attrs=prefrence_keys, values=prefrence_values_channel_show1)

    prefrence_values_open_time1 = [rec_ids[0],
                                    "channel_open_time", channel_id, get_timestamp()]
    MatterSqlClient.sql_post(
        table_name='preferences', attrs=prefrence_keys, values=prefrence_values_open_time1)
    
    prefrence_values_open_time1_reverse = [rec_ids_reverse[0],
                                    "channel_open_time", channel_id_reverse, get_timestamp()]
    MatterSqlClient.sql_post(
        table_name='preferences', attrs=prefrence_keys, values=prefrence_values_open_time1_reverse)

    prefrence_values_channel_show2 = [rec_ids[1],
                                        "direct_channel_show", rec_ids[0], "true"]
    MatterSqlClient.sql_post(
        table_name='preferences', attrs=prefrence_keys, values=prefrence_values_channel_show2)

    prefrence_values_open_time2 = [rec_ids[1],
                                    "channel_open_time", channel_id, get_timestamp()]

    MatterSqlClient.sql_post(
        table_name='preferences', attrs=prefrence_keys, values=prefrence_values_open_time2)
    
    prefrence_values_open_time2_reverse = [rec_ids_reverse[1],
                                    "channel_open_time", channel_id_reverse, get_timestamp()]

    MatterSqlClient.sql_post(
        table_name='preferences', attrs=prefrence_keys, values=prefrence_values_open_time2_reverse)