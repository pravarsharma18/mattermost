import json
import pathlib
import requests
from PIL import Image
from decouple import config
from datetime import datetime

from sql import MatterSqlClient
from utils import remove_punctions, generate_id
from zoho_client import ZohoClient

mattermost_base_path = config('MATTERMOST_PATH')
# {mattermost_base_path}
def image_data(channel_id, zoho_cliq_message, file, posts_keys, fileinfo_keys):
    try:
        timestamp = int(zoho_cliq_message['time'])
        date_folder = datetime.strftime(
            datetime.fromtimestamp(timestamp / 1000), '%Y%m%d')
        id_channel = channel_id[0]['id']
        sender_name = remove_punctions(
            json.loads(zoho_cliq_message['sender'])['name'])
        sender_id = MatterSqlClient.sql_get(
            "users", 'id', f"username like '%{sender_name}%'")

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

        r = requests.get(
            f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                "Authorization": f"Zoho-oauthtoken {ZohoClient().access_token}",
                "Content-Type": 'application/json'
            }).content
        with open(f"{file_path}{file['name']}", 'wb') as handler:
            handler.write(r)
        im = Image.open(f"{file_path}{file['name']}")

        im.save(file_path + '_preview.'.join(file['name'].split('.')),
                optimize=True)
        newsize = (120, 120)
        im1 = im.resize(newsize)
        im1.save(file_path + '_thumb.'.join(file['name'].split('.')), optimize=True)
        MatterSqlClient.sql_post(
            table_name="posts", attrs=posts_keys, values=image_posts_values)

        MatterSqlClient.sql_post(
            table_name="fileinfo", attrs=fileinfo_keys, values=type_images_fileinfo_values)
    except:
        pass


def xlsx_data(channel_id, zoho_cliq_message, file, posts_keys, fileinfo_keys, type):
        print(type)
    # try:
        timestamp = int(zoho_cliq_message['time'])
        date_folder = datetime.strftime(
        datetime.fromtimestamp(timestamp / 1000), '%Y%m%d')
        id_channel = channel_id[0]['id']
        sender_name = remove_punctions(
            json.loads(zoho_cliq_message['sender'])['name'])
        sender_id = MatterSqlClient.sql_get(
            "users", 'id', f"username='{sender_name}'")

        xml_fileinfo_id = generate_id(26)
        posts_id = generate_id(26)
        path = f"{date_folder}/teams/noteam/channels/{id_channel}/users/{sender_id[0]['id']}/{xml_fileinfo_id}/"
        file_path = f"{mattermost_base_path}{path}" 
        print(file['name'])
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
        
        r = requests.get(
            f"https://cliq.zoho.in/api/v2/files/{file['id']}", headers={
                "Authorization": f"Zoho-oauthtoken {ZohoClient().access_token}",
                "Content-Type": 'application/json'
            }).content
        with open(f"{file_path}{file['name']}", 'wb') as f:
            f.write(r)
        
        MatterSqlClient.sql_post(
            table_name="posts", attrs=posts_keys, values=xml_posts_values)

        MatterSqlClient.sql_post(
            table_name="fileinfo", attrs=fileinfo_keys, values=type_xml_fileinfo_values)
    # except:
    #     pass
    