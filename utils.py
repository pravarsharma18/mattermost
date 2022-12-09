
from sql import MatterSqlClient, ZohoSqlClient
import string
import random
import logging
import traceback
import sys
from datetime import datetime
import time
from decouple import config
import requests

logger = logging.getLogger('StackDriverHandler')

def generate_id(size_of_id) -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size_of_id))

def card_propeties_values_id(properties, name, value) -> list:
    for property in properties:
        if property['name'] == name:
            return list(filter(lambda x: x['value'] == value, property['options']))


def card_propety_id(properties, name) -> str:
    for property in properties:
        if property['name'] == name:
            return property['id']


def get_owners_id(owners) -> list:
    owners_id = []
    for owner in owners:
        u = MatterSqlClient.sql_get(
            'users', 'id', f"username like '%{owner}%'")
        try:
            owners_id.append(u[0]['id'])
        except:
            pass
    return owners_id


def get_owners_id_by_email(owners) -> list:
    owners_id = []
    for owner in owners:
        u = MatterSqlClient.sql_get(
            'users', 'id', f"email like '%{owner}%'")
        try:
            owners_id.append(u[0]['id'])
        except:
            pass
    return owners_id


def create_new_column(old_columns, columns, table_name):
    new_columns = [i for i in old_columns if i not in columns]
    if new_columns:  # Alter table columns as per field from api.
        for column in new_columns:
            ZohoSqlClient.update_table_column(
                table_name, f'"{column}"')


def remove_punctions(value):
    if type(value) != str:
        return value
    punctuations = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~'
    return '.'.join("".join([i for i in value if i not in punctuations]).split()).lower()

def save_logs(e):
    print('print_exception():')
    var = traceback.format_exc()
    print(var)
    print(e)
    ZohoSqlClient.sql_post(
                table_name="logs", attrs=['data'], values=[var])


def replace_escape_characters(value):
    return value.replace("'", "''")

def get_timestamp_from_date(date) -> int:
        if date != 'NaN':
            date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
            tuple = date.timetuple()
            return int(time.mktime(tuple) * 1000)
        else:
            return int(time.time() * 1000)
    

def check_token_revoke_project(request):
    base_url = "https://accounts.zoho.in/oauth/v2/token"
    refresh_token = config('ZOHO_PROJECT_REFRESH_TOKEN')
    grant_type = "refresh_token"
    scope = config('SCOPE')
    client_id = config('CLIENT_ID')
    client_secret = config('CLIENT_SECRET')
    url = f"{base_url}?refresh_token={refresh_token}&grant_type={grant_type}&scope={scope}&client_id={client_id}&client_secret={client_secret}"
    
    new_access_token = None
    if request.status_code == 401:
        print("Generating new access token for PROJECT...")
        new_request = requests.post(url)
        new_token_request = new_request.json()

        new_access_token = new_token_request.get('access_token')
        # sys.exit()
        new_row=[]
        with open('.env', 'r') as f:
            rows = f.readlines()
        for row in rows:
            if 'ZOHO_PROJECT_API_KEY' == row.split('=')[0]:
                row = row.replace(row, f"{row.split('=')[0]}={new_access_token}\n")
            new_row.append(row)

        with open('.env', 'w') as f:
            f.writelines( new_row )
        print(f"new access token for PROJECT is ... {new_access_token}")
    return new_access_token


def check_token_revoke_cliq(request):
    base_url = "https://accounts.zoho.in/oauth/v2/token"
    refresh_token = config('ZOHO_CLIQ_REFRESH_TOKEN')
    grant_type = "refresh_token"
    scope = config('SCOPE')
    client_id = config('CLIENT_ID')
    client_secret = config('CLIENT_SECRET')
    url = f"{base_url}?refresh_token={refresh_token}&grant_type={grant_type}&scope={scope}&client_id={client_id}&client_secret={client_secret}"
    
    new_access_token = None
    if request.status_code == 401:
        print("Generating new access token for CLIQ...")
        new_request = requests.post(url)
        new_token_request = new_request.json()

        new_access_token = new_token_request.get('access_token')
        # sys.exit()
        new_row=[]
        with open('.env', 'r') as f:
            rows = f.readlines()
        for row in rows:
            if 'ZOHO_CLIQ_API_KEY' == row.split('=')[0]:
                row = row.replace(row, f"{row.split('=')[0]}={new_access_token}\n")
            new_row.append(row)

        with open('.env', 'w') as f:
            f.writelines( new_row )
        print(f"new access token for CLIQ is ... {new_access_token}")
    return new_access_token