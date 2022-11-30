
from sql import MatterSqlClient, ZohoSqlClient
import string
import random
import logging
import traceback
import sys

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
    punctuations = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~'
    return '.'.join("".join([i for i in value if i not in punctuations]).split()).lower()

def save_logs():
    print('print_exception():')
    var = traceback.format_exc()
    print(var)
    ZohoSqlClient.sql_post(
                table_name="logs", attrs=['data'], values=[var])