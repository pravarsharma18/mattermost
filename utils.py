
from sql import MatterSqlClient, ZohoSqlClient
import json


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


def create_new_column(old_columns, columns, table_name):
    new_columns = [i for i in old_columns if i not in columns]
    if new_columns:  # Alter table columns as per field from api.
        for column in new_columns:
            ZohoSqlClient.update_table_column(
                table_name, f"{column}")
