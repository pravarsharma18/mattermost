
from sql import MatterSqlClient
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
