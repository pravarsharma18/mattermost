from sql import ZohoSqlClient
import json
import pandas as pd


def save_log_file():
    logs = ZohoSqlClient.sql_get('logs')
    df = pd.DataFrame(logs)
    df.to_csv('logs.xlsx')

save_log_file()