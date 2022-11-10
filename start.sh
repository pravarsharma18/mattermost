#!/bin/bash

### Delete data from Mattermost and Zoho DB.

python3 truncate_tables.py

### Create Zoho Tables in local DB

# python3 create_zoho_tables.py

### Get Data from zoho API and Insert in Local DB

python3 zoho_client.py

python3 zoho_cliq_client.py

### Get Insert in Mattermost DB

python3 mattermost_client.py
python3 mattermost_channel_client.py

echo "<============================Data Migration Completed============================>"
