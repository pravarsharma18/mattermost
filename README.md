# Installation Guide

### Install mattermost using postgres db:

    - Follow the instructions from below link
    - https://docs.mattermost.com/install/installing-ubuntu-2004-LTS.html

### Create database named 'zoho' in postgres.

### Rename .env.settings to .env and replace add zoho api token and path to mattermost upto '/data' folder

### Create Virtual environment, activate it.

```
python3 -m venv env
```

```
source env/bin/activate
```

```
pip install -r requirements.txt
```

### Run mattermost on local port 8065

```
cd /opt/mattermost && sudo -u mattermost ./bin/mattermost
```

```
./start.sh
```

## Manual commands in case error in above command

### Delete data from Mattermost and Zoho DB.

```
python3 truncate_tables.py
```

### Create Zoho Tables in local DB

```
python3 create_zoho_tables.py
```

### Get Data from zoho API and Insert in Local DB

```
python3 zoho_client.py
```

### Get Insert in Mattermost DB

```
python3 mattermost_client.py
```
