from pprint import pprint
import psycopg2
from datetime import datetime
import json
import time
import random
import sys
import string
from sql import ZohoSqlClient, MatterSqlClient
from models import CardProperties
from bs4 import BeautifulSoup
from colorama import Fore
import pandas as pd
from utils import card_propeties_values_id, card_propety_id, get_owners_id, get_owners_id_by_email, remove_punctions


class MattermostClient:
    def get_timestamp_from_date(self, date) -> int:
        date = datetime.strptime(date, "%m-%d-%Y")
        tuple = date.timetuple()
        return int(time.mktime(tuple) * 1000)

    def get_timestamp(self) -> int:
        return int(time.time() * 1000)

    def generate_id(self, size_of_id) -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size_of_id))

    def create_user_data(self) -> list:
        users = ZohoSqlClient.sql_get(
            "portal_users", "name, role, email,role_name")
        li = []
        keys = "id,createat,updateat,deleteat,username,password,authservice,email,nickname,firstname,lastname,emailverified,roles,allowmarketing,props,notifyprops,lastpasswordupdate,lastpictureupdate,failedattempts,locale,mfasecret,position,mfaactive,timezone"
        if not users:
            print(Fore.RED + "Zoho Database might be empty.")
            sys.exit()
        for user in users:
            if user['role_name'].lower() == ("administrator" or "manager"):
                roles = "system_user system_admin"
            else:
                roles = "system_user"
            notify_props = {
                "push": "mention",
                "email": "true",
                "channel": "true",
                "desktop": "mention",
                "comments": "never",
                "first_name": "false",
                "push_status": "away",
                "mention_keys": "",
                "push_threads": "all",
                "desktop_sound": "true",
                "email_threads": "all",
                "desktop_threads": "all"
            }
            time_zone = {
                "manualTimezone": "",
                "automaticTimezone": "Asia/Calcutta",
                "useAutomaticTimezone": "true"
            }
            values = [self.generate_id(26), self.get_timestamp(
            ), self.get_timestamp(), 0, user['name'], "Demo@1234", "", user['email'], "", "", "", json.dumps(False), roles, json.dumps(False), json.dumps({}), json.dumps(notify_props), 0, 0, 0, 'en', "", "", json.dumps(False), json.dumps(time_zone)]

            li.append(dict(zip(keys.split(','), values)))
        return li

    def insert_user_data(self) -> None:
        users = self.create_user_data()
        for user in users:
            values = user.values()
            project_list = [i for i in values]
            users_values = [json.loads(json.dumps(v)) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int) or isinstance(v, str)) else v for i, v in enumerate(
                project_list)]

            MatterSqlClient.sql_post(
                table_name="users", attrs=user.keys(), values=users_values)
        print(Fore.GREEN + "## Inserted User data ##")

    def insert_team_data(self) -> None:
        portals = ZohoSqlClient.sql_get(
            "portals", "name")
        users = ZohoSqlClient.sql_get(
            "portal_users", 'email', "role like '%Portal Owner%'")
        keys = MatterSqlClient.get_columns("teams")
        for portal in portals:
            values = [self.generate_id(
                26), self.get_timestamp(), self.get_timestamp(), 0, portal['name'], portal['name'].lower(), "", f"{users[0]['email']}", "O", "", "", self.generate_id(32), json.dumps(None), json.dumps(False), 0, json.dumps(False), json.dumps(False)]
            MatterSqlClient.sql_post(
                table_name="teams", attrs=keys, values=values)

        print(Fore.GREEN + "## Inserted Teams data ##")

    def create_channel(self) -> None:
        team = MatterSqlClient.sql_get("teams", "id")
        # MatterSqlClient.sql_update("channels", f"teamid='{team[0]['id']}'")
        keys = ['id', 'createat', 'updateat', 'deleteat', 'teamid', 'type', 'displayname', 'name',
                'header', 'purpose', 'lastpostat', 'totalmsgcount', 'extraupdateat', 'creatorid', 'totalmsgcountroot', 'lastrootpostat']
        values = [self.generate_id(26), self.get_timestamp(), self.get_timestamp(), 0, team[0]['id'],
                  "O", "Town Square", "town-square", "", "", self.get_timestamp(), 0, 0, "", 0, self.get_timestamp()]

        MatterSqlClient.sql_post(
            table_name="channels", attrs=keys, values=values)
        print(Fore.GREEN + "## Updated team id in channels ##")

    def insert_channel_members_data(self) -> None:
        channels = MatterSqlClient.sql_get(
            "channels", "id", "name='town-square'")
        users = MatterSqlClient.sql_get(
            'users', 'id,roles,username', "username not in ('channelexport','system-bot','boards','playbooks','appsbot','feedbackbot')")
        keys = MatterSqlClient.get_columns("channelmembers")
        notify_props = {
            "push": "default",
            "email": "default",
            "desktop": "default",
            "mark_unread": "all",
            "ignore_channel_mentions": "default"
        }
        for channel in channels:
            for user in users:
                if "system_admin" in user['roles']:
                    schemeadmin = json.dumps(True)
                else:
                    schemeadmin = json.dumps(False)
                values = [channel['id'], user['id'], "",
                          0, 0, 0, json.dumps(notify_props), self.get_timestamp(), json.dumps(True), schemeadmin, json.dumps(True), 0, 0]
                MatterSqlClient.sql_post(
                    table_name="channelmembers", attrs=keys, values=values)
        print(Fore.GREEN + "## Channels data saved ##")

    def insert_team_members_data(self) -> None:
        teams = MatterSqlClient.sql_get('teams', 'id')
        users = ZohoSqlClient.sql_get(
            'portal_users', 'id,role_name,email')
        keys = MatterSqlClient.get_columns("teammembers")
        for team in teams:
            for user in users:
                if user['role_name'].lower() == "administrator":
                    schemeadmin = json.dumps(True)
                else:
                    schemeadmin = json.dumps(False)
                user_id = MatterSqlClient.sql_get(
                    'users', 'id', f"email='{user['email']}'")
                values = [team['id'], user_id[0]['id'], "", 0,
                          json.dumps(True), schemeadmin, json.dumps(False), self.get_timestamp()]
                # print("user_id", user_id, user['name'])
                MatterSqlClient.sql_post(
                    table_name="teammembers", attrs=keys, values=values)
        print(Fore.GREEN + "## Inserted Team Members data ##")

    def insert_focalboard_boards_data(self) -> None:
        keys = MatterSqlClient.get_columns("focalboard_boards")
        teams = MatterSqlClient.sql_get('teams', 'id')
        projects = ZohoSqlClient.sql_get(
            'projects', 'created_by,name,description,created_date,updated_date')
        card = CardProperties(name="Select", type="select", option_names=[
            "Backlog", "Open", "In Progress", "PR-Submitted", "In Review", "Re-open", "Closed"])
        person = CardProperties(name="Created By", type="person")
        multi_person = CardProperties(name="Assignee", type="multiPerson")
        card_properties = [
            card.get_cards(), person.get_cards(), multi_person.get_cards()]
        for team in teams:
            for project in projects:
                user_id = MatterSqlClient.sql_get(
                    'users', 'id', f"username like '%{project['created_by']}%'")
                values = [self.generate_id(26), datetime.now(
                ), team['id'], "", user_id[0]['id'], user_id[0]['id'], "P", project['name'], BeautifulSoup(project['description'], "html.parser").get_text(), "", json.dumps(True), json.dumps(False), 4, json.dumps({}), json.dumps(card_properties), self.get_timestamp_from_date(project['created_date']), self.get_timestamp_from_date(project['updated_date']), 0, ""]
                MatterSqlClient.sql_post(
                    table_name="focalboard_boards", attrs=keys, values=values)
        print(Fore.GREEN + "## Inserted focalboard data ##")

    def insert_focalboard_board_members_data(self) -> None:
        keys = MatterSqlClient.get_columns("focalboard_board_members")
        boards = MatterSqlClient.sql_get(
            'focalboard_boards', 'id,title,created_by', "created_by!='system'")
        for board in boards:
            users = ZohoSqlClient.sql_get(
                'project_users', 'email,portal_role_name', f"project_name like '%{board['title']}%'")
            # print(users)
            for user in users:
                user_id = MatterSqlClient.sql_get(
                    'users', 'id', f"email='{user['email']}'")
                if ("Administrator" or "Manager") in user['portal_role_name']:
                    scheme_admin = True
                    scheme_editor = True
                    scheme_commenter = True
                    scheme_viewer = True
                else:
                    scheme_admin = False
                    scheme_editor = False
                    scheme_commenter = True
                    scheme_viewer = True
                values = [board['id'], user_id[0]['id'], "", json.dumps(
                    scheme_admin), json.dumps(scheme_editor), json.dumps(scheme_commenter), json.dumps(scheme_viewer)]
                MatterSqlClient.sql_post(
                    table_name="focalboard_board_members", attrs=keys, values=values)
        print(Fore.GREEN + "## Inserted focalboard members data ##")

    def insert_focalblocks_view_data(self) -> None:
        keys = MatterSqlClient.get_columns("focalboard_blocks")
        boards = MatterSqlClient.sql_get(
            'focalboard_boards', 'id,title,created_by,card_properties', "created_by!='system'")
        tasks = ZohoSqlClient.sql_get(
            'tasks', 'project_name,status,name,created_person')
        for board in boards:
            person_id = card_propety_id(
                board['card_properties'], "Assignee")
            for task in tasks:
                user_id = MatterSqlClient.sql_get(
                    'users', 'id', f"username like '%{task['created_person']}%'")
                if board['title'] == task['project_name']:
                    values = [self.generate_id(
                        27), datetime.now(), self.generate_id(27), 1, "view", f"By Status", json.dumps({"visiblePropertyIds": [person_id]}), self.get_timestamp(), self.get_timestamp(), 0, json.dumps(None), user_id[0]['id'], "", user_id[0]['id'], board['id']]
                    MatterSqlClient.sql_post(
                        table_name="focalboard_blocks", attrs=keys, values=values)
                break
        print(Fore.GREEN + "## Inserted Focal Blocks Views ##")

    def insert_focalblocks_card_data(self) -> None:
        keys = MatterSqlClient.get_columns("focalboard_blocks")
        boards = MatterSqlClient.sql_get(
            'focalboard_boards', 'id,title,created_by,card_properties', "created_by!='system'")
        tasks = ZohoSqlClient.sql_get(
            'tasks', 'project_name,status,name,details,description,created_person')

        for board in boards:
            select_id = card_propety_id(
                board['card_properties'], "Select")
            person_id = card_propety_id(
                board['card_properties'], "Created By")
            assign_id = card_propety_id(
                board['card_properties'], "Assignee")
            # print(select_id)
            # print(person_id)
            # print(assign_id)
            for task in tasks:
                assignees = list(owner['email']
                                 for owner in json.loads(task['details'])['owners'])
                assignee_ids = get_owners_id_by_email(assignees)
                user_id = MatterSqlClient.sql_get(
                    'users', 'id', f"username like '%{task['created_person']}%'")
                if board['title'] == task['project_name']:
                    type_ = json.loads(task['status'])['name']
                    type_id = card_propeties_values_id(
                        board['card_properties'], "Select", type_)
                    task_description = BeautifulSoup(
                        task['description'], "html.parser").get_text()
                    description_id = ""
                    if task_description:
                        description_values = [self.generate_id(
                            27), datetime.now(), self.generate_id(27), 1, "text", f"{task_description}", json.dumps({}), self.get_timestamp(), self.get_timestamp(), 0, json.dumps(None), user_id[0]['id'], "", user_id[0]['id'], board['id']]
                        description_id = MatterSqlClient.sql_post(
                            table_name='focalboard_blocks', attrs=keys, values=description_values, returning='id')
                    fields = {
                        "contentOrder": [description_id],
                        "isTemplate": False,
                        "properties": {
                            select_id: type_id[0]['id'],  # status
                            person_id: user_id[0]['id'],
                            assign_id: assignee_ids,
                        }
                    }
                    values = [self.generate_id(
                        27), datetime.now(), self.generate_id(27), 1, "card", f"{task['name']}", json.dumps(fields), self.get_timestamp(), self.get_timestamp(), 0, json.dumps(None), user_id[0]['id'], "", user_id[0]['id'], board['id']]
                    card_id = MatterSqlClient.sql_post(
                        table_name="focalboard_blocks", attrs=keys, values=values, returning="id")
                    if description_id:
                        '''
                        update the parent id of text component
                        '''
                        MatterSqlClient.sql_update(
                            table_name="focalboard_blocks", set=f"parent_id='{card_id}'", where=f"id='{description_id}'")
        print(Fore.GREEN + "## Inserted Focal Blocks Cards ##")

    def main(self) -> None:
        self.insert_user_data()
        time.sleep(1)
        self.insert_team_data()
        time.sleep(1)
        self.create_channel()
        time.sleep(1)
        self.insert_channel_members_data()
        time.sleep(1)
        self.insert_team_members_data()
        time.sleep(1)
        self.insert_focalboard_boards_data()
        time.sleep(1)
        self.insert_focalboard_board_members_data()
        time.sleep(1)
        self.insert_focalblocks_view_data()
        time.sleep(1)
        self.insert_focalblocks_card_data()


if __name__ == "__main__":
    print(Fore.YELLOW + "\n<========Saving Mattermost Data in DB=========>\n")
    client = MattermostClient()
    # client.save_focalboard_boards()
    # client.save_focalboard_board_members()
    # client.create_team()
    client.main()
    # client.insert_channel_members_data()
    # client.insert_focalboard_board_members_data()
    # client.insert_focalblocks_card_data()
