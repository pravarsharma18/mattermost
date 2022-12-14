from pprint import pprint
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
from utils import ( card_propeties_values_id, card_propety_id, get_owners_id_by_email, 
                    save_logs,remove_punctions, replace_escape_characters 
                    ) 


class MattermostClient:
    def get_timestamp_from_date(self, date) -> int:
            date = datetime.strptime(date, "%m-%d-%Y")
            tuple = date.timetuple()
            return int(time.mktime(tuple) * 1000)

    def get_timestamp(self) -> int:
        return int(time.time() * 1000)

    def generate_id(self, size_of_id) -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size_of_id))

    def create_user_data(self, users) -> list:
        li = []
        keys = "id,createat,updateat,deleteat,username,password,authservice,email,nickname,firstname,lastname,emailverified,roles,allowmarketing,props,notifyprops,lastpasswordupdate,lastpictureupdate,failedattempts,locale,mfasecret,position,mfaactive,timezone"
        if not users:
            print(Fore.RED + "Zoho Database might be empty.")
            sys.exit()
        for user in users:
            if user.get('role_name'):
                if user['role_name'].lower() == ("administrator" or "manager"):
                    roles = "system_user system_admin"
                else:
                    roles = "system_user"
            else:
                roles = "system_user"
            if user.get('email'):
                email = user.get('email')
            else:
                email = user.get('email_id')
            
            username = user['name']
            if "trooinbound.com" in email:
                username += ".tib"

            if user.get('active'):
                if not user.get('active'):
                    delete_at = self.get_timestamp()
                else:
                    delete_at = 0
            else:
                if not user.get('status'):
                    delete_at = self.get_timestamp()
                else:
                    delete_at = 0
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
            ), self.get_timestamp(), delete_at, username, "$2a$10$DuCXLy27NqVbs/6j6fY5/.hWXqCtmlS0QAodGp4p7D2IGRNBKJgiG", "", email, "", "", "", json.dumps(False), roles, json.dumps(False), json.dumps({}), json.dumps(notify_props), 0, 0, 0, 'en', "", "", json.dumps(False), json.dumps(time_zone)]

            li.append(dict(zip(keys.split(','), values)))
        return li

    def insert_portal_user_data(self) -> None:
        portal_users = ZohoSqlClient.sql_get(
            "portal_users", "name, email, role_name, active")
        users = self.create_user_data(portal_users)
        for user in users:
            try:
                values = user.values()
                project_list = [i for i in values]
                users_values = [json.loads(json.dumps(v)) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int) or isinstance(v, str)) else v for i, v in enumerate(
                    project_list)]
                user_name = MatterSqlClient.sql_get("users", "email", f"email='{user['email']}'")
                if user_name:
                    if user_name[0].get("email") != user['email']:
                        MatterSqlClient.sql_post(
                        table_name="users", attrs=user.keys(), values=users_values)
                else:
                    MatterSqlClient.sql_post(
                        table_name="users", attrs=user.keys(), values=users_values)
            except Exception as e:
                print(f"Exception saving portal user data: {user['email']}")
                save_logs(e)
        print(Fore.GREEN + "## Inserted User data ##")
    
    def insert_cliq_user_data(self) -> None:
        cliq_users = ZohoSqlClient.sql_get(
            "cliq_users", "name,email_id,status")
        users = self.create_user_data(cliq_users)
        for user in users:
            try:
                values = user.values()
                project_list = [i for i in values]
                users_values = [json.loads(json.dumps(v)) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int) or isinstance(v, str)) else v for i, v in enumerate(
                    project_list)]
                user_name = MatterSqlClient.sql_get("users", "email", f"email='{user['email']}'")
                if user_name:
                    if user_name[0].get("email") != user['email']:
                        MatterSqlClient.sql_post(
                        table_name="users", attrs=user.keys(), values=users_values)
                else:
                    MatterSqlClient.sql_post(
                        table_name="users", attrs=user.keys(), values=users_values)
            except Exception as e:
                print(f"Exception saving cliq user data: {user['email']}")
                save_logs(e)
        print(Fore.GREEN + "## Inserted User data ##")

    def insert_team_data(self) -> None:
        portals = ZohoSqlClient.sql_get(
            "portals", "name,portal_owner")
        keys = MatterSqlClient.get_columns("teams")
        for portal in portals:
            try:
                owner = json.loads(portal['portal_owner'])
                portal_name = MatterSqlClient.sql_get("teams", "name", f"name='{portal['name']}' ")
                values = [self.generate_id(
                    26), self.get_timestamp(), self.get_timestamp(), 0, portal['name'], portal['name'].lower(), "", f"{owner['email']}", "O", "", "", self.generate_id(32), json.dumps(None), json.dumps(False), 0, json.dumps(False), json.dumps(False)]
                if portal_name:
                    if portal_name[0].get("name") != portal['name']:
                        MatterSqlClient.sql_post(
                            table_name="teams", attrs=keys, values=values)
                else:
                    MatterSqlClient.sql_post(
                            table_name="teams", attrs=keys, values=values)
            except Exception as e:
                print(f"Exception saving team data: {portal['name']}")
                save_logs(e)
        
        ## Team for trooinbound
        # dipen.patel@trootech.com
        portal_name = MatterSqlClient.sql_get("teams", "name", f"name='trooinbound'")
        p_name = "TrooInBound"
        values = [self.generate_id(
                    26), self.get_timestamp(), self.get_timestamp(), 0, p_name, p_name.lower(), "", f"dipen.patel@trootech.com", "O", "", "", self.generate_id(32), json.dumps(None), json.dumps(False), 0, json.dumps(False), json.dumps(False)]

        if not portal_name:
            MatterSqlClient.sql_post(
                table_name="teams", attrs=keys, values=values)
    
        print(Fore.GREEN + "## Inserted Teams data ##")

    def create_channel(self) -> None:
        teams = MatterSqlClient.sql_get("teams", "id")
        for team in teams:
            try:
                # MatterSqlClient.sql_update("channels", f"teamid='{team[0]['id']}'")
                keys = ['id', 'createat', 'updateat', 'deleteat', 'teamid', 'type', 'displayname', 'name',
                        'header', 'purpose', 'lastpostat', 'totalmsgcount', 'extraupdateat', 'creatorid', 'totalmsgcountroot', 'lastrootpostat']
                values = [self.generate_id(26), self.get_timestamp(), self.get_timestamp(), 0, team['id'],
                        "O", "Town Square", "town-square", "", "", self.get_timestamp(), 0, 0, "", 0, self.get_timestamp()]
                channel_name = MatterSqlClient.sql_get("channels", "name,teamid", f"name='town-square' and teamid='{team['id']}'")
                if channel_name:
                    if channel_name[0].get("teamid") != team['id'] and channel_name[0].get("name") != 'town-square':
                        MatterSqlClient.sql_post(
                            table_name="channels", attrs=keys, values=values)
                else:
                    MatterSqlClient.sql_post(
                            table_name="channels", attrs=keys, values=values)
            except Exception as e:
                print(f"Exception saving team data: {team['name']}")
                save_logs(e)
        print(Fore.GREEN + "## Channel Created ##")

    def insert_channel_members_data(self) -> None:
            team_trooinbound = MatterSqlClient.sql_get(
                    "teams", "id", "name='trooinbound'")
            team_trootech = MatterSqlClient.sql_get(
                    "teams", "id", "name='trootech'")
            channels_trootech = MatterSqlClient.sql_get(
                "channels", "id", f"name='town-square' and teamid='{team_trootech[0]['id']}'")
            channels_trooinbound = MatterSqlClient.sql_get(
                "channels", "id", f"name='town-square' and teamid='{team_trooinbound[0]['id']}'")
            channels = MatterSqlClient.sql_get(
                "channels", "id", f"name='town-square'")
            users = MatterSqlClient.sql_get(
                'users', 'id,roles,username,email', "username not in ('channelexport','system-bot','boards','playbooks','appsbot','feedbackbot')")
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
                    try:
                        if "system_admin" in user['roles']:
                            schemeadmin = json.dumps(True)
                        else:
                            schemeadmin = json.dumps(False)
                        
                        values = [channel['id'], user['id'], "",
                                    0, 0, 0, json.dumps(notify_props), self.get_timestamp(), json.dumps(True), schemeadmin, json.dumps(True), 0, 0]

                        if "trooinbound" in user['email']:
                            values = [channels_trooinbound[0]['id'], user['id'], "",
                                    0, 0, 0, json.dumps(notify_props), self.get_timestamp(), json.dumps(True), schemeadmin, json.dumps(True), 0, 0]

                            channel_members = MatterSqlClient.sql_get("channelmembers", "channelid,userid", f"channelid='{channels_trooinbound[0]['id']}' and userid='{user['id']}'")
                        else:
                            values = [channels_trootech[0]['id'], user['id'], "",
                                    0, 0, 0, json.dumps(notify_props), self.get_timestamp(), json.dumps(True), schemeadmin, json.dumps(True), 0, 0]

                            channel_members = MatterSqlClient.sql_get("channelmembers", "channelid,userid", f"channelid='{channels_trootech[0]['id']}' and userid='{user['id']}'")

                        if channel_members:
                            if channel_members[0].get("channelid") != (channels_trootech[0]['id'] or channels_trooinbound[0]['id']) and channel_members[0].get("userid") != user['id']:
                                MatterSqlClient.sql_post(
                                    table_name="channelmembers", attrs=keys, values=values)
                        else:
                            MatterSqlClient.sql_post(
                                    table_name="channelmembers", attrs=keys, values=values)
                    except Exception as e:
                        print(f"Exception saving team data: {user}")
                        save_logs(e)
            print(Fore.GREEN + "## Channels data saved ##")

    def insert_team_members_data(self) -> None:
            teams = MatterSqlClient.sql_get('teams', 'id', "name='trootech'")
            users = ZohoSqlClient.sql_get(
                'portal_users', 'id,role_name,email')
            keys = MatterSqlClient.get_columns("teammembers")
            for team in teams:
                for user in users:
                    try:
                        if user['role_name'].lower() == "administrator":
                            schemeadmin = json.dumps(True)
                        else:
                            schemeadmin = json.dumps(False)
                        user_id = MatterSqlClient.sql_get(
                            'users', 'id', f"email='{user['email']}'")
                        values = [team['id'], user_id[0]['id'], "", 0,
                                    json.dumps(True), schemeadmin, json.dumps(False), self.get_timestamp()]

                        team_members = MatterSqlClient.sql_get("teammembers", "teamid,userid", f"teamid='{team['id']}' and userid='{user_id[0]['id']}'")

                        if team_members:
                            if team_members[0].get("teamid") != team['id'] and team_members[0].get("userid") != user['id']:
                                MatterSqlClient.sql_post(
                                    table_name="teammembers", attrs=keys, values=values)
                        else:
                            MatterSqlClient.sql_post(
                                    table_name="teammembers", attrs=keys, values=values)
                    except Exception as e:
                        print(f"Exception saving team data: {user}")
                        save_logs(e)
            print(Fore.GREEN + "## Inserted Team Members data ##")
        
    def insert_trooinbound_team_members_data(self) -> None:
            teams = MatterSqlClient.sql_get('teams', 'id', "name='trooinbound'")
            users = MatterSqlClient.sql_get('users', 'id,roles,email')
            keys = MatterSqlClient.get_columns("teammembers")
            for team in teams:
                for user in users:
                    if "trooinbound" in user['email']:
                        try:
                            if user['roles'] == "system_user system_admin":
                                schemeadmin = json.dumps(True)
                            else:
                                schemeadmin = json.dumps(False)
                            user_id = MatterSqlClient.sql_get(
                                'users', 'id', f"email='{user['email']}'")
                            values = [team['id'], user_id[0]['id'], "", 0,
                                        json.dumps(True), schemeadmin, json.dumps(False), self.get_timestamp()]

                            team_members = MatterSqlClient.sql_get("teammembers", "teamid,userid", f"teamid='{team['id']}' and userid='{user_id[0]['id']}'")

                            if team_members:
                                if team_members[0].get("teamid") != team['id'] and team_members[0].get("userid") != user['id']:
                                    MatterSqlClient.sql_post(
                                        table_name="teammembers", attrs=keys, values=values)
                            else:
                                MatterSqlClient.sql_post(
                                        table_name="teammembers", attrs=keys, values=values)
                        except Exception as e:
                            print(f"Exception saving team data: {user}")
                            save_logs(e)
            print(Fore.GREEN + "## Inserted TrooInBound Team Members data ##")

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
                    try:
                        if project.get('description'):
                            description = project.get('description')
                        else:
                            description = ""
                        if project.get('created_date'):
                            created_date = project.get('created_date')
                        else:
                            created_date = ""
                        if project.get('updated_date'):
                            updated_date = project.get('updated_date')
                        else:
                            updated_date = ""
                        user_id = MatterSqlClient.sql_get(
                            'users', 'id', f"username like '%{remove_punctions(project['created_by'])}%'")
                        
                        values = [self.generate_id(26), datetime.now(), team['id'], "", user_id[0]['id'], user_id[0]['id'], "P", project['name'], BeautifulSoup(description, "html.parser").get_text(), "", json.dumps(True), json.dumps(False), 4, json.dumps({}), json.dumps(card_properties), self.get_timestamp_from_date(created_date), self.get_timestamp_from_date(updated_date), 0, ""]

                        focalboard = MatterSqlClient.sql_get("focalboard_boards", "team_id,title", f"team_id='{team['id']}' and title='{project['name']}'")

                        if focalboard:
                            if focalboard[0].get("team_id") != team['id'] and focalboard[0].get("title") != project['name']:
                                MatterSqlClient.sql_post(
                                    table_name="focalboard_boards", attrs=keys, values=values)
                        else:
                            MatterSqlClient.sql_post(
                                    table_name="focalboard_boards", attrs=keys, values=values)
                    except Exception as e:
                        print(f"Exception saving project data: {project}")
                        save_logs(e)
            print(Fore.GREEN + "## Inserted focalboard data ##")

    def insert_focalboard_board_members_data(self) -> None:
            keys = MatterSqlClient.get_columns("focalboard_board_members")
            boards = MatterSqlClient.sql_get(
                'focalboard_boards', 'id,title,created_by', "created_by!='system'")
            for board in boards:
                try:
                    users = ZohoSqlClient.sql_get(
                        'project_users', 'email,portal_role_name', f"project_name like '%{board['title']}%'")
                    for user in users:
                        try:
                            user_id = MatterSqlClient.sql_get(
                                'users', 'id', f"email='{user['email']}'")
                            focalboard_board_members = MatterSqlClient.sql_get("focalboard_board_members", "board_id,user_id", f"board_id='{board['id']}' and user_id='{user_id[0]['id']}'")
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
                            if focalboard_board_members:
                                if focalboard_board_members[0].get("board_id") != board['id'] and focalboard_board_members[0].get("user_id") != user_id[0]['id']:
                                    MatterSqlClient.sql_post(
                                        table_name="focalboard_board_members", attrs=keys, values=values)
                            else:
                                MatterSqlClient.sql_post(
                                        table_name="focalboard_board_members", attrs=keys, values=values)
                        except Exception as e:
                            print(f"Exception saving user data: {user}")
                            save_logs(e)
                except Exception as e:
                    print(f"Exception saving board data: {board}")
                    save_logs(e)
            print(Fore.GREEN + "## Inserted focalboard members data ##")

    def insert_focalblocks_view_data(self) -> None:
            keys = MatterSqlClient.get_columns("focalboard_blocks")
            boards = MatterSqlClient.sql_get(
                'focalboard_boards', 'id,title,card_properties,created_by', "created_by!='system'")
            tasks = ZohoSqlClient.sql_get(
                'tasks', 'project_name,status,name,created_person')
            for board in boards:
                try:
                    person_id = card_propety_id(board['card_properties'], "Assignee")
                    for task in tasks:
                        try:
                            user_id = MatterSqlClient.sql_get('users', 'id', f"username like '%{remove_punctions(task['created_person'])}%'")
                            if not user_id:
                                user_id = [{"id":board["created_by"]}]

                            if user_id:
                                focalboardblocks = MatterSqlClient.sql_get("focalboard_blocks", "board_id", f"board_id='{board['id']}'")
                                if board['title'] == task['project_name']:
                                    values = [self.generate_id(
                                        27), datetime.now(), self.generate_id(27), 1, "view", f"By Status", json.dumps({"visiblePropertyIds": [person_id]}), self.get_timestamp(), self.get_timestamp(), 0, json.dumps(None), user_id[0]['id'], "", user_id[0]['id'], board['id']]

                                    if focalboardblocks:
                                        if focalboardblocks[0].get("board_id") != board['id']:
                                            MatterSqlClient.sql_post(
                                                table_name="focalboard_blocks", attrs=keys, values=values)
                                    else:
                                        MatterSqlClient.sql_post(
                                            table_name="focalboard_blocks", attrs=keys, values=values)
                                    break
                            else:
                                print(f"{remove_punctions(task['created_person'])} user Not Found")
                        except Exception as e:
                            print(f"Exception in getting task data: {task}")
                            save_logs(e)
                except Exception as e:
                    print(f"Exception in getting board data: {board}")
                    save_logs(e)
            print(Fore.GREEN + "## Inserted Focal Blocks Views ##")

    def insert_focalblocks_card_data(self) -> None:
            keys = MatterSqlClient.get_columns("focalboard_blocks")
            boards = MatterSqlClient.sql_get(
                'focalboard_boards', 'id,title,created_by,card_properties', "created_by!='system'")
            tasks = ZohoSqlClient.sql_get(
                'tasks', 'project_name,status,name,details,description,created_person')

            for board in boards:
                try:
                    select_id = card_propety_id(
                        board['card_properties'], "Select")
                    person_id = card_propety_id(
                        board['card_properties'], "Created By")
                    assign_id = card_propety_id(
                        board['card_properties'], "Assignee")
                    for task in tasks:
                        try:
                            assignees = list(owner['email'] for owner in json.loads(task['details'])['owners'])
                            assignee_ids = get_owners_id_by_email(assignees)
                        except:
                            assignee_ids = []
                        user_id = MatterSqlClient.sql_get(
                            'users', 'id', f"username like '%{remove_punctions(task['created_person'])}%'")
                        if not user_id:
                            user_id = [{"id":board["created_by"]}]

                        if user_id:
                            if board['title'] == task['project_name']:
                                type_ = json.loads(task['status'])['name']
                                type_id = card_propeties_values_id(
                                    board['card_properties'], "Select", type_)
                                if type_id:
                                    select_type = type_id[0]['id']
                                else:
                                    select_type = ""
                                if task.get('description'):
                                    task_description = BeautifulSoup(
                                        task['description'], "html.parser").get_text()
                                else:
                                    task_description = ""
                                description_id = ""
                                focalboardblocks_text = MatterSqlClient.sql_get("focalboard_blocks", "board_id,type,title", f"board_id='{board['id']}' and type='text' and title='{replace_escape_characters(task_description)}'")
                                description_values = [self.generate_id(
                                    27), datetime.now(), self.generate_id(27), 1, "text", f"{task_description}", json.dumps({}), self.get_timestamp(), self.get_timestamp(), 0, json.dumps(None), user_id[0]['id'], "", user_id[0]['id'], board['id']]
                                if focalboardblocks_text:
                                    if focalboardblocks_text[0]['board_id'] != board['id'] and focalboardblocks_text[0]['type'] != "text" and focalboardblocks_text[0]['title'] != task_description:
                                        description_id = MatterSqlClient.sql_post(
                                            table_name='focalboard_blocks', attrs=keys,values=description_values, returning='id')
                                else:
                                    description_id = MatterSqlClient.sql_post(
                                            table_name='focalboard_blocks', attrs=keys,values=description_values, returning='id')
                                fields = {
                                    "contentOrder": [description_id],
                                    "isTemplate": False,
                                    "properties": {
                                        select_id: select_type,  # status
                                        person_id: user_id[0]['id'],
                                        assign_id: assignee_ids,
                                    }
                                }
                                focalboardblocks_card = MatterSqlClient.sql_get("focalboard_blocks", "board_id,type,title", f"board_id='{board['id']}' and type='card' and title='{replace_escape_characters(task['name'])}'")
                                values = [self.generate_id(
                                    27), datetime.now(), self.generate_id(27), 1, "card", f"{task['name']}", json.dumps(fields), self.get_timestamp(), self.get_timestamp(), 0, json.dumps(None), user_id[0]['id'], "", user_id[0]['id'], board['id']]
                                if focalboardblocks_card:
                                    if focalboardblocks_card[0]['board_id'] != board['id'] and focalboardblocks_text[0]['type'] != "card" and focalboardblocks_text[0]['title'] != task['name']:
                                        card_id = MatterSqlClient.sql_post(
                                            table_name="focalboard_blocks", attrs=keys, values=values, returning="id")
                                else:
                                    card_id = MatterSqlClient.sql_post(
                                            table_name="focalboard_blocks", attrs=keys, values=values, returning="id")
                                if description_id:
                                    '''
                                    update the parent id of text component
                                    '''
                                    try:
                                        MatterSqlClient.sql_update(
                                            table_name="focalboard_blocks", set=f"parent_id='{card_id}'", where=f"id='{description_id}'")
                                    except:
                                        pass
                        else:
                            print(f"{remove_punctions(task['created_person'])} user Not Found")
                except Exception as e:
                    save_logs(e)
            print(Fore.GREEN + "## Inserted Focal Blocks Cards ##")

    def main(self) -> None:
        self.insert_portal_user_data()
        
        self.insert_cliq_user_data()
        # time.sleep(1)
        self.insert_team_data()
        # time.sleep(1)
        self.create_channel()
        # time.sleep(1)
        self.insert_channel_members_data()
        # time.sleep(1)
        self.insert_team_members_data()

        self.insert_trooinbound_team_members_data()
        # time.sleep(1)
        self.insert_focalboard_boards_data()
        # time.sleep(1)
        self.insert_focalboard_board_members_data()
        # time.sleep(1)
        self.insert_focalblocks_view_data()
        # time.sleep(1)
        self.insert_focalblocks_card_data()


if __name__ == "__main__":
    print(Fore.YELLOW + "\n<========Saving Mattermost Data in DB=========>\n")
    client = MattermostClient()
    client.main()
