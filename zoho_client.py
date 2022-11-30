import pandas as pd
import requests
from pprint import pprint
import json
from colorama import Fore
from sql import ZohoSqlClient
import sys
from typing import Tuple
from decouple import config
from utils import create_new_column, remove_punctions, save_logs
import time

class ZohoClient:
    zoho_project_base_url = "https://projectsapi.zoho.in/"
    zoho_chat_base_url = "https://cliq.zoho.in/api/v2/"

    access_token = config('ZOHO_PROJECT_API_KEY')
    api_range = 200
    index = 1
    # restapi/portal/{portal_id['id']}/projects/{project_id['id']}/users/

    def get_project_api(self, path) -> Tuple[int, dict]:
        url = f"{self.zoho_project_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r.status_code, r.json()
        else:
            return r.status_code, r

    def get_portal_ids(self) -> list:
        status_code, data = self.get_project_api("restapi/portals/")
        if not status_code in range(200, 299):
            return data.json()
        return data['portals']

    def save_portal_data(self) -> None:
        try:
            portal_ids = self.get_portal_ids()
            if isinstance(portal_ids, dict):
                print(Fore.RED + str(portal_ids))
                sys.exit()
            for portal in portal_ids:
                p_id = ZohoSqlClient.sql_get("portals", "id", f"id='{portal['id']}'")
                keys = list(portal.keys())
                values = list(portal.values())
                columns = ZohoSqlClient.get_columns("portals")

                # Alter table columns as per field from api.
                create_new_column(keys, columns, "portals")

                li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                    values)]

                if p_id:
                    if int(p_id[0]['id']) != portal['id']:
                        ZohoSqlClient.sql_post(
                            table_name="portals", attrs=portal.keys(), values=li)
                else:
                    ZohoSqlClient.sql_post(
                        table_name="portals", attrs=portal.keys(), values=li)
        except Exception as e:
            save_logs()

        print(Fore.GREEN + "## Portals saved in db ##")

    def save_projects_data(self) -> None:
        try:
            portal_ids = ZohoSqlClient.sql_get("portals", "id")
            for portal_id in portal_ids:
                status_code, projects = self.get_project_api(
                    f"restapi/portal/{portal_id['id']}/projects/")
                if not status_code in range(200, 299):
                    return projects
                # pprint(projects['projects'])
                for project in projects['projects']:
                    p_id = ZohoSqlClient.sql_get("projects", "id", f"id='{project['id']}'")
                    keys = list(project.keys())
                    columns = ZohoSqlClient.get_columns("projects")

                    # Alter table columns as per field from api.
                    create_new_column(keys, columns, "projects")

                    values = project.values()
                    project_list = [i for i in values]
                    li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                        project_list)]
                    if p_id:
                        if int(p_id[0]['id']) != project['id']:
                            ZohoSqlClient.sql_post(
                                table_name="projects", attrs=project.keys(), values=li)
                    else:
                        ZohoSqlClient.sql_post(
                            table_name="projects", attrs=project.keys(), values=li)
            print(Fore.GREEN + "## Projects saved in db ##")
        except Exception as e:
            save_logs()

    def save_portal_users_data(self) -> None:
        try:
            portal_ids = ZohoSqlClient.sql_get("portals", "id")
            
            for portal_id in portal_ids:
                while True:
                    status_code, users = self.get_project_api(
                        f"restapi/portal/{portal_id['id']}/users/?index={self.index}&range={self.api_range}&user_type=all")
                    self.index = self.api_range + self.index
                    # print(status_code)
                    if status_code == 204:
                        break

                    # to remove spaces and add '.' as mattermost username doesnot support spaces for username
                    df = pd.DataFrame(users['users'])
                    df['name'] = df['name'].apply(lambda x: remove_punctions(x))
                    df['active'] = df['active'].fillna(False)
                    users = df.to_dict('records')

                    for user in users:
                        portal_users_id = ZohoSqlClient.sql_get("portal_users", "email", f"email='{user['email']}'")
                        keys = list(user.keys())
                        columns = ZohoSqlClient.get_columns("portal_users")
                        # Alter table columns as per field from api.
                        create_new_column(keys, columns, "portal_users")

                        values = user.values()
                        user_list = [i for i in values]
                        li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                            user_list)]
                        if portal_users_id:
                            if portal_users_id[0]['email'] != user['email']:
                                ZohoSqlClient.sql_post(
                                    table_name="portal_users", attrs=user.keys(), values=li)
                        else:
                            ZohoSqlClient.sql_post(
                                    table_name="portal_users", attrs=user.keys(), values=li)
            print(Fore.GREEN + "## Portal Users saved in db ##")
        except Exception as e:
            save_logs()

    def save_project_users_data(self) -> None:
        try:
            portal_ids = ZohoSqlClient.sql_get("portals", "id")
            project_ids = ZohoSqlClient.sql_get("projects", "id,name")
            count = 1
            for portal_id in portal_ids:
                for project_id in project_ids:
                    if count == 90:
                        count = 1
                        print("Api is throttled, wait for 130 seconds....")
                        time.sleep(130)
                    status_code, users = self.get_project_api(
                        f"restapi/portal/{portal_id['id']}/projects/{project_id['id']}/users/")
                    count +=1
                    if not status_code in range(200, 299):
                        return users
                    # to remove spaces and add '.' as mattermost username doesnot support spaces for username
                    df = pd.DataFrame(users['users'])
                    df['name'] = df['name'].apply(lambda x: remove_punctions(x))
                    users = df.to_dict('records')

                    for user in users:
                        project_users = ZohoSqlClient.sql_get("project_users", "email,project_name", f"email='{user['email']}' and project_name='{project_id['name']}'")
                        keys = list(user.keys())
                        columns = ZohoSqlClient.get_columns("project_users")

                        create_new_column(keys, columns, "project_users")

                        # Alter table columns as per field from api.
                        columns = ZohoSqlClient.get_columns("project_users")
                        values = user.values()
                        user_list = [i for i in values]
                        li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                            user_list)]
                        li.append(project_id['name'])
                        if project_users:
                            if project_users[0]['email'] != user['email'] and project_users[0]['project_name'] != project_id['name']:
                                ZohoSqlClient.sql_post(
                                    table_name="project_users", attrs=columns, values=li)
                        else:
                            ZohoSqlClient.sql_post(
                                        table_name="project_users", attrs=columns, values=li)
            print(Fore.GREEN + "## Project Users saved in db ##")
        except Exception as e:
            save_logs()

    def save_tasks_data(self) -> None:
        try:
            portal_ids = ZohoSqlClient.sql_get("portals", "id")
            project_ids = ZohoSqlClient.sql_get("projects", "id,name")
            keys = ZohoSqlClient.get_columns('tasks')
            for portal_id in portal_ids:
                for project_id in project_ids:
                    status_code, tasks = self.get_project_api(
                        f"restapi/portal/{portal_id['id']}/projects/{project_id['id']}/tasks/")
                    if not status_code in range(200, 299):
                        return tasks

                    for task in tasks['tasks']:
                        tasks_id = ZohoSqlClient.sql_get("tasks", "id,project_name", f"id='{task['id']}' and project_name='{project_id['name']}'")
                        values = task.values()
                        keys = list(task.keys())
                        columns = ZohoSqlClient.get_columns("tasks")

                        # Alter table columns as per field from api.
                        create_new_column(keys, columns, "tasks")

                        values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                            values)]
                        values.append(project_id['name'])
                        keys.append('project_name')
                        if tasks_id:
                            if tasks_id[0]['id'] != str(task['id']) and tasks_id[0]['project_name'] != project_id['name']:
                                ZohoSqlClient.sql_post(
                                    table_name="tasks", attrs=keys, values=values)
                        else:
                            ZohoSqlClient.sql_post(
                                    table_name="tasks", attrs=keys, values=values)
            print(Fore.GREEN + "## Tasks saved in db ##")
        except Exception as e:
            save_logs()

    def main(self):
        """
        portals and projects must be saved before others
        """
        self.save_portal_data()
        self.save_projects_data()
        self.save_portal_users_data()
        self.save_project_users_data()
        self.save_tasks_data()


if __name__ == '__main__':
    print(Fore.YELLOW + "\n<========Saving Zoho Data in DB=========>\n")
    ZohoClient().main()
