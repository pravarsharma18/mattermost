import requests
from pprint import pprint
import json
from colorama import Fore
from sql import ZohoSqlClient
import sys
from typing import Tuple


class ZohoClient:
    zoho_project_base_url = "https://projectsapi.zoho.in/"
    zoho_chat_base_url = "https://cliq.zoho.in/api/v2/"

    access_token = "1000.e14fcf3cc18c6cf30cc6b35cc71cc00f.24b4dd1da06555d40f93259f3a862fee"
    # restapi/portal/{portal_id['id']}/projects/{project_id['id']}/users/

    def get_project_api(self, path) -> Tuple[int, dict]:
        url = f"{self.zoho_project_base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        r = requests.get(url, headers=headers)
        return r.status_code, r.json()

    def get_chat_api(self, path) -> Tuple[int, dict]:
        url = f"{self.zoho_chat_base_url}{path}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"

        }
        r = requests.get(url, headers=headers)
        print("******************", r)
        return r.status_code, r.json()

    def get_portal_ids(self) -> list:
        status_code, data = self.get_project_api("restapi/portals/")
        if not status_code in range(200, 299):
            return data
        return data['portals']

    def save_portal_data(self) -> None:
        portal_ids = self.get_portal_ids()
        if isinstance(portal_ids, dict):
            print(Fore.RED + str(portal_ids))
            sys.exit()
        p_id = ZohoSqlClient.sql_get("portals", "id")
        for portal in portal_ids:
            values = portal.values()
            portal_list = [i for i in values]
            print
            li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                portal_list)]
            # pprint(li)
            # print()
            # pprint(portal.keys())
            break
            # if p_id:
            #     for i in p_id:
            #         if not int(i['id']) == portal['id']:
            #             ZohoSqlClient.sql_post(
            #                 table_name="portals", attrs=portal.keys(), values=li)
            # else:
            #     ZohoSqlClient.sql_post(
            #         table_name="portals", attrs=portal.keys(), values=li)
        print(Fore.GREEN + "## Portals saved in db ##")

    def save_projects_data(self) -> None:
        portal_ids = ZohoSqlClient.sql_get("portals", "id")
        p_id = ZohoSqlClient.sql_get("projects", "id")
        for portal_id in portal_ids:
            status_code, projects = self.get_project_api(
                f"restapi/portal/{portal_id['id']}/projects/")
            if not status_code in range(200, 299):
                return projects
            # pprint(projects['projects'])
            for project in projects['projects']:
                keys = list(project.keys())
                c = ZohoSqlClient.get_columns("projects")
                new_columns = [i for i in keys if i not in c]
                if new_columns:  # Alter table columns as per field from api.
                    for column in new_columns:
                        ZohoSqlClient.update_table_column(
                            "projects", f"{column}")
                values = project.values()
                project_list = [i for i in values]
                li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                    project_list)]
                if p_id:
                    for i in p_id:
                        if not int(i['id']) == project['id']:
                            ZohoSqlClient.sql_post(
                                table_name="projects", attrs=project.keys(), values=li)
                else:
                    ZohoSqlClient.sql_post(
                        table_name="projects", attrs=project.keys(), values=li)
        print(Fore.GREEN + "## Projects saved in db ##")

    def save_users_data(self) -> None:
        portal_ids = ZohoSqlClient.sql_get("portals", "id")
        project_ids = ZohoSqlClient.sql_get("projects", "id")
        p_id = ZohoSqlClient.sql_get("users", "id")
        # print(portal_ids)
        for portal_id in portal_ids:
            for project_id in project_ids:
                status_code, users = self.get_project_api(
                    f"restapi/portal/{portal_id['id']}/projects/{project_id['id']}/users/")
                if not status_code in range(200, 299):
                    return users
                for user in users['users']:
                    values = user.values()
                    user_list = [i for i in values]
                    li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                        user_list)]
                    ZohoSqlClient.sql_post(
                        table_name="users", attrs=user.keys(), values=li)
        print(Fore.GREEN + "## Users saved in db ##")

    # def save_users_data_test_not_for_production(self) -> None:
    #     portal_ids = ZohoSqlClient.sql_get("portals", "id")
    #     project_ids = ZohoSqlClient.sql_get("projects", "id")
    #     p_id = ZohoSqlClient.sql_get("users", "id")
    #     for portal_id in portal_ids:
    #         for project_id in project_ids:
    #             status_code, users = self.get_project_api(
    #                 f"restapi/portal/{portal_id['id']}/projects/{project_id['id']}/users/")
    #             if not status_code in range(200, 299):
    #                 return users
    #             for user in users['users']:
    #                 values = user.values()
    #                 user_list = [i for i in values]
    #                 li = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
    #                     user_list)]
    #                 # if p_id:
    #                 #     for i in p_id:
    #                 #         if i['id'] != user['id']:
    #                 #             ZohoSqlClient.sql_post_method(
    #                 #                 table_name="users", attrs=keys, values=li)
    #                 # else:
    #                 ZohoSqlClient.sql_post(
    #                     table_name="users", attrs=user.keys(), values=li)
    #     print(Fore.GREEN + "## Users saved in db ##")

    # def remove_duplicate_entries_from_user_data(self) -> None:
    #     p_id = ZohoSqlClient.sql_get("users", "distinct name")
    #     print(p_id)
        # for i in p_id:

    def save_tasks_data(self) -> None:
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
                    values = task.values()
                    keys = list(task.keys())
                    values = [json.dumps(v) if (isinstance(v, dict) or isinstance(v, bool) or isinstance(v, list) or isinstance(v, int)) else v for i, v in enumerate(
                        values)]
                    values.append(project_id['name'])
                    keys.append('project_name')
                    ZohoSqlClient.sql_post(
                        table_name="tasks", attrs=keys, values=values)
        print(Fore.GREEN + "## Tasks saved in db ##")

    def main(self):
        """
        portals and projects must be saved before others
        """
        self.save_portal_data()
        self.save_projects_data()
        self.save_users_data()
        # self.save_users_data_test_not_for_production()
        self.save_tasks_data()


if __name__ == '__main__':
    print(Fore.YELLOW + "\n<========Saving Zoho Data in DB=========>\n")
    ZohoClient().save_portal_data()
    # ZohoClient().save_users_data()
    # ZohoClient().remove_duplicate_entries_from_user_data()
