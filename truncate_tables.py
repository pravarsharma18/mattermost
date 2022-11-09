from colorama import Fore
from sql import ZohoSqlClient, MatterSqlClient


class DeleteMattermostTables:
    def truncate_users(self) -> None:
        MatterSqlClient.sql_delete(
            "users", "username not in ('channelexport','system-bot','boards','playbooks','appsbot','feedbackbot')")
        print(Fore.RED + "**Users Deleted**")

    def truncate_teams(self) -> None:
        MatterSqlClient.sql_delete("teams")
        print(Fore.RED + "**Teams Deleted**")

    def truncate_teammembers(self) -> None:
        MatterSqlClient.sql_delete("teammembers")
        print(Fore.RED + "**TeamMembers Deleted**")

    def truncate_channlemembers(self) -> None:
        MatterSqlClient.sql_delete("channelmembers")
        print(Fore.RED + "**ChannelMembers Deleted**")

    def truncate_focalboard_board_members(self) -> None:
        MatterSqlClient.sql_delete(
            "focalboard_board_members", "user_id !='system'")
        print(Fore.RED + "**Focal Board Members Deleted**")

    def truncate_focalboard_blocks(self) -> None:
        board_ids = MatterSqlClient.sql_get(
            "focalboard_boards", "id", "type='P'")
        for board_id in board_ids:
            MatterSqlClient.sql_delete(
                "focalboard_blocks", f"board_id ='{board_id['id']}'")
        print(Fore.RED + "**Focal Board Blocks Deleted**")

    def truncate_focalboard_boards(self) -> None:
        MatterSqlClient.sql_delete(
            "focalboard_boards", "type='P'")
        print(Fore.RED + "**Focal Boards Deleted**")

    def main(self) -> None:
        self.truncate_users()
        self.truncate_teams()
        self.truncate_teammembers()
        self.truncate_channlemembers()
        self.truncate_focalboard_board_members()
        self.truncate_focalboard_blocks()
        self.truncate_focalboard_boards()


class DeleteZohoTables:
    def truncate_portals(self) -> None:
        ZohoSqlClient.sql_delete("portals")
        print(Fore.RED + "**Portals Deleted**")

    def truncate_projects(self) -> None:
        ZohoSqlClient.sql_delete("projects")
        print(Fore.RED + "**Projects Deleted**")

    def truncate_tasks(self) -> None:
        ZohoSqlClient.sql_delete("tasks")
        print(Fore.RED + "**Tasks Deleted**")

    def truncate_users(self) -> None:
        ZohoSqlClient.sql_delete("users")
        print(Fore.RED + "**Users Deleted**")

    def main(self) -> None:
        self.truncate_portals()
        self.truncate_projects()
        self.truncate_tasks()
        self.truncate_users()


if __name__ == '__main__':
    print(Fore.YELLOW+"\n<========Truncate Mattermost Tables=========>\n")
    DeleteMattermostTables().main()
    # print(Fore.YELLOW+"\n<========Truncate Zoho Tables=========>\n")
    # DeleteZohoTables().main()
