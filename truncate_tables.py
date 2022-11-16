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

    def truncate_channles(self) -> None:
        MatterSqlClient.sql_delete("channels")
        print(Fore.RED + "**Channels Deleted**")

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

    def truncate_focalboard_boards(self) -> None:
        MatterSqlClient.sql_delete("posts")
        print(Fore.RED + "**Posts Deleted**")

    def main(self) -> None:
        self.truncate_users()
        self.truncate_teams()
        self.truncate_teammembers()
        self.truncate_channles()
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

    def truncate_portal_users(self) -> None:
        ZohoSqlClient.sql_delete("portal_users")
        print(Fore.RED + "**Portal Users Deleted**")

    def truncate_project_users(self) -> None:
        ZohoSqlClient.sql_delete("project_users")
        print(Fore.RED + "**Project Users Deleted**")

    def truncate_cliq_users(self) -> None:
        ZohoSqlClient.sql_delete("cliq_users")
        print(Fore.RED + "**Cliq Users Deleted**")

    def truncate_cliq_messages(self) -> None:
        ZohoSqlClient.sql_delete("cliq_messages")
        print(Fore.RED + "**Cliq Messages Deleted**")

    def truncate_cliq_chats(self) -> None:
        ZohoSqlClient.sql_delete("cliq_chats")
        print(Fore.RED + "**Cliq Chats Deleted**")

    def truncate_cliq_channels(self) -> None:
        ZohoSqlClient.sql_delete("cliq_channels")
        print(Fore.RED + "**Cliq Channels Deleted**")

    def truncate_cliq_channel_members(self) -> None:
        ZohoSqlClient.sql_delete("cliq_channel_members")
        print(Fore.RED + "**Cliq Channel Members Deleted**")

    def main(self) -> None:
        self.truncate_portals()
        self.truncate_projects()
        self.truncate_tasks()
        self.truncate_portal_users()
        self.truncate_project_users()
        self.truncate_cliq_users()
        self.truncate_cliq_messages()
        self.truncate_cliq_chats()
        self.truncate_cliq_channels()
        self.truncate_cliq_channel_members()


if __name__ == '__main__':
    print(Fore.YELLOW+"\n<========Truncate Mattermost Tables=========>\n")
    DeleteMattermostTables().main()
    print(Fore.YELLOW+"\n<========Truncate Zoho Tables=========>\n")
    DeleteZohoTables().main()
