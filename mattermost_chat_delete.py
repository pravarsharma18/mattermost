
from colorama import Fore
from sql import MatterSqlClient


class DeleteMattermostTables:
    def truncate_channles(self) -> None:
        MatterSqlClient.sql_delete("channels", "name !='town-square'")
        print(Fore.RED + "**Channels Deleted**")
    
    def truncate_posts(self) -> None:
        MatterSqlClient.sql_delete("posts")
        print(Fore.RED + "**Posts Deleted**")
    
    def truncate_fileinfo(self) -> None:
        MatterSqlClient.sql_delete("fileinfo")
        print(Fore.RED + "**Fileinfo Deleted**")
    
    def truncate_preference(self) -> None:
        MatterSqlClient.sql_delete("preferences")
        print(Fore.RED + "**Preferences Deleted**")

    def remove_channel_extracolumn(self):
        try:
            MatterSqlClient.delete_column('channels', 'chat_id')
        except:
            pass
        print("Deleted chat_id from channels")


    def main(self):
        self.truncate_channles()
        self.truncate_posts()
        self.truncate_fileinfo()
        self.remove_channel_extracolumn()
        self.truncate_preference()

if __name__ == '__main__':
    a = DeleteMattermostTables().main()