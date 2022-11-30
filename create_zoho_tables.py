
from sql import ZohoSqlClient
from colorama import Fore


class CreateZohoTables:
    def portals(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()
        query = '''
            CREATE TABLE if not exists portals (
            storage_type varchar(255),trial_enabled varchar(255),can_create_project varchar(255),
            gmt_time_zone varchar(255),project_count text,role varchar(255),is_sprints_integrated varchar(255),
            avail_user_count varchar(255),is_crm_partner varchar(255),link text,bug_plan varchar(255),
            can_add_template varchar(255),locale text,"IS_LOGHR_RESTRICTEDBY_WORKHR" varchar(255),
            layouts text,gmt_time_zone_offset varchar(255),role_details text,new_user_plan varchar(255),
            sprints_zoid varchar(255),available_projects varchar(255), "default" text, 
            sprints_project_permission_temp varchar(255),portal_owner text,id varchar(255),bug_plural varchar(255),
            is_new_plan varchar(255),plan varchar(255),percentage_calculation varchar(255),settings text,
            avail_client_user_count varchar(255),is_tags_available varchar(255),sprints_project_permission varchar(255),
            is_display_taskprefix varchar(255),image_url text,profile_details text,bug_singular varchar(255),
            login_zpuid varchar(255),is_display_projectprefix varchar(255),project_prefix varchar(255),
            max_user_count varchar(255),max_client_user_count varchar(255),extensions text,profile_id varchar(255),
            "api&mobile_access" text,name varchar(255),id_string varchar(255),
            is_time_log_restriction_enabled varchar(255),integrations text
        );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Portals table created ##")

    def projects(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()
        query = '''
            CREATE TABLE if not exists projects (
                is_strict varchar(255),bug_count text,owner_id varchar(255),
                bug_client_permission varchar(255),taskbug_prefix varchar(255),start_date_long varchar(255),
                updated_date_long varchar(255),show_project_overview varchar(255),task_count text,
                updated_date_format varchar(255),workspace_id varchar(255),bug_defaultview varchar(255),
                id varchar(255),is_chat_enabled varchar(255),is_sprints_project varchar(255),
                owner_name varchar(255),created_date_long varchar(255),created_by varchar(255),
                created_date_format varchar(255),profile_id varchar(255),name varchar(255),
                updated_by varchar(255),updated_by_id varchar(255),created_by_id varchar(255),
                updated_by_zpuid varchar(255),bug_prefix varchar(255),status varchar(255),
                end_date varchar(255),project_percent varchar(255),role varchar(255),
                "IS_BUG_ENABLED" varchar(255),link text,created_by_zpuid varchar(255),
                custom_status_id varchar(255),description text,milestone_count text,
                end_date_long varchar(255),custom_status_name varchar(255),owner_zpuid varchar(255),
                is_client_assign_bug varchar(255),billing_status varchar(255),currency varchar(255),
                key text,start_date varchar(255),owner_email varchar(255),custom_status_color varchar(255),
                currency_symbol varchar(255),enabled_tabs text,is_public varchar(255),id_string varchar(255),
                created_date varchar(255),updated_date varchar(255),cascade_setting text,
                layout_details text,  "TAGS" text
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Projects table created ##")

    def project_users(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()
        query = '''
            CREATE TABLE if not exists project_users (
                project_name varchar(255), profile_type varchar(255),role varchar(255),portal_role_name varchar(255),
                active varchar(255),zpuid varchar(255),project_profile_id varchar(255),
                profile_id varchar(255),name varchar(255),portal_profile_name varchar(255),
                portal_role_id varchar(255),id varchar(255),email varchar(255),chat_access varchar(255),
                is_resource varchar(255)
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Project Users table created ##")

    def portal_users(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()
        query = '''
            CREATE TABLE if not exists portal_users (
                active boolean, email varchar(255), id varchar(255), is_resource boolean, name varchar(255), 
                profile_id varchar(255), profile_name varchar(255), profile_type varchar(255), role varchar(255), 
                role_id varchar(255), role_name varchar(255), zpuid varchar(255)
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Portal Users table created ##")

    def tasks(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()
        query = '''
            CREATE TABLE if not exists tasks (
                project_name varchar(255), start_date_long varchar(255), is_comment_added varchar(255), end_date_format varchar(255), 
                added_via varchar(255), last_updated_time_long varchar(255), is_forum_associated varchar(255), 
                details text, id varchar(255), created_time varchar(255), work text,
                start_date_format varchar(255), isparent varchar(255), completed_time_long varchar(255), 
                work_type text, completed varchar(255), task_followers text, priority text, 
                created_by varchar(255), tags text, last_updated_time varchar(255), name varchar(255), 
                is_docs_assocoated text,tasklist text, last_updated_time_format varchar(255), 
                billingtype varchar(255), order_sequence varchar(255), status text, end_date varchar(255), 
                is_sprints_task varchar(255), milestone_id varchar(255), link text, description text,
                created_by_zpuid varchar(255), work_form varchar(255), completed_time_format varchar(255), 
                end_date_long varchar(255), duration varchar(255), created_by_email varchar(255), 
                start_date varchar(255), created_person varchar(255), created_time_long varchar(255), 
                is_reminder_set varchar(255),is_recurrence_set varchar(255), created_time_format varchar(255), 
                subtasks varchar(255), duration_type varchar(255), percent_complete varchar(255), 
                "GROUP_NAME" text, completed_time varchar(255), id_string varchar(255), 
                log_hours text, key varchar(255)
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Tasks table created ##")

    def cliq_users(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()
        query = '''
            CREATE TABLE if not exists cliq_users (
                email_id varchar(255), zuid varchar(255), zoid varchar(255), display_name varchar(255), 
                name varchar(255), type varchar(255), organization_id varchar(255), id varchar(255)
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Cliq Users table created ##")

    def cliq_channels(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()
        query = '''
            CREATE TABLE if not exists cliq_channels (
                name varchar(255),channel_id varchar(255), total_message_count int, participant_count int,
                creation_time varchar(255), description text, creator_id varchar(255)
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Cliq Channels table created ##")

    def cliq_chats(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()

        query = '''
            CREATE TABLE if not exists cliq_chats (
                title varchar(255),chat_id varchar(255),participant_count int,total_message_count int,
                creator_id varchar(255),creation_time varchar(255),last_modified_time varchar(255),
                recipients_summary text
            );
        '''
        # query = '''
        #     CREATE TABLE if not exists cliq_chats (
        #         chat_id varchar(255),chat_type varchar(255),creation_time varchar(255),
        #         creator_id varchar(255),last_message_info text, last_modified_time varchar(255),
        #         name varchar(255),participant_count varchar(255),pinned boolean,
        #         recipients_summary text,removed boolean,unread_message_count varchar(255),
        #         unread_time varchar(255)
        #     );
        # '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Cliq chats table created ##")

    def cliq_messages(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()

        query = '''
            CREATE TABLE if not exists cliq_messages (
                chat_id varchar(255), ack_key varchar(255), content text, id varchar(255), is_read boolean, revision int, 
                sender text, time varchar(255), type varchar(255)
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Cliq Messages table created ##")

    def cliq_channel_members(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()

        query = '''
            CREATE TABLE if not exists cliq_channel_members (
                channel_id varchar(255), user_id varchar(255),email varchar(255), name varchar(255), user_role varchar(255)
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Cliq Channel Members table created ##")
    
    def cliq_logs(self):
        mycursor = ZohoSqlClient.zohomydb.cursor()

        query = '''
            CREATE TABLE if not exists logs (
                data text, timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        '''
        mycursor.execute(query)
        ZohoSqlClient.zohomydb.commit()
        print(Fore.GREEN + "## Cliq Log table created ##")

    def main(self):
        self.portals()
        self.projects()
        self.project_users()
        self.portal_users()
        self.tasks()
        self.cliq_users()
        self.cliq_channels()
        self.cliq_chats()
        self.cliq_messages()
        self.cliq_channel_members()
        self.cliq_logs()


if __name__ == '__main__':
    print(Fore.YELLOW + "\n<========Creating Zoho Table=========>\n")
    CreateZohoTables().main()
