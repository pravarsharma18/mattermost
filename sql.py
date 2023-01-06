import psycopg2
from decouple import config

class ZohoSqlClient:
    zohomydb = psycopg2.connect(host=config('ZOHO_HOST'), database=config('ZOHO_DATABASE'),
                                user=config('ZOHO_USER'), password=config('ZOHO_PASSWORD'), port=config('ZOHO_PORT'))

    @classmethod
    def get_cursor(cls):
        try:
            cursor = cls.zohomydb.cursor()
        except psycopg2.InterfaceError as e:
            # Reconnect
            cls.zohomydb = psycopg2.connect(host=config('ZOHO_HOST'), database=config('ZOHO_DATABASE'),
                                user=config('ZOHO_USER'), password=config('ZOHO_PASSWORD'), port=config('ZOHO_PORT'))
            cursor = cls.zohomydb.cursor()
        return cursor

    @classmethod
    def sql_get(cls, table_name, fields="*", params="") -> list:
        try:
            mycursor = cls.get_cursor()
            if params:
                query = f"SELECT {fields} FROM {table_name} where {params}"
            else:
                query = f"SELECT {fields} FROM {table_name}"
            mycursor.execute(query)
            myresult = mycursor.fetchall()
            if myresult:
                li = []
                for i in myresult:
                    row = dict(zip([desc[0] for desc in mycursor.description], i))
                    li.append(row)
                mycursor.close()
                return li
            else:
                mycursor.close()
                return myresult
        except (Exception, psycopg2.Error) as error:
            print(f"Failed to get record from {table_name} table in zoho db", error)
        finally:
            if cls.zohomydb is not None:
                cls.zohomydb.close()

    @classmethod
    def sql_post(cls, **kwargs):
        try:
            returning = ""
            if not kwargs.get('table_name'):
                print("table_name required")
                return
            if not kwargs.get('attrs'):
                print("attrs required")
                return
            if not kwargs.get('values'):
                print("values required")
                return
            if kwargs.get('returning'):
                returning = f"RETURNING {kwargs.get('returning')}"

            keys = ",".join(kwargs['attrs'])
            keys = '"' + keys.replace(',', '","') + '"'
            sql = f"INSERT INTO {kwargs['table_name']} ({keys}) VALUES ({'s,'.join('%' * len(keys.split(','))) + 's'}) {returning}"
            val = (kwargs['values'])
            try:
                mycursor = cls.get_cursor()
                mycursor.execute(sql, val)
                cls.zohomydb.commit()
                if kwargs.get('returning'):
                    id = mycursor.fetchone()[0]
                    return id
            except (Exception, psycopg2.Error) as error:
                print("Failed to insert record into mobile table", error)
        except (Exception, psycopg2.Error) as error:
            print(f"Failed to get record from {table_name} table in zoho db", error)
        finally:
            if cls.zohomydb is not None:
                cls.zohomydb.close()
    
    @classmethod
    def sql_update(cls, table_name, set, where="") -> None:
        if where:
            query = f"UPDATE {table_name} SET {set} where {where}"
        else:
            query = f"UPDATE {table_name} SET {set}"
        try:
            mycursor = cls.get_cursor()
            mycursor.execute(query)
            cls.zohomydb.commit()
            mycursor.close()
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into mobile table", error)

    @classmethod
    def sql_delete(cls, table_name, where=""):
        mycursor = cls.get_cursor()
        if where:
            query = f"delete from {table_name} where {where}"
        else:
            query = f"delete from {table_name}"
        mycursor.execute(query)
        cls.zohomydb.commit()

    @classmethod
    def update_table_column(cls, table_name, column_name):
        mycursor = cls.get_cursor()
        query = f"ALTER TABLE {table_name} ADD {column_name} text;"
        mycursor.execute(query)
        cls.zohomydb.commit()

    @classmethod
    def get_columns(self, table_name):
        mycursor = self.get_cursor()
        query = f"select column_name from information_schema.columns where table_name = '{table_name}' order by ordinal_position"
        mycursor.execute(query)
        columns = mycursor.fetchall()
        keys = [column[0] for column in columns]
        return keys
    
    @classmethod
    def get_count(self, table_name):
        mycursor = self.get_cursor()
        query = f"select count(*) from {table_name}"
        mycursor.execute(query)
        count = mycursor.fetchall()
        return count


class MatterSqlClient:
    mattermydb = psycopg2.connect(host=config('MATTERMOST_HOST'), database=config('MATTERMOST_DATABASE'),
                                user=config('MATTERMOST_USER'), password=config('MATTERMOST_PASSWORD'), port=config('MATTERMOST_PORT'))

    @classmethod
    def get_cursor(cls):
        try:
            cursor = cls.mattermydb.cursor()
        except psycopg2.InterfaceError as e:
            # Reconnect
            cls.mattermydb = psycopg2.connect(host=config('MATTERMOST_HOST'), database=config('MATTERMOST_DATABASE'),
                                user=config('MATTERMOST_USER'), password=config('MATTERMOST_PASSWORD'), port=config('MATTERMOST_PORT'))
            cursor = cls.mattermydb.cursor()
        return cursor

    @classmethod
    def sql_get(cls, table_name, fields="*", params=""):
        try:
            mycursor = cls.get_cursor()
            if params:
                query = f"SELECT {fields} FROM {table_name} where {params}"
            else:
                query = f"SELECT {fields} FROM {table_name}"
            mycursor.execute(query)
            myresult = mycursor.fetchall()
            
            if myresult:
                li = []
                for i in myresult:
                    row = dict(zip([desc[0] for desc in mycursor.description], i))
                    li.append(row)
                mycursor.close()
                return li
            else:
                mycursor.close()
                return myresult
        except (Exception, psycopg2.Error) as error:
            print(f"Failed to get record from {table_name} table in mattermost db", error)
        finally:
            if cls.mattermydb is not None:
                cls.mattermydb.close()

    @classmethod
    def sql_post(cls, **kwargs) -> None or str:
        try:
            mycursor = cls.get_cursor()
            returning = ""
            if not kwargs.get('table_name'):
                print("table_name required")
                return
            if not kwargs.get('attrs'):
                print("attrs required")
                return
            if not kwargs.get('values'):
                print("values required")
                return
            if kwargs.get('returning'):
                returning = f"RETURNING {kwargs.get('returning')}"
            keys = ",".join(kwargs['attrs'])
            keys = '"' + keys.replace(',', '","') + '"'
            sql = f"INSERT INTO {kwargs['table_name']} ({keys}) VALUES ({'s,'.join('%' * len(keys.split(','))) + 's'}) {returning}"
            val = (kwargs['values'])

            mycursor.execute(sql, val)
            cls.mattermydb.commit()
            if kwargs.get('returning'):
                id = mycursor.fetchone()[0]
                return id
        except (Exception, psycopg2.Error) as error:
            print(f"Failed to get record from {kwargs['table_name']} table in mattermost db", error)
        finally:
            if cls.mattermydb is not None:
                cls.mattermydb.close()


    @classmethod
    def sql_update(cls, table_name, set, where="") -> None:
        if where:
            query = f"UPDATE {table_name} SET {set} where {where}"
        else:
            query = f"UPDATE {table_name} SET {set}"
        try:
            mycursor = cls.get_cursor()
            mycursor.execute(query)
            cls.mattermydb.commit()
            mycursor.close()
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into mobile table", error)
        finally:
            if cls.mattermydb is not None:
                cls.mattermydb.close()

    @classmethod
    def sql_delete(cls, table_name, where="") -> None:
        mycursor = cls.get_cursor()
        if where:
            query = f"delete from {table_name} where {where}"
        else:
            query = f"delete from {table_name}"
        mycursor.execute(query)
        cls.mattermydb.commit()

    @classmethod
    def add_column(cls, table_name, column_name, data_type):
        mycursor = cls.get_cursor()
        query = f"""
            ALTER TABLE {table_name}
            ADD {column_name} {data_type};
        """
        mycursor.execute(query)
        cls.mattermydb.commit()

    @classmethod
    def delete_column(cls, table_name, column_name):
        mycursor = cls.get_cursor()
        query = f"""
            ALTER TABLE {table_name}
            DROP COLUMN {column_name};
        """
        mycursor.execute(query)
        cls.mattermydb.commit()

    @classmethod
    def raw_query(cls, query, type=None):
        mycursor = cls.get_cursor()
        
        mycursor.execute(query)
        cls.mattermydb.commit()
        if type =="get":
            result = mycursor.fetchall()
            return result

    @classmethod
    def get_columns(cls, table_name) -> list:
        mycursor = cls.get_cursor()
        query = f"select column_name from information_schema.columns where table_name = '{table_name}' order by ordinal_position"
        mycursor.execute(query)
        columns = mycursor.fetchall()
        keys = [column[0] for column in columns]
        return keys

    @classmethod
    def get_count(self, table_name):
        mycursor = self.get_cursor()
        query = f"select count(*) from {table_name}"
        mycursor.execute(query)
        count = mycursor.fetchall()
        return count


class DatabaseClient:
    def __init__(self, connection_for="ZOHO") -> None:
        self.connection_for = connection_for
        self.db = None

        self.connect_db()

    def connect_db(self):
        self.db = psycopg2.connect(host=config(f'{self.connection_for}_HOST'), database=config(f'{self.connection_for}_DATABASE'),
                                user=config(f'{self.connection_for}_USER'), password=config(f'{self.connection_for}_PASSWORD'), port=config(f'{self.connection_for}_PORT'))

    def close_db(self) -> None:
        self.db.close()

    def get_cursor(self):
        try:
            cursor = self.db.cursor()
        except psycopg2.InterfaceError as e:
            self.connect_db()
            cursor = self.db.cursor()
        return cursor

    def sql_get(self, table_name, fields="*", params="") -> list:
        try:
            mycursor = self.get_cursor()
            if params:
                query = f"SELECT {fields} FROM {table_name} where {params}"
            else:
                query = f"SELECT {fields} FROM {table_name}"
            mycursor.execute(query)
            myresult = mycursor.fetchall()
            if myresult:
                li = []
                for i in myresult:
                    row = dict(zip([desc[0] for desc in mycursor.description], i))
                    li.append(row)
                mycursor.close()
                return li
            else:
                mycursor.close()
                return myresult
        except (Exception, psycopg2.Error) as error:
            print(f"Failed to get record from {table_name} table in zoho db", error)
        finally:
            if self.db is not None:
                self.db.close()

    def sql_post(self, **kwargs):
        try:
            returning = ""
            if not kwargs.get('table_name'):
                print("table_name required")
                return
            if not kwargs.get('attrs'):
                print("attrs required")
                return
            if not kwargs.get('values'):
                print("values required")
                return
            if kwargs.get('returning'):
                returning = f"RETURNING {kwargs.get('returning')}"

            keys = ",".join(kwargs['attrs'])
            keys = '"' + keys.replace(',', '","') + '"'
            sql = f"INSERT INTO {kwargs['table_name']} ({keys}) VALUES ({'s,'.join('%' * len(keys.split(','))) + 's'}) {returning}"
            val = (kwargs['values'])
            try:
                mycursor = self.get_cursor()
                mycursor.execute(sql, val)
                self.db.commit()
                if kwargs.get('returning'):
                    id = mycursor.fetchone()[0]
                    return id
            except (Exception, psycopg2.Error) as error:
                print("Failed to insert record into mobile table", error)
        except (Exception, psycopg2.Error) as error:
            print(f"Failed to get record from {kwargs['table_name']} table in zoho db", error)
        finally:
            if self.db is not None:
                self.db.close()
    
    def sql_update(self, table_name, set, where="") -> None:
        if where:
            query = f"UPDATE {table_name} SET {set} where {where}"
        else:
            query = f"UPDATE {table_name} SET {set}"
        try:
            mycursor = self.get_cursor()
            mycursor.execute(query)
            self.db.commit()
            mycursor.close()
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into mobile table", error)

    def sql_delete(self, table_name, where=""):
        mycursor = self.get_cursor()
        if where:
            query = f"delete from {table_name} where {where}"
        else:
            query = f"delete from {table_name}"
        mycursor.execute(query)
        self.db.commit()

    def update_table_column(self, table_name, column_name):
        mycursor = self.get_cursor()
        query = f"ALTER TABLE {table_name} ADD {column_name} text;"
        mycursor.execute(query)
        self.db.commit()

    def get_columns(self, table_name):
        mycursor = self.get_cursor()
        query = f"select column_name from information_schema.columns where table_name = '{table_name}' order by ordinal_position"
        mycursor.execute(query)
        columns = mycursor.fetchall()
        keys = [column[0] for column in columns]
        return keys
    
    def get_count(self, table_name):
        mycursor = self.get_cursor()
        query = f"select count(*) from {table_name}"
        mycursor.execute(query)
        count = mycursor.fetchall()
        return count
