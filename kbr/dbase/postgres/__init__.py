"""

postgres extension to the db.utils

"""

import records
import kbr.db_utils as db_utils


class DB(db_utils.DB):

    def __init__(self, uri:str, **kwargs) -> None:
        kwargs['isolation_level']="AUTOCOMMIT"
        self._db = records.Database( uri, **kwargs )
        self._fetchall = False
    
    
    def user_get(self, name:str) -> dict:
        # Quite a hack!
        q = f"pg_catalog.pg_roles WHERE rolname = '{name}'"
        return self.get_single(q)
    
    def user_list(self) -> dict:
    
        q = f"SELECT rolname, rolsuper FROM pg_catalog.pg_roles WHERE rolcanlogin"
        return self.get_as_dict(q)
    
    
    def user_create(self, name:str, passwd:str) -> None:
        q = f"CREATE ROLE {name} LOGIN PASSWORD '{passwd}';"
        return self.do(q)
    
    
    def user_delete(self, name:str) -> None:
        q = f"DROP USER {name};"
        return self.do(q)
    
    
    def database_exists(self, dbname:str) -> bool:
        q = f"SELECT 1 FROM pg_database WHERE datname='{dbname}'"
        return bool(len(self.get_as_dict(q)))
    
    
    def database_create(self, dbname:str, name:str=None) -> None:
        q = f"CREATE DATABASE {dbname}"
        if name is not None:
            q += f" OWNER {name}"
    #    q = f" SET AUTOCOMMIT =  ON;"
    
        return self.do(q)
    
    def database_delete(self, dbname:str) -> None:
        q = f"DROP DATABASE IF EXISTS {dbname};"
        return self.do(q)
    
    
    def database_list(self) -> None:
        q = f"SELECT datname FROM pg_database WHERE datistemplate = false;"
        return self.get_as_dict(q)

    def database_change_owner( self, database:str, new_role:str) -> None:
        q = f"ALTER DATABASE {database} OWNER TO {new_role};"
        return self.do(q)


    def tables_list(self) -> list:
        return self.table_names()
    
    def tables_change_owner( self, old_role:str, new_role:str) -> None:
        q = f"REASSIGN OWNED BY {old_role} to {new_role};"

        return self.do(q)

    def tables_delete(self) -> None:
        return self.drop_tables()
    
    def table_details(self, tbl_name:str) -> dict:
        q = f"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE  table_name = '{tbl_name}'  ORDER BY dtd_identifier;"
    
        return self.get_as_dict(q)
    
    def tables_create(self, filename:str) -> dict:
    
        return self.from_file(filename)
    
    
    def table_foreign_keys(self, tbl_name:str) -> dict:
        q = f"""
    SELECT
        tc.table_schema, 
        tc.constraint_name, 
        tc.table_name, 
        kcu.column_name, 
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{tbl_name}';"""
    
        return self._db.get_as_dict(q)
    
