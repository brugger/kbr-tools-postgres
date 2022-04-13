"""

Generica low level function for interacting with a database through the records package


"""

import kbr.db_utils as db_utils

db = None

def _connected() -> None:
    if db is None:
        raise RuntimeError('database is not connected!')

def connect(uri:str, **kwargs) -> None:
    global db
    kwargs['isolation_level']="AUTOCOMMIT"
    db = db_utils.DB(uri, **kwargs)


def user_get(name:str) -> dict:
    # Quite a hack!
    q = f"pg_catalog.pg_roles WHERE rolname = '{name}'"
    _connected()
    return db.get_single(q)

def user_list() -> dict:

    q = f"SELECT rolname, rolsuper FROM pg_catalog.pg_roles WHERE rolcanlogin"
    _connected()
    return db.get_as_dict(q)


def user_create(name:str, passwd:str) -> None:
    q = f"CREATE ROLE {name} LOGIN PASSWORD '{passwd}';"
    return db.do(q)


def user_delete(name:str) -> None:
    q = f"DROP USER {name};"
    return db.do(q)


def database_exists(dbname:str) -> bool:
    q = f"SELECT 1 FROM pg_database WHERE datname='{dbname}'"
    return bool(len(db.get_as_dict(q)))


def database_create(dbname:str, name:str=None) -> None:
    q = f"CREATE DATABASE {dbname}"
    if name is not None:
        q += f" OWNER {name}"
#    q = f" SET AUTOCOMMIT =  ON;"

    return db.do(q)

def database_delete(dbname:str) -> None:
    q = f"DROP DATABASE IF EXISTS {dbname};"
    return db.do(q)


def database_list() -> None:
    q = f"SELECT datname FROM pg_database WHERE datistemplate = false;"
    return db.get_as_dict(q)

def tables_list() -> list:
    return db.table_names()


def tables_delete() -> None:
    return db.drop_tables()

def table_details(tbl_name:str) -> dict:
    q = f"SELECT column_name, data_type, , is_nullable FROM information_schema.columns WHERE  table_name = '{tbl_name}'  ORDER BY dtd_identifier;"

    return db.get_as_dict(q)

def tables_create(filename:str) -> dict:

    return db.from_file(filename)


def table_foreign_keys(tbl_name:str) -> dict:
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

    return db.get_as_dict(q)
