#!/usr/bin/env python3
#
# Kim Brugger (25.03.2022) kbr(at)brugger.dk

import sys
import os
import pprint as pp
from inspect import getmembers, isfunction

sys.path.append(".")



import kbr2.dbase.postgres as postgres_utils

uri  = "postgresql://admin:admin@localhost/postgres"


def main():
    print('hello world')
#    print( dir(postgres_utils ))
#    print(getmembers(postgres_utils, isfunction))
#    help(postgres_utils)
    db = postgres_utils.DB(uri)
    print( dir(db) )
    user = db.user_get('biobanker')
    if user is None:
        print('create user')
        user = db.user_create('biobanker', 'secret')


#    user = postgres_utils.database_exists('maf')
    bb_db = db.database_exists('brainbanker')
    if bb_db == False:
        print('creating db')
        bb_db = db.database_create('brainbanker', 'biobanker')


    print( bb_db )

    db.do("CREATE TABLE IF NOT EXISTS meta ( id INT, name varchar(20) );")

    db.tables_change_owner('admin', 'biobanker')

    print(db.tables_list())
    print(db.table_details('meta'))
#    postgres_utils.tables_delete()


#    postgres_utils.database_delete('brainbanker')
    print('drop db')
    user = db.database_delete('brainbanker')
    print('drop user')
    user = db.user_delete('biobanker')


if __name__ == "__main__":
    main()
