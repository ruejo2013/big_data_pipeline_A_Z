#!/usr/bin/env python3

from configparser import ConfigParser

# using postgres db
import psycopg2 as db

file = "config.ini"
config = ConfigParser()
config.read(file)

dbname = config["database"]["dbname"]
host = config["database"]["host"]
user = config["database"]["user"]
password = config["database"]["password"]
port = config["database"]["port"]
table = config["database"]["table"]

create_table = """
create table if not EXISTS {} \
(id varchar(50) primary key, name varchar(50) not null, age int, \
street varchar(100), city varchar(50), state varchar(50), zip varchar(50), phone varchar(50), \
email varchar(70))
""".format(table)

query = """
insert into {} (id, name, age, street, city, state, zip, phone, email) 
values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
""".format(table)


__all__ = ["insertData"]


def _connect():
    """Helper Func to make connection to the db"""
    conn_string = "dbname={0} host={1} user={2} password={3} port={4}".format(
        dbname, host, user, password, port
    )
    conn = db.connect(conn_string)
    cur = conn.cursor()
    return (conn, cur)

def createTable():
    '''
    :return: creates a table
    '''
    conn, cur = _connect()
    cur.execute(create_table)
    conn.commit()
    conn.close()

def insertData(data):
    """ Func to insert the json file into the database"""
    conn, cur = _connect()
    av = 0
    key_value = []
    data_len = len(data)
    while av < data_len:
        for k, v in data[av].items():
            key_value.append(v)
        datainsert = tuple(key_value)  # repeatation
        cur.execute(query, datainsert)
        conn.commit()
        key_value.clear()
        av += 1
