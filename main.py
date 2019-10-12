import os
import shutil

import pymysql
import yaml


class ConnectionError(Exception):
    pass


# conn creates connection to database
def conn():
    try:
        import config

        configs = config
    except ImportError as e:
        raise ConnectionError("Missing config: {}".format(e))

    try:
        host = configs.host
        port = configs.port
        user = configs.user
        password = configs.password
        database = configs.database
        charset = configs.charset
    except AttributeError as e:
        raise ConnectionError("Missing config item: {}".format(e))

    return pymysql.connect(host=host,
                           port=port,
                           user=user,
                           password=password,
                           db=database,
                           charset=charset,
                           cursorclass=pymysql.cursors.DictCursor)


# get_table_names returns list of table names in a database
def get_table_names(conn):
    tables = []
    with conn.cursor() as cursor:
        sql = "show tables"
        cursor.execute(sql)
        for row in cursor.fetchall():
            for v in row:
                tables.append(row[v])
    return tables


# get_create_table_stmt returns create table statement of a table
def get_create_table_stmt(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute("show create table {}".format(table_name))
        result = cursor.fetchone()
    return result["Create Table"]


# get_columns returns list of columns in a table
def get_columns(conn, table_name):
    columns = []
    with conn.cursor() as cursor:
        cursor.execute("desc {}".format(table_name))
        for row in cursor.fetchall():
            columns.append(row)
    return columns


# get_rows returns list of rows in a table
def get_rows(conn, table_name):
    rows = []
    with conn.cursor() as cursor:
        cursor.execute("select * from {}".format(table_name))
        for row in cursor.fetchall():
            rows.append(row)
    return rows


def dump_table(c, table_dir, table_name):
    print("Processing table {}:".format(table_name))
    os.mkdir(table_dir)
    with open(os.path.join(table_dir, "create_table.sql"), "w") as f:
        f.write(get_create_table_stmt(c, table_name))
    with open(os.path.join(table_dir, "desc_table.yaml"), "w") as f:
        columns = get_columns(c, table_name)
        f.write(yaml.dump(columns))
    # os.mkdir(os.path.join(table_dir, "rows"))
    for column in columns:
        if column["Type"] in ["text", "mediumtext", "longtext"]:
            os.mkdir(os.path.join(table_dir,
                                  "column_{}".format(column["Field"])))
    rows = ""
    for i, row in enumerate(get_rows(c, table_name)):
        text_list = []
        for column in columns:
            if column["Type"] in ["text", "mediumtext", "longtext"]:
                with open(os.path.join(table_dir,
                                       "column_{}".format(column["Field"]),
                                       "{}.txt".format(i)), "w") as c:
                    c.write(str(row[column["Field"]]))
                text_list.append("_")
            else:
                text_list.append(str(row[column["Field"]]) \
                                 .replace("\\", "\\\\") \
                                 .replace(",", "\\,") \
                                 .replace("\n", "\\n"))
        rows += ",".join(text_list) + "\n"
    with open(os.path.join(table_dir, "rows.txt"), "w") as f:
        f.write(rows)


def dump():
    dataDir = "data"
    if os.path.isdir(dataDir):
        shutil.rmtree(dataDir)
    elif os.path.isfile(dataDir):
        os.remove(dataDir)
    os.mkdir(dataDir)

    try:
        c = conn()
        try:
            for table in get_table_names(c):
                dump_table(c, os.path.join(dataDir, table), table)
        finally:
            c.close()
    except ConnectionError as e:
        print(e)
        exit(1)


if __name__ == '__main__':
    dump()
