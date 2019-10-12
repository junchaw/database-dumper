import pymysql


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


# tables returns list of table names in a database
def tables(conn):
    tables = []
    with conn.cursor() as cursor:
        sql = "show tables"
        cursor.execute(sql)
        for row in cursor.fetchall():
            for v in row:
                tables.append(row[v])
    return tables


# create_table returns create table statement of a table
def create_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute("show create table {}".format(table_name))
        result = cursor.fetchone()
    return result["Create Table"]


# columns returns list of columns in a table
def columns(conn, table_name):
    columns = []
    with conn.cursor() as cursor:
        cursor.execute("desc {}".format(table_name))
        for row in cursor.fetchall():
            columns.append(row)
    return columns


try:
    c = conn()
    try:
        for table in tables(c):
            print("Processing table {}:".format(table))
            print(create_table(c, table), "\n\n", columns(c, table), "\n\n")
    finally:
        c.close()
except ConnectionError as e:
    print(e)
    exit(1)
