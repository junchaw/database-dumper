import json
import os
import shutil
import sys

import click
import pymysql
import yaml

VERBOSE_NONE = 0

VERBOSE_IMPORTANT = 1

VERBOSE_EVERYTHING = 2


class ConnectionError(Exception):
    pass


# connection creates connection to database
def connection():
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


class RecoverError(Exception):
    pass


def drop_table_if_exists(conn, table_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE {}".format(table_name))
        conn.commit()
    except pymysql.err.InternalError as e:
        try:
            # ignore error if table does not exist
            str(e).index("Unknown table ")
        except ValueError:
            raise e


def dump_table(conn, table_dir, table_name, verbose=VERBOSE_NONE):
    print('Processing table "{}"...'.format(table_name))
    os.mkdir(table_dir)
    with open(os.path.join(table_dir, "create_table.sql"), "w") as f:
        if verbose > VERBOSE_NONE:
            print("  Saving create table statement...")
        f.write(get_create_table_stmt(conn, table_name))
    with open(os.path.join(table_dir, "desc_table.yaml"), "w") as f:
        if verbose > VERBOSE_NONE:
            print("  Saving desc table output...")
        columns = get_columns(conn, table_name)
        f.write(yaml.dump(columns))
    if verbose > VERBOSE_NONE:
        print("  Checking columns...")
    for column in columns:
        if column["Type"] in ["text", "mediumtext", "longtext"]:
            if verbose > VERBOSE_NONE:
                click.echo(click.style(
                    '  Column "{}" is long data.'.format(column["Field"]),
                    fg='yellow'))
            os.mkdir(os.path.join(table_dir,
                                  "column_{}".format(column["Field"])))
    rows = ""
    for i, row in enumerate(get_rows(conn, table_name)):
        if verbose > VERBOSE_IMPORTANT:
            print('  -- {}'.format(i))
        elif verbose > VERBOSE_NONE:
            print('.', end='')
        text_list = []
        for column in columns:
            if column["Type"] in ["text", "mediumtext", "longtext"]:
                with open(os.path.join(table_dir,
                                       "column_{}".format(column["Field"]),
                                       "{}.txt".format(i)), "w") as conn:
                    conn.write(str(row[column["Field"]]))
                text_list.append("_")
            else:
                text_list.append(str(row[column["Field"]]))
        rows += json.dumps(text_list) + "\n"
    with open(os.path.join(table_dir, "rows.txt"), "w") as f:
        f.write(rows)
    print()


def dump(data_dir, verbose=VERBOSE_NONE):
    if os.path.isdir(data_dir):
        print('"{}" is a directory, removing...'.format(data_dir))
        shutil.rmtree(data_dir)
    elif os.path.isfile(data_dir):
        print('"{}" is a file, removing...'.format(data_dir))
        os.remove(data_dir)
    print('Creating directory "{}"...'.format(data_dir))
    os.mkdir(data_dir)

    try:
        conn = connection()
        try:
            for table in get_table_names(conn):
                dump_table(conn, os.path.join(data_dir, table), table,
                           verbose=verbose)
        finally:
            conn.close()
        print("Done!")
    except ConnectionError as e:
        print(e)
        exit(1)


def recover_table(conn, table_dir, table_name):
    print("Recovering table {}:".format(table_name))
    if not os.path.isdir(table_dir):
        raise RecoverError("\"{}\" is not a directory".format(table_dir))

    print("... dropping table if exists ...")
    drop_table_if_exists(conn, table_name)

    print("... creating table ...")
    with open(os.path.join(table_dir, "create_table.sql")) as f:
        with conn.cursor() as cursor:
            cursor.execute(f.read())
        conn.commit()

    print("... recovering data ...")
    with open(os.path.join(table_dir, "desc_table.yaml")) as f:
        columns = yaml.load(f.read(), yaml.Loader)
    placeholders = []
    bigDataFlags = {}
    for i, column in enumerate(columns):
        placeholders.append("%s")
        if column["Type"] in ["text", "mediumtext", "longtext"]:
            bigDataFlags[i] = "column_{}".format(column["Field"])
    stmt = "INSERT INTO {} VALUE ({})".format(table_name,
                                              ", ".join(placeholders))
    with open(os.path.join(table_dir, "rows.txt")) as f:
        lineNumber = 0
        for line in f:
            data = json.loads(line)
            for i, value in enumerate(data):
                if i in bigDataFlags:
                    with open(os.path.join(table_dir, bigDataFlags[i],
                                           "{}.txt".format(lineNumber))) as c:
                        data[i] = c.read()
                else:
                    data[i] = value
            with conn.cursor() as cursor:
                cursor.execute(stmt, data)
            conn.commit()
            lineNumber += 1


def recover(data_dir, verbose=VERBOSE_NONE):
    if not os.path.isdir(data_dir):
        raise RecoverError("\"{}\" is not a directory".format(data_dir))

    try:
        conn = connection()
        try:
            for table in os.listdir(data_dir):
                recover_table(conn, os.path.join(data_dir, table), table)
        finally:
            conn.close()
    except ConnectionError as e:
        print(e)
        exit(1)


@click.command()
@click.argument('action')
@click.option('--verbose', default=VERBOSE_NONE, help='Verbose level.')
def main(verbose, action):
    if action == "dump":
        dump("data", verbose=verbose)
    elif action == "recover":
        recover("data", verbose=verbose)
    else:
        print("No such action.")


if __name__ == '__main__':
    main()
