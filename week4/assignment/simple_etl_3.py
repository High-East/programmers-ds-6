import psycopg2
import requests

from utils import get_redshift_connection2, Color


def extract(url):
    f = requests.get(url)
    return (f.text)


def transform(text):
    lines = text.split("\n")
    return lines


def load(data, table, autocommit=True, is_error=False):
    conn = get_redshift_connection2(autocommit=autocommit)
    cur = conn.cursor()

    # autocommit = True version
    if autocommit:
        try:
            cur.execute('BEGIN;')
            cur.execute(f'DELETE FROM {table};')
            for r in data[1:]:
                if r != '':
                    name, gender = r.split(",")
                    if not is_error:
                        sql = f"INSERT INTO {table} VALUES ('{name}', '{gender}');"
                    else:
                        sql = f"INSERT INTO {table} VALUES ({name}, {gender});"
                    print(sql)
                    cur.execute(sql)
            cur.execute('END;')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK;")
        finally:
            conn.close()

    # autocommit = False version
    else:
        try:
            cur.execute(f'DELETE FROM {table};')
            for r in data[1:]:
                if r != '':
                    name, gender = r.split(",")
                    if not is_error:
                        sql = f"INSERT INTO {table} VALUES ('{name}', '{gender}');"
                    else:
                        sql = f"INSERT INTO {table} VALUES ({name}, {gender});"
                    print(sql)
                    cur.execute(sql)
            cur.execute("COMMIT;")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK;")
        finally:
            conn.close()

def select(table, limit):
    print(f"\n{Color.BOLD}{Color.BLUE}SELECT{Color.END}")
    conn = get_redshift_connection2(autocommit=True)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} LIMIT {limit};")
    res = cur.fetchall()
    for r in res:
        print(r)
    conn.close()


def count(table):
    print(f"\n{Color.BOLD}{Color.BLUE}COUNT{Color.END}")
    conn = get_redshift_connection2(autocommit=True)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(1) FROM {table};")
    print(cur.fetchall())
    conn.close()


def etl(source, table, autocommit=True, is_error=False):
    # Extract
    print(f"\n{Color.BOLD}{Color.BLUE}Extract{Color.END}")
    raw_data = extract(source)

    # Transform
    print(f"\n{Color.BOLD}{Color.BLUE}Transform{Color.END}")
    data = transform(raw_data)

    # Load
    print(f"\n{Color.BOLD}{Color.BLUE}Load{Color.END}")
    load(data, table, autocommit, is_error)


if __name__ == '__main__':
    source = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    etl(source, table='name_gender', autocommit=True, is_error=False)
    count('name_gender')
