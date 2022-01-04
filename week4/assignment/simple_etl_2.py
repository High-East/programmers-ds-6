import requests

from utils import get_redshift_connection, Color


def drop_table(cur):
    sql = "DROP TABLE IF EXISTS name_gender"
    cur.execute(sql)


def create_table(cur):
    sql = "CREATE TABLE name_gender(name varchar(32), gender varchar(8))"
    cur.execute(sql)


def extract(url):
    f = requests.get(url)
    return (f.text)


def transform(text):
    lines = text.split("\n")
    return lines


def load(lines, cur):
    for r in lines[1:]:  # Remove header
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = f"INSERT INTO name_gender VALUES ('{name}', '{gender}')"
            print(sql)
            cur.execute(sql)


def etl(url):
    # Extract
    print(f"\n{Color.BOLD}{Color.BLUE}Extract{Color.END}")
    data = extract(url)

    # Transform
    print(f"\n{Color.BOLD}{Color.BLUE}Transform{Color.END}")
    lines = transform(data)

    # Load
    cur = get_redshift_connection()

    # Guarantee idempotent
    drop_table(cur)
    create_table(cur)

    # Load data
    print(f"\n{Color.BOLD}{Color.BLUE}Load{Color.END}")
    load(lines, cur)


if __name__ == '__main__':
    url = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    etl(url)
