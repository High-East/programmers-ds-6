import requests

from utils import get_Redshift_connection


def extract(url):
    f = requests.get(url)
    return (f.text)


def transform(text):
    lines = text.split("\n")
    return lines


def load(lines, cur):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;
    #  DELETE FROM (본인의스키마).name_gender;
    #  INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;
    # END;
    for r in lines[1:]:  # Assignment 1-1: exclude header
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO rhehd127.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
            print(sql)
            cur.execute(sql)


def drop_table(cur):
    sql = "DROP TABLE IF EXISTS rhehd127.name_gender"
    cur.execute(sql)


def create_table(cur):
    sql = "CREATE TABLE rhehd127.name_gender(name varchar(32), gender varchar(8))"
    cur.execute(sql)


def run_data_pipeline(url, idempotent=True):
    # Extract
    print("[+] Extract")
    data = extract(url)

    # Transform
    print("[+] Transform")
    lines = transform(data)

    # Load
    cur = get_Redshift_connection()
    # Assignment 1-2: idempotent job
    if idempotent:
        # Guarantee idempotent
        drop_table(cur)
        create_table(cur)

    # Load data
    print("[+] Load")
    load(lines, cur)
    print("[-] Finish ETL job")


if __name__ == '__main__':
    url = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    run_data_pipeline(url)
