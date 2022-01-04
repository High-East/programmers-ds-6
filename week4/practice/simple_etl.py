import requests
from utils import get_redshift_connection


def extract(url):
    f = requests.get(url)
    return (f.text)


def transform(text):
    lines = text.split("\n")
    return lines


def load(lines):
    cur = get_redshift_connection()
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
            print(sql)
            cur.execute(sql)


# Data source
link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"

# ETL
data = extract(link)
lines = transform(data)
load(lines)
