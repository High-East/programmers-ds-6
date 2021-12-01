import yaml
import psycopg2


def read_yaml(path):
    with open(path) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


def get_Redshift_connection():
    # Get login information
    login = read_yaml("/Users/KDH/project/programmers-ds-6/login.yaml")
    host = login['HOST']
    redshift_user = login['ID']
    redshift_pass = login['PW']
    port = login['PORT']
    dbname = login['NAME']
    # Login Redshift
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()
