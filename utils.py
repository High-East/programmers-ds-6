import yaml
import psycopg2


def read_yaml(path):
    with open(path) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


def get_redshift_connection():
    # Get login information
    login = read_yaml("/Users/donghee/Library/Mobile Documents/com~apple~CloudDocs/Project/programmers-ds-6/login.yaml")
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


def get_redshift_connection2(autocommit=True):
    # Get login information
    login = read_yaml("/Users/donghee/Library/Mobile Documents/com~apple~CloudDocs/Project/programmers-ds-6/login.yaml")
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
    conn.set_session(autocommit=autocommit)
    return conn


class Color:
    # print(Color.BOLD + 'Hello World !' + Color.END)
    RED = '\033[91m'
    YELLOW = '\033[93m'
    GREEN = '\033[92m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    PURPLE = '\033[95m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

    @staticmethod
    def show():
        print(
            'Color(',
            f'    {Color.RED}RED{Color.END}',
            f'    {Color.YELLOW}YELLOW{Color.END}',
            f'    {Color.GREEN}GREEN{Color.END}',
            f'    {Color.BLUE}BLUE{Color.END}',
            f'    {Color.CYAN}CYAN{Color.END}',
            f'    {Color.DARKCYAN}DARKCYAN{Color.END}',
            f'    {Color.PURPLE}PURPLE{Color.END}',
            f'    {Color.BOLD}BOLD{Color.END}',
            f'    {Color.UNDERLINE}UNDERLINE{Color.END}',
            f'    {Color.END}END{Color.END}',
            ')',
            sep='\n'
        )
