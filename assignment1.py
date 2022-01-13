import psycopg2
import json
import sys
from typing import Dict

def get_host() -> str:
    return sys.argv[1]

def read_config() -> Dict[str, str]:
    f = open('config.json', "r")
    config = json.loads(f.read())
    if 'host' not in config or config['host'] == "":
        config['host'] = get_host()
    return config


def connect():
    # inspired by this tutorial: https://www.postgresqltutorial.com/postgresql-python/connect/
    conn = None
    try:
        conf = read_config()
        print('Connecting to Postgres')
        conn = psycopg2.connect(**conf)

        cursor = conn.cursor()
        cursor.execute('SELECT * FROM movies LIMIT 5;')
        print(cursor.fetchone())
	# close the communication with the PostgreSQL
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()
