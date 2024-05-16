"""
    module to extract data from databases
"""
import os

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
# Load environment variables
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

MYSQL_DB = os.getenv('MYSQL_DATABASE')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_ROOT_PASSWORD')
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')


def get_mysql_data():
    """
    method to fetch the data from mysql db
    :return: mysql records, mysql columns
    """
    # Create SQLAlchemy engine for MySQL
    mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')

    # Fetch data from MySQL
    mysql_conn = mysql_engine.connect()
    mysql_query = """
    SELECT user_id, lesson_id, completion_date
    FROM lesson_completion
    """
    # mysql_data = pd.read_sql_query(mysql_query, mysql_engine)

    mysql_result = mysql_conn.execute(text(mysql_query))
    mysql_data, mysql_keys = mysql_result.fetchall(), mysql_result.keys()
    # mysql_data = pd.DataFrame(mysql_result.fetchall(), columns=mysql_result.keys())
    mysql_conn.close()
    print("Data is successfully fetched from mysql")
    return mysql_data, mysql_keys


def get_pg_data():
    """
    method to fetch the data from the postgres db
    :return: pg records, pg columns
    """
    # Create SQLAlchemy engine for PostgresSQL
    postgres_engine = create_engine(
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

    # Fetch data from PostgresSQL
    postgres_conn = postgres_engine.connect()
    pg_query = """
    SELECT user_id, user_name, active_status
    FROM mindtickle_users
    WHERE active_status = 'active'
    """
    # pg_data = pd.read_sql_query(pg_query, postgres_engine)

    postgres_result = postgres_conn.execute(text(pg_query))
    pg_data, pg_keys = postgres_result.fetchall(), postgres_result.keys()
    # pg_data = pd.DataFrame(postgres_result.fetchall(), columns=postgres_result.keys())
    postgres_conn.close()
    print("Data is successfully fetched from postgres")
    return pg_data, pg_keys
