"""
    module to generate the daily report using airflow dag
"""

import os
from datetime import datetime, timedelta
import logging
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
SES_EMAIL_SOURCE = os.getenv('SES_EMAIL_SOURCE')
SES_EMAIL_DESTINATION = os.getenv('SES_EMAIL_DESTINATION')
REGION_NAME = os.getenv('REGION_NAME')
CSV_FILE_NAME = os.getenv("CSV_FILE_NAME")

default_args = {
    'owner': 'Ashu Dagar',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 16),
    'email': ['dagar.ashu053@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='daily_lesson_report',
        default_args=default_args,
        schedule_interval='@daily',  # Run daily
        catchup=False,
) as dag:
    # Define helper functions
    def get_mysql_data(ti):
        """
        method to fetch the data from mysql db
        :return: mysql records, mysql columns
        """
        # Create SQLAlchemy engine for MySQL
        mysql_engine = create_engine(
            f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')

        # Fetch data from MySQL
        mysql_conn = mysql_engine.connect()
        mysql_query = """
        SELECT user_id, lesson_id, completion_date
        FROM lesson_completion
        """
        # mysql_data = pd.read_sql_query(mysql_query, mysql_engine)

        mysql_result = mysql_conn.execute(text(mysql_query))
        mysql_data = pd.DataFrame(mysql_result.fetchall(), columns=mysql_result.keys())
        mysql_conn.close()
        print("Data is successfully fetched from mysql")
        # Push data to XCom
        ti.xcom_push(key='mysql_data', value=mysql_data)


    def get_pg_data(ti):
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
        pg_data = pd.DataFrame(postgres_result.fetchall(), columns=postgres_result.keys())
        postgres_conn.close()
        print("Data is successfully fetched from postgres")
        # Push data to XCom
        ti.xcom_push(key='pg_data', value=pg_data)


    def transform_data(ti):
        """Combines and transforms data from both sources."""
        mysql_data = ti.xcom_pull(task_ids='extract_data.extract_mysql_data', key='mysql_data')
        pg_data = ti.xcom_pull(task_ids='extract_data.extract_postgres_data', key='pg_data')

        # clean data
        # Remove duplicates
        mysql_data.drop_duplicates(inplace=True)
        pg_data.drop_duplicates(inplace=True)

        # Fill missing usernames with 'Unknown'
        pg_data.fillna({"user_name": "Unknown"}, inplace=True)

        # Ensure completion_date is in correct format
        mysql_data['completion_date'] = pd.to_datetime(mysql_data['completion_date'], errors='coerce')

        # Drop rows with invalid completion_date
        mysql_data.dropna(subset=['completion_date'], inplace=True)

        # Merge the data based on user_id
        merged_data = pd.merge(mysql_data, pg_data, on='user_id')

        # Group by user_name and completion_date to get the count of lessons completed
        report_data = merged_data.groupby(['user_name', 'completion_date']).size().reset_index(name='lessons_completed')

        # Rename columns to match the required output
        report_data.rename(
            columns={'user_name': 'Name', 'completion_date': 'Date',
                     'lessons_completed': 'Number of lessons completed'},
            inplace=True)
        print(f"Data is successfully transformed in below format \n {report_data}")
        # Push transformed data to XCom
        ti.xcom_push(key='report_data', value=report_data.to_dict())

    def generate_csv_report(ti):
        """Generates a CSV report from the transformed data."""
        report_data = pd.DataFrame.from_dict(ti.xcom_pull(task_ids='transform_data', key='report_data'))

        csv_file = CSV_FILE_NAME + datetime.now().strftime("-%Y-%m-%d_%H-%M-%S") + ".csv"
        report_data.to_csv(csv_file, index=False)
        print(f"{csv_file} is successfully generated")
        # Push CSV file path to XCom
        ti.xcom_push(key='csv_file', value=csv_file)

    def upload_to_s3(ti):
        """
        upload file to aws s3
        :param s3_client: s3 boto client
        :param csv_file: file needs to be uploaded
        :return: None
        """
        csv_file = ti.xcom_pull(task_ids='generate_csv_report', key='csv_file')

        try:
            s3_client = boto3.client('s3',
                                     aws_access_key_id=AWS_ACCESS_KEY,
                                     aws_secret_access_key=AWS_SECRET_KEY,
                                     region_name=REGION_NAME)
            s3_client.upload_file(csv_file, S3_BUCKET_NAME, csv_file)
            print(f"{csv_file} uploaded successfully to S3")
        except FileNotFoundError:
            print("The file was not found while uploading to s3")
        except NoCredentialsError:
            print("AWS Credentials not available to upload file to s3")
        except Exception as e:
            print(f"Exception raised {e} while uploading file to s3")


    def send_email(ti):
        """
        method to send the email using ses
        :param ses_client: boto3 ses client
        :param csv_file: file that need to be sent as attachment
        :return: None
        """
        csv_file = ti.xcom_pull(task_ids='generate_csv_report', key='csv_file')

        # Send email via SES
        CHARSET = "UTF-8"
        SUBJECT = "Daily Lesson Completion Report"
        BODY_TEXT = ("Please find the attached daily lesson completion report.\n\n"
                     "Best Regards,\nAshu Dagar")

        with open(csv_file, 'rb') as file:
            attachment = file.read()

        try:
            ses_client = boto3.client('ses',
                                      aws_access_key_id=AWS_ACCESS_KEY,
                                      aws_secret_access_key=AWS_SECRET_KEY,
                                      region_name=REGION_NAME)
            response = ses_client.send_raw_email(
                Source=SES_EMAIL_SOURCE,
                Destinations=[
                    SES_EMAIL_DESTINATION
                ],
                RawMessage={
                    'Data': (
                        "From: {}\n"
                        "To: {}\n"
                        "Subject: {}\n"
                        "MIME-Version: 1.0\n"
                        "Content-Type: multipart/mixed; boundary=\"NextPart\"\n\n"
                        "--NextPart\n"
                        "Content-Type: text/plain; charset={}\n\n"
                        "{}\n\n"
                        "--NextPart\n"
                        "Content-Type: text/csv; name=\"{}\"\n"
                        "Content-Description: {}\n"
                        "Content-Disposition: attachment; filename=\"{}\"; size={}\n"
                        "Content-Transfer-Encoding: base64\n\n"
                        "{}\n"
                        "--NextPart--"
                    ).format(
                        SES_EMAIL_SOURCE,
                        SES_EMAIL_DESTINATION,
                        SUBJECT,
                        CHARSET,
                        BODY_TEXT,
                        csv_file,
                        'Daily Lesson Completion Report',
                        csv_file,
                        len(attachment),
                        attachment.decode('utf-8')
                    )
                }
            )

            print("Email sent! Message ID:"),
            print(response['MessageId'])
        except NoCredentialsError:
            print("AWS Credentials not available to send email")
        except Exception as e:
            print(f"Exception raised while sending the email {e}")


    # Extract data from PostgresSQL and MySQL
    with TaskGroup(group_id='extract_data') as extract_data_group:
        extract_postgres_data = PythonOperator(
            task_id='extract_postgres_data',
            provide_context=True,
            python_callable=get_pg_data,
        )

        extract_mysql_data = PythonOperator(
            task_id='extract_mysql_data',
            provide_context=True,
            python_callable=get_mysql_data,
        )

    # Transform and merge data
    transform_data = PythonOperator(
        task_id='transform_data',
        provide_context=True,
        python_callable=transform_data,
    )

    # Generate CSV report
    generate_csv_report = PythonOperator(
        task_id='generate_csv_report',
        python_callable=generate_csv_report,
    )

    # Upload CSV to S3
    upload_csv_to_s3 = PythonOperator(
        task_id='upload_csv_to_s3',
        python_callable=upload_to_s3,
    )

    # Send Email
    send_ses_email = PythonOperator(
        task_id='send_ses_email',
        python_callable=send_email,
    )

    [extract_postgres_data, extract_mysql_data] >> transform_data >> generate_csv_report >> [upload_csv_to_s3,
                                                                                             send_ses_email]
