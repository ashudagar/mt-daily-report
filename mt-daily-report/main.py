"""
    main module to load data, transform data, generate report,
    upload to s3 and send email as a python script
"""
import os

from dotenv import load_dotenv

from boto_client import get_boto_client
from generate_report import generate_csv_report
from send_email import send_email
from upload_to_s3 import upload_to_s3
from datetime import datetime
# Load environment variables from .env file
load_dotenv()
csv_file_name = os.getenv("CSV_FILE_NAME")


def daily_lesson_report():
    """
    script to generate daily report for lesson completed by each active user
    :return:
    """
    csv_file = csv_file_name + datetime.now().strftime("-%Y-%m-%d_%H-%M-%S") + ".csv"
    generate_csv_report(csv_file=csv_file)
    s3_client = get_boto_client(service_name='s3')
    upload_to_s3(s3_client=s3_client, csv_file=csv_file)

    # send email
    ses_client = get_boto_client(service_name='ses')
    send_email(ses_client=ses_client, csv_file=csv_file)


if __name__ == "__main__":
    daily_lesson_report()