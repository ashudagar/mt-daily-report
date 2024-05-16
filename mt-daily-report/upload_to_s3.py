"""
    module to upload file to s3 for given bucket
"""
import os

from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')


def upload_to_s3(s3_client, csv_file):
    """
    upload file to aws s3
    :param s3_client: s3 boto client
    :param csv_file: file needs to be uploaded
    :return: None
    """
    try:
        s3_client.upload_file(csv_file, S3_BUCKET_NAME, csv_file)
        print(f"{csv_file} uploaded successfully to S3")
    except FileNotFoundError:
        print("The file was not found while uploading to s3")
    except NoCredentialsError:
        print("AWS Credentials not available to upload file to s3")
    except Exception as e:
        print(f"Exception raised {e} while uploading file to s3")
