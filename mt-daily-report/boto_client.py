"""
    create boto client to connect with AWS Services
"""

import os

import boto3
from dotenv import load_dotenv

load_dotenv()
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
REGION_NAME = os.getenv('REGION_NAME')


def get_boto_client(service_name, **kwargs):
    """
    this method will be used to create the boto client
    :param service_name: name of the service for which client needs to be created
    :param kwargs
             AWS_ACCESS_KEY: aws access key
             AWS_SECRET_KEY: aws secret key
             REGION_NAME: aws region name in which service hosted
    :return: boto client for the given service
    """
    return boto3.client(service_name,
                        aws_access_key_id=AWS_ACCESS_KEY,
                        aws_secret_access_key=AWS_SECRET_KEY,
                        region_name=REGION_NAME)
