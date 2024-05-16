"""
    module to send the email using amazon SES
"""
import os

from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()
SES_EMAIL_SOURCE = os.getenv('SES_EMAIL_SOURCE')
SES_EMAIL_DESTINATION = os.getenv('SES_EMAIL_DESTINATION')


def send_email(ses_client, csv_file):
    """
    method to send the email using ses
    :param ses_client: boto3 ses client
    :param csv_file: file that need to be sent as attachment
    :return: None
    """
    # Send email via SES
    CHARSET = "UTF-8"
    SUBJECT = "Daily Lesson Completion Report"
    BODY_TEXT = ("Please find the attached daily lesson completion report.\n\n"
                 "Best Regards,\nAshu Dagar")

    with open(csv_file, 'rb') as file:
        attachment = file.read()

    try:
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
