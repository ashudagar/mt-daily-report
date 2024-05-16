"""
    module to transform the data in the format of report that needs to be generated
"""
import pandas as pd
from extract_data_from_db import get_mysql_data, get_pg_data


def transform_data():
    """
    method to transform the data fetched from mysql and postgres db using pandas lib
    :return: pandas dataframe will be returned to generate the report
    """
    mysql_data, mysql_keys = get_mysql_data()
    pg_data, pg_keys = get_pg_data()

    mysql_data = pd.DataFrame(mysql_data, columns=mysql_keys)

    pg_data = pd.DataFrame(pg_data, columns=pg_keys)

    # Merge the data based on user_id
    merged_data = pd.merge(mysql_data, pg_data, on='user_id')

    # Group by user_name and completion_date to get the count of lessons completed
    report_data = merged_data.groupby(['user_name', 'completion_date']).size().reset_index(name='lessons_completed')

    # Rename columns to match the required output
    report_data.rename(
        columns={'user_name': 'Name', 'completion_date': 'Date', 'lessons_completed': 'Number of lessons completed'},
        inplace=True)
    print(f"Data is successfully transformed in below format \n {report_data}")
    return report_data
