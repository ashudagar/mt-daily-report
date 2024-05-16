"""
    module to generate the csv resport
"""
from transform_data import transform_data


def generate_csv_report(csv_file):
    """
    method to generate the csv report
    :param csv_file: file name in which data will be stored
    :return: None
    """
    report_data = transform_data()
    report_data.to_csv(csv_file, index=False)
    print(f"{csv_file} is successfully generated")
