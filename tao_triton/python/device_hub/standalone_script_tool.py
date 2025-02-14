from typing import List
import os
import time
import datetime
import argparse


def function_purge_logs_by_file_mtime(path_to_logs_folder: str, purge_logs_older_than_days: int):
    """
    This function will purge logs older than 2 days
    @param path_to_logs_foler: path to the logs folder, sample: /media/kevin/DATA1/tao-toolkit-triton-apps/tao_triton/python/device_hub/log
    """
    for root, dirs, files in os.walk(path_to_logs_folder):
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if os.path.isdir(dir_path):
                for file in os.listdir(dir_path):
                    file_path = os.path.join(dir_path, file)
                    if os.path.isfile(file_path):
                        file_age = time.time() - os.path.getmtime(file_path)
                        if file_age > 60 * 60 * 24 * purge_logs_older_than_days:
                            os.remove(file_path)


def function_purge_logs_by_file_name_datetime(path_to_logs_folder: str, purge_logs_older_than_days: int):
    """
    This function will purge logs older than n days by comparing the datetime in the file name
    @param path_to_logs_folder: path to the logs folder, sample: /media/kevin/DATA1/tao-toolkit-triton-apps/tao_triton/python/device_hub/log
    """
    for root, dirs, files in os.walk(path_to_logs_folder):
        for file in files:
            file_path = os.path.join(root, file)
            print("checking: {}".format(file_path))
            if os.path.isfile(file_path):
                file_name = os.path.basename(file_path)
                if '.log.' not in file_name:
                    continue
                file_name_datetime = file_name.split('.log.')[1]
                file_name_datetime = datetime.datetime.strptime(
                    file_name_datetime, '%Y-%m-%d_%H-%M-%S')
                current_datetime = datetime.datetime.now()
                kept_days = (current_datetime - file_name_datetime).days
                if kept_days >= purge_logs_older_than_days:
                    try:
                        os.remove(file_path)
                        print(f'    Deleted file: {file_path}')
                    except:
                        print(f'    !Error deleting file: {file_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',
                        '--function',
                        type=str,
                        required=False,
                        default='purge_logs',
                        help='function code for this script to exectute')
    parser.add_argument('-p',
                        '--parameters',
                        type=str,
                        required=False,
                        default='/media/kevin/DATA1/tao-toolkit-triton-apps/tao_triton/python/device_hub/log',
                        help='path to log folder')
    FLAGS = parser.parse_args()
    if FLAGS.function == 'purge_logs':
        function_purge_logs_by_file_name_datetime(FLAGS.parameters, 20)
