import urllib.request
import os
import logging

FILES_URL = 'https://files.planning.data.gov.uk'
RAW_GITHUB_URL = 'https://raw.githubusercontent.com/digital-land/'

def download_dataset(dataset,collection,output_dir_path):
    dataset_file_name = f'{dataset}.sqlite3'
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    final_url = os.path.join(FILES_URL,collection,'dataset',dataset_file_name)
    logging.info(f'downloading data from {final_url}')
    urllib.request.urlretrieve(final_url,os.path.join(output_dir_path,dataset_file_name))

def download_dataset_expectations_yaml(dataset,collection,output_dir_path):
    dataset_yaml_file_name = f'{dataset}.yaml'
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    final_url = os.path.join(RAW_GITHUB_URL,collection,'main','expectations',dataset_yaml_file_name)
    logging.info(f'downloading data from {final_url}')
    urllib.request.urlretrieve(final_url,os.path.join(output_dir_path,dataset_yaml_file_name))