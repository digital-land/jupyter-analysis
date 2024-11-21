import urllib.request
import os
import logging

FILES_URL = 'https://files.planning.data.gov.uk'
DATASETTE_URL = 'https://datasette.planning.data.gov.uk/'
RAW_GITHUB_URL = 'https://raw.githubusercontent.com/digital-land/'

def download_dataset(dataset,collection,output_dir_path,overwrite=False):
    dataset_file_name = f'{dataset}.sqlite3'
    
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    
    output_file_path = os.path.join(output_dir_path,dataset_file_name)

    if overwrite is False and os.path.exists(output_file_path):
        return
    
    final_url = os.path.join(FILES_URL,collection,'dataset',dataset_file_name)
    logging.info(f'downloading data from {final_url}')
    urllib.request.urlretrieve(final_url,os.path.join(output_dir_path,dataset_file_name))



def download_datasette_dataset(dataset,output_dir_path,overwrite=False):
    
    dataset_file_name = f'{dataset}.sqlite3'
    
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    
    output_file_path = os.path.join(output_dir_path,dataset_file_name)

    if overwrite is False and os.path.exists(output_file_path):
        return
    
    final_url = os.path.join(DATASETTE_URL,f'{dataset}.db')
    logging.info(f'downloading data from {final_url}')
    urllib.request.urlretrieve(final_url,os.path.join(output_dir_path,dataset_file_name))
