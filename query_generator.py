#!/usr/bin/env python3
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
import os
import time
from os import listdir
from os.path import isfile, join



def uploadToBlobStorage(blob_service_client: BlobServiceClient, file_path: str, file_name :str, container_name: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    with open(file_path,'rb') as data:
        blob_client.upload_blob(data)
        print(f'\n\nUploaded {file_path} to {file_name}.\n\n')

def download_blob_to_file(blob_service_client: BlobServiceClient, file_out_path: str, file_name_blob: str, container_name: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name_blob)
    os.makedirs(os.path.dirname(file_out_path), exist_ok=True)
    with open(file_out_path,"wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())
    print(f'Downloaded {file_name_blob} to {file_out_path}.\n\n')

def delete_blob(blob_service_client: BlobServiceClient,container_name: str):
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    format_list = ['csv','parquet','xlsx']
    for blob in blob_list:
        if ('query_generator' in blob.name):
            try:
                if blob.name.split('.')[1] in format_list:
                    container_client.delete_blob(blob.name)
                    print(f'{blob.name} Blob deleted.')
            except:
                pass
    print('\n\n')


def check_blob(connection_string:str, container_name: str, blob_name: str):
    blob = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=blob_name)
    exists = blob.exists()
    return exists

def check_csv(blob_service_client:BlobServiceClient, container_name: str, connection_string: str):
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    blob_name = 'query_generator/none.csv'
    for blob in blob_list:
        if ('query_generator' in blob.name):
            try:
                if '.csv' in blob.name and '_temporary' not in blob.name:
                    blob_name = blob.name
            except:
                pass
    check = check_blob(connection_string, container_name, blob_name)
    return check, blob_name

def files_path(input_path: str):
    filename = ''
    only_files = [f for f in listdir(input_path) if (isfile(join(input_path, f)) and 'xlsx' in f)]
    try:
        filename = only_files[0]
    except IndexError:
        print('Error: cannot found an Excel file')
    file_name_blob_in = 'query_generator/'+filename
    return 'result/'+filename.replace('xlsx','csv'), file_name_blob_in, filename



def main(blob_service_client:BlobServiceClient, container_name: str, connection_string: str):
    file_input_path = os.getcwd()
    file_out_path, file_name_blob_in, file_input = files_path(file_input_path)
    uploadToBlobStorage(blob_service_client, file_input, file_name_blob_in, container_name)
    check, file_name_blob_out = check_csv(blob_service_client, container_name, connection_string)

    while(check == False):
        check, file_name_blob_out = check_csv(blob_service_client, container_name, connection_string)
    if('fail.csv' in file_name_blob_out):
        print('\n\nERROR: The specific name for table columns are not correct.\n')
    else:
        time.sleep(2)
        download_blob_to_file(blob_service_client, file_out_path, file_name_blob_out, container_name)
        time.sleep(2)
    delete_blob(blob_service_client,container_name)



connection_string = 'xxxxxxx'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name = 'xxxxxx'



main(blob_service_client, container_name, connection_string)


