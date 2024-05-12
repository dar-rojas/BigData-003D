""" Cloud function para descarga de datos historicos """

import functions_framework
import requests
import zipfile
import re
import tempfile
from google.cloud import storage

@functions_framework.http
def download_historical_data(request):

    api_url = ''
    bucket_id = ''

    # obtencion de urls para descarga de archivos historicos
    url_data = get_data(api_url).json()
    if url_data:
        resources = url_data['result']['resources'] 
        urls = {i['id']:i['url'] for i in resources}

    # google cloud bucket
    client = storage.Client()
    bucket = client.bucket(bucket_id)

    #descarga de archivos zip
    for key in urls.keys:
        save_data(filename=key, bucket=bucket, url=urls[key])

    zip_pattern = re.compile(r'.*\.zip$')
    zip_files = [blob.name for blob in list(bucket.list_blobs(prefix='Downloads')) if zip_pattern.match(blob.name)]
    # decompresion de archivos
    for i in zip_files:
        unzip(i, bucket)
    return "OK"

# get request 
def get_data(url:str):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Could't get data from {url}. Status code: {response.status_code}")
            return None
        else:
            return response
    except requests.exceptions.RequestException as e:
        print(f"Connetion error: {e}")
        return None

# descarga datos en un bucket
def save_data(filename, bucket, url):
    data = get_data(url).content
    blob = bucket.blob(f'Downloads/{filename}.zip')
    with blob.open('wb') as file:
        file.write(data)

# decompresión de archivos zip
def unzip(object_name, bucket):
    blob = bucket.blob(object_name)
    
    with tempfile.NamedTemporaryFile() as temp_file:
        blob.download_to_filename(temp_file.name)

        with zipfile.ZipFile(temp_file.name, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                destination_blob = bucket.blob(f'Unziped/{object_name[10:-4]}/{file_info.filename}')
                with destination_blob.open('wb') as f:
                    f.write(zip_ref.read(file_info))