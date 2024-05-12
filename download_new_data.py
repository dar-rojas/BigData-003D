import requests
import functions_framework
import zipfile
import re
import tempfile
from google.cloud import storage

@functions_framework.http
def download_data(request):
    dtpm_url = 'https://www.dtpm.cl/'
    #URL de DTPM a scrapear
    dtpm_scrap_url = 'https://www.dtpm.cl/index.php/gtfs-vigente'
    #bucket donde se guardan los archivos descargas
    bucket_id: str = ' '
    #patron de regex para obtener la info a scrapear
    patron = r'(\/descargas\/gtfs\/((GTFS-V\d+-PO)(\d+)(\.zip)))'


    #Scrap de la URL
    url_text: str = get_data(dtpm_scrap_url).text
    if url_text:
        #zip_url[0][0] path del archivo
        #zip_url[0][1] nombre del archivo
        #zip_url[0][2] ignorable
        #zip_url[0][3] fecha
        #zip_url[0][4] extension
        zip_url = re.findall(patron, url_text)

    #bucket
    client = storage.Client()
    bucket = client.bucket(bucket_id)

    save_data(zip_url[0][1], bucket=bucket, url=f'{dtpm_url}{zip_url[0][0][1:]}')
    unzip(date=zip_url[0][3], object_name=zip_url[0][1], bucket=bucket)
    return "Ok"


def get_data(url: str) -> requests.Response:
    try:
        response = requests.get(url=url)
        if response.status_code != 200:
            print(f"Could't get data from {url}. Status code: {response.status_code}")
        else:
            return response.content
    except requests.exceptions.RequestException as e:
        print(f'Connection Error: {e}')
        return None

def save_data(filename: str, bucket: storage.Bucket, url: requests.Response):
    data = get_data(url).content
    blob = bucket.blob(f'Downloads/{filename}.zip')
    with blob.open('wb') as file:
        file.write(data)

def unzip(date: str, object_name: str, bucket: storage.Bucket):
    blob: storage.Blob =  bucket.blob(object_name)

    with tempfile.NamedTemporaryFile() as temp_file:
        blob.download_to_filename(temp_file.name)

        with zipfile.ZipFile(temp_file.name, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                destination_blob = bucket.blob(f'Unziped/{date}/{file_info.filename}')
                with destination_blob.open('wb') as f:
                    f.write(zip_ref.read(file_info))