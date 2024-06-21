import requests
from google.cloud import storage
import csv
import functions_framework

'''
Serializa un recorrido del api de RED a un  csv
Un recorrido consiste en un SERVICIO, dos posibles DESTINOS
cada (Servicio, Destino) tiene un unico PARADERO
Cada paradero tiene los siguientes elementos:
Un CODIGO, un NOMBRE, una COMUNA, una LATITUD y una LONGITUD

El formato de salida del CSV debe ser

servicio, destino, codigo_paradero, nombre_paradero, comuna_paradero, latitud_paradero, longitud_paradero
'''

@functions_framework.http
def descarga_recorrido(request):

    request_json = request.get_json(silent=True)
    if not request_json or not 'servicios'in request_json:
        return 'Error: Bad Request'

    servicios = request_json['servicios']
    
    # google cloud bucket
    client = storage.Client()
    bucket = client.bucket('bigdata003d')
    
    result = {serv: serializar(serv,bucket) for serv in servicios}
    return result

def get_recorrido(url: str):
    '''
    Devuelve los diccionarios de ida y vuelta de un recorrido determinado
    ida: Diccionario con los datos de uno de los dos posibles viajes de un recorrido. Contiene las siguientes llaves
        paraderos: Lista, donde cada miembros es un diccionario con datos del viaje
                diccionario del paradero: Contiene las siguientes llaves:
                    id: no es de nuestro interes
                    cod: codigo del paradero
                    name: nombre del paradero
                    comuna: comuna del paradero
                    pos: lista con los valores de longitud y latitud. pos[0] = latitud, pos[1]= longitud
    vuelta: equivalente a ida, con el segundo destino
    '''
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Could't get data from {url}. Status code: {response.status_code}")
            return None
        # verificar que existan los campos
        if 'ida' not in response.json():
            return None
        if 'regreso' not in response.json():
            return None
        # retornar campos
        else:
            response.encoding = 'utf-8'
            return response.json()['ida'], response.json()['regreso']
        
    except requests.exceptions.RequestException as e:
        print(f"Connetion error: {e}")
        return None

def annadir_paraderos(servico: str, rec: dict, recorrido: list=[]):
    for paradero in rec['paraderos']:
        entrada = {
            'servicio': servico,
            'destino': rec['destino'],
            'codigo_paradero': paradero['cod'],
            'nombre_paradero': paradero['name'],
            'comuna_paradero': paradero['comuna'],
            'latitud_paradero': paradero['pos'][0],
            'longitud_paradero': paradero['pos'][1]
        }
        recorrido.append(entrada)
    return recorrido

def serializar(servicio: str, bucket:storage.Client):
    keys = ['servicio', 'destino', 'codigo_paradero', 'nombre_paradero', 'comuna_paradero', 'latitud_paradero', 'longitud_paradero']
    url_base = 'https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint='
    url_servicio = f'{url_base}{servicio}'

    ida, vuelta = get_recorrido(url=url_servicio)

    if not ida and vuelta:
        return False
    
    recorrido = annadir_paraderos(servico=servicio, rec=ida)
    recorrido += annadir_paraderos(servico=servicio, rec=vuelta)

    filename=f'rec_{servicio}.csv'
    saved = save_data(filename, bucket, keys, recorrido)
    if saved:
        return True
    else:
        return False

# descarga datos en un bucket
def save_data(filename, bucket, keys, recorrido):
    blob = bucket.blob(f'rec_data/{filename}')
    try:
        with blob.open('w', newline='', encoding='UTF-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)

            writer.writeheader()
            for paradero in recorrido:
                writer.writerow(paradero)
        return True
    except:
        return False