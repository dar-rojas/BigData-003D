import requests
#from google.cloud import storage
import csv

'''
Serializa un recorrido del api de RED a un  csv
Un recorrido consiste en un SERVICIO, dos posibles DESTINOS
cada (Servicio, Destino) tiene un unico PARADERO
Cada paradero tiene los siguientes elementos:
Un CODIGO, un NOMBRE, una COMUNA, una LATITUD y una LONGITUD

El formato de salida del CSV debe ser

servicio, destino, codigo_paradero, nombre_paradero, comuna_paradero, latitud_paradero, longitud_paradero
'''

lista_servicios = ['101']


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
    req = requests.get(url=url)
    return req.json()['ida'], req.json()['regreso']

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

def serializar(servicio: str):
    keys = ['servicio', 'destino', 'codigo_paradero', 'nombre_paradero', 'comuna_paradero', 'latitud_paradero', 'longitud_paradero']
    url_base = 'https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint='
    url_servicio = f'{url_base}{servicio}'

    ida, vuelta = get_recorrido(url=url_servicio)

    recorrido = annadir_paraderos(servico=servicio, rec=ida)
    recorrido += annadir_paraderos(servico=servicio, rec=vuelta)

    filename=f'rec_{servicio}.csv'

    with open(filename, 'w', newline='', encoding='UTF-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)

        writer.writeheader()
        for paradero in recorrido:
            writer.writerow(paradero)