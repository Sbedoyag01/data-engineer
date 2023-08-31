import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Carga las variables de entorno desde el archivo .env
load_dotenv(dotenv_path="crede.env")

# Lee las variables de entorno
db_params = {
    "host": os.environ["DB_HOST"],
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "port": os.environ["DB_PORT"]
}

api_url = "https://www.datos.gov.co/resource/r9fv-awbc.json"

# Función para obtener datos de la API
def get_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener datos:", response.status_code)
        return []

# Función para insertar datos en Redshift
def insert_data_to_redshift(data):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        for item in data:
            impacto = item.get('impacto')[:250] if item.get('impacto') is not None else None
            circuito = item.get('circuito')[:250] if item.get('circuito') is not None else None
            servicio = item.get('servicio')[:250] if item.get('servicio') is not None else None
            motivo = item.get('motivo')[:250] if item.get('motivo') is not None else None
            solicita = item.get('solicita')[:250] if item.get('solicita') is not None else None
            numeroinstalacion = item.get('numeroinstalacion')[:250] if item.get('numeroinstalacion') is not None else None
            municipio = item.get('municipio')[:250] if item.get('municipio') is not None else None
            nombreresponsable = item.get('nombreresponsable')[:250] if item.get('nombreresponsable') is not None else None
            estado = item.get('estado')[:250] if item.get('estado') is not None else None
            barrio = item.get('barrio')[:250] if item.get('barrio') is not None else None
            nombrecontratista = item.get('nombrecontratista')[:250] if item.get('nombrecontratista') is not None else None
            tipoaviso = item.get('tipoaviso')[:250] if item.get('tipoaviso') is not None else None

            query = """
            INSERT INTO datos_gov_co (impacto, circuito, servicio, motivo, solicita,
            numeroinstalacion, municipio, nombreresponsable, fecha_y_hora_esperada,
            inicio, fin, horas, estado, fecharegistro, barrio, nombrecontratista, tipoaviso)
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                impacto, circuito, servicio, motivo, solicita,
                numeroinstalacion, municipio, nombreresponsable,
                item.get('fecha_y_hora_esperada'), item.get('inicio'), item.get('fin'),
                item.get('horas'), estado, item.get('fecharegistro'),
                barrio, nombrecontratista, tipoaviso
            )
            cursor.execute(query, values)

        connection.commit()
        print("Datos insertados exitosamente:", len(data), "registros")
    except Exception as e:
        print("Error al insertar datos:", e)
    finally:
        cursor.close()
        connection.close()

def main():
    print("Obteniendo datos de la API...")
    data = get_data_from_api(api_url)
    if data:
        insert_data_to_redshift(data)

if __name__ == "__main__":
    main()
