import requests
import psycopg2
from datetime import datetime, timedelta
import time



# Declaración de credenciales y configuración
db_params = {
    "host": "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    "dbname": "data-engineer-database",
    "user": "sebasbg01_coderhouse",
    "password": "********", 
    "port": "5439"
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
            query = """
            INSERT INTO datos_gov_co (impacto, circuito, servicio, motivo, solicita,
            numeroinstalacion, municipio, direccion, nombreresponsable, fecha_y_hora_esperada,
            inicio, fin, horas, estado, fecharesgistro, explicacion, barrio, nombrecontratista, tipoaviso)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            values = (
                item.get('impacto'), item.get('circuito'), item.get('servicio'),
                item.get('motivo'), item.get('solicita'), item.get('numeroinstalacion'),
                item.get('municipio'), item.get('direccion'), item.get('nombreresponsable'),
                item.get('fecha_y_hora_esperada'), item.get('inicio'), item.get('fin'),
                item.get('horas'), item.get('estado'), item.get('fecharesgistro'),
                item.get('explicacion'), item.get('barrio'), item.get('nombrecontratista'),
                item.get('tipoaviso')
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
    while True:
        current_time = datetime.now()
        print("Obteniendo datos de la API...")
        data = get_data_from_api(api_url)
        if data:
            insert_data_to_redshift(data)

        # Tiempo de espera antes de la próxima actualización (cada 7 días)
        next_update_time = current_time + timedelta(days=7)
        print("Esperando hasta la próxima actualización a las:", next_update_time)
        time_to_wait = (next_update_time - datetime.now()).total_seconds()
        
        if time_to_wait > 0:
            time.sleep(time_to_wait)

if __name__ == "__main__":
    main()