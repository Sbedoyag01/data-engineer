# Utiliza una imagen de Python como base
FROM python:3.8-slim

# Instala las dependencias necesarias para psycopg2
RUN apt-get update && apt-get install -y libpq-dev

# Instala Apache Airflow
RUN pip install apache-airflow

# Copia tus archivos al contenedor
COPY data_api_emp.py /usr/local/airflow/dags/
COPY crede.env /usr/local/airflow/dags/
COPY airflow.cfg /usr/local/airflow/

# Establece la ubicación de tus DAGs en Airflow
ENV AIRFLOW_HOME=/usr/local/airflow

# Inicializa la base de datos de Airflow (SQLite por defecto)
RUN airflow initdb

# Expon el puerto 8080 para acceder a la interfaz web de Airflow
EXPOSE 8080

# Comando para iniciar Airflow
CMD ["airflow", "webserver"]
