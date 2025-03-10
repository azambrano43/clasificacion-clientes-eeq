from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import sys

# Se pone en el path para que sean accesibles
sys.path.append('/opt/airflow/data')
sys.path.append('/opt/airflow/utils')
#from utilities import construir_columna_fecha, obtener_cliente_db


def extraer_datos_grupo01(**kwargs):
    
    # Leer el archivo en la hoja específica que contiene los datos de potencia
    path_archivo = r"/opt/airflow/data/Resumen_datos.xlsx"
    df_demanda_potencia = pd.read_excel(path_archivo, engine="openpyxl",sheet_name="Pot_max_coin", skiprows=2)

    # Cargar los valores con xcom
    return df_demanda_potencia

def extraer_datos_grupo02(**kwargs):
    
    # Leer el archivo en la hoja específica que contiene los datos de potencia
    path_archivo = r"/opt/airflow/data/Resumen_datos.xlsx"
    df_demanda_potencia = pd.read_excel(path_archivo, engine="openpyxl",sheet_name="Pot_max_coin", skiprows=2)

    # Cargar los valores con xcom
    return df_demanda_potencia


def transformar_datos_grupo01(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_demanda_potencia = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Obtención de las demandas máximas por cada año
    melted_demanda_potencia = df_demanda_potencia.melt(id_vars="Meses", var_name="Año", value_name="Potencia (MW)")

    # Obtener solo las demandas máximas por año y construir el dataframe final
    df_potencia_max = melted_demanda_potencia[melted_demanda_potencia["Meses"]=="MAX AÑO"].copy()
    df_potencia_max = df_potencia_max[["Año", "Potencia (MW)"]]
    construir_columna_fecha(df_potencia_max)
    df_potencia_max = df_potencia_max[["Fecha", "Potencia (MW)"]].reset_index(drop=True).sort_values(by="Fecha", ascending=True)

    return df_potencia_max



def transformar_datos_grupo02(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_demanda_potencia = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Usar .melt para transformar la forma del dataframe
    melted_demanda_potencia = df_demanda_potencia.melt(id_vars="Meses", var_name="Año", value_name="Potencia (MW)")

    # Obtener solo las demandas mensuales por año y construir el dataframe final
    df_potencia = melted_demanda_potencia[melted_demanda_potencia["Meses"]!="MAX AÑO"].copy()
    construir_columna_fecha(df_potencia)
    df_potencia = df_potencia[["Fecha", "Potencia (MW)"]].reset_index(drop=True).sort_values(by="Fecha", ascending=True)

    return df_potencia



def generar_entregables_por_cliente(**kwargs):
    # Obtener el dataframe con los datos de las demandas máximas
    ti = kwargs['ti']
    demandas_maximas = ti.xcom_pull(task_ids='transformar_demandas_maximas', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = demandas_maximas.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.potencia.mediciones_max_anuales.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.potencia.mediciones_max_anuales.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.potencia.mediciones_max_anuales.insert_many(datos_insertar)


def cargar_datos_curvas_tipo(**kwargs):
    # Obtener el dataframe con los datos de las demandas mensuales
    ti = kwargs['ti']
    demandas_mensuales = ti.xcom_pull(task_ids='transformar_demandas_mensuales', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = demandas_mensuales.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.potencia.mediciones_mensuales.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.potencia.mediciones_mensuales.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.potencia.mediciones_mensuales.insert_many(datos_insertar)




with DAG('etl_dag_datos_potencia',
        start_date=datetime(2025, 2, 1), 
        schedule_interval=None, 
        catchup=False,
        description='DAG de proceso ETL para datos de clientes no regulados',
        ) as dag:
    extraer_grupo01_task = PythonOperator(
        task_id='extraer_datos_grupo01',
        python_callable=extraer_datos_grupo01,
        provide_context=True
    )
    
    extraer_grupo02_task = PythonOperator(
        task_id='extraer_datos_grupo02',
        python_callable=extraer_datos_grupo02,
        provide_context=True
    )

    transformar_datos_grupo01_task = PythonOperator(
        task_id='transformar_datos_grupo01',
        python_callable=transformar_datos_grupo01,
        provide_context=True
    )

    transformar_datos_grupo02_task = PythonOperator(
        task_id='transformar_datos_grupo02',
        python_callable=transformar_datos_grupo02,
        provide_context=True
    )

    generar_entregables_task = PythonOperator(
        task_id='generar_entregables_por_cliente',
        python_callable=generar_entregables_por_cliente,
        provide_context=True
    )

    cargar_datos_curvas_tipo_task = PythonOperator(
        task_id='cargar_datos_curvas_tipo',
        python_callable=cargar_datos_curvas_tipo,
        provide_context=True
    )

    extraer_grupo01_task >> transformar_datos_grupo01_task
    extraer_grupo02_task >> transformar_datos_grupo02_task
    transformar_datos_grupo01_task >> generar_entregables_task
    transformar_datos_grupo02_task >> generar_entregables_task
    generar_entregables_task >> cargar_datos_curvas_tipo_task
