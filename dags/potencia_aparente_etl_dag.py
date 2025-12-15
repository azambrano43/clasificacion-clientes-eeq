from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.decorators import task

from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
from datetime import timedelta
mmscaler = MinMaxScaler()
from datetime import datetime
import pandas as pd
import numpy as np

import gc
import sys
import os
import warnings

# Ignorar las alerrtas
warnings.filterwarnings("ignore")

# Se pone en el path para que sean accesibles
sys.path.append('/opt/airflow/data')
sys.path.append('/opt/airflow/utils')
sys.path.append('/opt/airflow/outputs')

from utilities import *

def extraer_datos_grupo01(**kwargs):
    # Obtener una lista con los nombres de los archivos de las mediciones del grupo 01
    mediciones_clientes_g1 = r"/opt/airflow/data/mediciones_por_mes_g1"
    archivos_mediciones_g1 = list(os.scandir(mediciones_clientes_g1))
    
    # Definir las columnas de interés
    columnas_extraer_g1 = ["Fecha","Demanda activa DEL","Demanda reactiva DEL"]
    
    # Iteramos sobre la lista y almacenamos los clientes únicos sobre los que hay que iterar
    clientes_unicos_g1 = set()
    for medicion in archivos_mediciones_g1:
        cliente = medicion.name.split('-')[1]
        clientes_unicos_g1.add(cliente)
        
    # Diccionario para almacenar los clientes con sus respectivos datos
    dict_dfs_clientes_g1 = {}

    # Iterar sobre cada cliente
    for cliente in clientes_unicos_g1:
        datos_cliente = []
        # Iterar sobre cada archivo de medicion del grupo 01
        for medicion in archivos_mediciones_g1:
            if cliente == medicion.name.split("-")[1]:
                df_cliente = intentar_abrir_archivo_datos(f"{mediciones_clientes_g1}/{medicion.name}")
                datos_cliente.extend(df_cliente[columnas_extraer_g1].values)

        # Convertir a DataFrame los datos concatenados
        df_datos_anual_cliente = pd.DataFrame(datos_cliente, columns=columnas_extraer_g1)
        
        # Almacenar en el diccionario (Clave->Cliente   Valor->DataFrame)
        dict_dfs_clientes_g1[cliente]=df_datos_anual_cliente
        
        # Eliminar dataframe concatenado para liberar memoria
        del df_datos_anual_cliente
        gc.collect()

    # Cargar los valores con xcom
    return dict_dfs_clientes_g1

def extraer_datos_grupo02(**kwargs):
    # Obtener una lista con los nombres de los archivos de las mediciones del grupo 02
    mediciones_clientes_g2 = "/opt/airflow/data/mediciones_por_mes_g2"
    archivos_mediciones_g2 = list(os.scandir(mediciones_clientes_g2))
    
    # Definir las columnas de interés
    columnas_extraer_g2 = ["Fecha", "AS (kWh)"]
    
    # Diccionario para almacenar los clientes con sus datos
    dict_dfs_clientes_g2 = {}

    # Iterar sobre cada cliente
    for archivos_cliente in archivos_mediciones_g2:
        nombre_cli = archivos_cliente.name.strip()
        df_concat = pd.DataFrame()

        # Obtener los archivos de las mediciones mensuales del cliente
        mediciones_mensuales_cliente = os.scandir(rf"{mediciones_clientes_g2}/{nombre_cli}")

        for medicion in mediciones_mensuales_cliente:
            medicion_mensual = pd.read_csv(rf"{mediciones_clientes_g2}/{nombre_cli}/{medicion.name}", sep=";", skiprows=2, encoding='ISO-8859-1')
            medicion_mensual = medicion_mensual[columnas_extraer_g2]
            df_concat = pd.concat([df_concat, medicion_mensual])

        # Convertir a tipo de dato serializable
        df_concat["Fecha"] = df_concat["Fecha"].astype("string")
        df_concat["AS (kWh)"] = df_concat["AS (kWh)"].astype("string")
        
        # Almacenar en el diccionario (Clave->Cliente   Valor->DataFrame)
        dict_dfs_clientes_g2[nombre_cli] = df_concat
        
        # Eliminar dataframe concatenado para liberar memoria
        del df_concat
        gc.collect()
    
    # Retornar el diccionario
    return dict_dfs_clientes_g2


def transformar_datos_grupo01(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    dict_dfs_clientes_g1 = ti.xcom_pull(task_ids='ETL.extraer_datos_grupo01', key='return_value')

    gc.collect()
    dict_dfs_procesados_g1 = {}

    for cliente, df_archivo_g1 in dict_dfs_clientes_g1.items():

        # Transformar columna fecha a cadena
        df_archivo_g1["Fecha"] = df_archivo_g1["Fecha"].astype("string")

        # Eliminar los valores nulos en la columna 'Fecha'
        df_archivo_g1 = df_archivo_g1.dropna(subset="Fecha")

        # Separar para obtener columna Hora
        df_archivo_g1["Hora"] = df_archivo_g1["Fecha"].apply(lambda x: x.split()[1].strip())

        # Separar para obtener columna Fecha
        df_archivo_g1["Fecha"] = df_archivo_g1["Fecha"].apply(lambda x: x.split()[0].strip())

        # Transformar la columna Fecha a un formato único
        df_archivo_g1["Fecha"] = df_archivo_g1["Fecha"].apply(fecha_formato_unico)
        
        # Transformar la columna Hora a un formato único hh:mm
        df_archivo_g1["Hora"] = df_archivo_g1["Hora"].apply(hora_formato_unico)


        # Debido a que a veces se ponen datos del 2024 para reemplazar los faltantes del 2023
        # debemos descartar la fecha 29 de febrero, pues en 2023 no existe
        df_archivo_g1 = df_archivo_g1[df_archivo_g1["Fecha"] != "2023/02/29"]
        
        # Eliminar duplicados y conservar el original del 2023
        df_archivo_g1["Fecha-Hora"] = df_archivo_g1["Fecha"] + " " + df_archivo_g1["Hora"]
        df_archivo_g1 = df_archivo_g1.drop_duplicates(subset="Fecha-Hora", keep="first")

        # Transformar columna Fecha a datetime
        df_archivo_g1["Fecha"] = pd.to_datetime(df_archivo_g1["Fecha"], format='%Y/%m/%d')

        # Obtener la potencia aparente
        df_archivo_g1["Potencia_aparente"] = np.sqrt((df_archivo_g1["Demanda activa DEL"]**2) + (df_archivo_g1["Demanda reactiva DEL"]**2))

        # Eliminar días feriados y días de fin de semana
        df_archivo_g1 = df_archivo_g1[~df_archivo_g1['Fecha'].isin(feriados_nacionales) & ~df_archivo_g1['Fecha'].dt.weekday.isin([5, 6])]

        # Interpolar valores nulos usando una función polinomial
        df_archivo_g1["Potencia_aparente"] = df_archivo_g1["Potencia_aparente"].interpolate(method='cubicspline')

        # Conservar solo las columnas de interés
        df_archivo_g1 = df_archivo_g1[["Fecha", "Hora", "Potencia_aparente"]]

        # Escalar las mediciones (cada día se escala individualmente)
        df_archivo_g1["Potencia_aparente_escalada"] = df_archivo_g1.groupby("Fecha")["Potencia_aparente"].transform(
            lambda x: (x - x.min()) / (x.max() - x.min()) if (x.max() - x.min()) != 0 else 0
        )

        # Transformar a float32 para reducir consumo de memoria
        df_archivo_g1["Potencia_aparente"] = df_archivo_g1["Potencia_aparente"].astype("float32")
        df_archivo_g1["Potencia_aparente_escalada"] = df_archivo_g1["Potencia_aparente_escalada"].astype("float32")

        # Guardar en un nuevo diccionario los datos procesados
        dict_dfs_procesados_g1[cliente] = df_archivo_g1
        
        # Liberar memoria
        del df_archivo_g1
    
    # Liberar memoria
    del dict_dfs_clientes_g1
    gc.collect()
    
    # Retornar el dicccionario con los DataFrames que tienen los datos procesados
    return dict_dfs_procesados_g1


def transformar_datos_grupo02(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    dict_dfs_clientes_g2 = ti.xcom_pull(task_ids='ETL.extraer_datos_grupo02', key='return_value')

    dict_dfs_procesados_g2 = {}

    for cliente, df_archivo_g2 in dict_dfs_clientes_g2.items():

        # Transformar columna fecha a cadena
        df_archivo_g2["Fecha"] = df_archivo_g2["Fecha"].astype("string")

        # Eliminar registros que contienen el total
        df_archivo_g2 = df_archivo_g2[~df_archivo_g2["Fecha"].str.contains("Total")]

        # Eliminar los valores nulos en la columna 'Fecha'
        df_archivo_g2 = df_archivo_g2.dropna(subset="Fecha")

        # Separar para obtener columna Hora
        df_archivo_g2["Hora"] = df_archivo_g2["Fecha"].apply(lambda x: x.split()[1].strip())

        # Separar para obtener columna Fecha
        df_archivo_g2["Fecha"] = df_archivo_g2["Fecha"].apply(lambda x: x.split()[0].strip())

        # Eliminar duplicados y conservar el original del 2023
        df_archivo_g2["Fecha-Hora"] = df_archivo_g2["Fecha"] + " " + df_archivo_g2["Hora"]
        df_archivo_g2["Fecha-Hora"] = pd.to_datetime(df_archivo_g2["Fecha-Hora"], yearfirst=True)
        df_archivo_g2 = df_archivo_g2.drop_duplicates(subset="Fecha-Hora", keep="first")

        # Restar un timedelta de 15 a todas las fechas (Solo en este caso por que la ultima fecha)
        # se pasa al siguiente mes
        df_archivo_g2["Fecha-Hora"] = df_archivo_g2["Fecha-Hora"] - pd.Timedelta(minutes=15)

        # Separar nuevamente para obtener columna Hora
        df_archivo_g2["Hora"] = df_archivo_g2["Fecha-Hora"].astype("string").apply(lambda x: x.split()[1][:-3].strip())

        # Separar nuevamente para obtener columna Fecha
        df_archivo_g2["Fecha"] = df_archivo_g2["Fecha-Hora"].astype("string").apply(lambda x: x.split()[0].strip())

        # Transformar la columna Fecha a un formato único
        df_archivo_g2["Fecha"] = df_archivo_g2["Fecha"].apply(fecha_formato_unico)

        # Debido a que a veces se ponen datos del 2024 para reemplazar los faltantes del 2023
        # debemos descartar la fecha 29 de febrero, pues en 2023 no existe
        df_archivo_g2 = df_archivo_g2[df_archivo_g2["Fecha"] != "2023/02/29"]

        # Transformar columna Fecha a datetime
        df_archivo_g2["Fecha"] = pd.to_datetime(df_archivo_g2["Fecha"], format='%Y/%m/%d')

        # Limpiar la columna 'SE (KVah)'
        df_archivo_g2["AS (kWh)"] = df_archivo_g2["AS (kWh)"].astype("string").str.replace(",", "").replace('"','')
        df_archivo_g2["AS (kWh)"] = df_archivo_g2["AS (kWh)"].astype("float")

        # Obtener la potencia aparente
        df_archivo_g2["Potencia_aparente"] = df_archivo_g2["AS (kWh)"] * 4

        # Eliminar días feriados y días de fin de semana
        df_archivo_g2 = df_archivo_g2[~df_archivo_g2['Fecha'].isin(feriados_nacionales) & ~df_archivo_g2['Fecha'].dt.weekday.isin([5, 6])]

        # Interpolar valores nulos usando una función polinomial
        df_archivo_g2["Potencia_aparente"] = df_archivo_g2["Potencia_aparente"].interpolate(method='cubicspline')

        # Conservar solo las columnas de interés
        df_archivo_g2 = df_archivo_g2[["Fecha", "Hora", "Potencia_aparente"]]

        # Escalar las mediciones (cada día se escala individualmente)
        df_archivo_g2["Potencia_aparente_escalada"] = df_archivo_g2.groupby("Fecha")["Potencia_aparente"].transform(
            lambda x: (x - x.min()) / (x.max() - x.min()) if (x.max() - x.min()) != 0 else 0
        )
        
        # Transformar a float32 para reducir consumo de memoria
        df_archivo_g2["Potencia_aparente"] = df_archivo_g2["Potencia_aparente"].astype("float32")
        df_archivo_g2["Potencia_aparente_escalada"] = df_archivo_g2["Potencia_aparente_escalada"].astype("float32")

        # Guardar en un nuevo diccionario los datos procesados
        dict_dfs_procesados_g2[cliente] = df_archivo_g2
        
        # Liberar memoria
        del df_archivo_g2
    
    # Liberar memoria
    del dict_dfs_clientes_g2
    gc.collect()
    
    # Retornar el dicccionario con los DataFrames que tienen los datos procesados
    return dict_dfs_procesados_g2

def transformar_datos_unificados(**kwargs):
    # Obtener los diccionarios con los datos del grupo de clientes 01 y 02
    ti = kwargs['ti']
    dict_dfs_procesados_g1 = ti.xcom_pull(task_ids='ETL.transformar_datos_grupo01', key='return_value')
    dict_dfs_procesados_g2 = ti.xcom_pull(task_ids='ETL.transformar_datos_grupo02', key='return_value')    

    # Unificar todos los clientes procesados
    dict_todos_los_clientes = dict_dfs_procesados_g1 | dict_dfs_procesados_g2
    del dict_dfs_procesados_g1, dict_dfs_procesados_g2
    gc.collect()

    # Construir el DataFrame consolidado de curvas tipo (30 min) por cliente
    registros_curvas_todas = []
    columnas_df_todas_las_curvas = None

    for cliente, df_medicion_anual in dict_todos_los_clientes.items():
        df_curva_tipo = obtener_coords_curva_tipo(df_medicion_anual)

        # Armar registro: [Cliente, valores escalados...]
        fila = [cliente]
        fila.extend(df_curva_tipo["Potencia_aparente_escalada"].values.tolist())
        registros_curvas_todas.append(fila)

        # Guardar columnas una sola vez
        if columnas_df_todas_las_curvas is None:
            columnas_df_todas_las_curvas = ["Cliente"] + df_curva_tipo["Hora"].values.tolist()

    del dict_todos_los_clientes
    gc.collect()

    # Construir DF final
    df_registros_todas_las_curvas = pd.DataFrame(registros_curvas_todas, columns=columnas_df_todas_las_curvas)
    
    # Retornar a la tarea de carga
    return df_registros_todas_las_curvas


def generar_entregables_por_cliente(**kwargs):
    # Obtener los diccionarios con los datos del grupo de clientes 01 y 02
    ti = kwargs['ti']
    dict_dfs_procesados_g1 = ti.xcom_pull(task_ids='ETL.transformar_datos_grupo01', key='return_value')
    dict_dfs_procesados_g2 = ti.xcom_pull(task_ids='ETL.transformar_datos_grupo02', key='return_value')

    # Definir path de salida para los entregables
    path_entregables = "/opt/airflow/outputs"
    path_curvas_tipo = '/opt/airflow/outputs/curvas_tipo'

    # Unificar en un solo diccionario y liberar memoria
    dict_todos_los_clientes = dict_dfs_procesados_g1 | dict_dfs_procesados_g2
    del dict_dfs_procesados_g1
    del dict_dfs_procesados_g2
    gc.collect()

    # Lista para almacenar los datos de todas las curvas
    registros_curvas_todas = []

    for cliente, df_medicion_anual in dict_todos_los_clientes.items():

        # Lista para almacenar todos los valores de la curva tipo del cliente
        registros_curva_cliente = []

        # Directorio entregables cliente
        dir_entregables_cli = fr"{path_entregables}/entregables_por_cliente/{cliente}"

        # Crear el directorio para los entregables (Si no existe)
        if not os.path.exists(dir_entregables_cli):
            os.makedirs(os.path.normpath(dir_entregables_cli))
            
        # Crear el directorio para las curvas tipo (Si no existe)
        if not os.path.exists(path_curvas_tipo):
            os.makedirs(os.path.normpath(path_curvas_tipo))

        # Generar el archivo con los datos de la curva tipo
        df_curva_tipo = obtener_coords_curva_tipo(df_medicion_anual)
        #df_curva_tipo = agrupar_30_min(df_curva_tipo)
        df_curva_tipo.to_csv(f"{dir_entregables_cli}/datos_curva_tipo_{cliente}.csv", index=False)

        # Guardar los datos en una lista
        registros_curva_cliente.append(cliente)
        for valor in df_curva_tipo["Potencia_aparente_escalada"].values:
            registros_curva_cliente.append(valor)

        # Generar el archivo con los datos de la curva del día que hubo la demanda máxima
        fecha_max_dem, df_curva_dia_dem_max = obtener_coords_dia_demanda_max(df_medicion_anual)
        df_curva_dia_dem_max = df_curva_dia_dem_max.sort_values(by="Hora", ascending=True)
        df_curva_dia_dem_max.to_csv(f"{dir_entregables_cli}/datos_curva_dia_demanda_max.csv", index=False)

        # Generar la gráfica de la curva tipo
        graficar_curva_tipo(df_curva_tipo, cliente, dir_entregables_cli)

        # Generar la gráfica de la curva del día de demanda máxima
        graficar_dia_max_demanda(df_curva_dia_dem_max, cliente, dir_entregables_cli, str(fecha_max_dem).split()[0])

        # Generar un archivo plano con la demanda máximo y mínima
        open(rf'{dir_entregables_cli}/Potencia_max_min.txt', 'w')\
            .write(f'Pot_aparente_max: {df_medicion_anual["Potencia_aparente"].max()}\nPot_aparente_min: {df_medicion_anual["Potencia_aparente"].min()}')

        # Guardar la lista con los registros de un cliente en otra lista
        registros_curvas_todas.append(registros_curva_cliente)
        
        # Liberar memoria
        del df_curva_dia_dem_max
        del registros_curva_cliente
        gc.collect()

    # Construir las columnas deseadas para los datos de las curvas tipo
    columnas_df_todas_las_curvas = ["Cliente"]
    columnas_df_todas_las_curvas.extend(df_curva_tipo["Hora"].values)
    
    # Liberar memoria
    del dict_todos_los_clientes
    gc.collect()
    
    # Construir el DataFrame
    df_registros_todas_las_curvas = pd.DataFrame(registros_curvas_todas)
    df_registros_todas_las_curvas.columns = columnas_df_todas_las_curvas
    
    # Retornar el DataFrame 
    return df_registros_todas_las_curvas


def cargar_datos_curvas_tipo(**kwargs):
    # Obtener el DataFrame con los datos unificados de la tarea transformar_datos_unificados
    ti = kwargs['ti']
    df_registros_todas_las_curvas = ti.xcom_pull(task_ids='ETL.transformar_datos_unificados', key='return_value')

    # Tratar el nombre o identificador del cliente como texto
    df_registros_todas_las_curvas["Cliente"] = df_registros_todas_las_curvas["Cliente"].astype("string")

    # Obtener cliente de base de datos y cargar
    db_cliente = obtener_cliente_db()
    datos_insertar = df_registros_todas_las_curvas.to_dict(orient='records')

    db_cliente.CurvasTipo.CurvasTipoAnuales.delete_many({})
    db_cliente.CurvasTipo.CurvasTipoAnuales.create_index([("Cliente", 1)])
    db_cliente.CurvasTipo.CurvasTipoAnuales.insert_many(datos_insertar)


with DAG(
    'etl_dag_datos_consumo',
    start_date=datetime(2025, 2, 1),
    schedule_interval=None,
    catchup=False,
    description='DAG de proceso ETL correspondiente a los datos de consumo',
) as dag:
    
    inicio_etl = EmptyOperator(task_id='Inicio_ETL')
    fin_etl = EmptyOperator(task_id='Fin_ETL')

    # -------------------------
    #   TaskGroup ETL (finaliza en cargar_datos_curvas_tipo)
    # -------------------------
    with TaskGroup("ETL", tooltip="ETL Group") as etl:
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
            provide_context=True,
            retries=1,
            retry_delay=timedelta(minutes=2),
            execution_timeout=timedelta(minutes=30)
        )

        transformar_datos_grupo02_task = PythonOperator(
            task_id='transformar_datos_grupo02',
            python_callable=transformar_datos_grupo02,
            provide_context=True
        )
        
        transformar_datos_unificados_task = PythonOperator(
            task_id='transformar_datos_unificados',
            python_callable=transformar_datos_unificados,
            provide_context=True
        )

        cargar_datos_curvas_tipo_task = PythonOperator(
            task_id='cargar_datos_curvas_tipo',
            python_callable=cargar_datos_curvas_tipo,
            provide_context=True
        )

        # Dependencias internas del grupo ETL
        extraer_grupo01_task >> transformar_datos_grupo01_task
        extraer_grupo02_task >> transformar_datos_grupo02_task
        [transformar_datos_grupo01_task, transformar_datos_grupo02_task] >> transformar_datos_unificados_task
        transformar_datos_unificados_task >> cargar_datos_curvas_tipo_task
        
    # -------------------------
    #   Tarea de entregables (FUERA del grupo y SIN conexión a fin_etl)
    # -------------------------
    generar_entregables_task = PythonOperator(
        task_id='generar_entregables_por_cliente',
        python_callable=generar_entregables_por_cliente,
        provide_context=True
    )
    # Depende solo de las transformaciones
    [transformar_datos_grupo01_task, transformar_datos_grupo02_task] >> generar_entregables_task

    # Cadena principal: inicio -> ETL -> fin (entregables NO la bloquea)
    inicio_etl >> etl >> fin_etl