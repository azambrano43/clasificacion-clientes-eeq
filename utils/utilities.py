import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import re
import os


# | ===========================================================================|

feriados_nacionales = ["2/7/2023", "20/2/2023", "21/2/2023", "7/4/2023",  \
                       "1/5/2023", "26/5/2023", "11/8/2023", "9/10/2023", \
                       "2/10/2023", "3/10/2023", "25/12/2023"]

feriados_nacionales = pd.to_datetime(feriados_nacionales, format='%d/%m/%Y')

dict_meses = {"01": "Enero",
              "02": "Febrero",
              "03": "Marzo",
              "04": "Abril",
              "05": "Mayo",
              "06": "Junio",
              "07": "Julio",
              "08": "Agosto",
              "09": "Septiembre",
              "10": "Octubre",
              "11": "Noviembre",
              "12": "Diciembre"}


# | ===========================================================================|

def obtener_cliente_db():
    # Cargar las variables del archivo .env
    load_dotenv()

    # Obtener las credenciales y el cluster de MongoDB
    username = os.getenv("DB_USER_T")
    password = os.getenv("DB_PASS_T")
    cluster = os.getenv("DB_CLUSTER_T")

    # Construir la uri con las credenciales
    uri = f"mongodb+srv://{username}:{password}@{cluster.lower()}.gypwd.mongodb.net/?retryWrites=true&w=majority&appName={cluster}"

    # Crear un cliente y conectarlo al servidor
    client = MongoClient(uri, 
                        server_api=ServerApi('1'),
                        connectTimeoutMS=60000,
                        socketTimeoutMS=60000,
                        serverSelectionTimeoutMS=60000,
                        tls=True)

    # Si el cliente existe, retornarlo
    if client is not None:
        return client
    
# | ===========================================================================|

def agrupar_30_min(df, columna_hora="Hora", columna_valor="Potencia_aparente_escalada"):

    # Convertir la columna Hora a tipo datetime
    df["Hora"] = pd.to_datetime(df[columna_hora], format="%H:%M")

    # Redondear hacia arriba al final del intervalo de 30 minutos
    df["Hora"] = df["Hora"] + pd.Timedelta(minutes=30)
    df["Hora"] = df["Hora"].dt.floor("H") + (df["Hora"].dt.minute // 30) * pd.Timedelta(minutes=30)

    # Recortar a HH:MM
    df["Hora"] = df["Hora"].astype("string").apply(lambda x: x.split()[1][:-3])

    # Agrupar por la nueva columna de hora y calcular el promedio de los valores
    return df.groupby("Hora")[columna_valor].apply(np.mean).reset_index()

# | ===========================================================================|

def agrupar_horas(df, columna_hora="Hora", columna_valor="Potencia_aparente_escalada"):

    # Convertir la columna Hora a tipo datetime
    df["Hora"] = pd.to_datetime(df[columna_hora], format="%H:%M")

    # Agrupar por hora y tomar el punto medio (hh:30)
    df["Hora"] = df["Hora"].dt.floor("H") + pd.Timedelta(minutes=30)

    # Recortar a HH:MM
    df["Hora"] = df["Hora"].astype("string").apply(lambda x: x.split()[1][:-3])

    # Agrupar por la nueva columna de hora y calcular el promedio de los valores
    return df.groupby("Hora")[columna_valor].apply(np.mean).reset_index()

# | ===========================================================================|

def graficar_dia_max_demanda(df, cod_cli, path, fecha):

    # Graficar la potencia aparente escalada a lo largo del día
    _ = plt.figure(figsize=(16, 6))
    _ = plt.plot(df["Hora"], df["Potencia_aparente_escalada"], marker='o', color='r', linestyle='-', label=f'Potencia Aparente Escalada')
    _ = plt.title(f'Curva del día de demanda máxima {fecha} para cliente {cod_cli}')
    _ = plt.xlabel('Hora')
    _ = plt.ylabel('Potencia Aparente Escalada')
    _ = plt.grid(True)
    _ = plt.legend()

    # Rotar etiquetas para que no se vea acumulado el eje X
    _ = plt.xticks(df["Hora"].values[::2], rotation=45)

    # Para que no se distorsione la dimenisión del eje Y
    _ = plt.yticks(np.arange(0, 1.1, 0.1))

    # Evitar recortes en las etiquetas
    _ = plt.tight_layout()

    # Guardar la gráfica en un archivo (por ejemplo, como archivo PNG)
    _ = plt.savefig(f"{path}/curva_dia_demanda_max_{cod_cli}.png", format='png')  # Puedes cambiar el formato a 'jpg', 'pdf', etc.
    
    # Cerrar la figura después de guardarla para liberar recursos
    _ = plt.close()

# | ===========================================================================|

def graficar_curva_tipo(df, cod_cli, path):

    # Generar el gráfico de la curva tipo
    _ = plt.figure(figsize=(16, 6))
    _ = plt.plot(df["Hora"], df["Potencia_aparente_escalada"], marker='o', color='b', linestyle='-', label='Potencia Aparente Escalada')
    _ = plt.title(f'Curva tipo cliente {cod_cli}')
    _ = plt.xlabel('Hora')
    _ = plt.ylabel('Potencia Aparente Escalada')
    _ = plt.grid(True)

    # Rotar etiquetas para que no se vea acumulado el eje X
    _ = plt.xticks(df["Hora"].values[::2], rotation=45)

    # Para que no se distorsione la dimenisión del eje Y
    _ = plt.yticks(np.arange(0, 1.1, 0.1))

    # Evitar recortes en las etiquetas
    _ = plt.tight_layout()

    # Guardar la gráfica en un directorio
    _ = plt.savefig(f"{path}/curva_tipo_{cod_cli}.png", format='png')  # Puedes cambiar el formato a 'jpg', 'pdf', etc.
    _ = plt.savefig(f"/opt/airflow/outputs/curvas_tipo/curva_tipo_{cod_cli}.png", format='png')  # Puedes cambiar el formato a 'jpg', 'pdf', etc.
    
    # Cerrar la figura después de guardarla para liberar recursos
    _ = plt.close()  

# | ===========================================================================|

def obtener_coords_curva_tipo(df):

    # Agrupar por hora y aplicar mediana
    df_grouped = df.groupby("Hora")["Potencia_aparente_escalada"].apply(np.median).sort_index(ascending=True).reset_index(drop=False)

    # Retornar el array con los 96 valores de demanda
    return df_grouped

# | ===========================================================================|

def obtener_coords_dia_demanda_max(df):

    # Obtener el máximo valor de potencia aparente
    max_potencia = df['Potencia_aparente'].max()

    # Encontrar la fecha correspondiente a la máxima potencia aparente
    fecha_max_potencia = df[df['Potencia_aparente'] == max_potencia]['Fecha'].iloc[0]

    # Filtrar los registros correspondientes a esa fecha
    df_max_fecha = df[df['Fecha'] == fecha_max_potencia]

    # Ordenar el resultado de manera ascendente por 'Hora'
    df_max_fecha = df_max_fecha.sort_values(by="Hora", ascending=True)

    return fecha_max_potencia, df_max_fecha[["Hora","Potencia_aparente_escalada"]]

# | ===========================================================================|

def fecha_formato_unico(fecha_str):

    # Poner separador único el '/' y año únicamente 2023
    fecha_str = fecha_str.replace('-','/').replace('2024','2023')

    if len(fecha_str.split('/')[0]) == 4: # Cuando el formato es año/mes/día
        return fecha_str
    elif len(fecha_str.split('/')[0]) != 4: # Cuando el formato es día/mes/año
        seps = fecha_str.split('/')
        return f"{seps[-1]}/{seps[1]}/{seps[0]}"
    
def hora_formato_unico(hora_str):
    if pd.isna(hora_str):
        return np.nan
    match = re.match(r'^(\d{1,2}:\d{2})(:\d{2})?$', hora_str.strip())
    return match.group(1) if match else np.nan

# | ===========================================================================|

def intentar_abrir_archivo_datos(path_archivo):

    df_archivo_telem_tab = pd.DataFrame([1,2,3], columns=["prueba"])
    df_archivo_telem_pc = pd.DataFrame([1,2,3], columns=["prueba"])
    df_archivo_telem_c = pd.DataFrame([1,2,3], columns=["prueba"])

    # Intentar leer el archivo con separador 'tab'
    try:
        #print("Leyendo con tab")
        df_archivo_telem_tab = pd.read_csv(path_archivo,
                                            sep="\t",
                                            #decimal=",",
                                            skiprows=11,
                                            #na_values="N/D",
                                            encoding="utf-16",
                                            #on_bad_lines="skip",
                                            encoding_errors="ignore"
                                            ) 
    except:
        pass

    # Intentar leer el archivo con separador 'punto y coma'
    try:
        #print("Leyendo con ;")
        df_archivo_telem_pc = pd.read_csv(path_archivo,
                                            sep=";",
                                            #decimal=",",
                                            skiprows=11,
                                            #na_values="N/D",
                                            #encoding="utf-16",
                                            #on_bad_lines="skip",
                                            encoding_errors="ignore" 
                                            )

    except:
        pass

    # Intentar leer el archivo con separador 'coma'
    try:
        #print("Leyendo con ,")
        df_archivo_telem_c = pd.read_csv(path_archivo,
                                            sep=",",
                                            #decimal=",",
                                            skiprows=11,
                                            #na_values="N/D",
                                            #encoding="utf-16",
                                            #on_bad_lines="skip",
                                            encoding_errors="ignore" 
                                            )

    except:
        pass



    dfs = [df_archivo_telem_tab, df_archivo_telem_pc, df_archivo_telem_c]
    
    # Elegir el primer DF válido (más de 1 columna) o el último como fallback
    for df in dfs:
        if df.shape[1] > 1:
            return df

    return dfs[-1]

# | ===========================================================================|