# El archivo "funciones" nos permite usar el principio DRY
# Importaremos las librerias necesarias para crear nuestras funciones.

from sqlalchemy import create_engine, inspect
import pandas as pd


# Creamos funcion 'leer_tabla' para leer tablas completas desde la base de datos en dataframes independientes.

def leer_tabla(tabla, engine):
    """
    Creamos una función propia para agilizar el proceso del código de la prueba. 
    Esta función permite construir Rápidamente Dataframes de las tablas que componen la base de datos.
                
    tabla(texto) ==> Nombre de la BD
    engine(sqlalchemy/engine) ==> engine creado previamente en SQLALCHEMY
    """
    
    df_table = pd.read_sql(f"select * from {tabla};", engine)
    return df_table



# Creamos funcion 'filtrar_por_nombre'

def filtro_fecha(df, columna_fecha, fecha_inicio, fecha_fin):
    """
    Filtra un DataFrame por un rango de fechas.

    Parametro df: El DataFrame que se quiere filtrar.
    Parametro columna_fecha: El nombre de la columna que tiene la fecha
    Parametro fecha_inicio: Fecha de inicio que se quiere considerar
    Parametro fecha_fin:  Fecha final que se quiere considerar
    Return El DataFrame filtrado por el rango de fechas.
    """
    # Convertimos las fechas a formato datetime
    df[columna_fecha] = pd.to_datetime(df[columna_fecha])
    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_fin = pd.to_datetime(fecha_fin)
    # Filtrar el DataFrame por el rango de fechas
    df_filtrado = df[(df[columna_fecha] >= fecha_inicio) & (df[columna_fecha] <= fecha_fin)]
    return df_filtrado




# Creamos la función 'generar_reporte'

def generar_reporte(df, filas, valores,columnas=None, medida='sum'):
    """
    Genera un reporte pivotado.

    Parametro df: El DataFrame de entrada.
    Parametro filas: Las columnas que se utilizarán como filas en el pivot table.
    Parametro columnas: Las columnas que se utilizarán como columnas en el pivot table.
    Parametro valores: Las columnas que se utilizarán como valores en el pivot table.
    Parametro medida: La función de agregación a aplicar sobre los valores.
    Parametro: Un DataFrame pivotado.
    """
    # Crear el pivot table
    pivot_table = pd.pivot_table(df,
                                 index=filas,
                                 columns=columnas,
                                 values=valores,
                                 aggfunc=medida,
                                 fill_value=0)
    pivot_table = pivot_table[valores]
    return pivot_table


    
# Definir función 'escribir_en_base_de_datos'

def escribir_en_base_de_datos(df, nombre_tabla, engine, if_exists='replace'):
    """
    Escribe un DataFrame en una tabla de una base de datos.

    :param df: El DataFrame que se quiere escribir en la base de datos.
    :param nombre_tabla: El nombre de la tabla en la que se desea escribir.
    :param engine: El objeto Engine de SQLAlchemy para la conexión a la base de datos.
    :param if_exists: Comportamiento en caso de que la tabla ya exista ('fail', 'replace', 'append').
                      Por defecto, 'fail' (falla si la tabla ya existe).
    """
    try:
        # Escribir el DataFrame en la base de datos
        df.to_sql(nombre_tabla, con=engine, if_exists=if_exists, index=False)
        print(f"Datos escritos correctamente en la tabla '{nombre_tabla}'.")
    except Exception as e:
        print(f"Error al escribir datos en la tabla '{nombre_tabla}': {str(e)}")