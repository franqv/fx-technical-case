import os
import functions_framework

from cloud_functions.fetch_ecb_data_for_ytd.main import etl_fx_function
from cloud_functions.utils.commons import load_to_bigquery
from datetime import date

@functions_framework.http
def etl_fx_load_all_year_data_function(request):
    """
    Función principal de Cloud Function que orquesta la E, T y L.
    """
    # 1. Parámetros YTD (definidos aquí para la orquestación, asumiendo una llamada diaria)
    current_year = date.today().year
    start_date = f"{current_year}-01-01"
    end_date = date.today().strftime("%Y-%m-%d")
    df_fact = etl_fx_function(start_date, end_date)

    if df_fact.empty:
        return {"status": "success", "message": "No se encontraron datos para transformar."}, 200

    # 4. Carga (L) - Pendiente de implementar
    # Usamos os.environ.get() para leer las variables, usando un valor de fallback si no existe
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
    TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")

    # Verificación básica para asegurar que las variables están disponibles
    if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
        raise ValueError("Faltan variables de entorno (GCP_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID). Asegúrate de que .env está cargado o configurado en Cloud Function.")

    BIGQUERY_TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    load_result = load_to_bigquery(df_fact, PROJECT_ID, BIGQUERY_TABLE_FULL_ID) 
    
    if load_result['status'] == 'success':
         return {"status": "success", "message": f"ETL finalizado: {load_result['rows_inserted']} filas cargadas en BigQuery."}, 200
    else:
         return {"status": "error", "message": load_result['message']}, 500

@functions_framework.cloud_event
def update_today_ebc_data_function(cloudevent):
    """
    Función principal de Cloud Function que orquesta la E, T y L.
    """
    today_date = date.today().strftime("%Y-%m-%d")
    df_fact = etl_fx_function(today_date, today_date)

    if df_fact.empty:
        print("INFO: No se encontraron datos para transformar.")
        return # Devuelve None en lugar de un JSON/HTTP 200

    # 3. Carga (L)
    # Las variables de entorno ya están cargadas en el entorno de la función
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
    TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")

    if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
        # Esto lanzará un error en los logs, lo cual es correcto
        raise ValueError("Faltan variables de entorno para BigQuery.")

    BIGQUERY_TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    load_result = load_to_bigquery(df_fact, PROJECT_ID, BIGQUERY_TABLE_FULL_ID) 
    
    if load_result['status'] == 'success':
        print(f"ÉXITO: ETL finalizado: {load_result['rows_inserted']} filas cargadas.")
    else:
        # Lanza un error para que la ejecución falle y se registre en los logs.
        raise RuntimeError(f"FALLO en la carga: {load_result['message']}")