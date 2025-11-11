from itertools import product
import os
import requests
import pandas as pd
from datetime import date, datetime
from google.cloud import bigquery

# 🎯 Divisas objetivo (se usa EUR como base por defecto del BCE)
TARGET_CURRENCIES = ['NOK', 'EUR', 'SEK', 'PLN', 'RON', 'DKK', 'CZK']
BASE_CURRENCY = 'EUR' # El BCE siempre cotiza frente al EUR.

def fetch_ecb_data_for_ytd(start_date: str, end_date: str):
    """
    Función que extrae los tipos de cambio del BCE para un periodo de tiempo YTD.

    Args:
        request: El objeto request de la Cloud Function (ignoramos el contenido por simplicidad).
    Returns:
        Un DataFrame de pandas con los datos extraídos o un mensaje de error.
    """

    # 1. Definición de Fechas YTD

    # 2. Construcción de la URL de la API del BCE (SDMX)
    # El formato pide: D.TARGET_CURRENCIES_VS_EUR.SP00.A
    # D: Serie de tiempo diaria
    # TARGET_CURRENCIES: Las divisas que queremos (separadas por '+')
    # SP00: Tipo de cambio de referencia
    # A: Tipo de tasa (promedio)

    currencies_str = "+".join(c for c in TARGET_CURRENCIES if c != BASE_CURRENCY)
    
    ECB_API_URL = (
        "https://data-api.ecb.europa.eu/service/data/EXR/D."
        f"{currencies_str}.{BASE_CURRENCY}.SP00.A?startPeriod={start_date}&endPeriod={end_date}"
    )
    # El formato para solicitar es JSON (aunque es un formato SDMX-JSON)
    headers = {'Accept': 'application/json'}

    print(f"Buscando datos desde {start_date} hasta {end_date}...")
    print(f"URL de la API: {ECB_API_URL}")

    # 3. Llamada a la API
    try:
        response = requests.get(ECB_API_URL, headers=headers)
        response.raise_for_status() # Lanza una excepción para códigos de estado HTTP 4xx/5xx
        data = response.json()
    except requests.exceptions.RequestException as e:
        error_msg = f"Error al acceder a la API del BCE: {e}"
        print(error_msg)
        return {"status": "error", "message": error_msg}, 500

    # 4. Procesamiento de los datos JSON/SDMX
    try:
        series_data = data['dataSets'][0]['series']
        
        # Lista para almacenar los registros normalizados
        normalized_data = []

        # Recorrer cada serie (una por divisa)
        for series_key, series_info in series_data.items():
            # series_key tiene el formato "0:0:N:0:0"
            # Determinamos la divisa cotizada
            quote_currency = data['structure']['dimensions']['series'][1]['values'][series_info['attributes'][-4]]['id']
            
            # Recorrer los tipos de cambio (observaciones)
            for observation_key, observation_value in series_info['observations'].items():
                
                # observation_key es el índice de tiempo. Lo buscamos en la estructura 'dimensions'
                time_index = int(observation_key.split(':')[0])
                exchange_date = data['structure']['dimensions']['observation'][0]['values'][time_index]['id']
                
                # El valor de la tasa es el primer elemento del array observation_value
                rate = observation_value[0]
                
                # Añadir el registro
                normalized_data.append({
                    'exchange_date': exchange_date,
                    'base_currency': BASE_CURRENCY,
                    'quote_currency': quote_currency,
                    'rate': float(rate)
                })

        # 5. Añadir la tasa EUR/EUR (1.0) para cada día
        dates_fetched = set(d['exchange_date'] for d in normalized_data)
        for d in dates_fetched:
            normalized_data.append({
                'exchange_date': d,
                'base_currency': BASE_CURRENCY,
                'quote_currency': BASE_CURRENCY,
                'rate': 1.0
            })
            
        df = pd.DataFrame(normalized_data)
        
        print(f"Extracción exitosa. Filas encontradas: {len(df)}")
        
        # En este punto, 'df' contiene los datos en formato EUR/X y EUR/EUR
        # La función debe devolver este DataFrame para la siguiente etapa (Transformación)
        return df

    except (KeyError, IndexError, ValueError) as e:
        error_msg = f"Error al procesar el JSON SDMX del BCE: {e}"
        print(error_msg)
        return {"status": "error", "message": error_msg}, 500


def transform_to_fact_table(df_base_rates: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la transformación del DataFrame de tasas base (EUR/X)
    al DataFrame final con todos los pares cruzados, listo para cargar en BigQuery.

    Args:
        df_base_rates: DataFrame de pandas con las tasas en formato EUR/X.
                       Columnas esperadas: ['exchange_date', 'base_currency', 'quote_currency', 'rate']

    Returns:
        DataFrame de pandas con todos los pares cruzados y columnas de metadatos.
    """
    
    if df_base_rates.empty:
        print("El DataFrame de entrada está vacío. No se realiza ninguna transformación.")
        return pd.DataFrame()

    # 1. Pivotar el DataFrame para tener las tasas como columnas (EUR/X)
    # Queremos: | exchange_date | EUR | NOK | SEK | PLN | ...
    df_pivot = df_base_rates.pivot(
        index='exchange_date',
        columns='quote_currency',
        values='rate'
    ).reset_index()

    # Asegurarse de que el EUR está presente (debería estar por la lógica de extracción)
    if BASE_CURRENCY not in df_pivot.columns:
         raise ValueError("La divisa base (EUR) no se encuentra en el DataFrame pivotado.")

    final_records = []
    
    # Generar todos los pares cruzados posibles
    # Genera: (NOK, SEK), (SEK, NOK), (EUR, NOK), etc.
    currency_pairs = list(product(TARGET_CURRENCIES, TARGET_CURRENCIES))

    # 2. Calcular los pares cruzados y generar los registros finales
    for index, row in df_pivot.iterrows():
        exchange_date = row['exchange_date']
        
        for base, quote in currency_pairs:
            
            # Tasa EUR/X (base) y EUR/Y (quote)
            rate_eur_vs_base = row[base] 
            rate_eur_vs_quote = row[quote]

            # Fórmula del Cross-Rate: Rate_X/Y = Rate_EUR/Y / Rate_EUR/X
            # Esto funciona incluso para el caso EUR/X, donde Rate_EUR/EUR = 1.0
            
            # Manejo de división por cero (aunque la tasa del BCE nunca debería ser 0)
            if rate_eur_vs_base == 0:
                 rate = 0.0 # O gestionar como un error/NaN, pero asumiremos divisas válidas
            else:
                 rate = rate_eur_vs_quote / rate_eur_vs_base
                 
            rate_inverse = 1.0 / rate if rate != 0 else 0.0

            # 3. Añadir metadatos
            final_records.append({
                'exchange_date': exchange_date,
                'base_currency': base,
                'quote_currency': quote,
                'rate': rate,
                'rate_inverse': rate_inverse,
                'data_source': 'ECB',
                'load_timestamp': datetime.now().isoformat(sep=' ', timespec='seconds')
            })

    # 4. Crear el DataFrame final
    df_final = pd.DataFrame(final_records)
    
    # 5. Ajustar tipos de datos para BigQuery (aunque BIGNUMERIC es flexible)
    # Es buena práctica asegurar que las tasas sean float para la carga
    df_final['rate'] = df_final['rate'].astype(float)
    df_final['rate_inverse'] = df_final['rate_inverse'].astype(float)

    print(f"Transformación completa. Total de pares cruzados generados: {len(df_final)}")
    return df_final

def load_to_bigquery(df_fact: pd.DataFrame, PROJECT_ID: str, BIGQUERY_TABLE_FULL_ID: str) -> dict:
    """
    Carga el DataFrame final de tipos de cambio en la tabla de BigQuery,
    leyendo la configuración de destino de las variables de entorno.

    Args:
        df_fact: DataFrame de pandas con la estructura de la tabla fact_exchange_rates.

    Returns:
        Un diccionario con el estado de la operación de carga.
    """
    if df_fact.empty:
        print("El DataFrame a cargar está vacío. Omitiendo la carga en BigQuery.")
        return {"status": "skipped", "message": "DataFrame is empty."}

    # Inicializar el cliente de BigQuery
    try:
        # El cliente usa el PROJECT_ID si está definido en el entorno
        client = bigquery.Client(project=PROJECT_ID) 
    except Exception as e:
        error_msg = f"Error al inicializar el cliente de BigQuery: {e}"
        print(error_msg)
        return {"status": "error", "message": error_msg}


    # 1. Ajustar tipos de datos para la carga
    df_fact['exchange_date'] = pd.to_datetime(df_fact['exchange_date']).dt.date
    df_fact['load_timestamp'] = pd.to_datetime(df_fact['load_timestamp'])

    # 2. Definir la configuración del job de carga
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"Iniciando la carga de {len(df_fact)} filas en {BIGQUERY_TABLE_FULL_ID}...")

    # 3. Ejecutar la carga
    try:
        job = client.load_table_from_dataframe(
            df_fact, BIGQUERY_TABLE_FULL_ID, job_config=job_config
        )  
        
        # Esperar a que el Job de carga finalice
        job.result() 

        print(f"Carga exitosa en BigQuery. Filas insertadas: {job.output_rows}.")
        return {
            "status": "success", 
            "message": "Data successfully loaded to BigQuery.",
            "rows_inserted": job.output_rows,
            "destination_table": BIGQUERY_TABLE_FULL_ID
        }

    except Exception as e:
        error_msg = f"Error fatal durante la carga en BigQuery: {e}"
        print(error_msg)
        return {"status": "error", "message": error_msg}

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    current_year = date.today().year
    start_date = f"{current_year}-01-01"
    end_date = date.today().strftime("%Y-%m-%d")
    df = fetch_ecb_data_for_ytd(start_date, end_date)
    df_final = transform_to_fact_table(df)
    # df_today = fetch_ecb_data_for_ytd(end_date, end_date)
    # df_today_final = transform_to_fact_table(df_today)
    
    # --- Configuración de BigQuery (Leída desde el entorno) ---
    # Usamos os.environ.get() para leer las variables, usando un valor de fallback si no existe
    PROJECT_ID = os.getenv("GCP_PROJECT_ID", "fx-technical-case")
    DATASET_ID = os.getenv("BIGQUERY_DATASET_ID","fx_data")
    TABLE_ID = os.getenv("BIGQUERY_TABLE_ID","fact_exchange_rates")

    # Verificación básica para asegurar que las variables están disponibles
    if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
        raise ValueError("Faltan variables de entorno (GCP_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID). Asegúrate de que .env está cargado o configurado en Cloud Function.")

    BIGQUERY_TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    load_to_bigquery(df_final, PROJECT_ID, BIGQUERY_TABLE_FULL_ID)
    df.head()