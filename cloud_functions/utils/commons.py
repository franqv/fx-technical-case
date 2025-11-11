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
    Function that extracts ECB exchange rates for a YTD time period.

    Args:
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.
    Returns:
        A pandas DataFrame with the extracted data or an error message.
    """
    currencies_str = "+".join(c for c in TARGET_CURRENCIES if c != BASE_CURRENCY)
    
    ECB_API_URL = (
        "https://data-api.ecb.europa.eu/service/data/EXR/D."
        f"{currencies_str}.{BASE_CURRENCY}.SP00.A?startPeriod={start_date}&endPeriod={end_date}"
    )
    headers = {'Accept': 'application/json'}

    print(f"Buscando datos desde {start_date} hasta {end_date}...")
    print(f"URL de la API: {ECB_API_URL}")

    try:
        response = requests.get(ECB_API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        error_msg = f"Error al acceder a la API del BCE: {e}"
        print(error_msg)
        return {"status": "error", "message": error_msg}, 500

    try:
        series_data = data['dataSets'][0]['series']
        
        normalized_data = []

        for series_key, series_info in series_data.items():
            quote_currency = data['structure']['dimensions']['series'][1]['values'][series_info['attributes'][-4]]['id']
            
            for observation_key, observation_value in series_info['observations'].items():
                
                time_index = int(observation_key.split(':')[0])
                exchange_date = data['structure']['dimensions']['observation'][0]['values'][time_index]['id']
                
                rate = observation_value[0]
                
                normalized_data.append({
                    'exchange_date': exchange_date,
                    'base_currency': BASE_CURRENCY,
                    'quote_currency': quote_currency,
                    'rate': float(rate)
                })

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
        
        return df

    except (KeyError, IndexError, ValueError) as e:
        error_msg = f"Error al procesar el JSON SDMX del BCE: {e}"
        print(error_msg)
        return {"status": "error", "message": error_msg}, 500


def transform_to_fact_table(df_base_rates: pd.DataFrame) -> pd.DataFrame:
    """
    Make the transformation to create all cross currency pairs from base rates.
    Args:
        df_base_rates: DataFrame with base rates (EUR/X).
    Returns:
        DataFrame transformed to the fact_exchange_rates structure.
    """
    
    if df_base_rates.empty:
        print("El DataFrame de entrada está vacío. No se realiza ninguna transformación.")
        return pd.DataFrame()

    df_pivot = df_base_rates.pivot(
        index='exchange_date',
        columns='quote_currency',
        values='rate'
    ).reset_index()

    if BASE_CURRENCY not in df_pivot.columns:
         raise ValueError("La divisa base (EUR) no se encuentra en el DataFrame pivotado.")

    final_records = []
    
    currency_pairs = list(product(TARGET_CURRENCIES, TARGET_CURRENCIES))

    for index, row in df_pivot.iterrows():
        exchange_date = row['exchange_date']
        
        for base, quote in currency_pairs:
            
            rate_eur_vs_base = row[base] 
            rate_eur_vs_quote = row[quote]

            if rate_eur_vs_base == 0:
                 rate = 0.0
            else:
                 rate = rate_eur_vs_quote / rate_eur_vs_base
                 
            rate_inverse = 1.0 / rate if rate != 0 else 0.0

            final_records.append({
                'exchange_date': exchange_date,
                'base_currency': base,
                'quote_currency': quote,
                'rate': rate,
                'rate_inverse': rate_inverse,
                'data_source': 'ECB',
                'load_timestamp': datetime.now().isoformat(sep=' ', timespec='seconds')
            })

    df_final = pd.DataFrame(final_records)
    
    df_final['rate'] = df_final['rate'].astype(float)
    df_final['rate_inverse'] = df_final['rate_inverse'].astype(float)

    print(f"Transformación completa. Total de pares cruzados generados: {len(df_final)}")
    return df_final

def load_to_bigquery(df_fact: pd.DataFrame, PROJECT_ID: str, BIGQUERY_TABLE_FULL_ID: str) -> dict:
    """
    Load final exchange rates DataFrame into BigQuery table,
    reading target configuration from environment variables.

    Args:
        df_fact: pandas DataFrame with the fact_exchange_rates structure.
        PROJECT_ID: GCP project ID.
        BIGQUERY_TABLE_FULL_ID: BigQuery table ID.

    Returns:
        A dictionary with the status of the load operation.
    """
    if df_fact.empty:
        print("El DataFrame a cargar está vacío. Omitiendo la carga en BigQuery.")
        return {"status": "skipped", "message": "DataFrame is empty."}

    try:
        client = bigquery.Client(project=PROJECT_ID) 
    except Exception as e:
        error_msg = f"Error al inicializar el cliente de BigQuery: {e}"
        print(error_msg)
        return {"status": "error", "message": error_msg}


    df_fact['exchange_date'] = pd.to_datetime(df_fact['exchange_date']).dt.date
    df_fact['load_timestamp'] = pd.to_datetime(df_fact['load_timestamp'])

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"Iniciando la carga de {len(df_fact)} filas en {BIGQUERY_TABLE_FULL_ID}...")

    try:
        job = client.load_table_from_dataframe(
            df_fact, BIGQUERY_TABLE_FULL_ID, job_config=job_config
        )  
        
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

    PROJECT_ID = os.getenv("GCP_PROJECT_ID", "fx-technical-case")
    DATASET_ID = os.getenv("BIGQUERY_DATASET_ID","fx_data")
    TABLE_ID = os.getenv("BIGQUERY_TABLE_ID","fact_exchange_rates")

    if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
        raise ValueError("Faltan variables de entorno (GCP_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID). Asegúrate de que .env está cargado o configurado en Cloud Function.")

    BIGQUERY_TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    load_to_bigquery(df_final, PROJECT_ID, BIGQUERY_TABLE_FULL_ID)
    df.head()