import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv(override=True)
api_url = "https://api.coincap.io/v2/assets"

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro ao acessar a API: {response.status_code}")

response_data = fetch_data(api_url)

data = pd.json_normalize(response_data, "data")

print("==== Informações inciais dos Dados ====")

print(data.info())

def convert_columns_to_datatypes(df, column_datatypes):
        for column, datatype in column_datatypes.items():
                if column in df.columns:
                        df[column] = df[column].astype(datatype)
        return df

column_datatypes = {
        'rank' : int,             
        'supply' : float,          
        'maxSupply' : float,        
        'marketCapUsd' : float,     
        'volumeUsd24Hr' : float,    
        'priceUsd' : float,         
        'changePercent24Hr' : float,
        'vwap24Hr' : float
}              

data = convert_columns_to_datatypes(data, column_datatypes)

data.fillna({
    "rank": 'not available',
    "supply": 0,
    "maxSupply": 0,
    "marketCapUsd": 0,
    "volumeUsd24Hr": 0,
    "priceUsd": 0,
    "vwap24Hr": 0,
    "explorer": 'not available'
}, inplace=True)

print("\n=== Informações Após Preenchimento de Dados Faltantes ===")
print(data.info())

def round_to_two_decimal_places(number):
        return round(number, 2)

selected_columns = ['supply', 'maxSupply', 'maxSupply', 'marketCapUsd', 
                    'priceUsd', 'changePercent24Hr', 'vwap24Hr']

for col in selected_columns:
    if col in data.columns:
        data[col] = pd.to_numeric(data[col], errors='coerce').map(round_to_two_decimal_places)

db_username = os.getenv("db_username")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")

engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

table_name = 'Cripto_dados'

data.to_sql(table_name, engine, if_exists='replace', index=False)

engine.dispose()