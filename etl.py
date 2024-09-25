#%%
import requests
import pandas as pd

#%%
api_url = "https://api.coincap.io/v2/assets"

response = requests.get(api_url)
print(response)

response_data = response.json()

#%%
data = pd.json_normalize(response_data, "data")
data.head()
data.info() # descrição do dataset

# %%
# converter dados categóricos em dados numericos
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

print("=" * 12)
data.info()
# %%
# dados faltando e completando com zero
data["rank"] = data["rank"].fillna('not available')
data["supply"] = data["supply"].fillna(0)
data["maxSupply"] = data["maxSupply"].fillna(0)
data["marketCapUsd"] = data["marketCapUsd"].fillna(0)
data["volumeUsd24Hr"] = data["volumeUsd24Hr"].fillna(0)
data["priceUsd"] = data["priceUsd"].fillna(0)
data["vwap24Hr"] = data["vwap24Hr"].fillna(0)
data["explorer"] = data["explorer"].fillna('not available')

data.info()



# %%
#arrendodar para duas casas decimas
def round_to_two_decimal_places(number):
        return round(number, 2)

selected_columns = ['supply', 'maxSupply', 'maxSupply', 'marketCapUsd', 'priceUsd', 'changePercent24Hr', 'vwap24Hr']

for col in selected_columns:
    if col in data.columns:
        data[col] = pd.to_numeric(data[col], errors='coerce').map(round_to_two_decimal_places)

data.head()

# %%
# Carregar dado em Postgresql