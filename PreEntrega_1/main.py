import os
import pandas as pd
from sqlalchemy import create_engine
from requests import Session
from modules import config_api
from dotenv import load_dotenv

load_dotenv()

# Variáveis de ambiente
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_db = os.getenv('REDSHIFT_DB')

url = config_api.api_url
key = config_api.api_key

# Parâmetros da API
parameters = {
    'start': config_api.start,
    'limit': config_api.limit,
    'convert': config_api.api_currency
}

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': key
}

session = Session()
session.headers.update(headers)

name = []
symbol = []
data_added = []
last_updated = []
price = []
volume_24h = []

try:
    response = session.get(url, params=parameters)
    response.raise_for_status()
    data = response.json()
    
    if 'data' not in data:
        raise ValueError("A resposta da API não contém a chave 'data'.")
    
    for coin in data['data']:
        name.append(coin['name'])
        symbol.append(coin['symbol'])
        data_added.append(coin['date_added'])
        last_updated.append(coin['last_updated'])
        price.append(coin['quote']['USD']['price'])
        volume_24h.append(coin['quote']['USD']['volume_24h'])
    
    df = pd.DataFrame({
        "name": name,
        "symbol": symbol,
        "data_added": data_added,
        "last_updated": last_updated,
        "price": price,
        "volume_24h": volume_24h
    })
    
    print("Data estruturada no dataframe")
    print(df.head())
    
    # Construir string de conexão Redshift
    redshift_string_connection = f'postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_db}'
    engine = create_engine(redshift_string_connection)

    # Inserir dados na tabela
    df.to_sql('Coins', con=engine, index=False, if_exists='replace')
    print('Dados carregados com sucesso no Amazon Redshift')
    
except Exception as e:
    print(f"Ocorreu um erro: {e}")