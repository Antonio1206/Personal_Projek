# link dataset : https://drive.google.com/drive/folders/1GsAs6-WKlN9gnKfs9xVYFhzNO4ED0ZXZ?usp=share_link
 
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch

def connect_to_postgresql():
    db_config = {
        "dbname": "hactiv8",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",  
        "port": "5432"  
    }
    conn = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/hactiv8")
    return conn

def read_data_from_postgresql():
    conn = connect_to_postgresql()
    df = pd.read_sql_query('select * from ev_car', conn)
    return df

def map_column_names(df):
    column_mapping = {
        'VIN (1-10)': 'vin',
        'County': 'county',
        'City': 'city',
        'State': 'state',
        'Postal Code': 'postal_code',
        'Model Year': 'model_year',
        'Make': 'make',
        'Model': 'model',
        'Electric Vehicle Type': 'ev_type',
        'Clean Alternative Fuel Vehicle (CAFV) Eligibility': 'cafv_eligibility',
        'Electric Range': 'electric_range',
        'Base MSRP': 'base_msrp',
        'Legislative District': 'legislative_district',
        'DOL Vehicle ID': 'dol_vehicle_id',
        'Vehicle Location': 'vehicle_location',
        'Electric Utility': 'electric_utility',
        '2020 Census Tract': 'census_tract'
    }
    df.rename(columns=column_mapping, inplace=True)
    return df

def save_to_csv(df):
    df.to_csv('P2G7_antonius_daeng_data_clean.csv', index=False)

def connect_to_elasticsearch():
    es = Elasticsearch("http://localhost:9200")
    return es

def migrate_to_elasticsearch(df):
    es = connect_to_elasticsearch()
    for i, row in df.iterrows():
        doc = row.to_json()
        es.index(index='gc7', id=i+1, body=doc)
    print("Migration success")

if __name__ == "__main__":
    data_frame = read_data_from_postgresql()
    cleaned_data_frame = map_column_names(data_frame)
    save_to_csv(cleaned_data_frame)
    migrate_to_elasticsearch(cleaned_data_frame)
