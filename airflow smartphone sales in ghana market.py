
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
import pandas as pd
import psycopg2


def extract_data():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="airflow",
        password="airflow"
    )
    query = "SELECT * FROM table_m3"
    data = pd.read_sql(query, conn)
    conn.close()

    # Simpan data ke dalam file CSV "data_kotor.csv"
    data.to_csv('/opt/airflow/data/P2M3_antonius_daeneg_data_raw.csv', index=False)

def clean_data():
    data = pd.read_csv('/opt/airflow/data/P2M3_antonius_daeneg_data_raw2.csv')

    # Mapping kolom
    kolom_mapping = {
        'brand': 'brand',
        'model': 'model',
        'sd_card': 'sd_card',
        'main_camera': 'main_camera',
        'resolution': 'resolution',
        'display': 'display',
        'sim_card': 'sim_card',
        'os': 'Sistem_operasi',
        'color': 'color',
        'region': 'region',
        'location': 'location',
        'screen_size(inch)': 'screen_size',
        'battery(mAh)': 'battery',
        'storage(GB)': 'storage',
        'ram(GB)': 'ram',
        'selfie_camera(MP)': 'selfie_camera',
        'price(¢)': 'price'
    }

    # Mengganti nama kolom
    data.rename(columns=kolom_mapping, inplace=True)

    # Menghapus duplikat
    data = data.drop_duplicates()

    # Menghapus nilai NULL
    data = data.dropna()

    # Mengubah tipe data kolom 'price' menjadi int
    data['price'] = data['price'].astype(int)

    # Simpan data yang sudah bersih ke dalam file "data_bersih.csv"
    data.to_csv('/opt/airflow/data/P2M3_antonius_daeng_data_clean.csv', index=False)
def send_to_elasticsearch(data):
    # Inisialisasi koneksi ke Elasticsearch
    es = Elasticsearch(hosts="http://localhost:9200")

    # Nama indeks Elasticsearch yang akan digunakan
    index_name = 'milestone_final'

    # Mengirim data ke Elasticsearch
    for _, row in data.iterrows():
        document = row.to_dict()
        es.index(index=index_name, body=document)

    print("Data berhasil dikirim ke Elasticsearch.")

def main():
    cleaned_data = pd.read_csv('/opt/airflow/data/P2M3_antonius_daeng_data_clean.csv')
    send_to_elasticsearch(cleaned_data)


default_args = {
    'owner': 'anjas', 
    'depends_on_past': False, 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=60), 
}


with DAG('anjas_ml3',
         description='Ini adalah DAG untuk tugas milestone',
         default_args=default_args, 
         schedule_interval='@daily', 
         start_date=datetime(2023, 10, 30, 16, 30, 00) - timedelta(hours=7), 
         catchup=False) as dag:  


    # Task to fetch data from PostgreSQL
    fetch_task = PythonOperator( 
        task_id='extract_data',
        python_callable=extract_data 
    )

    clean_task = PythonOperator( 
        task_id='clean_data',
        python_callable=clean_data,
    )

    elasticsearch_task = PythonOperator( 
        task_id='send_to_elasticsearch',
        python_callable=send_to_elasticsearch,
    )


    fetch_task >> clean_task >> elasticsearch_task


