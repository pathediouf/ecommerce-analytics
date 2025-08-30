
import os.path
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import pandas as pd
import sqlite3


SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "massive-glyph-323908-5809907c52ef.json"
DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")

def connect_to_drive():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileExistsError("Credentials file not exists")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    return service

def extract_clients(date: datetime, service = connect_to_drive()):
    FOLDER = "clients"
    folder_exist = True if len(service.files().list(
    q=f"name='{FOLDER}' and mimeType='application/vnd.google-apps.folder'",
    spaces='drive',
    fields='files(id, name)'
    ).execute().get('files', [])) > 0 else False
    if not folder_exist:
        raise FileExistsError("Data folder not exists")
    
    filename = f"clients_{date.strftime('%Y-%m-%d')}.csv"
    print(filename)
    daily_clients_files = service.files().list(
    q=f"name='{filename}' and mimeType!='application/vnd.google-apps.folder'",
    spaces='drive',
    fields='files(id, name)'
    ).execute().get('files', [])
    if not daily_clients_files:
        print(f"Aucun fichier trouvé avec le nom {filename}.")
    
    file_id = daily_clients_files[0]['id']
    print(f"Fichier trouvé : {daily_clients_files[0]['name']} (ID : {file_id})")

    os.makedirs(f"{RAW_DATA_DIR}/clients/{date.year}/{date.month}", exist_ok=True)
    local_path = os.path.join(f"{RAW_DATA_DIR}/clients/{date.year}/{date.month}", f"{date.day}.csv")
    request = service.files().get_media(fileId=file_id)
    with open(local_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Téléchargé {int(status.progress() * 100)}%")

    print(f"Fichier téléchargé et renommé dans : {local_path}")


def extract_products(date: datetime, service = connect_to_drive()):
    filename = f"products.csv"
    print(filename)
    products_files = service.files().list(
    q=f"name='{filename}' and mimeType!='application/vnd.google-apps.folder'",
    spaces='drive',
    fields='files(id, name)'
    ).execute().get('files', [])
    if not products_files:
        print(f"Aucun fichier trouvé avec le nom {filename}.")
    file_id = products_files[0]['id']
    print(f"Fichier trouvé : {products_files[0]['name']} (ID : {file_id})")

    os.makedirs(f"{RAW_DATA_DIR}/products/{date.year}/{date.month}", exist_ok=True)
    local_path = os.path.join(f"{RAW_DATA_DIR}/products/{date.year}/{date.month}", f"{date.day}.csv")
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Téléchargé {int(status.progress() * 100)}%")

    print(f"Fichier téléchargé et renommé dans : {local_path}")
    fh.seek(0)
    data = pd.read_csv(fh, sep = ",")
    final_data = data[data.date == date.strftime("%Y-%m-%d")]
    if final_data.shape[0]>0:
        final_data.to_csv(local_path, index=False)

def extract_orders(date: datetime, db_path: str = "ecommerce_orders_may2024.db", table_name: str="ecommerce_orders"):
    conn = sqlite3.connect(db_path)
    try:
        date_str = date.strftime("%Y-%m-%d")
        df = pd.read_sql_query(f'SELECT * FROM {table_name} where order_date="{date_str}" ', conn)
    finally:
        conn.close()
    
    if df.shape[0]> 0:
        os.makedirs(f"{RAW_DATA_DIR}/orders/{date.year}/{date.month}", exist_ok=True)
        local_path = os.path.join(f"{RAW_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
        df.to_csv(local_path, index=False)



if __name__=="__main__":
    
    extract_orders(datetime.strptime("2024-05-03", "%Y-%m-%d"))
