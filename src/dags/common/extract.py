
import os.path
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import pandas as pd
import sqlite3
import io
import unicodedata
import glob


SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "C:/Users/ASUS/Desktop/DATA_AFRIK_HUB/data_env/ecommerce-analytics/dahprojet-9efb47da2d33.json"

DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")
CLEANED_DATA_DIR = os.path.join(DATA_DIR, "cleaned_data")


def connect_to_drive():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileExistsError("Credentials file not exists")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    return service

# fonction d'extraction des clients
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

# fonction d'extraction des produits
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
    print(final_data.head())
    if final_data.shape[0]>0:
        final_data.to_csv(local_path, index=False)

# fonction d'extraction des commandes
def extract_orders(date: datetime, db_path: str = "ecommerce_orders_may2024.db", table_name: str = "ecommerce_orders"):
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

# fonction de nettoyage des colonnes
def nettoyage_dataframe(df: pd.DataFrame):
    """ Suppression des espaces autour des colonnes
        Suppression des doublons
    """
    df = df.dropna(axis=1, how="all")
    df = df.drop_duplicates()

    # fonction de nettoyage des colonnes
    def nettoyage_colonne(col: str):
        col = col.lower()
        col = col.replace(' ', '_')
        col = col.strip()
        col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8', 'ignore')
        return col
    df.columns = [nettoyage_colonne(col) for col in df.columns]
    return df

# fonction de nettoyage des fichiers csv  
""" Cette fonction en argument un dossier source des données brutes et un dossier de destination."""  

def nettoyer_fichier_csv(dossier_source: str, dossier_destination: str):
    
    os.makedirs(dossier_source, exist_ok=True)

    for filepath in glob.glob(os.path.join(dossier_source, "*.csv")):
        filename = os.path.basename(filepath)

        df = pd.read_csv(os.path.join(dossier_source, filename), sep=",")

        df_cleaned = nettoyage_dataframe(df)

        df_cleaned.to_csv(os.path.join(dossier_destination, filename), index=False)
        print(f"Fichier nettoyé sauvegardé dans {dossier_destination}")
        

# calcul du stock journalier des produits pour les magasins

def calcul_stock_journalier_magasins(date : datetime) :
    
    dossier_prod = "C:\\Users\\ASUS\\Desktop\\DATA_AFRIK_HUB\\data_env\\data\\raw_data\\products\\2024\\5"
    dossier_comm = "C:\\Users\\ASUS\\Desktop\\DATA_AFRIK_HUB\\data_env\\data\\raw_data\\orders\\2024\\5"

    jour = datetime.strptime(date, "%Y-%m-%d").day

    
    # lire le stock du jour
    df_stock = pd.read_csv(f"{dossier_prod}/{jour}.csv", sep=",")
    stock = df_stock.groupby("product_name", as_index=False)["stock"].sum()
    

    # lire les commandes du jour
    df_commandes = pd.read_csv(f"{dossier_comm}/{jour}.csv", sep=",")
    comm = df_commandes.groupby("product_name")["quantity"].sum()

    # fusion des deux tables
    df_final = pd.merge(stock, comm, on="product_name", how="left")
    df_final["stock_restant"] = df_final["stock"] - df_final["quantity"].fillna(0)

    print(df_final.head(10))

# calcul du stock journalier pour le site e-commerce
"""def calcul_stock_journalier_db(date: datetime, db_path: str = "ecommerce_orders_may2024.db", table_name: str = "ecommerce_orders"):
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT sum(quantity), product_name FROM {table_name} where order_date = {date} group by product_name", conn)
    finally:
        conn.close()
    return(df.head())"""



if __name__=="__main__":  

    # extract_clients(datetime.strptime("2024-05-04", "%Y-%m-%d"))
    # extract_products(datetime.strptime("2024-05-01", "%Y-%m-%d"))
    # extract_orders(datetime.strptime("2024-05-05", "%Y-%m-%d"))
    # dossier_source = f"{RAW_DATA_DIR}/clients/2024/5"
    # dossier_destination = f"{CLEANED_DATA_DIR}/clients/2024/"
    # nettoyer_fichier_csv(dossier_source,dossier_destination)
    calcul_stock_journalier_magasins("2024-05-03")
    # calcul_stock_journalier_db("2024-05-03")