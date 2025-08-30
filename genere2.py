import sqlite3
import random
from datetime import datetime, timedelta

# Base SQLite
DB_PATH = "ecommerce_orders_may2024.db"
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Supprimer la table si elle existe
cur.execute("DROP TABLE IF EXISTS ecommerce_orders")

# Créer la table
cur.execute("""
CREATE TABLE ecommerce_orders (
    order_id INTEGER PRIMARY KEY,
    order_date TEXT,
    customer_id INTEGER,
    customer_name TEXT,
    product_id INTEGER,
    product_name TEXT,
    quantity INTEGER,
    price REAL
)
""")

# Paramètres
start_date = datetime(2024, 5, 1)
end_date = datetime(2024, 5, 31)
order_id = 1
orders = []

# Boucle sur chaque jour de mai 2024
current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    
    # Nombre aléatoire de commandes ce jour (0 à 5)
    num_orders = random.randint(0, 30)
    
    for _ in range(num_orders):
        # Choisir un client et un produit aléatoires
        customer_id = random.randint(1, 34)
        product_id = random.randint(1, 20)
        
        quantity = random.randint(1, 5)
        price = round(random.uniform(5, 100), 2)
        
        orders.append((
            order_id,
            date_str,
            customer_id,
            f"Customer_{customer_id}",
            product_id,
            f"Product_{product_id}",
            quantity,
            price
        ))
        order_id += 1
    
    current_date += timedelta(days=1)

# Insérer les données dans SQLite
cur.executemany("""
INSERT INTO ecommerce_orders 
(order_id, order_date, customer_id, customer_name, product_id, product_name, quantity, price)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", orders)

conn.commit()
conn.close()
print(f"Base SQLite créée avec {len(orders)} commandes pour mai 2024.")
