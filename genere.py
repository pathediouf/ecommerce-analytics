import pandas as pd
from datetime import datetime, timedelta
import random
import os

# Paramètres
start_date = datetime(2024, 5, 1)
end_date = datetime(2024, 5, 31)
output_dir = "clients_daily_may2024"
os.makedirs(output_dir, exist_ok=True)

initial_customers = 20
next_customer_id = initial_customers + 1
existing_customers = list(range(1, initial_customers+1))

# Boucle sur chaque jour
current_date = start_date
while current_date <= end_date:
    daily_records = []
    
    # Ajouter clients existants (simulation activité)
    for cid in existing_customers:
        if random.random() < 0.7:  # 70% chance d'être présent ce jour
            daily_records.append({
                "date": current_date.strftime("%Y-%m-%d"),
                "customer_id": cid,
                "firstname": f"Firstname_{cid}",
                "lastname": f"Lastname_{cid}",
                "email": f"user{cid}@example.com"
            })
    
    # Ajouter nouveaux clients certains jours
    if random.random() < 0.3:  # 30% chance d'avoir des nouveaux clients
        num_new = random.randint(1, 3)
        for _ in range(num_new):
            daily_records.append({
                "date": current_date.strftime("%Y-%m-%d"),
                "customer_id": next_customer_id,
                "firstname": f"Firstname_{next_customer_id}",
                "lastname": f"Lastname_{next_customer_id}",
                "email": f"user{next_customer_id}@example.com"
            })
            existing_customers.append(next_customer_id)
            next_customer_id += 1
    
    # Sauvegarder fichier CSV du jour
    df_daily = pd.DataFrame(daily_records)
    df_daily.to_csv(f"{output_dir}/clients_{current_date.strftime('%Y-%m-%d')}.csv", index=False)
    
    current_date += timedelta(days=1)
