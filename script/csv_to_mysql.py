import pandas as pd
from sqlalchemy import create_engine

DB_USER = 'root'
DB_PASSWORD = '05040302'  
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'smart_city'
TABLE_NAME = 'street_light_faults'

csv_path = 'street_light_fault_prediction_dataset.csv'
df = pd.read_csv(csv_path)

engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
df.to_sql(name=TABLE_NAME, con=engine, if_exists='append', index=False)

print(f"✅ Données insérées dans `{TABLE_NAME}`.")
