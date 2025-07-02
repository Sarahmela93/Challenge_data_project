import os
import pandas as pd
from sqlalchemy import create_engine

# Configuration
PARQUET_DIR = "../cleaned/street-light"  # Dossier où sont tes fichiers .parquet
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_DB = "SMART_CITY"
TABLE_NAME = "smart_city_lights"  # Nom de la table MySQL

# Créer la connexion SQLAlchemy
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

COLUMN_MAPPING = {
    "power_consumption (Watts)":            "power_consumption_watts",
    "voltage_levels (Volts)":               "voltage_levels_volts",
    "current_fluctuations (Amperes)":       "current_fluctuations_amperes",
    "temperature (Celsius)":                "temperature_celsius",
    "current_fluctuations_env (Amperes)":   "current_fluctuations_env_amperes",
    # Les autres (« bulb_number », « timestamp », « environmental_conditions », « fault_type »)
    # n’ont pas besoin d’être renommés : ils sont déjà identiques.
}


# Lecture et insertion fichier par fichier
for filename in sorted(os.listdir(PARQUET_DIR)):
    if filename.endswith(".parquet"):
        filepath = os.path.join(PARQUET_DIR, filename)
        print(f"Lecture de {filepath}...")

        try:
            # Lire le fichier Parquet
            df = pd.read_parquet(filepath)

            # ✅ Appliquer le renommage ici AVANT insertion
            df.rename(columns=COLUMN_MAPPING, inplace=True)

            # ✅ Vérifier que toutes les colonnes attendues existent
            print(f"Colonnes après renommage : {df.columns.tolist()}")

            # Insérer dans la table SQL
            df.to_sql(name=TABLE_NAME, con=engine, if_exists="append", index=False)
            print(f"✅ Données insérées depuis {filename}")

        except Exception as e:
            print(f"❌ Erreur avec {filename} : {e}")
