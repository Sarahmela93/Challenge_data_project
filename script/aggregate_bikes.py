import os
import pandas as pd

# Détection du dossier date sous cleaned/bikes 
cleaned_base = os.path.join("./cleaned", "bikes")
date_folders = [d for d in os.listdir(cleaned_base)
                if os.path.isdir(os.path.join(cleaned_base, d))]
if not date_folders:
    raise FileNotFoundError("Aucun dossier 'cleaned/bikes/YYYY-MM-DD' trouvé.")
date_folder = sorted(date_folders)[-1]

input_file = os.path.join(cleaned_base, date_folder, "clean_bikes.parquet")
print(f" Chargement du fichier Parquet : {input_file}")
df = pd.read_parquet(input_file)

if 'start_time' not in df.columns:
    raise KeyError("La colonne 'start_time' est manquante dans le DataFrame.")
df['hour'] = df['start_time'].dt.floor('H')
df['date'] = df['start_time'].dt.normalize()

# Nombre de trajets & durée moyenne
hourly_stats = df.groupby('hour').agg(
    rides=('ride_id', 'count'),
    avg_duration=('trip_duration', 'mean')
).reset_index()

# Nombre de trajets & durée moyenne
daily_stats = df.groupby('date').agg(
    rides=('ride_id', 'count'),
    avg_duration=('trip_duration', 'mean')
).reset_index()

df= df[[ "ride_id","bike_type","user_type","trip_duration","start_time","end_time","start_station","end_station"]]

# Creation du dossier gold/ created
curated_dir = os.path.join("curated", "bikes")
os.makedirs(curated_dir, exist_ok=True)

#hourly_csv = os.path.join(curated_dir, "hourly_stats.csv")
#daily_csv  = os.path.join(curated_dir, "daily_stats.csv")
df.to_csv("curated/bikes/bike.csv")

#hourly_stats.to_csv(hourly_csv, index=False)
#print(f" Statistiques horaires CSV enregistrées : {hourly_csv}")

#daily_stats.to_csv(daily_csv, index=False)
#print(f" Statistiques quotidiennes CSV enregistrées : {daily_csv}") 
