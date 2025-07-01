import os
import pandas as pd

landing_base = os.path.join("./landing", "bikes")
date_folders = [d for d in os.listdir(landing_base) 
                if os.path.isdir(os.path.join(landing_base, d))]
if not date_folders:
    raise FileNotFoundError("Aucun dossier 'landing/bikes/YYYY-MM-DD' trouvé.")
date_folder = sorted(date_folders)[-1]

source_dir = os.path.join(landing_base, date_folder)
target_dir = os.path.join("cleaned", "bikes", date_folder)
os.makedirs(target_dir, exist_ok=True)

csv_filename = "202301-divvy-tripdata.csv"
input_file   = os.path.join(source_dir, csv_filename)

print(f"🔍 Lecture du fichier : {input_file}")
df = pd.read_csv(input_file)

df_clean = df[[
    'ride_id', 'started_at', 'ended_at',
    'rideable_type',
    'start_station_name', 'end_station_name',
    'member_casual'
]].rename(columns={
    'started_at': 'start_time',
    'ended_at':   'end_time',
    'rideable_type': 'bike_type',
    'start_station_name': 'start_station',
    'end_station_name':   'end_station',
    'member_casual':      'user_type'
})

# Conversion des dates et calcul de la durée 
df_clean['start_time'] = pd.to_datetime(df_clean['start_time'])
df_clean['end_time']   = pd.to_datetime(df_clean['end_time'])
df_clean['trip_duration'] = (
    df_clean['end_time'] - df_clean['start_time']
).dt.total_seconds()

df_clean = df_clean[df_clean['trip_duration'] > 0]
df_clean.dropna(subset=['ride_id'], inplace=True)

output_file = os.path.join(target_dir, "clean_bikes.parquet")

df_clean.to_parquet(output_file, index=False, engine="pyarrow")
print(f" Nettoyage terminé, fichier Parquet enregistré : {output_file}")