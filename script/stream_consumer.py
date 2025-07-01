import os
import time
import pandas as pd

# Dossier à surveiller
input_dir = "data/streaming_input"
master_csv = "./cleaned/street-light.csv"
seen_files = set()

# Créer le fichier principal s'il n'existe pas
if not os.path.exists(master_csv):
    # S'il y a déjà un fichier event, on copie le header
    initial_files = sorted(f for f in os.listdir(input_dir) if f.startswith("event_") and f.endswith(".csv"))
    if initial_files:
        first_df = pd.read_csv(os.path.join(input_dir, initial_files[0]))
        first_df.iloc[0:0].to_csv(master_csv, index=False)  # Écrit juste le header

print("🚀 Démarrage du consumer...")

while True:
    files = sorted(f for f in os.listdir(input_dir) if f.startswith("event_") and f.endswith(".csv"))
    for file in files:
        path = os.path.join(input_dir, file)
        if file not in seen_files:
            print(f"📥 Nouveau fichier détecté : {file}")
            try:
                df = pd.read_csv(path)
                df.to_csv(master_csv, mode="a", header=False, index=False)
                print(f"✅ Données ajoutées à {master_csv}")
                seen_files.add(file)
            except Exception as e:
                print(f"❌ Erreur lecture de {file} : {e}")
    time.sleep(1)
