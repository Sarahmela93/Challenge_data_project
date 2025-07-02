import os
import pandas as pd
import time

# Paramètres
source_csv = "../landing/street_light_fault_prediction_dataset.csv"  # CSV source complet
output_dir = "../landing/street-light"
rows_per_event =8000  # nombre de lignes par fichier simulé
interval_seconds = 8  # délai entre chaque "push" de fichier (en secondes)

# Crée le dossier cible si nécessaire
os.makedirs(output_dir, exist_ok=True)

# Charge les données
df = pd.read_csv(source_csv)

# Découpe et simulation du streaming
for i in range(0, len(df), rows_per_event):
    chunk = df.iloc[i:i + rows_per_event]
    chunk_filename = os.path.join(output_dir, f"event_{i // rows_per_event:04}.csv")
    chunk.to_csv(chunk_filename, index=False)
    print(f"📤 Événement {i // rows_per_event:04} écrit dans : {chunk_filename}")
    time.sleep(interval_seconds)

print("✅ Simulation de streaming terminée.")
