import os
import json
from datetime import datetime

# Dossier source (landing)
source_dir = "./landing/air_quality/2025-06-30/"

# Dossier cible (raw)
target_dir = "./cleaned/air_quality/2025/06/30/"
os.makedirs(target_dir, exist_ok=True)

# Fonction de nettoyage
def clean_file(file_path, output_path):
    with open(file_path, "r") as f:
        raw = json.load(f)

    cleaned = {
        "city": raw.get("city", "unknown"),
        "timestamp": raw.get("timestamp", ""),
        "pm10": raw.get("air_quality", {}).get("pm10", None),
        "pm25": raw.get("air_quality", {}).get("pm25", None),
        "no2": raw.get("air_quality", {}).get("no2", None),
        "o3": raw.get("air_quality", {}).get("o3", None),
        "lat": raw.get("location", {}).get("lat", None),
        "lon": raw.get("location", {}).get("lon", None),
    }

    with open(output_path, "w") as f:
        json.dump(cleaned, f, indent=4)

# Parcours de tous les fichiers
for filename in os.listdir(source_dir):
    if filename.endswith(".json"):
        src = os.path.join(source_dir, filename)
        dst = os.path.join(target_dir, filename)
        clean_file(src, dst)
        print(f"✅ Nettoyé : {filename}")
