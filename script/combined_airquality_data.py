import pandas as pd  # pandas pour DataFrame
from pathlib import Path  # pathlib pour gestion de chemins
import json  # pour charger les JSON non-Lines

# 1. Définir le chemin vers 'cleaned/air_quality' à partir de la racine du projet
test_script = Path(__file__).resolve()
base_dir = test_script.parents[1] / 'cleaned' / 'air_quality'

# Vérifier que le dossier existe
if not base_dir.exists():
    raise FileNotFoundError(f"Le dossier {base_dir} n'existe pas.")

all_dfs = []

# 2. Lister les sous-dossiers (chaque dossier est une date)
for date_dir in base_dir.iterdir():
    if not date_dir.is_dir():
        continue
    # 3. Pour chaque dossier-date, lire tous les JSON
    for json_path in date_dir.glob('*.json'):
        try:
            # Essayer JSON Lines
            df = pd.read_json(json_path, lines=True)
        except ValueError:
            # Fallback JSON classique
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            df = pd.json_normalize(data)
        
        # 4. Ajouter les colonnes city (fichier) et date (dossier)
        df['city'] = json_path.stem
        df['date'] = date_dir.name
        all_dfs.append(df)

# 5. Concaténer tous les DataFrames
if not all_dfs:
    print("Aucun fichier JSON trouvé dans cleaned/air_quality.")
    exit()
combined_df = pd.concat(all_dfs, ignore_index=True)

# 6. Réordonner les colonnes pour que 'city' soit la première (et 'date' en deuxième)
cols = ['city', 'date'] + [c for c in combined_df.columns if c not in ['city', 'date']]
combined_df = combined_df[cols]

# 7. Créer un dossier 'combined' sous 'air_quality' et y enregistrer la donnée combinée
output_dir = base_dir / 'combined'
output_dir.mkdir(exist_ok=True)
combined_file = output_dir / 'combined_data.json'
combined_df.to_json(combined_file, orient='records', lines=True, force_ascii=False)

# 8. Afficher les données combinées avec la colonne 'city' en première position
print(f"Fichier combiné enregistré dans : {combined_file}\n")
print(combined_df)
