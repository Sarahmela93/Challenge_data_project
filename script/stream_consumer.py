import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

SOURCE_DIR = "data/streaming_input"
DEST_DIR = "../cleaned/street-light"

os.makedirs(DEST_DIR, exist_ok=True)

def wait_for_file_complete(filepath, timeout=5, delay=0.2):
    """Attend que la taille du fichier soit stable (fin d'écriture)."""
    start_time = time.time()
    last_size = -1

    while time.time() - start_time < timeout:
        current_size = os.path.getsize(filepath)
        if current_size == last_size:
            return True
        last_size = current_size
        time.sleep(delay)

    return False

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return

        filepath = event.src_path
        print(f"Nouveau fichier détecté : {filepath}")

        if not wait_for_file_complete(filepath):
            print(f"⚠️ Le fichier {filepath} ne semble pas complètement écrit. Ignoré.")
            return

        try:
            df = pd.read_csv(filepath)
            if df.empty:
                print(f"⚠️ Le fichier {filepath} est vide ou invalide.")
                return

            filename = os.path.basename(filepath).replace(".csv", ".parquet")
            dest_path = os.path.join(DEST_DIR, filename)

            df.to_parquet(dest_path, engine="pyarrow", index=False)
            print(f"✅ Fichier converti et sauvegardé : {dest_path}")

        except Exception as e:
            print(f"❌ Erreur lors du traitement de {filepath} : {e}")

if __name__ == "__main__":
    event_handler = CSVHandler()
    observer = Observer()
    observer.schedule(event_handler, path=SOURCE_DIR, recursive=False)
    observer.start()
    print(f"📡 Surveillance du dossier : {SOURCE_DIR}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
