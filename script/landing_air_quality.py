from datetime import datetime
import os, requests, json

API_TOKEN = 'xqC205OsoBKTcV0Wp4JQ1lVCAhmvYGlg'
API_URL = "https://api.meersens.com/environment/public/air/current"

US_CITIES = [
    ("New York", 40.7128, -74.0060),
    ("Los Angeles", 34.0522, -118.2437),
    ("Chicago", 41.8781, -87.6298),
    ("Houston", 29.7604, -95.3698),
    ("Phoenix", 33.4484, -112.0740),
    ("Philadelphia", 39.9526, -75.1652),
    ("San Antonio", 29.4241, -98.4936),
    ("San Diego", 32.7157, -117.1611),
    ("Dallas", 32.7767, -96.7970),
    ("San Jose", 37.3382, -121.8863),
]

def create_folder(path):
    os.makedirs(path, exist_ok=True)
    
def fetch_air_quality(city, lat, lon):
    url = f"{API_URL}?lat={lat}&lng={lon}"
    headers = {"apikey": API_TOKEN}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"{city} — Erreur {resp.status_code}")
        return None
    return resp.json()

def save_data(city, data):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    folder = f"./landing/air_quality/{today}"
    create_folder(folder)
    filename = f"{folder}/{city.replace(' ', '_')}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Données enregistrées pour {city} dans {filename}")
    
def main():
    for city, lat, lon in US_CITIES:
        data = fetch_air_quality(city, lat, lon)
        if data:
            save_data(city, data)
            
if __name__ == "__main__":
    main()