import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sqlalchemy import create_engine
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import joblib


# Paramètres MySQL
MYSQL_HOST     = "localhost"
MYSQL_PORT     = 3306
MYSQL_USER     = "root"
MYSQL_PASSWORD = "ngyngy10"
MYSQL_DB       = "SMART_CITY"
TABLE_NAME     = "smart_city_lights"

# Connexion via SQLAlchemy
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

# Requête SQL
query = f"SELECT * FROM {TABLE_NAME}"

# Chargement dans un DataFrame
df = pd.read_sql(query, engine)

df['timestamp'] = pd.to_datetime(df['timestamp'])

# RANDOM FOREST

features = ['power_consumption_watts', 'voltage_levels_volts', 'current_fluctuations_amperes', 'temperature_celsius', 'current_fluctuations_env_amperes']
target = 'fault_type'

for feature in features:
    df[feature] = pd.to_numeric(df[feature].astype(str).replace('[^0-9.]', '', regex=True), errors='coerce')
    
df = pd.get_dummies(df, columns=['environmental_conditions'], drop_first=True)

X_train, X_test, y_train, y_test = train_test_split(df[features + list(df.columns[df.columns.str.startswith('environmental_conditions')])], df[target], test_size=0.2, random_state=42)

rf_model = RandomForestClassifier(random_state=42)
rf_model.fit(X_train, y_train)

y_pred = rf_model.predict(X_test)


accuracy = accuracy_score(y_test, y_pred)
conf_matrix = confusion_matrix(y_test, y_pred)
class_report = classification_report(y_test, y_pred)

print(f"Accuracy: {accuracy:.2f}")
print("\nConfusion Matrix:")
print(conf_matrix)
print("\nClassification Report:")
print(class_report)

# API
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# 🌐 Dictionnaire de description des pannes
FAULT_TYPES = {
    0: "✅ Aucun défaut",
    1: "⚡ Surtension",
    2: "💡 Ampoule défectueuse",
    3: "🌡️ Problème thermique",
    4: "🧠 Anomalie multiple"
}

@app.get("/", response_class=HTMLResponse)
def read_form(request: Request):
    return templates.TemplateResponse("form.html", {
        "request": request,
        "fault_types": FAULT_TYPES,
        "result": None
    })
    
@app.post("/", response_class=HTMLResponse)
def predict_fault(
    request: Request,
    power_consumption: float = Form(...),
    voltage: float = Form(...),
    current_fluctuations: float = Form(...),
    temperature: float = Form(...),
    current_fluctuations_env: float = Form(...),
    environmental_cloudy: str = Form(...), 
    environmental_rainy: str = Form(...)
):
    env_cond_val = 1 if environmental_cloudy.lower() == "true" else 0
    env_rainy_val = 1 if environmental_rainy.lower() == "true" else 0
    
    # Créer un DataFrame d’entrée pour le modèle
    input_data = pd.DataFrame([{
        "power_consumption_watts": power_consumption,
        "voltage_levels_volts": voltage,
        "current_fluctuations_amperes": current_fluctuations,
        "temperature_celsius": temperature,
        "current_fluctuations_env_amperes": current_fluctuations_env,
        "environmental_conditions_Cloudy": env_cond_val,
        "environmental_conditions_Rainy": env_rainy_val
    }])

    prediction = rf_model.predict(input_data)[0]
    prediction_label = FAULT_TYPES.get(prediction, "Type inconnu")

    return templates.TemplateResponse("form.html", {
        "request": request,
        "fault_types": FAULT_TYPES,
        "result": f"Résultat : Fault Type {prediction} - {prediction_label}"
    })
    
print("http://127.0.0.1:8000")