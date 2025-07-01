from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from db import SessionLocal
import pandas as pd

app = FastAPI()

# Dépendance pour gérer les sessions DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Route d'accueil
@app.get("/")
def read_root():
    return {"message": "Bienvenue dans l'API Smart City - Street Light Faults"}

# Liste des enregistrements avec limite
@app.get("/faults")
def list_faults(limit: int = 10, db: Session = Depends(get_db)):
    limit = max(1, min(limit, 1000))  # sécurité de plage
    query = f"SELECT * FROM street_light_faults LIMIT {limit}"
    df = pd.read_sql(query, db.bind)
    return df.to_dict(orient="records")

# Obtenir un enregistrement par ID
@app.get("/faults/{id}")
def get_fault(id: int, db: Session = Depends(get_db)):
    query = "SELECT * FROM street_light_faults WHERE id = :id"
    df = pd.read_sql(query, db.bind, params={"id": id})
    if df.empty:
        raise HTTPException(status_code=404, detail="Fault not found")
    return df.iloc[0].to_dict()
