# app/main.py
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi.security import OAuth2PasswordRequestForm
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import pandas as pd
import os

from . import models, schemas, auth, database

# Création des tables (Users + Predictions)
models.Base.metadata.create_all(bind=database.engine)

app = FastAPI(title="Smart LogiTrack API", version="1.0")

# --- SPARK SETUP (Identique à avant) ---
spark = None
loaded_model = None

@app.on_event("startup")
def startup_event():
    global spark, loaded_model
    spark = SparkSession.builder \
        .appName("FastAPI_Inference") \
        .master("local[*]") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    
    model_path = "models/eta_gbt_pipeline"
    if os.path.exists(model_path):
        loaded_model = PipelineModel.load(model_path)
        print("✅ Modèle chargé.")
    else:
        print(f"⚠️ Modèle introuvable à {model_path}")

# --- AUTH & USERS ENDPOINTS ---

@app.post("/register", response_model=schemas.UserResponse)
def register_user(user: schemas.UserCreate, db: Session = Depends(database.get_db)):
    # Vérifier si l'utilisateur existe déjà
    db_user = db.query(models.User).filter(models.User.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    # Hachage du mot de passe et création
    hashed_pwd = auth.get_password_hash(user.password)
    new_user = models.User(username=user.username, hashed_password=hashed_pwd)
    
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

@app.post("/token", response_model=schemas.Token)
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(database.get_db)):
    # Récupérer l'utilisateur en DB
    user = db.query(models.User).filter(models.User.username == form_data.username).first()
    
    # Vérifier mot de passe
    if not user or not auth.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Générer le token
    access_token = auth.create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

# --- PREDICTION ENDPOINT (Sécurisé par JWT) ---
@app.post("/predict", response_model=schemas.PredictionResponse)
def predict_eta(
    trip: schemas.TripData, 
    current_user: models.User = Depends(auth.get_current_user), # Protection ici
    db: Session = Depends(database.get_db)
):
    if not loaded_model:
        raise HTTPException(status_code=500, detail="Model not loaded")

    # Inférence Spark
    input_data = pd.DataFrame([trip.dict()])
    spark_df = spark.createDataFrame(input_data)
    predictions = loaded_model.transform(spark_df)
    result = predictions.select("prediction").collect()[0][0]
    
    # Log en base
    log_entry = models.EtaPrediction(
        trip_distance=trip.trip_distance,
        passenger_count=trip.passenger_count,
        pickuphour=trip.pickuphour,
        predicted_duration=result
    )
    db.add(log_entry)
    db.commit()
    
    return {"estimated_duration": round(result, 2)}

# --- ANALYTICS ENDPOINTS (SQL Brut) ---
# Ces endpoints sont aussi protégés par `current_user`

@app.get("/analytics/avg-duration-by-hour")
def get_avg_duration_by_hour(
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(database.get_db)
):
    sql_query = text("""
        WITH HourlyStats AS (
            SELECT pickuphour, AVG(duration_minutes) as avg_duration
            FROM silver_taxi_trips GROUP BY pickuphour
        )
        SELECT * FROM HourlyStats ORDER BY pickuphour;
    """)
    # Note : Assurez-vous que la table silver_taxi_trips existe dans votre DB
    try:
        result = db.execute(sql_query).fetchall()
        return [{"pickuphour": r[0], "avgduration": round(r[1], 1)} for r in result]
    except Exception as e:
        # Retourne une liste vide ou erreur si la table n'est pas encore peuplée
        print(f"Erreur Analytics: {e}")
        return []

@app.get("/analytics/payment-analysis")
def get_payment_analysis(
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(database.get_db)
):
    sql_query = text("""
        SELECT payment_type, COUNT(*) as total, AVG(duration_minutes) as avg
        FROM silver_taxi_trips GROUP BY payment_type
    """)
    try:
        result = db.execute(sql_query).fetchall()
        return [{"payment_type": r[0], "total_trips": r[1], "avg_duration": round(r[2], 1)} for r in result]
    except Exception:
        return []