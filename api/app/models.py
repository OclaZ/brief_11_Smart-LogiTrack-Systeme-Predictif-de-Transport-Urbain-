# app/models.py
from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy.sql import func
from .database import Base

# --- Table pour les Utilisateurs ---
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)

# --- Table pour le Monitoring ---
class EtaPrediction(Base):
    __tablename__ = "eta_predictions"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    
    trip_distance = Column(Float)
    passenger_count = Column(Integer)
    pickuphour = Column(Integer)
    
    predicted_duration = Column(Float)
    model_version = Column(String, default="v1.0")