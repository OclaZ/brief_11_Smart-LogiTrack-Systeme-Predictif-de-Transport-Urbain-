# app/schemas.py
from pydantic import BaseModel
from typing import Optional

# --- Schémas Utilisateurs ---
class UserCreate(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    username: str
    is_active: bool
    
    class Config:
        orm_mode = True

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

# --- Schémas Prédictions (Inchangés) ---
class TripData(BaseModel):
    trip_distance: float
    passenger_count: int
    pickuphour: int
    dayof_week: int
    month: int
    payment_type: Optional[int] = 1 

class PredictionResponse(BaseModel):
    estimated_duration: float