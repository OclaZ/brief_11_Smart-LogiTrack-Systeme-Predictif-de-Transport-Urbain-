from fastapi.testclient import TestClient
from app.main import app
import pytest

client = TestClient(app)

# 1. Test Basique : Vérifier que l'API répond
def test_read_root():
    # Comme nous n'avons pas défini de route racine "/", on teste la doc
    response = client.get("/docs")
    assert response.status_code == 200

# 2. Test du cycle d'Authentification (Inscription -> Token)
def test_auth_flow():
    fake_user = "test_user_pytest"
    fake_pwd = "test_password"

    # A. Inscription
    response = client.post("/register", json={"username": fake_user, "password": fake_pwd})
    # Si l'utilisateur existe déjà (tests précédents), on accepte 400, sinon 200
    assert response.status_code in [200, 400]

    # B. Connexion (Récupération du Token)
    response = client.post("/token", data={"username": fake_user, "password": fake_pwd})
    assert response.status_code == 200
    token = response.json()["access_token"]
    assert token is not None
    return token

# 3. Test de Prédiction (Preprocessing & Endpoint)
def test_prediction_endpoint():
    # On a besoin d'un token pour accéder à /predict
    token = test_auth_flow()
    headers = {"Authorization": f"Bearer {token}"}

    # Données de test (simule une requête frontend)
    payload = {
        "trip_distance": 2.5,
        "passenger_count": 1,
        "pickuphour": 14,
        "dayof_week": 3,
        "month": 5,
        "payment_type": 1
    }

    response = client.post("/predict", json=payload, headers=headers)
    
    # Deux issues possibles selon si le modèle Spark est chargé ou non dans l'environnement de test
    if response.status_code == 200:
        data = response.json()
        assert "estimated_duration" in data
        assert isinstance(data["estimated_duration"], float)
    else:
        # Si le modèle n'est pas trouvé (fréquent en test CI/CD léger), l'API doit renvoyer 500 proprement
        assert response.status_code == 500
        assert response.json()["detail"] == "Model not loaded"