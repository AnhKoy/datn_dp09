from fastapi import FastAPI
import joblib
import pandas as pd
import gdown
import os

app = FastAPI()

# Tải model từ Google Drive nếu chưa có
MODEL_PATH = "randomforest_model.pkl"
DRIVE_URL = "https://drive.google.com/uc?id=1LLjj8igfQPTC6ncAv8ybZfJvnAKJRIz8&export=download"

if not os.path.exists(MODEL_PATH):
    gdown.download(DRIVE_URL, MODEL_PATH, quiet=False)

model = joblib.load(MODEL_PATH)

@app.get("/")
def home():
    return {"message": "API dự báo giá bất động sản đang hoạt động!"}

@app.post("/predict")
def predict(data: dict):
    df = pd.DataFrame([data])
    prediction = model.predict(df)[0]
    return {"gia_du_doan": float(prediction)}
