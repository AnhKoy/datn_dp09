from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import pandas as pd

# Load mô hình
model = joblib.load("E:\\DATA\\DuAn\\BatDongSan\\clean\\random_forest_model (1).pkl")

# Tạo ứng dụng FastAPI
app = FastAPI()

# Định nghĩa input cho API
class BDSInput(BaseModel):
    dien_tich: float
    mat_tien: float
    duong_vao: float
    loai_bds: str
    giay_to: str
    xa_phuong: str
    quan_huyen: str

@app.get("/")
def home():
    return {"message": "API dự đoán giá BĐS đang chạy"}

@app.post("/predict")
def predict(data: BDSInput):
    # Tạo DataFrame từ input
    df_input = pd.DataFrame([{
        "Diện tích (m²)": data.dien_tich,
        "Mặt tiền (m)": data.mat_tien,
        "Đường vào (m)": data.duong_vao,
        "Loại BDS": data.loai_bds,
        "Giấy tờ pháp lý": data.giay_to,
        "Xã/Phường": data.xa_phuong,
        "Quận/Huyện": data.quan_huyen
    }])

    # Dự đoán
    prediction = model.predict(df_input)[0]

    return {"gia_du_doan": round(prediction, 2)}
