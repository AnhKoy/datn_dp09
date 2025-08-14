import pandas as pd
import numpy as np
import re
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler

import sys
import io
# Cho phép in Unicode ra terminal
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

#=============================================================================================================================

# Đọc file CSV đã làm sạch
df = pd.read_csv(r'E:\DATA\DuAn\BatDongSan\n8n\lam_sach\dat\dat.csv', encoding='utf-8-sig')

# dung knn điền giá trị trống cột mat_tien_m va cọt duong_vao_m
df_clean = df.copy()

# Chọn các cột số liên quan để KNN dựa vào
cols_knn = ['mat_tien_m', 'duong_vao_m', 'dien_tich_m2', 'muc_gia_ty', 'gia_m2_trieu']

# Chuyển đổi các cột số sang định dạng số
for col in cols_knn:
    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

# Chọn các cột số liên quan để KNN dựa vào
df_knn = df_clean[cols_knn]
# Khởi tạo KNN Imputer
imputer = KNNImputer(n_neighbors=5)
# Điền giá trị trống bằng KNN
df_clean[cols_knn] = imputer.fit_transform(df_knn)
#=============================================================================================================================

# -----------------------
# luu file sau khi da dung knn
# -----------------------
output_file = 'E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\dat\\dat_cleaned.csv'
df_clean.to_csv(output_file, index=False, encoding='utf-8-sig')
print("✅ Đã dọn sạch dữ liệu và lưu vào file:", output_file)


