import pandas as pd
import sys
import io
import os
import numpy as np
from sklearn.impute import KNNImputer

# Cho phép in Unicode ra terminal
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ─── 1. ĐỌC DỮ LIỆU TỪ FILE CSV ──────────────────────────────────────────────

# Đường dẫn đến file CSV đã làm sạch
input_path = r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\can_ho\can_ho.csv"
df = pd.read_csv(input_path, encoding='utf-8-sig')

# Tính tỉ lệ phần trăm các cột bị thiếu
missing_percentage = df.isnull().mean() * 100

# Chỉ xử lý các cột có tỷ lệ thiếu <= 90%
columns_to_process = missing_percentage[missing_percentage <= 90].index.tolist()

# === Tạo bản sao gốc để làm sạch (không loại bỏ cột nào)
df_clean = df.copy()

# ─── 2. XỬ LÝ CÁC CỘT SỐ BẰNG KNN ─────────────────────────────────────────────

# Chọn các cột số cần xử lý
cols_knn = ['so_phong_ngu', 'so_phong_tam_ve_sinh', 'dien_tich_m2', 'muc_gia_ty', 'gia_m2_trieu']

# Chuyển các cột này về kiểu số (đề phòng lỗi chuỗi)
for col in cols_knn:
    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

# Dữ liệu đưa vào KNN (chỉ lấy các hàng không null toàn bộ)
df_knn_input = df_clean[cols_knn]

# Khởi tạo KNN Imputer
imputer = KNNImputer(n_neighbors=5)

# Điền khuyết bằng KNN
df_knn_filled = pd.DataFrame(imputer.fit_transform(df_knn_input), columns=cols_knn)

# Gán lại dữ liệu sau khi điền vào bảng chính
for col in cols_knn:
    if col in df_clean.columns:
        df_clean[col] = df_knn_filled[col]

# Làm tròn số phòng ngủ, tắm thành số nguyên
for col in ['so_phong_ngu', 'so_phong_tam_ve_sinh']:
    if col in df_clean.columns:
        df_clean[col] = np.round(df_clean[col]).astype('Int64')

# ─── 3. XỬ LÝ TEXT THIẾU ÍT (HUONG_NHA, HUONG_BAN_CONG) ───────────────────────

def fill_missing_text_with_mode(series):
    mode_value = series.mode().iloc[0] if not series.mode().empty else np.nan
    return series.fillna(mode_value)

for text_col in ['huong_nha', 'huong_ban_cong']:
    if text_col in df_clean.columns:
        df_clean[text_col] = fill_missing_text_with_mode(df_clean[text_col])

# ─── 4. GHI RA FILE ─────────────────────────────────────────────────────────────

output_path = r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\can_ho\can_ho_cleaned.csv"
df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ Đã làm sạch dữ liệu và lưu vào file: {output_path}")
