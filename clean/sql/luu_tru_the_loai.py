import pandas as pd
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import os
import numpy as np
import pyodbc

# ket noi sql server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=NGUYENANH;'
    'DATABASE=DATN_batDongSan_clean;'
    'Trusted_Connection=yes;'
)

cursor = conn.cursor()

# doc tung file csv
files = [
    r'E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\nha_rieng\\nha_rieng_cleaned.csv',
    r'E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\can_ho\\can_ho_cleaned.csv',
    r'E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\dat\\dat_cleaned.csv',
    r'E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\con_lai\\con_lai_cleaned.csv'
]

# tao cac bang trong sql

table_names = [
    'nha_rieng',
    'can_ho',
    'dat',
    'con_lai'
]

# tạo bảng nếu chưa tồn tại 
# link,tieu_de,dia_chi,loai_hinh,muc_gia_ty,gia_m2_trieu,dien_tich_m2,so_phong_ngu,so_phong_tam_ve_sinh,phap_ly,noi_that,so_tang,mat_tien_m,duong_vao_m,huong_nha,huong_ban_cong,ngay_dang,phuong,quan
for table_name in table_names:
    cursor.execute(f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
    CREATE TABLE {table_name} (
        link NVARCHAR(MAX),
        tieu_de NVARCHAR(MAX),
        dia_chi NVARCHAR(MAX),
        loai_hinh NVARCHAR(100),
        muc_gia_ty NVARCHAR(50),
        gia_m2_trieu FLOAT,
        dien_tich_m2 FLOAT,
        so_phong_ngu INT,
        so_phong_tam_ve_sinh INT,
        phap_ly NVARCHAR(100),
        noi_that NVARCHAR(MAX),
        so_tang INT,
        mat_tien_m FLOAT,
        duong_vao_m FLOAT,
        huong_nha NVARCHAR(50),
        huong_ban_cong NVARCHAR(50),
        ngay_dang DATETIME,
        phuong NVARCHAR(100),
        quan NVARCHAR(100)
    )
    """)
    conn.commit()
    
# day du lieu vao cac bang
# Mỗi file sẽ được đưa vào bảng tương ứng theo thứ tự đã định nghĩa trong table_names
# Giả sử thứ tự file là: nha_rieng, can_ho, dat, con_lai
for file, table_name in zip(files, table_names):
    df = pd.read_csv(file, encoding='utf-8-sig')
    
    # Chuyển đổi kiểu dữ liệu cho các cột số
    numeric_cols = ['so_phong_ngu', 'so_phong_tam_ve_sinh', 'so_tang', 'mat_tien_m', 'duong_vao_m', 'dien_tich_m2', 'muc_gia_ty', 'gia_m2_trieu']
    for col in numeric_cols:
        if col in ['gia_m2_trieu', 'dien_tich_m2', 'mat_tien_m', 'duong_vao_m']:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(6)
    
    # Chuyển đổi kiểu dữ liệu cho các cột ngày tháng
    if 'ngay_dang' in df.columns:
        df['ngay_dang'] = pd.to_datetime(df['ngay_dang'], errors='coerce')
    # Chuyển đổi kiểu dữ liệu cho các cột văn bản
    text_cols = ['link', 'tieu_de', 'dia_chi', 'loai_hinh', 'phap_ly', 'noi_that', 'huong_nha', 'huong_ban_cong', 'phuong', 'quan']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
    # Đảm bảo các cột float không chứa giá trị không hợp lệ
    float_cols = ['gia_m2_trieu', 'dien_tich_m2', 'mat_tien_m', 'duong_vao_m']
    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].replace('', np.nan)  # thay chuỗi rỗng bằng NaN
            df[col] = pd.to_numeric(df[col], errors='coerce').round(6)  # chuyển về số, làm tròn

    # Chèn dữ liệu vào bảng SQL
    # Khi insert, dùng None cho giá trị thiếu
    for index, row in df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} (link, tieu_de, dia_chi, loai_hinh, muc_gia_ty, gia_m2_trieu, dien_tich_m2, so_phong_ngu, so_phong_tam_ve_sinh, phap_ly, noi_that, so_tang, mat_tien_m, duong_vao_m, huong_nha, huong_ban_cong, ngay_dang, phuong, quan)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['link'],
            row['tieu_de'],
            row['dia_chi'],
            row['loai_hinh'],
            row['muc_gia_ty'],
            row['gia_m2_trieu'] if pd.notnull(row['gia_m2_trieu']) else None,
            row['dien_tich_m2'] if pd.notnull(row['dien_tich_m2']) else None,
            row['so_phong_ngu'] if pd.notnull(row['so_phong_ngu']) else None,
            row['so_phong_tam_ve_sinh'] if pd.notnull(row['so_phong_tam_ve_sinh']) else None,
            row['phap_ly'],
            row['noi_that'],
            row['so_tang'] if pd.notnull(row['so_tang']) else None,
            row['mat_tien_m'] if pd.notnull(row['mat_tien_m']) else None,
            row['duong_vao_m'] if pd.notnull(row['duong_vao_m']) else None,
            row['huong_nha'],
            row['huong_ban_cong'],
            row['ngay_dang'] if pd.notnull(row['ngay_dang']) else None,
            row['phuong'],
            row['quan']
        ))
    conn.commit()
# Đóng kết nối
cursor.close()
conn.close()
print("✅ Đã gộp dữ liệu từ các file CSV vào các bảng SQL tươngứng.")
print("✅ Hoàn thành quá trình gộp dữ liệu.")