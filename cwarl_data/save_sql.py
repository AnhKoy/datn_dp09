import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
from sqlalchemy import create_engine

# Thông tin kết nối SQL Server
server = 'NGUYENANH'  # hoặc tên máy chủ SQL, ví dụ: 'DESKTOP-12345\SQLEXPRESS'
database = 'batdongsan'
trust = 'yes'  # nếu sử dụng xác thực Windows
#username = 'sa'       # hoặc user có quyền ghi dữ liệu
#password = 'matkhau'
driver = 'ODBC Driver 17 for SQL Server'

# Đường dẫn file CSV
csv_file_path = r'E:\\DATA\\DuAn\\BatDongSan\\n8n\\data_bds.csv'  # sửa lại đúng đường dẫn

# Đọc dữ liệu từ CSV
df = pd.read_csv(csv_file_path, encoding='utf-8-sig')

# mapping tên cột file csv sang tên cột trong sql table
# link,tieu_đe,đia_chi,muc_gia,giam²,dien_tich,so_phong_ngu,so_phong_tam_ve_sinh,phap_ly,noi_that,so_tang,mat_tien,đuong_vao,huong_nha,huong_ban_cong,ngay_đang
# cot trong sql table create table bat_dong_san (
#     id int identity(1,1) primary key,
#     link nvarchar(max),
#     tieu_de nvarchar(max),
#     dia_chi nvarchar(max),
#     muc_gia nvarchar(100),
#     gia_m2 nvarchar(100),
#     dien_tich nvarchar(100),
#     so_phong_ngu nvarchar(50),
#     so_phong_tam_ve_sinh nvarchar(50),
#     phap_ly nvarchar(100),
#     noi_that nvarchar(100),
#     so_tang nvarchar(50),
#     mat_tien nvarchar(100),
#     duong_vao nvarchar(100),
#     huong_nha nvarchar(50),
#     huong_ban_cong nvarchar(50),
#     ngay_dang date
# );

column_mapping = {
    'link': 'link',
    'tieu_đe': 'tieu_de',
    'đia_chi': 'dia_chi',
    'muc_gia': 'muc_gia',
    'giam²': 'gia_m2',
    'dien_tich': 'dien_tich',
    'so_phong_ngu': 'so_phong_ngu',
    'so_phong_tam_ve_sinh': 'so_phong_tam_ve_sinh',
    'phap_ly': 'phap_ly',
    'noi_that': 'noi_that',
    'so_tang': 'so_tang',
    'mat_tien': 'mat_tien',
    'đuong_vao': 'duong_vao',
    'huong_nha': 'huong_nha',
    'huong_ban_cong': 'huong_ban_cong',
    'ngay_đang': 'ngay_dang'
}


# doi ten cot
df.rename(columns=column_mapping, inplace=True)
# Chuyển đổi kiểu dữ liệu ngày tháng
df['ngay_dang'] = pd.to_datetime(df['ngay_dang'], errors='coerce')
# Xử lý lỗi chuyển đổi ngày tháng
df['ngay_dang'] = df['ngay_dang'].dt.strftime('%Y-%m-%d')

# Tạo kết nối tới SQL Server sử dụng Windows Authentication
conn_str = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection={trust}"
engine = create_engine(conn_str, fast_executemany=True)

# Xóa dữ liệu cũ trong bảng nếu cần
# df.to_sql('bat_dong_san', con=engine, if_exists='replace', index=False)
# Nếu không muốn xóa dữ liệu cũ, sử dụng 'append' thay vì 'replace'

# Đẩy dữ liệu vào SQL Server
df.to_sql('bat_dong_san', con=engine, if_exists='append', index=False)
# save_sql.py
print("✅ Đã đẩy dữ liệu vào SQL Server thành công.")

