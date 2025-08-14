import pandas as pd
import pyodbc
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Kết nối SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=NGUYENANH;'
    'DATABASE=DATN_DP09_Batdongsan_final;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

files = [
    r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\nha_rieng\nha_rieng_cleaned.csv",
    r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\can_ho\can_ho_cleaned.csv",
    r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\dat\dat_cleaned.csv",
    r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\con_lai\con_lai_cleaned.csv"
]

# link,tieu_de,dia_chi,loai_hinh,muc_gia_ty,gia_m2_trieu,dien_tich_m2,so_phong_ngu,so_phong_tam_ve_sinh,phap_ly,noi_that,so_tang,mat_tien_m,duong_vao_m,huong_nha,huong_ban_cong,ngay_dang,phuong,quan

# Tạo bảng star schema nếu chưa tồn tại
table_sql = [
    # Bảng ngày
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_date' AND xtype='U')
    CREATE TABLE dim_date (
        date_id INT PRIMARY KEY,
        date DATE,
        day INT,
        month INT,
        year INT,
        quarter INT
    )
    """,
    # Bảng loại hình BĐS
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_loai_bds' AND xtype='U')
    CREATE TABLE dim_loai_bds (
        bds_id INT IDENTITY(1,1) PRIMARY KEY,
        loai_hinh NVARCHAR(100)
    )
    """,
    # Bảng địa chỉ
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_dia_chi' AND xtype='U')
    CREATE TABLE dim_dia_chi (
        dia_chi_id INT IDENTITY(1,1) PRIMARY KEY,
        dia_chi NVARCHAR(255),
        phuong NVARCHAR(100),
        quan NVARCHAR(100),
        dia_chi_p_q_tp NVARCHAR(255)
    )
    """,
    # Bảng pháp lý
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_phap_ly' AND xtype='U')
    CREATE TABLE dim_phap_ly (
        phap_ly_id INT IDENTITY(1,1) PRIMARY KEY,
        phap_ly NVARCHAR(100)
    )
    """,
    # Bảng nội thất
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_noi_that' AND xtype='U')
    CREATE TABLE dim_noi_that (
        noi_that_id INT IDENTITY(1,1) PRIMARY KEY,
        noi_that NVARCHAR(100)
    )
    """,
    # Bảng fact
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_bat_dong_san' AND xtype='U')
    CREATE TABLE fact_bat_dong_san (
        fact_id INT IDENTITY(1,1) PRIMARY KEY,
        date_id INT FOREIGN KEY REFERENCES dim_date(date_id),
        bds_id INT FOREIGN KEY REFERENCES dim_loai_bds(bds_id),
        dia_chi_id INT FOREIGN KEY REFERENCES dim_dia_chi(dia_chi_id),
        phap_ly_id INT FOREIGN KEY REFERENCES dim_phap_ly(phap_ly_id),
        noi_that_id INT FOREIGN KEY REFERENCES dim_noi_that(noi_that_id),
        link NVARCHAR(MAX),
        tieu_de NVARCHAR(MAX),
        so_tang INT,
        so_phong_ngu INT,
        so_phong_tam_ve_sinh INT,
        mat_tien_m VARCHAR(50),
        duong_vao_m VARCHAR(50),
        huong_nha NVARCHAR(50),
        huong_ban_cong NVARCHAR(50),
        muc_gia_ty VARCHAR(50),
        gia_m2_trieu VARCHAR(50),
        dien_tich_m2 NVARCHAR(50),
        phuong NVARCHAR(100),
        quan NVARCHAR(100)
    )
    """
]


for sql in table_sql:
    cursor.execute(sql)
conn.commit()

# Hàm để lấy ID từ bảng dim
def get_id_from_dim(table_name, column_name, value):
    cursor.execute(f"SELECT {column_name}_id FROM {table_name} WHERE {column_name} = ?", value)
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute(f"INSERT INTO {table_name} ({column_name}) VALUES (?)", value)
        conn.commit()
        cursor.execute("SELECT SCOPE_IDENTITY()")
        return cursor.fetchone()[0]
# Hàm để lấy hoặc tạo ID cho địa chỉ
def get_or_create_dia_chi_id(row):
    dia_chi = str(row.get('dia_chi', '')).strip().lower()
    phuong = str(row.get('phuong', '')).strip().lower()
    quan = str(row.get('quan', '')).strip().lower()
    dia_chi_p_q_tp = f"{phuong}, {quan}, hà nội"

    cursor.execute(
        "SELECT dia_chi_id FROM dim_dia_chi WHERE dia_chi = ? AND phuong = ? AND quan = ? AND dia_chi_p_q_tp = ?",
        dia_chi, phuong, quan, dia_chi_p_q_tp
    )
    row_result = cursor.fetchone()
    if row_result:
        return row_result[0]
    else:
        try:
            cursor.execute(
                "INSERT INTO dim_dia_chi (dia_chi, phuong, quan, dia_chi_p_q_tp) VALUES (?, ?, ?, ?)",
                dia_chi, phuong, quan, dia_chi_p_q_tp
            )
            conn.commit()
            cursor.execute("SELECT SCOPE_IDENTITY()")
            return cursor.fetchone()[0]
        except Exception as e:
            print(f"[LỖI] Insert dim_dia_chi thất bại: {e}")
            return None


# Hàm để lấy hoặc tạo ID cho ngày
def get_or_create_date_id(date):
    if date is None:
        return None
    cursor.execute("SELECT date_id FROM dim_date WHERE date = ?", date)
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        day = date.day
        month = date.month
        year = date.year
        quarter = (month - 1) // 3 + 1
        date_id = year * 10000 + month * 100 + day  # Generate date_id as yyyymmdd
        cursor.execute(
            "INSERT INTO dim_date (date_id, date, day, month, year, quarter) VALUES (?, ?, ?, ?, ?, ?)",
            date_id, date, day, month, year, quarter
        )
        conn.commit()
        return date_id
# Hàm để lấy hoặc tạo ID cho pháp lý
def get_or_create_phap_ly_id(phap_ly):
    cursor.execute("SELECT phap_ly_id FROM dim_phap_ly WHERE phap_ly = ?", phap_ly)
    row = cursor.fetchone()
    
    if row:
        return row[0]
    else:
        cursor.execute("INSERT INTO dim_phap_ly (phap_ly) VALUES (?)", phap_ly)
        conn.commit()
        cursor.execute("SELECT SCOPE_IDENTITY()")
        return cursor.fetchone()[0]
# Hàm để lấy hoặc tạo ID cho nội thất
def get_or_create_noi_that_id(noi_that):
    cursor.execute("SELECT noi_that_id FROM dim_noi_that WHERE noi_that = ?", noi_that)
    row = cursor.fetchone()
    
    if row:
        return row[0]
    else:
        cursor.execute("INSERT INTO dim_noi_that (noi_that) VALUES (?)", noi_that)
        conn.commit()
        cursor.execute("SELECT SCOPE_IDENTITY()")
        return cursor.fetchone()[0]
# Hàm để lấy hoặc tạo ID cho loại hình bất động sản
def get_or_create_bds_id(loai_hinh):
    cursor.execute("SELECT bds_id FROM dim_loai_bds WHERE loai_hinh = ?", loai_hinh)
    row = cursor.fetchone()
    
    if row:
        return row[0]
    else:
        cursor.execute("INSERT INTO dim_loai_bds (loai_hinh) VALUES (?)", loai_hinh)
        conn.commit()
        cursor.execute("SELECT SCOPE_IDENTITY()")
        return cursor.fetchone()[0]
# Hàm để lưu dữ liệu vào bảng fact_bat_dong_san
def save_to_fact_bat_dong_san(row):
    row['ngay_dang'] = pd.to_datetime(row['ngay_dang'], errors='coerce')
    if pd.notnull(row['ngay_dang']):
        date_value = row['ngay_dang'].date()
    else:
        date_value = None

    date_id = get_or_create_date_id(date_value)
    bds_id = get_or_create_bds_id(row['loai_hinh'])
    dia_chi_id = get_or_create_dia_chi_id(row)
    phap_ly_id = get_or_create_phap_ly_id(row['phap_ly'])
    noi_that_id = get_or_create_noi_that_id(row['noi_that'])

    # Ép kiểu các cột số sang string nếu không null
    float_fields = ['mat_tien_m', 'duong_vao_m', 'muc_gia_ty', 'gia_m2_trieu', 'dien_tich_m2']
    for field in float_fields:
        value = row.get(field, None)
        if pd.isna(value) or value == '':
            row[field] = None
        else:
            row[field] = str(round(float(value), 2))  # ép float thành string 2 chữ số

    # Ép luôn các cột nullable khác thành None nếu thiếu
    nullable_fields = ['so_tang', 'so_phong_ngu', 'so_phong_tam_ve_sinh', 'huong_nha', 'huong_ban_cong', 'phuong', 'quan']
    for field in nullable_fields:
        if pd.isna(row.get(field, None)) or row[field] == '':
            row[field] = None

    cursor.execute("""
        INSERT INTO fact_bat_dong_san (
            date_id, bds_id, dia_chi_id, phap_ly_id, noi_that_id, link, tieu_de, so_tang, 
            so_phong_ngu, so_phong_tam_ve_sinh, mat_tien_m, duong_vao_m, huong_nha, 
            huong_ban_cong, muc_gia_ty, gia_m2_trieu, dien_tich_m2, phuong, quan
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        date_id, bds_id, dia_chi_id, phap_ly_id, noi_that_id,
        row['link'], row['tieu_de'], row['so_tang'],
        row['so_phong_ngu'], row['so_phong_tam_ve_sinh'], row['mat_tien_m'],
        row['duong_vao_m'], row['huong_nha'], row['huong_ban_cong'],
        row['muc_gia_ty'], row['gia_m2_trieu'], row['dien_tich_m2'],
        row['phuong'], row['quan']
    ))

    conn.commit()

# Đọc và lưu dữ liệu từ các file CSV
for file in files:
    df = pd.read_csv(file, parse_dates=['ngay_dang'], dayfirst=True)
    for index, row in df.iterrows():
        save_to_fact_bat_dong_san(row)
# Đóng kết nối
cursor.close()
conn.close()
# In thông báo hoàn thành
print("Dữ liệu đã được lưu vào cơ sở dữ liệu SQL Server thành công.")