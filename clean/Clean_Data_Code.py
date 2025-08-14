#============================================================================================================================
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import re
import numpy as np
import pandas as pd
import unicodedata


# -----------------------
# 0. Đọc dữ liệu đầu vào
# -----------------------
# Thay 'raw_data.xlsx' bằng đường dẫn file của bạn
input_file = r"E:\\DATA\\DuAn\\BatDongSan\\n8n\\data_bds.csv"
df = pd.read_csv(input_file)

df_clean = df.copy()

# -----------------------
# 1. Chuẩn hóa loại hình bất động sản từ URL
# -----------------------
mapping_loaihinh = {
    'ban-can-ho-chung-cu-mini': 'Căn hộ chung cư mini',
    'ban-can-ho-chung-cu': 'Căn hộ chung cư',
    'ban-nha-rieng': 'Nhà riêng',
    'ban-nha-biet-thu-lien-ke': 'Nhà biệt thự liền kề',
    'ban-nha-mat-pho': 'Nhà mặt phố',
    'ban-shophouse-nha-pho-thuong-mai': 'Shophouse nhà phố thương mại',
    'ban-dat-nen-du-an': 'Đất nền dự án',
    'ban-dat': 'Đất nền dự án',
    'ban-trang-trai-khu-nghi-duong': 'Trang trại khu nghỉ dưỡng',
    'ban-condotel': 'Condotel',
    'ban-kho-nha-xuong': 'Kho nhà xưởng',
    'ban-loai-bat-dong-san-khac': 'Loại bất động sản khác'
}

def extract_property_type(url):
    if pd.isna(url) or 'cho-thue' in url.lower():
        return np.nan
    url_lower = url.lower()
    for key, value in mapping_loaihinh.items():
        if key in url_lower:
            return value
    return np.nan

df_clean['loai_hinh'] = df_clean['link'].apply(extract_property_type)

# -----------------------
# 1.1. Sửa lại giá trị của 2 cột muc_gia_ty và gia_m2_trieu
# -----------------------
# đổi lại giá trị giữa 2 cột nếu giá/m2 không phải triệu/m²

def is_not_trieu_m2(val):
    val = str(val).lower()
    return not ('triệu/m' in val or 'trieu/m' in val)

def swap_gia(row):
    if is_not_trieu_m2(row['giam²']):
        return pd.Series({'muc_gia': row['giam²'], 'gia_m2': row['muc_gia']})
    else:
        return pd.Series({'muc_gia': row['muc_gia'], 'gia_m2': row['giam²']})

df_clean[['muc_gia', 'giam²']] = df_clean.apply(swap_gia, axis=1)

# -----------------------


# Loại bỏ các dòng cho thuê & trùng URL
df_clean = df_clean[~df_clean['link'].str.contains('cho-thue', case=False, na=False)]
df_clean = df_clean.drop_duplicates(subset='link')

# loai bo dòng có muc_gia là 'Thỏa thuận'
df_clean = df_clean[df_clean['muc_gia'] != 'Thỏa thuận']

# Loai bo  các dòng có giá trị trống ở cột giam² và muc_gia
df_clean = df_clean.dropna(subset=['giam²', 'muc_gia'])
df_clean = df_clean.dropna(subset=['tieu_đe', 'đia_chi'])

# -----------------------
# 2. Chuẩn hóa và tính toán giá, diện tích
# -----------------------
def extract_number(text):
    if pd.isna(text):
        return np.nan
    text = str(text).lower()
    num_match = re.findall(r"[\d.,]+", text)
    if not num_match:
        return np.nan
    num = num_match[0].replace('.', '').replace(',', '.')
    try:
        return float(num)
    except:
        return np.nan

df_clean['dien_tich_m2'] = df_clean['dien_tich'].apply(extract_number)

# Giá -> tỷ
def normalize_price(text):
    if pd.isna(text):
        return np.nan
    text = str(text).lower()
    value = extract_number(text)
    if 'tỷ' in text:
        return value
    elif 'triệu' in text:
        return value / 1000
    elif 'thỏa thuận' in text or '~' in text:
        return np.nan
    else:
        return value

df_clean['muc_gia_ty'] = df_clean['muc_gia'].apply(normalize_price)

# Giá/m2 -> triệu/m2
def normalize_price_per_m2(text):
    if pd.isna(text):
        return np.nan
    text = str(text).lower()
    value = extract_number(text)
    if 'tỷ' in text:
        return value * 1000
    elif 'triệu' in text:
        return value
    elif '~' in text:
        return np.nan
    else:
        return value

df_clean['gia_m2_trieu'] = df_clean['giam²'].apply(normalize_price_per_m2)

# Điền giá trị khi thiếu
for i, row in df_clean.iterrows():
    if pd.isna(row['gia_m2_trieu']) and pd.notna(row['muc_gia_ty']) and pd.notna(row['dien_tich_m2']):
        df_clean.at[i, 'gia_m2_trieu'] = row['muc_gia_ty']*1000/row['dien_tich_m2']
    if pd.isna(row['muc_gia_ty']) and pd.notna(row['gia_m2_trieu']) and pd.notna(row['dien_tich_m2']):
        df_clean.at[i, 'muc_gia_ty'] = row['gia_m2_trieu']*row['dien_tich_m2']/1000

# -----------------------
# 3. Chuyển ngày đăng về dd/mm/yyyy
# -----------------------
df_clean['ngay_dang'] = pd.to_datetime(df_clean['ngay_đang'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')

# -----------------------
# 4. Lấy Phường & Quận
# -----------------------
def extract_phuong_quan(address: str):
    if pd.isna(address) or str(address).strip() == "":
        return pd.Series([np.nan, np.nan])
    parts = [x.strip() for x in str(address).split(',') if x.strip()]
    
    phuong = np.nan
    quan = np.nan

    # Tìm phường/xã
    for p in parts:
        lower = p.lower()
        if lower.startswith('phường') or lower.startswith('xã'):
            phuong = p
            break

    # Nếu không tìm thấy, lấy phần trước quận/huyện
    if pd.isna(phuong) and len(parts) >= 3:
        phuong = parts[-3]

    # Quận là phần áp chót
    if len(parts) >= 2:
        quan = parts[-2]

    return pd.Series([phuong, quan])

df_clean[['phuong', 'quan']] = df_clean['đia_chi'].apply(extract_phuong_quan)

# -----------------------
# 5. Chuẩn hóa các cột số
# -----------------------
def extract_numeric(text):
    if pd.isna(text):
        return np.nan
    text = str(text)
    nums = re.findall(r"[\d.,]+", text)
    if not nums:
        return np.nan
    raw = nums[0].replace('.', '').replace(',', '.')
    try:
        return float(raw)
    except:
        return np.nan

cols_numeric = {
    'so_phong_ngu': 'so_phong_ngu',
    'so_phong_tam_ve_sinh': 'so_phong_tam_ve_sinh',
    'so_tang': 'so_tang',
    'mat_tien': 'mat_tien_m',
    'đuong_vao': 'duong_vao_m'
}

for old_col, new_col in cols_numeric.items():
    df_clean[new_col] = df_clean[old_col].apply(extract_numeric)

# -----------------------
# 6. Giấy tờ pháp lý
# -----------------------
def clean_legal(text):
    if pd.isna(text):
        return 'Không'
    text_lower = str(text).lower()
    if any(x in text_lower for x in ['sổ', 'giấy', 'đầy đủ', 'hồng', 'đỏ', 'sh']):
        return 'Có'
    return 'Không'

df_clean['phap_ly'] = df_clean['phap_ly'].apply(clean_legal)

# -----------------------
# 7. Nội thất
# -----------------------
def clean_interior(text):
    if pd.isna(text) or str(text).strip() == '':
        return 'Không có'
    text_lower = str(text).lower()
    if any(x in text_lower for x in ['đầy đủ', 'full', 'sang trọng']):
        return 'Đầy đủ'
    elif any(x in text_lower for x in ['cơ bản', 'basic', '1 phần', 'một phần']):
        return 'Cơ bản'
    else:
        return 'Không có'

df_clean['noi_that'] = df_clean['noi_that'].apply(clean_interior)

# -----------------------
# 8. Xuất dữ liệu cuối
# -----------------------
cleaned_cols = [
    'link', 'tieu_đe', 'đia_chi', 'loai_hinh', 'muc_gia_ty', 
    'gia_m2_trieu', 'dien_tich_m2', 'so_phong_ngu', 
    'so_phong_tam_ve_sinh', 'phap_ly', 'noi_that', 
    'so_tang', 'mat_tien_m', 'duong_vao_m', 
    'huong_nha', 'huong_ban_cong', 'ngay_dang',
    'phuong', 'quan'
]

# Chuyển đổi tên cột mapping ko dau tieng viet
# link,tieu_đe,đia_chi,muc_gia,giam²,dien_tich,so_phong_ngu,so_phong_tam_ve_sinh,phap_ly,noi_that,so_tang,mat_tien,đuong_vao,huong_nha,huong_ban_cong,ngay_đang

column_mapping = {
    'link': 'link',
    'tieu_đe': 'tieu_de',
    'đia_chi': 'dia_chi',
    'muc_gia': 'muc_gia_ty',
    'giam²': 'gia_m2_trieu',
    'dien_tich': 'dien_tich_m2',
    'so_phong_ngu': 'so_phong_ngu',
    'so_phong_tam_ve_sinh': 'so_phong_tam_ve_sinh',
    'phap_ly': 'phap_ly',
    'noi_that': 'noi_that',
    'so_tang': 'so_tang',
    'mat_tien': 'mat_tien_m',
    'duong_vao': 'duong_vao_m',
    'huong_nha': 'huong_nha',
    'huong_ban_cong': 'huong_ban_cong',
    'ngay_đang': 'ngay_dang'
}
df_final = df_clean[cleaned_cols].rename(columns=column_mapping)

# ghi ra file CSV
output_file = 'E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\batdongsan_clean.csv'
df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"✅ Đã xử lý xong. File kết quả: {output_file}")

