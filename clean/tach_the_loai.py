#============================================================================================================================
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd

# doc file CSV đã làm sạch
df_clean_final = pd.read_csv(r"E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\batdongsan_clean.csv", encoding='utf-8-sig')

# tách theo loại batdongsan

# Thể loại 1: Nhà riêng
df_nha_rieng = df_clean_final[df_clean_final['loai_hinh'] == 'Nhà riêng']
df_nha_rieng.to_csv(r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\nha_rieng\nha_rieng.csv", index=False, encoding='utf-8-sig')

# Thể loại 2: Căn hộ chung cư mini, căn hộ dịch vụ và căn hộ chung cư
df_can_ho = df_clean_final[df_clean_final['loai_hinh'].isin([
    'Căn hộ chung cư mini, căn hộ dịch vụ',
    'Căn hộ chung cư'
])]
df_can_ho.to_csv(r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\can_ho\can_ho.csv", index=False, encoding='utf-8-sig')

# Thể loại 3: Đất, Đất nền dự án
df_dat = df_clean_final[df_clean_final['loai_hinh'].isin([
    'Đất',
    'Đất nền dự án'
])]
df_dat.to_csv(r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\dat\dat.csv", index=False, encoding='utf-8-sig')

# Thể loại 4: Các loại còn lại (loại trừ 3 thể loại trên)
df_con_lai = df_clean_final[~df_clean_final['loai_hinh'].isin([
    'Nhà riêng',
    'Căn hộ chung cư mini, căn hộ dịch vụ',
    'Căn hộ chung cư',
    'Đất',
    'Đất nền dự án'
])]
df_con_lai.to_csv(r"E:\DATA\DuAn\BatDongSan\n8n\lam_sach\con_lai\con_lai.csv", index=False, encoding='utf-8-sig')

print("✅ Đã tách dữ liệu thành công theo các thể loại và lưu vào các file CSV tương ứng.")
#============================================================================================================================