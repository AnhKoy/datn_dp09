import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# clear_files.py

# Danh sách các file cần xóa dữ liệu
paths = [
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\hrefs.txt",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\data_bds.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\batdongsan_clean.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\nha_rieng\\nha_rieng.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\can_ho\\can_ho.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\dat\\dat.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\con_lai\\con_lai.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\nha_rieng\\nha_rieng_cleaned.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\can_ho\\can_ho_cleaned.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\dat\\dat_cleaned.csv",
    "E:\\DATA\\DuAn\\BatDongSan\\n8n\\lam_sach\\con_lai\\con_lai_cleaned.csv"    
]
# Xóa nội dung của từng file
for path in paths:
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("")  # Ghi một chuỗi rỗng để xóa nội dung
        print(f"✅ Đã dọn sạch dữ liệu trong file: {path}")
    except Exception as e:
        print(f"❌ Lỗi khi dọn sạch file {path}: {str(e)}")

# for path in paths:
#     with open(path, "w", encoding="utf-8") as f:
#         f.write("")

# print("Đã dọn sạch dữ liệu cũ.")
