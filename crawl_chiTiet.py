#============================================================================================================================
import sys
import io

from sqlalchemy import true
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import selenium.webdriver.support.expected_conditions as EC
import undetected_chromedriver as uc
import time
import numpy as np
import pandas as pd
from urllib.parse import urlparse, parse_qs, unquote
import re
import unicodedata

#=============================================================================================================================

# Cấu hình Chrome
options = uc.ChromeOptions()
options.add_argument("--headless=new")  # KHÔNG nên bật headless khi test, hãy để mở
options.add_argument("--start-maximized")
options.add_argument("--disable-infobars")
options.add_argument("--window-size=1920,1080")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

options.add_argument("--disable-extensions")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--ignore-certificate-errors")
options.add_argument("--ignore-ssl-errors")

# sửa đúng đường dẫn file chromedriver
driver = uc.Chrome(driver_executable_path=r"E:\DATA\DuAn\BatDongSan\drivers\chromedriver-win64\chromedriver.exe",
                   options=options,
                   headless=true)

# --------------------
# HÀM LẤY THÔNG TIN CHI TIẾT TỪ TRANG BDS
import time
import pandas as pd
import numpy as np
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



def create_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")  # Chạy ẩn
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    options.page_load_strategy = 'normal'
    
    return uc.Chrome(driver_executable_path=r"E:\DATA\DuAn\BatDongSan\drivers\chromedriver-win64\chromedriver.exe",
                     options=options,
                     headless=true)

# Khởi tạo driver lần đầu
driver = create_driver()

# --------------------
# HÀM LẤY THÔNG TIN CHI TIẾT TỪ TRANG BDS
# --------------------
def extract_property_info(url):
    info = {
        'Link': url,
        'Tiêu đề': np.nan,
        'Địa chỉ': np.nan,
        #'Mô tả': np.nan,
        'Mức giá': np.nan,
        'Giá/m²': np.nan,
        'Diện tích': np.nan,
        'Số phòng ngủ': np.nan,
        'Số phòng tắm, vệ sinh': np.nan,
        'Pháp lý': np.nan,
        'Nội thất': np.nan,
        'Số tầng': np.nan,
        'Mặt tiền': np.nan,
        'Đường vào': np.nan,
        'Hướng nhà': np.nan,
        'Hướng ban công': np.nan,
        'Ngày đăng': np.nan,
        # 'Huyện': np.nan,
        # 'Latitude': np.nan,
        # 'Longitude': np.nan
    }

    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "re__pr-specs-content-item-value"))
        )

        # Tiêu đề
        try:
            info['Tiêu đề'] = driver.find_element(By.CLASS_NAME, "re__pr-title").text.strip()
        except: pass

        # Địa chỉ
        try:
            info['Địa chỉ'] = driver.find_element(By.CLASS_NAME, "re__pr-short-description").text.strip()
        except: pass

        # Mức giá + Giá/m2
        try:
            info['Mức giá'] = driver.find_element(By.CSS_SELECTOR, ".re__pr-short-info-item .value").text.strip()
            info['Giá/m²'] = driver.find_element(By.CSS_SELECTOR, ".re__pr-short-info-item .ext").text.strip()
        except: pass

        # Ngày đăng
        try:
            date_el = driver.find_element(By.XPATH, "//span[@class='title' and contains(text(),'Ngày đăng')]/following-sibling::span[@class='value']")
            info['Ngày đăng'] = date_el.text.strip()
        except: pass

        # # Breadcrumb lấy huyện
        # try:
        #     crumbs = driver.find_elements(By.CSS_SELECTOR, ".re__breadcrumb .re__link-se")
        #     huyen = next((b.text.strip() for b in crumbs if b.get_attribute("level") == "3"), np.nan)
        #     info["Huyện"] = huyen
        # except: pass

        # Mô tả
        # try:
        #     des = driver.find_element(By.CLASS_NAME, "re__pr-description").text.strip()
        #     info["Mô tả"] = des
        # except: pass

        # Tọa độ
        # try:
        #     iframe = driver.find_element(By.CSS_SELECTOR, "iframe.lazyload")
        #     data_src = iframe.get_attribute("data-src")
        #     lat, lon = data_src.split("q=")[1].split(",")
        #     info["Latitude"] = lat.strip()
        #     info["Longitude"] = lon.strip()
        # except: pass

        # Các trường trong block thông tin chi tiết
        specs = driver.find_elements(By.CLASS_NAME, "re__pr-specs-content-item")
        for spec in specs:
            try:
                title = spec.find_element(By.CLASS_NAME, "re__pr-specs-content-item-title").text.strip()
                value = spec.find_element(By.CLASS_NAME, "re__pr-specs-content-item-value").text.strip()
                if title in info:
                    info[title] = value
            except: pass

        return info
    
    except Exception as e:
        print(f"❌ Lỗi khi tải {url} - {str(e)}")
        return info


# --------------------
# ĐỌC LINK TỪ FILE
# --------------------
# Đọc danh sách URL từ file hrefs.txt
with open("E:\\DATA\\DuAn\\BatDongSan\\n8n\\hrefs.txt", "r", encoding="utf-8") as file:
    urls = file.read().splitlines()

# Loại bỏ các link trùng lặp
urls = list(dict.fromkeys(urls))


# Đường dẫn file CSV để lưu kết quả
csv_filename = "E:\\DATA\\DuAn\\BatDongSan\\n8n\\data_bds.csv"
total_urls = len(urls)

def normalize_column_name(col):
    # Loại bỏ dấu tiếng Việt
    col = unicodedata.normalize('NFD', col)
    col = ''.join([c for c in col if unicodedata.category(c) != 'Mn'])
    col = col.strip().lower()
    col = re.sub(r"[^\w\s]", "", col)  # Loại bỏ ký tự đặc biệt
    col = re.sub(r"\s+", "_", col)     # Thay khoảng trắng bằng _
    return col

def normalize_columns(df):
    df.columns = [normalize_column_name(col) for col in df.columns]
    return df

# --------------------
# GHI TỪNG DÒNG VÀO CSV
# --------------------
for index, url in enumerate(urls, start=1):
    try:
        print(f"📌 Đang xử lý link thứ {index}/{total_urls}: {url}")
        property_info = extract_property_info(url)
        df = pd.DataFrame([property_info])
        df = normalize_columns(df)  # Chuẩn hóa tên cột

        # Ghi lần đầu có header
        if index == 1:
            df.to_csv(csv_filename, mode='w', header=True, index=False, encoding='utf-8-sig')
        else:
            df.to_csv(csv_filename, mode='a', header=False, index=False, encoding='utf-8-sig')

        print(f"✅ Đã lưu thông tin vào file: {csv_filename}")
        time.sleep(1)

    except Exception as e:
        print(f"⚠️ Lỗi khi xử lý link thứ {index}: {str(e)}")
        try:
            driver.quit()
        except:
            pass
        driver = create_driver()

# --------------------
driver.quit()
print(f"\n🎉 Đã crawl xong toàn bộ {total_urls} link và lưu vào {csv_filename}")
