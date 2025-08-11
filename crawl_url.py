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

#=============================================================================================================================

# Cấu hình Chrome
options = uc.ChromeOptions()
options.add_argument("--headless")  # KHÔNG nên bật headless khi test, hãy để mở
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

def get_hrefs_from_page(url):
    driver.get(url)
    print("Page title:", driver.title)
    print("HTML length:", len(driver.page_source))
    
    try:
        # Đợi thẻ <a> chứa class và data-product-id xuất hiện
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.js__product-link-for-product-id[data-product-id]"))
        )
        elements = driver.find_elements(By.CSS_SELECTOR, "a.js__product-link-for-product-id[data-product-id]")

        hrefs = []
        for el in elements:
            href = el.get_attribute("href")
            if href:
                # Ghép domain nếu là đường dẫn tương đối
                if not href.startswith("http"):
                    href = "https://batdongsan.com.vn" + href
                hrefs.append(href)

        return hrefs

    except Exception as e:
        print(f"❌ Không tìm thấy sản phẩm trên trang: {url} - Lỗi: {e}")
        return []

# Bắt đầu crawl từng trang và lưu từng trang ngay vào file
base_url = "https://batdongsan.com.vn/nha-dat-ban-ha-noi"
page = 1
total_count = 0

output_path = r"E:\DATA\DuAn\BatDongSan\n8n\hrefs.txt"

with open(output_path, "w", encoding="utf-8") as file:
    while page <= 30:
        url = f"{base_url}/p{page}" if page > 1 else base_url
        print(f"\n📄 Đang xử lý trang {page}: {url}")

        hrefs = get_hrefs_from_page(url)
        if not hrefs:
            print("⛔ Không tìm thấy href nào. Kết thúc.")
            break

        for href in hrefs:
            file.write(href + "\n")
        total_count += len(hrefs)

        print(f"✅ Lưu {len(hrefs)} href từ trang {page} vào file.")

        # Kiểm tra còn nút trang không (tức là còn next page không)
        next_page = driver.find_elements(By.CSS_SELECTOR, 'a.re__pagination-icon:not(.re__pagination-icon--no-effect)')
        if not next_page:
            print("✅ Hết trang.")
            break

        page += 1
        time.sleep(2)

print(f"\n🎉 Tổng cộng đã lưu: {total_count} href vào file hrefs.txt")
driver.quit()
