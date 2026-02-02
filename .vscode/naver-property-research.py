

import time
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime

def setup_driver():
    """웹드라이버 설정"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

def search_properties(driver, location, options):
    """네이버 부동산에서 매물 검색"""
    driver.get("https://new.land.naver.com/")
    time.sleep(3)
    
    try:
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input.input_text"))
        )
        search_box.clear()
        search_box.send_keys(location)
        time.sleep(2)
        
        first_result = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.item_unit"))
        )
        first_result.click()
        time.sleep(2)
        
        return True
        
    except Exception as e:
        print(f"검색 중 오류 발생: {e}")
        return False

def scrape_property_list(driver, max_pages=3):
    """매물 목록 스크래핑"""
    property_data = []
    
    for page in range(1, max_pages + 1):
        try:
            print(f"페이지 {page} 스크래핑 중...")
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.item_list"))
            )
            time.sleep(2)
            
            items = driver.find_elements(By.CSS_SELECTOR, "div.item_inner")
            if not items:
                break
                
            for item in items:
                try:
                    data = {
                        "가격": item.find_element(By.CSS_SELECTOR, "span.price").text,
                        "정보": item.find_element(By.CSS_SELECTOR, "span.text").text,
                        "위치": item.find_element(By.CSS_SELECTOR, "span.address").text
                    }
                    property_data.append(data)
                except:
                    continue
                    
            time.sleep(2)
            
        except Exception as e:
            print(f"스크래핑 중 오류: {e}")
            break
            
    return property_data

def get_timestamp_filename(base_filename):
    """타임스탬프가 포함된 파일명 생성"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name, ext = os.path.splitext(base_filename)
    return f"{name}_{timestamp}{ext}"

def save_to_excel(data, filename):
    """데이터를 엑셀 파일로 저장"""
    try:
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False)
        print(f"\n데이터가 성공적으로 저장되었습니다: {filename}")
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    main()
