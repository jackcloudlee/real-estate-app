import time
import pandas as pd
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
    # chrome_options.add_argument("--headless")  # 필요시 헤드리스 모드 활성화
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")  # 브라우저 최대화
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

def search_properties(driver, location, options):
    """네이버 부동산에서 상세 옵션으로 매물 검색"""
    driver.get("https://new.land.naver.com/")
    time.sleep(3)  # 페이지 로딩 대기
    
    # 검색창 찾기 및 지역 검색어 입력
    search_box = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input.input_text"))
    )
    search_box.clear()
    search_box.send_keys(location)
    time.sleep(2)
    
    # 검색 결과에서 첫 번째 항목 선택 (드롭다운)
    try:
        first_result = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.item_unit"))
        )
        first_result.click()
        time.sleep(2)
    except TimeoutException:
        print("검색 결과가 없습니다. 다른 지역명을 입력해보세요.")
        return False
    
    # 거래 유형 선택 (매매/전세/월세)
    try:
        deal_type = options.get('deal_type', '매매')
        deal_type_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[contains(text(), '{deal_type}')]"))
        )
        deal_type_btn.click()
        time.sleep(2)
    except Exception as e:
        print(f"거래 유형 선택 중 오류: {e}")
    
    # 상세 필터 클릭
    try:
        filter_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '필터')]"))
        )
        filter_btn.click()
        time.sleep(2)
    except Exception as e:
        print(f"필터 버튼 클릭 중 오류: {e}")
        return False
    
    # 매물 종류 카테고리 선택
    try:
        property_category = options.get('property_category', '아파트,오피스텔')
        category_tabs = driver.find_elements(By.CSS_SELECTOR, "div.panel_wrap > div.tab_wrap button")
        
        # 카테고리 탭 이름 목록
        category_names = ["아파트,오피스텔", "빌라,주택", "원룸,투룸", "상가,업무,공장,토지"]
        
        # 카테고리 찾기 및 클릭
        if property_category in category_names:
            index = category_names.index(property_category)
            if index < len(category_tabs):
                category_tabs[index].click()
                time.sleep(1)
        
        # 세부 유형 선택
        property_subcategory = options.get('property_subcategory', None)
        if property_subcategory:
            subcategory_items = driver.find_elements(By.CSS_SELECTOR, "div.item_inner")
            for item in subcategory_items:
                try:
                    label = item.find_element(By.CSS_SELECTOR, "span.text").text
                    if property_subcategory in label:
                        checkbox = item.find_element(By.CSS_SELECTOR, "input[type='checkbox']")
                        driver.execute_script("arguments[0].click();", checkbox)
                        time.sleep(0.5)
                except Exception as e:
                    continue
    except Exception as e:
        print(f"매물 종류 선택 중 오류: {e}")
    
    # 방 갯수 선택
    try:
        room_count = options.get('room_count', None)
        if room_count:
            # 방 갯수 섹션 찾기
            room_section = driver.find_element(By.XPATH, "//div[contains(., '방수')]")
            room_buttons = room_section.find_elements(By.CSS_SELECTOR, "button")
            
            # 방 갯수 버튼 클릭
            for button in room_buttons:
                if room_count in button.text:
                    button.click()
                    time.sleep(0.5)
                    break
    except Exception as e:
        print(f"방 갯수 선택 중 오류: {e}")
    
    # 필터 적용 버튼 클릭
    try:
        apply_btn = driver.find_element(By.XPATH, "//button[contains(text(), '적용')]")
        apply_btn.click()
        time.sleep(3)
    except Exception as e:
        print(f"필터 적용 중 오류: {e}")
        return False
    
    return True

def scrape_property_list(driver, max_pages=5):
    """매물 목록 스크래핑 (여러 페이지)"""
    all_property_data = []
    current_page = 1
    
    while current_page <= max_pages:
        # 현재 페이지 매물 스크래핑
        print(f"페이지 {current_page} 스크래핑 중...")
        
        # 매물 목록 로딩 대기
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.item_list"))
            )
            time.sleep(2)  # 추가 대기 시간
        except TimeoutException:
            print("매물 목록을 찾을 수 없습니다.")
            break
        
        # 매물 아이템 가져오기
        try:
            property_items = driver.find_elements(By.CSS_SELECTOR, "div.item_inner")
            
            if not property_items:
                print("이 페이지에 매물이 없습니다.")
                break
                
            for item in property_items:
                try:
                    # 기본 정보 추출
                    data = {}
                    
                    # 가격
                    try:
                        price_elem = item.find_element(By.CSS_SELECTOR, "span.price")
                        data["가격"] = price_elem.text
                    except:
                        data["가격"] = "정보 없음"
                    
                    # 기본 정보 (면적 등)
                    try:
                        info_elem = item.find_element(By.CSS_SELECTOR, "span.info")
                        data["정보"] = info_elem.text
                    except:
                        data["정보"] = "정보 없음"
                    
                    # 주소
                    try:
                        address_elem = item.find_element(By.CSS_SELECTOR, "span.text")
                        data["주소"] = address_elem.text
                    except:
                        data["주소"] = "정보 없음"
                    
                    # 상세 정보
                    try:
                        detail_elem = item.find_element(By.CSS_SELECTOR, "span.spec")
                        data["상세정보"] = detail_elem.text
                    except:
                        data["상세정보"] = "정보 없음"
                    
                    # 중개사 정보
                    try:
                        agent_elem = item.find_element(By.CSS_SELECTOR, "div.agent_info")
                        data["중개사"] = agent_elem.text
                    except:
                        data["중개사"] = "정보 없음"
                    
                    all_property_data.append(data)
                    
                except Exception as e:
                    print(f"매물 정보 추출 중 오류: {e}")
            
            # 다음 페이지 이동
            try:
                pagination = driver.find_element(By.CSS_SELECTOR, "div.pagination")
                next_page_btn = pagination.find_element(By.XPATH, f"//a[text()='{current_page + 1}']")
                current_page += 1
                next_page_btn.click()
                time.sleep(3)  # 페이지 로딩 대기
            except NoSuchElementException:
                print("더 이상 페이지가 없습니다.")
                break
                
        except Exception as e:
            print(f"페이지 스크래핑 중 오류 발생: {e}")
            break
    
    return all_property_data

def save_to_excel(data, filename="naver_real_estate.xlsx"):
    """수집한 데이터를 엑셀 파일로 저장"""
    if data:
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False, encoding="utf-8-sig")  # 한글 인코딩 지원
        print(f"데이터가 {filename}에 저장되었습니다. 총 {len(data)}개의 매물 정보가 저장되었습니다.")
    else:
        print("저장할 데이터가 없습니다.")

def get_timestamp_filename(base_filename):
    """현재 시간을 포함한 파일명 생성"""
    # 현재 시간 가져오기
    now = datetime.now()
    # 년-월-일-시간 형식으로 포맷팅
    timestamp = now.strftime("%Y-%m-%d-%H%M")
    
    # 파일명에 시간 정보 추가
    if base_filename.endswith('.xlsx'):
        base_name = base_filename[:-5]  # .xlsx 확장자 제거
        return f"{timestamp}_{base_name}.xlsx"
    else:
        return f"{timestamp}_{base_filename}.xlsx"

def main():
    # 사용자 입력 받기
    location = input("검색할 지역명을 입력하세요 (예: 강남구 역삼동): ")
    
    # 거래 유형 선택
    print("\n거래 유형을 선택하세요:")
    print("1. 매매")
    print("2. 전세")
    print("3. 월세")
    deal_type_input = input("번호 선택 (기본: 1): ") or "1"
    deal_types = ["매매", "전세", "월세"]
    deal_type = deal_types[int(deal_type_input) - 1] if deal_type_input.isdigit() and 1 <= int(deal_type_input) <= 3 else "매매"
    
    # 매물 종류 카테고리 선택
    print("\n매물 종류 카테고리를 선택하세요:")
    print("1. 아파트,오피스텔")
    print("2. 빌라,주택")
    print("3. 원룸,투룸")
    print("4. 상가,업무,공장,토지")
    category_input = input("번호 선택 (기본: 1): ") or "1"
    categories = ["아파트,오피스텔", "빌라,주택", "원룸,투룸", "상가,업무,공장,토지"]
    property_category = categories[int(category_input) - 1] if category_input.isdigit() and 1 <= int(category_input) <= 4 else "아파트,오피스텔"
    
    # 매물 세부 유형 선택
    property_subcategory = None
    if property_category == "아파트,오피스텔":
        print("\n세부 유형을 선택하세요:")
        print("1. 아파트")
        print("2. 아파트분양권")
        print("3. 재건축")
        print("4. 오피스텔")
        print("5. 오피스텔분양권")
        print("6. 재개발")
        subcategory_input = input("번호 선택 (기본: 1): ") or "1"
        subcategories = ["아파트", "아파트분양권", "재건축", "오피스텔", "오피스텔분양권", "재개발"]
        property_subcategory = subcategories[int(subcategory_input) - 1] if subcategory_input.isdigit() and 1 <= int(subcategory_input) <= 6 else "아파트"
    
    elif property_category == "빌라,주택":
        print("\n세부 유형을 선택하세요:")
        print("1. 빌라/연립")
        print("2. 단독/다가구")
        print("3. 전원주택")
        print("4. 상가주택")
        print("5. 한옥주택")
        subcategory_input = input("번호 선택 (기본: 1): ") or "1"
        subcategories = ["빌라/연립", "단독/다가구", "전원주택", "상가주택", "한옥주택"]
        property_subcategory = subcategories[int(subcategory_input) - 1] if subcategory_input.isdigit() and 1 <= int(subcategory_input) <= 5 else "빌라/연립"
        
    elif property_category == "원룸,투룸":
        print("\n세부 유형을 선택하세요:")
        print("1. 원룸")
        print("2. 투룸")
        subcategory_input = input("번호 선택 (기본: 1): ") or "1"
        subcategories = ["원룸", "투룸"]
        property_subcategory = subcategories[int(subcategory_input) - 1] if subcategory_input.isdigit() and 1 <= int(subcategory_input) <= 2 else "원룸"
        
    elif property_category == "상가,업무,공장,토지":
        print("\n세부 유형을 선택하세요:")
        print("1. 상가")
        print("2. 사무실")
        print("3. 공장/창고")
        print("4. 지식산업센터")
        print("5. 건물")
        print("6. 토지")
        subcategory_input = input("번호 선택 (기본: 1): ") or "1"
        subcategories = ["상가", "사무실", "공장/창고", "지식산업센터", "건물", "토지"]
        property_subcategory = subcategories[int(subcategory_input) - 1] if subcategory_input.isdigit() and 1 <= int(subcategory_input) <= 6 else "상가"
    
    # 방 갯수 선택 (원룸,투룸이나 아파트 등에 적용)
    room_count = None
    if property_category in ["아파트,오피스텔", "빌라,주택", "원룸,투룸"]:
        print("\n방 갯수를 선택하세요:")
        print("1. 1개")
        print("2. 2개")
        print("3. 3개")
        print("4. 4개")
        print("5. 5개 이상")
        print("0. 선택 안함")
        room_input = input("번호 선택 (기본: 0): ") or "0"
        if room_input.isdigit() and 1 <= int(room_input) <= 5:
            room_counts = ["1개", "2개", "3개", "4개", "5개 이상"]
            room_count = room_counts[int(room_input) - 1]
    
    # 검색 옵션 설정
    search_options = {
        'deal_type': deal_type,
        'property_category': property_category,
        'property_subcategory': property_subcategory,
        'room_count': room_count
    }
    
    # 최대 페이지 수 설정
    max_pages = int(input("\n최대 몇 페이지까지 스크래핑할까요? (기본: 3): ") or "3")
    
    # 출력 파일명 설정
    base_filename = input("\n저장할 엑셀 파일명을 입력하세요 (기본: naver_real_estate.xlsx): ") or "naver_real_estate.xlsx"
    if not base_filename.endswith('.xlsx'):
        base_filename += '.xlsx'
    
    # 시간 정보를 포함한 파일명 생성
    timestamp_filename = get_timestamp_filename(base_filename)
    
    # 브라우저 실행 및 스크래핑
    print("\n브라우저를 실행하고 검색을 시작합니다...")
    driver = setup_driver()
    
    try:
        # 검색 실행
        search_success = search_properties(driver, location, search_options)
        
        if search_success:
            # 매물 데이터 스크래핑
            property_data = scrape_property_list(driver, max_pages)
            
            # 엑셀로 저장 (시간 정보가 포함된 파일명 사용)
            save_to_excel(property_data, timestamp_filename)
        else:
            print("검색에 실패했습니다. 다시 시도해주세요.")
    
    except Exception as e:
        print(f"오류 발생: {e}")
    
    finally:
        print("\n브라우저를 종료합니다...")
        driver.quit()
        
    print("\n프로그램을 종료합니다.")

if __name__ == "__main__":
    main()