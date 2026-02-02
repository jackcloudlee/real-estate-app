import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

def get_naver_real_estate_news():
    # 결과를 저장할 리스트
    news_list = []
    
    # 네이버 부동산 뉴스 URL (여러 페이지 수집)
    for page in range(1, 11):  # 10페이지까지 수집
        url = f"https://news.naver.com/main/list.naver?mode=LS2D&mid=shm&sid1=101&sid2=260&page={page}"
        
        # User-Agent 설정
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 뉴스 항목 추출
        news_items = soup.select('.type06_headline li, .type06 li')
        
        for item in news_items:
            try:
                title = item.select_one('dt:not(.photo) a').text.strip()
                link = item.select_one('dt:not(.photo) a')['href']
                content = item.select_one('.lede').text.strip()
                press = item.select_one('.writing').text.strip()
                date = item.select_one('.date').text.strip()
                
                news_list.append({
                    '제목': title,
                    '링크': link,
                    '내용': content,
                    '언론사': press,
                    '날짜': date
                })
            except:
                continue
                
        # 과도한 요청 방지를 위한 딜레이
        time.sleep(1)
    
    # DataFrame 생성
    df = pd.DataFrame(news_list)
    
    # 현재 날짜시간으로 파일명 생성
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'naver_real_estate_news_{current_time}.xlsx'
    
    # Excel 파일로 저장
    df.to_excel(filename, index=False, encoding='utf-8-sig')
    print(f'뉴스 데이터가 {filename}에 저장되었습니다.')

if __name__ == "__main__":
    get_naver_real_estate_news()
    # Excel 파일로 저장 시 encoding 인자 제거
    df.to_excel(filename, index=False)
    # Excel 파일로 저장 시 encoding 인자를 제거하고 저장
    df.to_excel(filename, index=False)
    print(f'뉴스 데이터가 {filename}에 저장되었습니다.')
