import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import difflib
import schedule
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def parse_date(date_text):
    now = datetime.now()
    date_time = now  # 기본적으로 현재 시간으로 설정

    if "시간 전" in date_text:
        try:
            hours_ago = int(date_text.replace("시간 전", "").strip())
            date_time = now - timedelta(hours=hours_ago)
        except ValueError:
            pass  # 변환 실패 시 현재 시간 유지
    elif "일 전" in date_text:
        try:
            days_ago = int(date_text.replace("일 전", "").strip())
            date_time = now - timedelta(days=days_ago)
        except ValueError:
            pass  # 변환 실패 시 현재 시간 유지
    elif "." in date_text:
        parts = date_text.split()
        date_str = parts[0]  # 날짜 부분만 추출
        if date_str.count(".") == 2:
            try:
                date_obj = datetime.strptime(date_str, "%Y.%m.%d.")
                date_time = datetime(date_obj.year, date_obj.month, date_obj.day, now.hour, now.minute)
            except ValueError:
                pass
        elif date_str.count(".") == 3:
            try:
                date_obj = datetime.strptime(date_str, "%Y.%m.%d. %H:%M")
                date_time = date_obj
            except ValueError:
                pass

    return date_time.strftime("%Y/%m/%d/%H:%M")


def fetch_and_save_news():
    today = datetime.now().strftime("%Y.%m.%d")  # 오늘 날짜
    excel_filename = f"전자담배_뉴스_{today}.xlsx"  # 엑셀 파일명

    # 검색 기간 설정 (오늘 하루)
    start_date = today
    end_date = today

    # 결과를 저장할 리스트
    news_list = []

    # 페이지 반복
    for page in range(1, 100):  # 최대 100페이지까지 수집
        url = f"https://search.naver.com/search.naver?where=news&query=전자담배&sort=1&ds={start_date}&de={end_date}&start={page*10-9}"

        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        news_items = soup.select('.news_area')

        if not news_items:  # 더 이상 뉴스가 없으면 종료
            break

        for item in news_items:
            try:
                title = item.select_one('.news_tit').text
                summary = item.select_one('.dsc_txt_wrap').text
                link = item.select_one('.news_tit')['href']
                date_text = item.select_one('.info_group').text
                date = parse_date(date_text)  # 수정된 날짜 파싱 함수 사용

                news_list.append({
                    '날짜': date,
                    '제목': title,
                    '요약내용': summary,
                    '링크': link
                })

            except Exception as e:
                print(f"Error processing item: {e}")
                continue

        time.sleep(1)  # 네이버 서버 부하 방지를 위한 딜레이

    # DataFrame 생성 및 엑셀 저장
    df = pd.DataFrame(news_list)

    # 중복 뉴스 제거 (기사 내용 비교 + 출처 고려)
    def remove_duplicate_news(df):
        unique_news = []
        used_contents = []
        used_sources = set()  # 출처를 저장할 set

        # TF-IDF 벡터화
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(df['요약내용'])

        for i, row in df.iterrows():
            content = row['요약내용']
            source = row.get('출처', 'Unknown')  # 출처 정보 추출 (가능하다면)

            # 출처가 이미 사용된 경우 스킵
            if source in used_sources:
                continue

            # 내용 유사도 검사
            is_duplicate = False
            if used_contents:
                content_vector = vectorizer.transform([content])
                similarity_scores = cosine_similarity(content_vector, tfidf_matrix)
                if max(similarity_scores[0]) > 0.8:  # 80% 이상 유사하면 중복으로 간주
                    is_duplicate = True

            if not is_duplicate:
                unique_news.append(row)
                used_contents.append(content)
                used_sources.add(source)  # 출처 추가

        return pd.DataFrame(unique_news)

    df = remove_duplicate_news(df)
    print(f"중복 제거 후 {len(df)}개의 뉴스가 남았습니다.")

    # 파일이 이미 존재하면 덮어쓰기 전에 삭제 (선택 사항)
    if os.path.exists(excel_filename):
        os.remove(excel_filename)

    df.to_excel(excel_filename, index=False)
    print(f"오늘자 뉴스가 '{excel_filename}'로 저장되었습니다.")


# 스케줄링 (선택 사항)
schedule.every().day.at("10:00").do(fetch_and_save_news)

while True:
    schedule.run_pending()
    time.sleep(60)  # 1분마다 스케줄 확인