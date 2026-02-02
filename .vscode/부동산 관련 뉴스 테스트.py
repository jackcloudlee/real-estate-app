import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import difflib
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def parse_date(date_text):
    now = datetime.now()
    date_time = now

    if "시간 전" in date_text:
        try:
            hours_ago = int(date_text.replace("시간 전", "").strip())
            date_time = now - timedelta(hours=hours_ago)
        except ValueError:
            pass
    elif "일 전" in date_text:
        try:
            days_ago = int(date_text.replace("일 전", "").strip())
            date_time = now - timedelta(days=days_ago)
        except ValueError:
            pass
    elif "." in date_text:
        parts = date_text.split()
        date_str = parts[0]
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


def fetch_and_save_news(keywords=["부동산", "아파트", "주택", "전세", "매매"]):
    today = datetime.now().strftime("%Y.%m.%d")
    excel_filename = f"부동산관련뉴스_{today}.xlsx"

    start_date = today
    end_date = today

    news_list = []

    for keyword in keywords:
        for page in range(1, 20):  # 페이지 수 조절
            url = f"https://search.naver.com/search.naver?where=news&query={keyword}&sort=1&ds={start_date}&de={end_date}&start={page*10-9}&nso=so%3Ar"  # 경제 카테고리 필터링 추가

            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            news_items = soup.select('.news_area')

            if not news_items:
                break

            for item in news_items:
                try:
                    title = item.select_one('.news_tit').text
                    summary = item.select_one('.dsc_txt_wrap').text
                    link = item.select_one('.news_tit')['href']
                    date_text = item.select_one('.info_group').text
                    date = parse_date(date_text)

                    news_list.append({
                        '날짜': date,
                        '키워드': keyword,
                        '제목': title,
                        '요약내용': summary,
                        '링크': link
                    })

                except Exception as e:
                    print(f"Error processing item: {e}")
                    continue

            time.sleep(1)

    df = pd.DataFrame(news_list)

    def remove_duplicate_news(df):
        unique_news = []
        used_contents = []
        used_titles = set()
        used_sources = set()

        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(df['요약내용'])

        for i, row in df.iterrows():
            content = row['요약내용']
            title = row['제목']
            source = row.get('출처', 'Unknown')

            if source in used_sources or title in used_titles:
                continue

            is_duplicate = False
            if used_contents:
                content_vector = vectorizer.transform([content])
                similarity_scores = cosine_similarity(content_vector, tfidf_matrix)
                if max(similarity_scores[0]) > 0.8:
                    is_duplicate = True

            if not is_duplicate:
                unique_news.append(row)
                used_contents.append(content)
                used_titles.add(title)
                used_sources.add(source)

        return pd.DataFrame(unique_news)

    df = remove_duplicate_news(df)
    print(f"중복 제거 후 {len(df)}개의 뉴스가 남았습니다.")

    if os.path.exists(excel_filename):
        os.remove(excel_filename)

    df.to_excel(excel_filename, index=False)
    print(f"오늘자 부동산 관련 뉴스가 '{excel_filename}'로 저장되었습니다.")


# 직접 실행
fetch_and_save_news()

