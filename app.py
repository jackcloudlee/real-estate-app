import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.express as px
import law_code_helper
from urllib.parse import unquote
import io
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

# 페이지 설정
st.set_page_config(page_title="부동산 실거래가 조회", layout="wide")

st.title("🏡 부동산 실거래가 조회 도구")
st.markdown("경매 분석을 위한 아파트/연립다세대 실거래가 1년치 조회")

# 세션 상태 초기화 (최상단)
if 'raw_df' not in st.session_state:
    st.session_state['raw_df'] = pd.DataFrame()
if 'search_params_display' not in st.session_state:
    st.session_state['search_params_display'] = {}

# 사이드바 설정
with st.sidebar:
    st.header("설정 (Settings)")
    
    # 1. API 키 입력
    api_key = st.text_input("공공데이터포털 API Key (Decoding)", type="password", help="공공데이터포털에서 발급받은 '일반 인증키 (Decoding)'을 입력하세요.")
    
    # 2. 건물 유형 선택
    property_type = st.radio("건물 유형", ["아파트", "연립다세대"], index=0)
    
    # 3. 지역 선택 (시/도 -> 구/군 분리)
    sido_list = law_code_helper.get_sido_list()
    # 서울특별시 기본 선택 (없으면 0번)
    default_sido_index = 0
    if "서울특별시" in sido_list:
        default_sido_index = sido_list.index("서울특별시")
    selected_sido = st.selectbox("시/도 선택", sido_list, index=default_sido_index)
    
    gugun_list = law_code_helper.get_gugun_list(selected_sido)
    selected_gugun = st.selectbox("구/군 선택", gugun_list)
    
    # 4. 기준 날짜 선택
    ref_date = st.date_input("기준 날짜", datetime.now())
    
    st.info("선택한 날짜로부터 과거 1년치 데이터를 조회합니다.")
    
    search_btn = st.button("실거래가 조회 시작")

# 데이터 가져오는 함수 (캐싱 적용)
@st.cache_data(show_spinner=False)
def fetch_transaction_data(api_key, property_type, region_code, start_date, end_date):
    # API 설정 (HTTPS로 변경)
    if property_type == "아파트":
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    else:  # 연립다세대
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade"

    # 기간 설정 (월 단위 반복)
    months = []
    curr = start_date
    while curr <= end_date:
        months.append(curr.strftime("%Y%m"))
        curr += relativedelta(months=1)
    
    all_data = []
    
    # 캐시 함수 내에서는 UI 업데이트(progress)가 매번 실행되지 않을 수 있음
    
    total_months = len(months)
    
    # API Key 공백 제거
    api_key = api_key.strip()
    decoded_key = unquote(api_key)

    # 브라우저 헤더
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for idx, month_str in enumerate(months):
        # 파라미터 구성 (수동 URL)
        base_query = f"?serviceKey={api_key}&LAWD_CD={region_code}&DEAL_YMD={month_str}&numOfRows=1000&pageNo=1"
        full_url = url + base_query
        
        try:
            response = requests.get(full_url, headers=headers, timeout=10, verify=False)
            
            if response.status_code == 200:
                try:
                    root = ET.fromstring(response.content)
                    items = root.findall('.//item')
                    for item in items:
                        data = {}
                        for child in item:
                            data[child.tag] = child.text
                        all_data.append(data)
                except:
                     pass
        except:
            pass
            
    return pd.DataFrame(all_data)


# 조회 버튼 로직 (사이드바 정의 이후 실행)
if search_btn:
    if not api_key:
        st.warning("API Key를 입력해주세요.")
    else:
        # 지역 코드 가져오기
        region_code = law_code_helper.get_region_code(selected_sido, selected_gugun)
        
        # 날짜 계산 (1년 전 ~ 기준 날짜)
        end_date_obj = ref_date
        start_date_obj = end_date_obj - relativedelta(years=1)
        
        # 검색 조건 저장 (화면 표시용)
        st.session_state['search_params_display'] = {
            'period': f"{start_date_obj.strftime('%Y년 %m월')} ~ {end_date_obj.strftime('%Y년 %m월')}",
            'sido': selected_sido,
            'gugun': selected_gugun,
            'type': property_type
        }
        
        with st.spinner('데이터를 불러오고 분석 중입니다... (API 응답 대기)'):
            # 데이터 가져오기
            df = fetch_transaction_data(api_key, property_type, region_code, start_date_obj, end_date_obj)
            
            # 데이터 전처리 (fetch 후 바로 수행)
            if not df.empty:
                # 금액 컬럼 숫자 변환 (콤마 제거)
                if 'dealAmount' in df.columns:
                    df['거래금액_만'] = df['dealAmount'].astype(str).str.replace(',', '').astype(int)
                
                # 날짜 컬럼 생성 (년/월/일 합치기)
                if 'dealYear' in df.columns and 'dealMonth' in df.columns and 'dealDay' in df.columns:
                    df['계약일'] = pd.to_datetime(df['dealYear'].astype(str) + '-' + 
                                               df['dealMonth'].astype(str).str.zfill(2) + '-' + 
                                               df['dealDay'].astype(str).str.zfill(2))
                
                # 층수 컬럼 숫자 변환 (존재하는 경우)
                if 'floor' in df.columns:
                    df['floor'] = pd.to_numeric(df['floor'], errors='coerce')
            
            # 세션 상태에 저장
            st.session_state['raw_df'] = df
            
            # UI 갱신을 위해 리런
            st.rerun()
            
# ---------------------------------------------------------
# 데이터 표시 로직 (세션 상태 기반)
# ---------------------------------------------------------

if not st.session_state['raw_df'].empty:
    df = st.session_state['raw_df']
    display_params = st.session_state['search_params_display']
    
    st.write(f"📅 조회 기간: {display_params.get('period', '-')}")
    
    # -----------------------------------------------------
    # 사이드바 동 필터 구현
    # -----------------------------------------------------
    # -----------------------------------------------------
    # 메인 화면 동 필터 구현
    # -----------------------------------------------------
    st.markdown("---")
    
    # 동 이름 컬럼 확인
    dong_col = 'umdNm' if 'umdNm' in df.columns else 'dong'
    
    selected_dongs = []
    if dong_col in df.columns:
        # 동 리스트 추출 및 정렬
        dong_list = sorted(df[dong_col].dropna().unique())
        
        # 메인 화면 상단에 동 선택 배치
        st.subheader("📍 상세 지역 선택 (동 단위 필터)")
        selected_dongs = st.multiselect(
            f"{display_params.get('gugun')} 내 법정동 선택 (비워두면 전체 조회)", 
            dong_list,
            default=[]
        )
    
    # -----------------------------------------------------
    # 데이터 필터링 (동 선택 적용)
    # -----------------------------------------------------
    if selected_dongs:
        df_display = df[df[dong_col].isin(selected_dongs)].copy()
    else:
        df_display = df.copy()
        
    # -----------------------------------------------------
    # 번지(본번) 필터 구현 (추가 요청사항)
    # -----------------------------------------------------
    if 'jibun' in df_display.columns:
        # 본번 추출 (예: '24-12' -> '24')
        # jibun 컬럼을 문자열로 변환 후 '-' 기준으로 분리하여 첫 번째 요소 추출
        df_display['bonbun'] = df_display['jibun'].astype(str).str.split('-').str[0]
        
        # 필터링된 데이터(동 선택 후) 기준으로 본번 리스트 생성
        # 숫자로 변환 가능한 경우는 숫자로 정렬, 그 외는 문자로 정렬하여 보기 좋게 만듦
        unique_bonbuns = df_display['bonbun'].dropna().unique()
        
        # 정렬 로직: 숫자인 경우 숫자 크기대로, 아니면 문자열 순서대로
        try:
             bonbun_list = sorted(unique_bonbuns, key=lambda x: int(x) if x.isdigit() else float('inf'))
        except:
             bonbun_list = sorted(unique_bonbuns)
             
        # 번지 선택 위젯 (동 선택 바로 아래 배치)
        if len(bonbun_list) > 0:
            st.subheader("📍 번지(본번) 선택")
            selected_bonbuns = st.multiselect(
                "앞번지(본번) 선택 (비워두면 전체 조회)",
                bonbun_list,
                default=[]
            )
            
            # 본번 필터링 적용
            if selected_bonbuns:
                df_display = df_display[df_display['bonbun'].isin(selected_bonbuns)].copy()
        
    # 결과 화면 출력
    if df_display.empty:
        st.warning("선택한 조건에 맞는 데이터가 없습니다.")
    else:
        # 1. 요약 통계
        st.success(f"✅ 총 {len(df_display)}건의 거래 내역을 찾았습니다.")
        
        # 면적단가 계산 (통계 표시용)
        if 'excluUseAr' in df_display.columns and '면적단가' not in df_display.columns:
            df_display['전용면적'] = pd.to_numeric(df_display['excluUseAr'], errors='coerce')
            df_display['면적단가'] = df_display['거래금액_만'] / df_display['전용면적']
            
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("평균 거래가", f"{df_display['거래금액_만'].mean():,.0f} 만원")
        col2.metric("최고 거래가", f"{df_display['거래금액_만'].max():,.0f} 만원")
        col3.metric("최저 거래가", f"{df_display['거래금액_만'].min():,.0f} 만원")
        
        if '면적단가' in df_display.columns:
             col4.metric("평균 면적단가", f"{df_display['면적단가'].mean():,.0f} 만원/㎡")
        else:
             col4.metric("평균 면적단가", "-")
        
        # 2. 차트 시각화
        # 월별 평균 거래가 추이
        df_monthly = df_display.groupby(df_display['계약일'].dt.to_period('M'))['거래금액_만'].mean().reset_index()
        df_monthly['계약일'] = df_monthly['계약일'].astype(str)
    
        # 호버 데이터에 사용할 컬럼 확인
        hover_cols = ['거래금액_만']
        if 'umdNm' in df_display.columns: hover_cols.append('umdNm') # 법정동 (연립)
        elif 'dong' in df_display.columns: hover_cols.append('dong') # 법정동 (아파트)
        
        if 'floor' in df_display.columns: hover_cols.append('floor')
        if 'excluUseAr' in df_display.columns: hover_cols.append('excluUseAr')
        
        fig = px.line(df_display, x='계약일', y='거래금액_만', 
                        title=f"{display_params.get('sido')} {display_params.get('gugun')} {display_params.get('type')} 실거래가 추이 (1년)",
                        hover_data=hover_cols)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 3. 데이터 테이블
        st.subheader("상세 거래 내역")
        
        # 컬럼명 한글화 (보기 좋게)
        rename_dict = {
            'dong': '동',
            'umdNm': '동',
            'apartmentName': '단지명',
            'mhouseNm': '단지명', 
            'muleonNm': '단지명',
            'excluUseAr': '전용면적(㎡)',
            'floor': '층',
            'buildYear': '건축년도',
            'jibun': '지번',
            'roadName': '도로명'
        }
        # 실제 존재하는 컬럼만 rename
        # df_display는 원본의 subset이므로 원본 컬럼명 유지 상태에서 rename 필요할 때만 적용
        # 화면 표시용 복사본 생성
        df_table = df_display.copy()
        df_table.rename(columns=rename_dict, inplace=True)
        
        # 주요 컬럼 위주로 보여주기
        # rename된 컬럼명 기준으로 선택
        main_cols = ['계약일', '동', '단지명', '거래금액_만', '전용면적(㎡)', '층', '건축년도']
        final_cols = [c for c in main_cols if c in df_table.columns]
        
        st.dataframe(df_table[final_cols].sort_values(by='계약일', ascending=False), use_container_width=True)

        # 엑셀 다운로드 버튼 (사용자 요청 포맷 적용)
        
        # 1. 엑셀용 데이터프레임 생성
        df_excel = df_table.copy() # 이미 rename된 df_table 사용
        
        # 2. 필요한 컬럼 생성 및 계산
        df_excel['NO'] = range(1, len(df_excel) + 1)
        df_excel['시군구'] = f"{display_params.get('sido')} {display_params.get('gugun')} " + df_excel['동'].astype(str)
        # 지번 컬럼이 있는지 확인
        if '지번' in df_excel.columns:
            df_excel['번지'] = df_excel['지번']
        else:
            df_excel['번지'] = ""

        if '단지명' in df_excel.columns:
            df_excel['건물명'] = df_excel['단지명']
        else:
            df_excel['건물명'] = ""

        # 전용면적은 이미 '전용면적(㎡)'로 존재
        df_excel['대지권면적(㎡)'] = "" # API에서 제공하지 않음
        
        # 계약일 포맷팅
        if pd.api.types.is_datetime64_any_dtype(df_excel['계약일']):
             df_excel['계약년월'] = df_excel['계약일'].dt.strftime('%Y%m')
        else:
             # 만약 문자열이라면 변환 시도
             try:
                 df_excel['계약년월'] = pd.to_datetime(df_excel['계약일']).dt.strftime('%Y%m')
             except:
                 df_excel['계약년월'] = df_excel['계약일']

        
        # 거래금액 (만원 단위 -> 원 단위 변환)
        df_excel['거래금액'] = df_excel['거래금액_만'] * 10000
        
        # 면적단가 (원 / 전용면적)
        # 전용면적이 문자열일 수 있으므로 숫자로 변환
        if '전용면적(㎡)' in df_excel.columns:
            df_excel['전용면적(㎡)'] = pd.to_numeric(df_excel['전용면적(㎡)'], errors='coerce').fillna(0)
            df_excel['면적단가'] = (df_excel['거래금액'] / df_excel['전용면적(㎡)']).round(0)
        else:
            df_excel['면적단가'] = 0
        
        # 컬럼 순서 지정
        target_cols = [
            'NO', '시군구', '번지', '건물명', 
            '전용면적(㎡)', '계약년월', 
            '거래금액', '면적단가', 
            '층', '건축년도'
        ]
        
        # 존재하는 컬럼만 선택 (방어 코드)
        final_excel_cols = [c for c in target_cols if c in df_excel.columns]
        
        # 메모리에 엑셀 파일 생성
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_excel[final_excel_cols].to_excel(writer, index=False, sheet_name='실거래가')
            
            # 워크시트 객체 가져오기
            ws = writer.sheets['실거래가']
            
            # 1. 모든 셀 가운데 정렬
            # 2. 숫자 포맷 적용 (거래금액, 면적단가)
            # 3. 오토 필터 적용
            
            # 헤더에 오토필터 적용
            ws.auto_filter.ref = ws.dimensions
            
            # 컬럼별 스타일 적용
            for col_idx, column_cell in enumerate(ws.columns, 1):
                # 컬럼 너비 자동 조절을 위한 최대 길이 계산
                max_length = 0
                col_letter = get_column_letter(col_idx)
                
                # 헤더 셀 스타일 (가운데 정렬)
                column_cell[0].alignment = Alignment(horizontal='center', vertical='center')
                header_val = column_cell[0].value
                if header_val:
                    max_length = len(str(header_val))
                
                # 데이터 셀 스타일
                for cell in column_cell[1:]:
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    
                    # 값의 길이 측정
                    if cell.value:
                        try:
                            # 한글의 경우 길이가 2배로 계산되어야 엑셀에서 비슷하게 맞음 (대략적인 보정)
                            curr_len = len(str(cell.value))
                            # 한글이 포함된 경우 가중치
                            if any(ord(c) > 128 for c in str(cell.value)):
                                curr_len = curr_len * 1.5
                            if curr_len > max_length:
                                max_length = curr_len
                        except:
                            pass
                    
                    # 숫자 포맷 적용
                    if header_val == '전용면적(㎡)':
                        cell.number_format = '#,##0.00'
                    elif header_val in ['거래금액', '면적단가', '층']:
                         # 쉼표 스타일 (정수)
                         cell.number_format = '#,##0'
                
                # 컬럼 너비 설정 (필터 버튼 공간 확보를 위해 여유 추가)
                adjusted_width = (max_length + 4) * 1.2
                ws.column_dimensions[col_letter].width = min(adjusted_width, 60) # 최대 60으로 제한

        excel_data = output.getvalue()
        
        st.download_button(
            label="📥 엑셀 파일로 다운로드 (서식 적용)",
            data=excel_data,
            file_name=f"{display_params.get('sido')}_{display_params.get('gugun')}_{display_params.get('type')}_실거래가.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
            
elif search_btn: # search_btn이 눌렸지만 raw_df가 비어있는 경우 (API 호출 결과 없음)
    st.warning("조회된 데이터가 없습니다. API 키를 확인하거나 다른 지역/날짜를 선택해보세요.")
else: # 초기 로드 상태
    st.info("왼쪽 사이드바에서 조건을 설정하고 '실거래가 조회 시작' 버튼을 눌러주세요.")
