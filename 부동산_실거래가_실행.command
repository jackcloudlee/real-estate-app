#!/bin/bash

# 스크립트가 있는 폴더로 이동 (중요: 이 코드가 있어야 더블클릭 시 경로 에러가 안 납니다)
cd "$(dirname "$0")"

# Streamlit 앱 실행
echo "부동산 실거래가 조회 도구를 실행합니다..."
streamlit run app.py
