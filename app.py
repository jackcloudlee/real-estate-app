import io, re, json, base64, mimetypes, html
import requests
import xml.etree.ElementTree as ET
import bcrypt
import pandas as pd
import streamlit as st
from urllib.parse import quote
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from PyPDF2 import PdfReader
import sqlite3
from urllib.parse import quote

APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
DB_PATH = DATA_DIR / "app.db"
LOCAL_TZ = ZoneInfo("Asia/Seoul")
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

def get_settings():
    delete_after_days = 30
    if "storage" in st.secrets and "delete_after_days" in st.secrets["storage"]:
        delete_after_days = max(30, int(st.secrets["storage"]["delete_after_days"]))
    return {"delete_after_days": delete_after_days, "case_keep_days": 30}

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cases(
        id TEXT PRIMARY KEY,
        created_at TEXT,
        created_by TEXT,
        status TEXT,
        case_no TEXT,
        address TEXT,
        property_type TEXT,
        area_m2 REAL,
        appraisal INTEGER,
        min_price INTEGER,
        auction_date TEXT,
        links TEXT,
        inputs_json TEXT,
        outputs_json TEXT,
        report_md TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS uploads(
        id TEXT PRIMARY KEY,
        case_id TEXT,
        file_type TEXT,
        storage_path TEXT,
        uploaded_at TEXT,
        delete_after TEXT,
        deleted_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tx_runs(
        id TEXT PRIMARY KEY,
        created_at TEXT,
        created_by TEXT,
        title TEXT,
        query TEXT,
        rows_json TEXT
    )""")
    con.commit()
    con.close()

def cleanup_uploads(delete_after_days: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    now = datetime.utcnow()
    cur.execute("""SELECT id, storage_path, delete_after FROM uploads WHERE deleted_at IS NULL""")
    for uid, path, delete_after in cur.fetchall():
        try:
            if delete_after and now >= datetime.fromisoformat(delete_after):
                p = Path(path)
                if p.exists():
                    p.unlink()
                cur.execute("""UPDATE uploads SET deleted_at=? WHERE id=?""", (now.isoformat(), uid))
        except Exception:
            pass
    con.commit()
    con.close()

def _parse_local_dt(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(LOCAL_TZ)
    except Exception:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_TZ)
        except Exception:
            return None

def cleanup_old_cases(keep_days: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    now_local = datetime.now(LOCAL_TZ)
    cutoff = now_local - timedelta(days=int(keep_days))

    cur.execute("""SELECT id, created_at FROM cases""")
    rows = cur.fetchall()
    expired_ids = []
    for rid, created_at in rows:
        dt = _parse_local_dt(created_at)
        if dt and dt < cutoff:
            expired_ids.append(rid)

    if not expired_ids:
        con.close()
        return

    for rid in expired_ids:
        cur.execute("""SELECT id, storage_path FROM uploads WHERE case_id=?""", (rid,))
        for uid, storage_path in cur.fetchall():
            try:
                p = Path(storage_path)
                if p.exists():
                    p.unlink()
            except Exception:
                pass
            cur.execute("""DELETE FROM uploads WHERE id=?""", (uid,))
        cur.execute("""DELETE FROM cases WHERE id=?""", (rid,))

    con.commit()
    con.close()

def cleanup_old_tx_runs(keep_days: int = 30):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    now_local = datetime.now(LOCAL_TZ)
    cutoff = now_local - timedelta(days=int(keep_days))
    cur.execute("""SELECT id, created_at FROM tx_runs""")
    rows = cur.fetchall()
    expired_ids = []
    for rid, created_at in rows:
        dt = _parse_local_dt(created_at)
        if dt and dt < cutoff:
            expired_ids.append(rid)
    for rid in expired_ids:
        cur.execute("""DELETE FROM tx_runs WHERE id=?""", (rid,))
    con.commit()
    con.close()

def allowed_users():
    users = []
    if "auth" in st.secrets and "allowed_users" in st.secrets["auth"]:
        users = st.secrets["auth"]["allowed_users"]
    norm = {}
    for u in users:
        email = str(u.get("email","")).strip().lower()
        pw_hash = str(u.get("password_hash","")).strip()
        if email and pw_hash:
            norm[email] = pw_hash
    return norm

def check_login(email: str, password: str) -> bool:
    email = email.strip().lower()
    users = allowed_users()
    if email not in users:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), users[email].encode("utf-8"))
    except Exception:
        return False

def require_login():
    if "user_email" not in st.session_state:
        st.session_state.user_email = None
    if st.session_state.user_email:
        return True

    st.set_page_config(page_title="경매 분석기 로그인", layout="wide")

    # 가운데 정렬 + 입력 폭 제한 CSS
    st.markdown(
        """
        <style>
          .login-wrap {max-width: 360px; margin: 0 auto; padding-top: 30px;}
          .login-card {padding: 20px 22px; border: 1px solid rgba(49,51,63,0.2); border-radius: 14px;}
          .login-title {font-size: 34px; font-weight: 800; margin-bottom: 6px;}
          .login-sub {opacity: 0.75; margin-bottom: 16px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="login-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="login-title">🔐 경매 분석기 로그인</div>', unsafe_allow_html=True)
    st.markdown('<div class="login-sub">회원가입 없이, 허용된 이메일(화이트리스트)만 로그인됩니다.</div>', unsafe_allow_html=True)

    with st.form("login_form", clear_on_submit=False):
        email = st.text_input("이메일", placeholder="you@example.com")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인", use_container_width=True)

    if submitted:
        if check_login(email, password):
            st.session_state.user_email = email.strip().lower()
            st.success("로그인 성공")
            st.rerun()
        else:
            st.error("로그인 실패: 이메일이 허용되어 있지 않거나 비밀번호가 틀립니다.")

    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

def save_upload(case_id: str, file_type: str, uploaded_file):
    import uuid
    settings = get_settings()
    uid = str(uuid.uuid4())
    suffix = Path(uploaded_file.name).suffix.lower() or ".bin"
    storage_path = UPLOAD_DIR / f"{uid}_{file_type}{suffix}"
    with open(storage_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    now = datetime.utcnow()
    delete_after = now + timedelta(days=settings["delete_after_days"])

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      INSERT INTO uploads(id, case_id, file_type, storage_path, uploaded_at, delete_after, deleted_at)
      VALUES(?,?,?,?,?,?,NULL)
    """, (uid, case_id, file_type, str(storage_path), now.isoformat(), delete_after.isoformat()))
    con.commit()
    con.close()
    return str(storage_path)

def parse_auction_pdf(pdf_bytes: bytes) -> dict:
    """옥션원 PDF 전용 파서(안정화).
    - 본 사건번호와 관련사건(중복)을 구분
    - 최저가(80%) / 2차 금액을 우선 추출
    - 새주소 우선 추출 + 반복 토큰 정리
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages = []
    for page in reader.pages[:6]:
        try:
            pages.append(page.extract_text() or "")
        except Exception:
            pages.append("")
    text = "\n".join(pages)

    flat = re.sub(r"[\t\r]", " ", text)
    flat = re.sub(r"\s+", " ", flat).strip()

    def norm_date(s: str):
        return s.replace("-", ".").replace("/", ".")

    def dedupe_tokens(s: str):
        toks = re.split(r"\s+", s.strip())
        out = []
        for t in toks:
            if not out or out[-1] != t:
                out.append(t)
        s2 = " ".join(out)
        s2 = re.sub(r"(서울특별시)(?:\s*\1)+", r"\1", s2)
        s2 = re.sub(r"(중랑구)(?:\s*\1)+", r"\1", s2)
        return s2.strip()

    # 사건번호: '매각기일' 라인 근처 우선
    case_no = None
    m = re.search(r"매각기일[^\n]{0,150}(\d{4}\s*타경\s*\d+)", text)
    if m:
        case_no = m.group(1).replace(" ", "")
    if not case_no:
        m = re.search(r"(?:지방법원|지원)[^0-9]{0,80}(\d{4}\s*타경\s*\d+)", flat)
        if m:
            case_no = m.group(1).replace(" ", "")
    if not case_no:
        for mm in re.finditer(r"(\d{4}\s*타경\s*\d+)", flat):
            span = flat[max(0, mm.start()-40):mm.end()+40]
            if "관련사건" in span:
                continue
            case_no = mm.group(1).replace(" ", "")
            break

    related_case = None
    m = re.search(r"관련사건\s*(\d{4}\s*타경\s*\d+)", flat)
    if m:
        related_case = m.group(1).replace(" ", "")

    auction_date = None
    m = re.search(r"매각기일\s*[:：]?\s*(\d{4}[\.-]\d{2}[\.-]\d{2})", flat)
    if m:
        auction_date = norm_date(m.group(1))

    base_right = None
    m = re.search(r"말소기준권리\s*[:：]?\s*(\d{4}[\.-]\d{2}[\.-]\d{2})", flat)
    if m:
        base_right = norm_date(m.group(1))

    
    address = None

    # 주소(옥션원): 텍스트 추출 시 토큰 반복(서울특별시서울특별시, 길길길길, 층층층층 등)이 흔합니다.
    # '새 주소' 또는 '소재지' 위치를 찾고, 다음 키워드 전까지만 짧게 잘라냅니다.
    def _slice_after(label: str, max_len: int = 140):
        i = flat.find(label)
        if i < 0:
            return None
        seg = flat[i + len(label): i + len(label) + max_len]
        for stop in ["물건종별", "감 정 가", "감정가", "평당", "대 지 권", "대지권", "최저매각", "최 저 가"]:
            j = seg.find(stop)
            if j > 5:
                seg = seg[:j]
                break
        return seg.strip()

    # 요청사항: 구주소(소재지) 우선 사용
    addr = _slice_after("소 재 지")
    if not addr:
        addr = _slice_after("새 주 소")

    if addr:
        address = re.sub(r"\s+", " ", addr).strip()
        # 반복 토큰 축약
        address = re.sub(r"(서울특별시)\1+", r"\1", address)
        address = re.sub(r"(중랑구)\1+", r"\1", address)
        address = re.sub(r"(길)\1+", r"\1", address)
        address = re.sub(r"(비동)\1+", r"\1", address)
        address = re.sub(r"(층)\1+", r"\1", address)
        address = re.sub(r"(호)\1+", r"\1", address)
        address = re.sub(r"([가-힣0-9]{1,6})\1{1,}", r"\1", address)
        # 기존 dedupe_tokens도 한번 적용(있으면)
        try:
            address = dedupe_tokens(address)
        except Exception:
            pass

        # 소재지에서 지번 주소(시/구/군/동/번지) 우선 추출
        raw = address.replace(",", " ")
        raw = re.sub(r"\s+", " ", raw).strip()
        raw = re.sub(r"(서울특별시)\1+", r"\1", raw)
        raw = re.sub(r"([가-힣]{1,12}(?:동|읍|면|리))\1+", r"\1", raw)

        jibun_patterns = [
            r"((?:서울특별시|부산광역시|대구광역시|인천광역시|광주광역시|대전광역시|울산광역시|세종특별자치시|[가-힣]+도)\s+[가-힣]+(?:시|군|구)\s+[가-힣0-9]+(?:동|읍|면|리)\s*\d+(?:-\d+)?)",
            r"([가-힣]+(?:시|군|구)\s+[가-힣0-9]+(?:동|읍|면|리)\s*\d+(?:-\d+)?)",
        ]
        jibun = None
        for ptn in jibun_patterns:
            m = re.search(ptn, raw)
            if m:
                jibun = m.group(1)
                break
        if jibun:
            jibun = re.sub(r"\s+", " ", jibun).strip()
            jibun = re.sub(r"([가-힣]{1,12}(?:동|읍|면|리))\1+", r"\1", jibun)
            address = jibun

    area_m2 = None
    m = re.search(r"건물면적\s*([0-9]+(?:\.[0-9]+)?)\s*㎡", flat)
    if m:
        area_m2 = float(m.group(1))

    def to_int_money(s: str):
        return int(s.replace(",", ""))

    appraisal = None
    m = re.search(r"감\s*정\s*가\s*([0-9]{1,3}(?:,[0-9]{3})+)\s*원", flat)
    if m:
        appraisal = to_int_money(m.group(1))
    else:
        cands = []
        for mm in re.finditer(r"감\s*정\s*가|감정가", flat):
            seg = flat[mm.end():mm.end()+200]
            for m1 in re.finditer(r"([0-9]{1,3}(?:,[0-9]{3})+)", seg):
                cands.append(to_int_money(m1.group(1)))
        appraisal = max(cands) if cands else None

    min_price = None
    # 최저가: PDF 텍스트 추출 시 '원' 글자가 'ਗ'처럼 깨질 수 있어, '원' 없이도 잡히도록 패턴을 구성합니다.

    # 1) "최 저 가(80%) 273,600,000" 형태
    m = re.search(r"최\s*저\s*가\s*\(\s*80\s*%\s*\)\s*([0-9]{1,3}(?:,[0-9]{3})+)", flat)
    if m:
        min_price = to_int_money(m.group(1))

    # 2) "2차 2026-03-04 273,600,000" 형태
    if not min_price:
        m = re.search(r"2차\s*\d{4}[\.-]\d{2}[\.-]\d{2}\s*([0-9]{1,3}(?:,[0-9]{3})+)", flat)
        if m:
            min_price = to_int_money(m.group(1))

    # 3) "273,600,000 (80%)" 형태
    if not min_price:
        m = re.search(r"([0-9]{1,3}(?:,[0-9]{3})+)\s*[^0-9]{0,3}\(\s*80\s*%\s*\)", flat)
        if m:
            min_price = to_int_money(m.group(1))

    # 4) 안전장치: '최저가' 키워드 주변 후보(감정가 이하 중 최대)
    if not min_price:
        cands = []
        for mm in re.finditer(r"최\s*저\s*가|최저가|최\s*저\s*매\s*각\s*가\s*격", flat):
            seg = flat[mm.end():mm.end()+260]
            for m1 in re.finditer(r"([0-9]{1,3}(?:,[0-9]{3})+)", seg):
                cands.append(to_int_money(m1.group(1)))
        if cands:
            if appraisal:
                under = [x for x in cands if x <= appraisal]
                min_price = max(under) if under else max(cands)
            else:
                min_price = max(cands)

    occupancy_hint = []
    if re.search(r"임차인이\s*없", flat):
        occupancy_hint.append("임차인 없음")
    if re.search(r"소유자가\s*점유", flat):
        occupancy_hint.append("소유자 점유")
    if re.search(r"전입세대확인서", flat):
        occupancy_hint.append("전입세대확인서 언급")

    special = []
    if re.search(r"제시외\s*건물", flat) or re.search(r"제시외\s*건물\s*포함", flat):
        special.append("제시외 건물 포함")
    if re.search(r"\(중복\)\s*-\s*정지|중복\)\-정지", flat):
        special.append("중복사건(정지) 표기")

    rights_rows = []
    for mm in re.finditer(r"(\d+)\((갑|을)\d+\)\s*(\d{4}\.\d{2}\.\d{2})\s*([가-힣]+)\s*([^0-9]+?)\s*([0-9]{1,3}(?:,[0-9]{3})+)\s*원\s*(말소기준등기)?\s*(소멸|인수|존속)?", flat):
        rights_rows.append({
            "no": mm.group(1),
            "ab": mm.group(2),
            "date": mm.group(3),
            "kind": mm.group(4).strip(),
            "holder": re.sub(r"\s+"," ",mm.group(5)).strip(),
            "amount": to_int_money(mm.group(6)),
            "is_base": True if mm.group(7) else False,
            "status": (mm.group(8) or "").strip(),
        })
    rights_summary = None
    if rights_rows:
        base_row = next((r for r in rights_rows if r.get("is_base")), None)
        if base_row:
            rights_summary = f"말소기준등기: {base_row['date']} {base_row['kind']}({base_row['holder']})"
            if not base_right:
                base_right = base_row["date"]
        else:
            rights_summary = f"등기 표 파싱 {len(rights_rows)}건(말소기준등기 표기 미발견)"

    # --- 최저가 정보(저감율/차수/유찰횟수) ---
    min_price_pct = None
    explicit_pct = None
    m = re.search(r"최\s*저\s*가\s*\(\s*([0-9]{2,3})\s*%\s*\)", flat)
    if m:
        explicit_pct = int(m.group(1))
    if explicit_pct is not None:
        min_price_pct = float(explicit_pct)
    elif appraisal and min_price:
        try:
            min_price_pct = round((float(min_price) / float(appraisal)) * 100.0, 1)
        except Exception:
            min_price_pct = None

    rounds = []
    for mm in re.finditer(r"(\d)차\s*(\d{4}[\.-]\d{2}[\.-]\d{2})\s*([0-9]{1,3}(?:,[0-9]{3})+)", flat):
        rno = int(mm.group(1))
        d = mm.group(2).replace("-", ".")
        price = int(mm.group(3).replace(",", ""))
        tail = flat[mm.end():mm.end()+20]
        status = "유찰" if "유찰" in tail else ("변경" if "변경" in tail else "")
        rounds.append({"round": rno, "date": d, "price": price, "status": status})

    current_round = None
    current_status = None
    if auction_date and rounds:
        for r in rounds:
            if r["date"] == auction_date:
                current_round = r["round"]
                current_status = r["status"] or None
                break

    if current_round is None and min_price and rounds:
        same = [r for r in rounds if r["price"] == int(min_price)]
        if same:
            same_sorted = sorted(same, key=lambda x: x["round"])
            current_round = same_sorted[0]["round"]
            current_status = same_sorted[0]["status"] or None

    prior_unsold_count = None
    if current_round and rounds:
        prior_unsold_count = sum(1 for r in rounds if r["round"] < current_round and r["status"] == "유찰")

    return {
        "case_no": case_no,
        "related_case": related_case,
        "address": address,
        "appraisal": appraisal,
        "min_price": min_price,
        "min_price_pct": min_price_pct,
        "current_round": current_round,
        "prior_unsold_count": prior_unsold_count,
        "current_status": current_status,
        "area_m2": area_m2,
        "auction_date": auction_date,
        "base_right": base_right,
        "occupancy_hint": " / ".join(occupancy_hint) if occupancy_hint else None,
        "special_hint": " / ".join(special) if special else None,
        "rights_rows": rights_rows,
        "rights_summary": rights_summary,
        "raw_text_snippet": text[:1200],
    }


def parse_comps_xlsx(xlsx_bytes: bytes) -> pd.DataFrame:
    """대표님 실거래 엑셀 포맷(고정)을 전제로 파싱합니다.
    기대 컬럼:
      - 전용면적(㎡)
      - 거래금액  (원 단위)
    """
    df = pd.read_excel(io.BytesIO(xlsx_bytes))

    # 컬럼명 정규화(공백 제거 등)
    cols = {str(c).strip(): c for c in df.columns}

    area_col = cols.get("전용면적(㎡)") or cols.get("전용면적")
    price_col = cols.get("거래금액") or cols.get("거래금액(원)") or cols.get("매매금액")

    if area_col is None or price_col is None:
        # 안전장치: 유사 키워드로라도 찾기
        for k, orig in cols.items():
            if area_col is None and ("전용" in k and "면적" in k):
                area_col = orig
            if price_col is None and ("거래" in k and ("금액" in k or "가격" in k)):
                price_col = orig

    if area_col is None or price_col is None:
        raise ValueError(f"실거래 엑셀에서 필수 컬럼을 찾지 못했습니다. 컬럼={list(df.columns)}")

    out = pd.DataFrame({
        "area_m2": pd.to_numeric(df[area_col], errors="coerce"),
        "price": pd.to_numeric(df[price_col], errors="coerce"),  # 이미 '원' 단위
    })

    # 비정상 값 제거(잡음 제거)
    out = out.dropna(subset=["area_m2", "price"])
    out = out[(out["area_m2"] > 5) & (out["price"] > 10_000_000)]
    return out

def parse_comps_view_xlsx(xlsx_bytes: bytes) -> pd.DataFrame:
    """실거래 조회/리스트 화면용 표 데이터."""
    df = pd.read_excel(io.BytesIO(xlsx_bytes))
    if "면적단가" not in df.columns and {"거래금액", "전용면적(㎡)"} <= set(df.columns):
        _p = pd.to_numeric(df["거래금액"], errors="coerce")
        _a = pd.to_numeric(df["전용면적(㎡)"], errors="coerce")
        df["면적단가"] = (_p / _a).round()

    keep_cols = [c for c in ["계약년월", "시군구", "번지", "건물명", "전용면적(㎡)", "거래금액", "면적단가", "층", "건축년도"] if c in df.columns]
    if keep_cols:
        df = df[keep_cols].copy()

    if "층" in df.columns:
        _floor_num = pd.to_numeric(df["층"], errors="coerce")
        df = df[_floor_num.ne(-1) | _floor_num.isna()]

    return df

def _secret_get(path: list[str], default=None):
    cur = st.secrets
    try:
        for p in path:
            cur = cur[p]
        return cur
    except Exception:
        return default

def _get_vworld_key() -> str:
    ui_key = (st.session_state.get("tx_api_vworld_key") or "").strip()
    if ui_key:
        return ui_key
    return (
        _secret_get(["vworld", "api_key"], "")
        or _secret_get(["apis", "vworld_api_key"], "")
        or ""
    ).strip()

def _get_molit_key() -> str:
    ui_key = (st.session_state.get("tx_api_molit_key") or "").strip()
    if ui_key:
        return ui_key
    return (
        _secret_get(["molit", "service_key"], "")
        or _secret_get(["apis", "molit_service_key"], "")
        or ""
    ).strip()

SIDO_GUGUN_OPTIONS = {
    "서울특별시": [
        "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
        "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구",
        "용산구","은평구","종로구","중구","중랑구",
    ],
    "경기도": [
        "수원시","성남시","고양시","용인시","부천시","안산시","안양시","남양주시","화성시","평택시",
        "의정부시","시흥시","파주시","김포시","광명시","광주시","군포시","오산시","이천시","양주시",
        "구리시","안성시","포천시","의왕시","하남시","여주시","동두천시","과천시","가평군","양평군","연천군"
    ],
}

def fetch_vworld_lot_candidates(sido: str, sigungu: str, size: int = 200):
    key = _get_vworld_key()
    if not key:
        return [], "VWORLD_API_KEY가 설정되지 않았습니다."
    q = f"{sido} {sigungu}".strip()
    url = "https://api.vworld.kr/req/search"
    params = {
        "service": "search",
        "request": "search",
        "version": "2.0",
        "crs": "EPSG:4326",
        "size": str(size),
        "page": "1",
        "query": q,
        "type": "PARCEL",
        "format": "json",
        "errorformat": "json",
        "key": key,
    }
    try:
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        obj = r.json()
        items = (((obj or {}).get("response") or {}).get("result") or {}).get("items") or []
    except Exception as e:
        return [], f"VWORLD 조회 실패: {e}"

    out = []
    seen = set()
    for it in items:
        addr = str(it.get("address") or "")
        pnu = str(it.get("id") or "")
        m = re.search(r"([가-힣0-9]+동)\s+(\d+)(?:-(\d+))?", addr)
        if not m:
            continue
        dong = m.group(1)
        bun_main = m.group(2)
        bun_sub = m.group(3) or "0"
        bunji = f"{bun_main}-{bun_sub}" if bun_sub != "0" else bun_main
        key2 = (dong, bunji, pnu)
        if key2 in seen:
            continue
        seen.add(key2)
        out.append({"dong": dong, "bunji": bunji, "pnu": pnu, "address": addr})
    return out, None

def _molit_fetch_month(lawd_cd: str, yyyymm: str, property_type: str = "연립다세대"):
    svc_key = _get_molit_key()
    if not svc_key:
        return [], "MOLIT_SERVICE_KEY가 설정되지 않았습니다."
    if property_type == "아파트":
        url = "https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"
    else:
        url = "https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade"
    params = {"serviceKey": svc_key, "LAWD_CD": lawd_cd, "DEAL_YMD": yyyymm}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        items = root.findall(".//item")
    except Exception as e:
        return [], f"MOLIT 조회 실패({yyyymm}): {e}"

    rows = []
    for it in items:
        def t(tag):
            v = it.findtext(tag)
            return (v or "").strip()
        rows.append(
            {
                "계약년월": f"{t('년')}{t('월').zfill(2)}",
                "시군구": t("법정동"),
                "번지": t("지번"),
                "건물명": t("건물명"),
                "전용면적(㎡)": t("전용면적"),
                "거래금액": (t("거래금액") or "").replace(",", ""),
                "층": t("층"),
                "건축년도": t("건축년도"),
            }
        )
    return rows, None

def fetch_molit_trades_by_lot(pnu: str, dong: str, bunji: str, months_back: int = 12, property_type: str = "연립다세대"):
    if not pnu or len(pnu) < 5:
        return pd.DataFrame(), "PNU를 찾지 못했습니다."
    lawd_cd = pnu[:5]
    today = datetime.now(LOCAL_TZ)
    yms = []
    y, m = today.year, today.month
    for _ in range(max(1, int(months_back))):
        yms.append(f"{y:04d}{m:02d}")
        m -= 1
        if m == 0:
            y -= 1
            m = 12

    all_rows = []
    last_err = None
    for ym in yms:
        rows, err = _molit_fetch_month(lawd_cd, ym, property_type=property_type)
        if err:
            last_err = err
            continue
        all_rows.extend(rows)
    if not all_rows:
        return pd.DataFrame(), (last_err or "실거래 데이터를 찾지 못했습니다.")

    df = pd.DataFrame(all_rows)
    if "시군구" in df.columns:
        df["시군구"] = df["시군구"].astype(str).str.strip()
    if "번지" in df.columns:
        df["번지"] = df["번지"].astype(str).str.strip()
    df = df[df["시군구"].astype(str).str.contains(str(dong).replace("동", ""), na=False)]
    df = df[df["번지"] == str(bunji)]
    if "층" in df.columns:
        fl = pd.to_numeric(df["층"], errors="coerce")
        df = df[fl.ne(-1) | fl.isna()]
    if "전용면적(㎡)" in df.columns:
        df["전용면적(㎡)"] = pd.to_numeric(df["전용면적(㎡)"], errors="coerce")
    if "거래금액" in df.columns:
        df["거래금액"] = pd.to_numeric(df["거래금액"], errors="coerce")
    if {"거래금액", "전용면적(㎡)"} <= set(df.columns):
        df["면적단가"] = (df["거래금액"] / df["전용면적(㎡)"]).round()
    keep = [c for c in ["계약년월", "시군구", "번지", "건물명", "전용면적(㎡)", "거래금액", "면적단가", "층", "건축년도"] if c in df.columns]
    return df[keep].copy(), None


def estimate_sale_price_range(comps: pd.DataFrame, subject_area: float) -> dict:
    """전용면적 유사표본 기반 매도가능가(하/중/상) 산정.
    - 기본: ±3㎡ (표본 부족 시 ±5㎡)
    - 분위수 25/50/75 사용
    - 이상치 방지용 간단 필터 포함
    """
    if subject_area is None or "area_m2" not in comps.columns or "price" not in comps.columns:
        return {"low": None, "mid": None, "high": None, "note": "실거래 데이터 컬럼 인식 실패 또는 대상면적 없음"}

    subject_area = float(subject_area)

    def pick(delta: float):
        return comps[(comps["area_m2"].between(subject_area - delta, subject_area + delta))].copy()

    f3 = pick(3.0)
    f = f3 if len(f3) >= 8 else pick(5.0)

    if len(f) == 0:
        return {"low": None, "mid": None, "high": None, "note": "유사면적 표본이 부족합니다(±5㎡ 내 거래 없음)"}

    # 이상치 필터(중앙값 대비 과도한 값 제거)
    med = float(f["price"].median())
    f2 = f[(f["price"] >= med * 0.5) & (f["price"] <= med * 1.7)]
    if len(f2) >= 5:
        f = f2  # 충분하면 필터 적용

    q25 = int(f["price"].quantile(0.25))
    q50 = int(f["price"].quantile(0.50))
    q75 = int(f["price"].quantile(0.75))

    delta_used = 3 if len(f3) >= 8 else 5
    return {
        "low": q25,
        "mid": q50,
        "high": q75,
        "n": int(len(f)),
        "note": f"유사면적 표본 {len(f)}건 기반(±{delta_used}㎡, 분위수 25/50/75, 이상치 필터 적용)",
    }


def build_profit_matrix(sale_prices, bid_start, bid_end, bid_step, tax_rate, loan_amount, interest_rate, holding_days, early_repay_fee_rate, repair_cost, eviction_cost):
    holding_years = holding_days / 365.0
    interest_cost = loan_amount * interest_rate * holding_years
    early_fee = loan_amount * early_repay_fee_rate

    bids = list(range(bid_start, bid_end + 1, bid_step))
    rows = []
    for bid in bids:
        row = {"입찰가": bid}
        for sp in sale_prices:
            profit = sp - bid - (bid * tax_rate) - repair_cost - eviction_cost - interest_cost - early_fee
            row[f"매도가 {sp/100_000_000:.2f}억"] = int(round(profit))
        rows.append(row)
    df = pd.DataFrame(rows)
    return df, {"interest_cost": int(round(interest_cost)), "early_fee": int(round(early_fee))}

def save_case(case: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    INSERT INTO cases(
        id, created_at, created_by, status, case_no, address, property_type, area_m2, appraisal, min_price, auction_date, links,
        inputs_json, outputs_json, report_md
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        case["id"], case["created_at"], case["created_by"], case["status"], case.get("case_no"), case.get("address"),
        case.get("property_type"), case.get("area_m2"), case.get("appraisal"), case.get("min_price"),
        case.get("auction_date"), json.dumps(case.get("links") or {}, ensure_ascii=False),
        json.dumps(case.get("inputs") or {}, ensure_ascii=False),
        json.dumps(case.get("outputs") or {}, ensure_ascii=False),
        case.get("report_md") or ""
    ))
    con.commit()
    con.close()

def list_cases():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # 최신 저장 순서 기준으로 조회(시간 문자열 파싱 이슈 회피)
    cur.execute("""SELECT rowid, id, created_at, case_no, address, status, auction_date, outputs_json FROM cases ORDER BY rowid DESC""")
    rows = cur.fetchall()
    con.close()
    out = []
    for rowid, rid, created_at, case_no, address, status, auction_date, outputs_json in rows:
        o = {}
        try:
            o = json.loads(outputs_json) if outputs_json else {}
        except Exception:
            o = {}
        out.append({
            "rowid": rowid,
            "id": rid, "created_at": created_at, "case_no": case_no, "address": address, "status": status,
            "auction_date": auction_date,
            "loss0_max_bid": o.get("loss0_max_bid"),
            "recommended_bid": o.get("recommended_bid"),
        })
    return out

def get_case(case_id: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""SELECT id, created_at, created_by, status, case_no, address, property_type, area_m2, appraisal, min_price, auction_date, links, inputs_json, outputs_json, report_md
                   FROM cases WHERE id=?""", (case_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    (rid, created_at, created_by, status, case_no, address, property_type, area_m2, appraisal, min_price, auction_date, links, inputs_json, outputs_json, report_md) = row
    return {
        "id": rid, "created_at": created_at, "created_by": created_by, "status": status, "case_no": case_no,
        "address": address, "property_type": property_type, "area_m2": area_m2, "appraisal": appraisal, "min_price": min_price,
        "auction_date": auction_date, "links": json.loads(links) if links else {},
        "inputs": json.loads(inputs_json) if inputs_json else {},
        "outputs": json.loads(outputs_json) if outputs_json else {},
        "report_md": report_md or ""
    }

def save_tx_run(run: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO tx_runs(id, created_at, created_by, title, query, rows_json)
        VALUES(?,?,?,?,?,?)
        """,
        (
            run["id"],
            run["created_at"],
            run.get("created_by"),
            run.get("title") or "",
            run.get("query") or "",
            json.dumps(run.get("rows") or [], ensure_ascii=False),
        ),
    )
    con.commit()
    con.close()

def list_tx_runs():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""SELECT rowid, id, created_at, title, query, rows_json FROM tx_runs ORDER BY rowid DESC""")
    rows = cur.fetchall()
    con.close()
    out = []
    for rowid, rid, created_at, title, query, rows_json in rows:
        rr = []
        try:
            rr = json.loads(rows_json) if rows_json else []
        except Exception:
            rr = []
        out.append(
            {
                "rowid": rowid,
                "id": rid,
                "created_at": created_at,
                "title": title or "-",
                "query": query or "",
                "count": len(rr),
            }
        )
    return out

def get_tx_run(run_id: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""SELECT id, created_at, created_by, title, query, rows_json FROM tx_runs WHERE id=?""", (run_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    rid, created_at, created_by, title, query, rows_json = row
    try:
        rows = json.loads(rows_json) if rows_json else []
    except Exception:
        rows = []
    return {
        "id": rid,
        "created_at": created_at,
        "created_by": created_by,
        "title": title or "-",
        "query": query or "",
        "rows": rows,
    }

def now_local_str():
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

def format_created_at_local(v):
    if v is None:
        return "-"
    s = str(v).strip()
    if not s:
        return "-"
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TZ)
        else:
            dt = dt.astimezone(LOCAL_TZ)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s.replace("T", " ").replace("Z", "")[:19]

def fmt_money(v):
    if v is None or v == "":
        return "-"
    try:
        return f"{int(v):,}원"
    except Exception:
        return str(v)

def fmt_area(v):
    if v is None or v == "":
        return "-"
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)

def parse_recommended_low(rec_text: str):
    if not rec_text:
        return None
    nums = re.findall(r"\d[\d,]*", str(rec_text))
    if not nums:
        return None
    try:
        return int(nums[0].replace(",", ""))
    except Exception:
        return None

def infer_round_and_unsold(appraisal: int, min_price: int):
    """감정가 대비 최저가 비율로 현재 차수/유찰횟수 추정(일반 패턴)."""
    if not appraisal or not min_price:
        return {"round": None, "unsold": None, "pct": None, "discount_pct": None}
    pct = (float(min_price) / float(appraisal)) * 100.0
    ratios = []
    r = 100.0
    for i in range(1, 9):
        ratios.append((i, r))
        r *= 0.8
    best = min(ratios, key=lambda x: abs(x[1] - pct))
    round_no = best[0]
    unsold = max(0, round_no - 1)
    return {"round": round_no, "unsold": unsold, "pct": round(pct, 1), "discount_pct": round(100.0 - pct, 1)}

def calc_auction_taxes(win_price: int):
    """대표님 기준 낙찰 세금(간이)"""
    acq_tax = int(round((win_price or 0) * 0.01))
    bond_cert = int(round(acq_tax * 0.10))
    bond_discount = 100_000
    reg_license = 100_000
    total = acq_tax + bond_cert + bond_discount + reg_license
    return {"acq_tax": acq_tax, "bond_cert": bond_cert, "bond_discount": bond_discount, "reg_license": reg_license, "total": total}
def parse_links(raw: str) -> list[str]:
    if not raw:
        return []
    out = []
    for line in str(raw).splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("http://") or s.startswith("https://"):
            out.append(s)
    return out

def extract_latlon_from_link(url: str):
    """지도 링크에서 위경도 후보를 추출합니다(가능한 경우에만)."""
    u = url
    lat = None
    lon = None

    m = re.search(r"[?&]lat(?:itude)?=([0-9]+\.[0-9]+)", u)
    if m:
        lat = float(m.group(1))
    m = re.search(r"[?&](?:lng|lon)(?:gitude)?=([0-9]+\.[0-9]+)", u)
    if m:
        lon = float(m.group(1))

    if lat is None or lon is None:
        m = re.search(r"([0-9]{2,3}\.[0-9]+)\s*,\s*([0-9]{2,3}\.[0-9]+)", u)
        if m:
            a = float(m.group(1))
            b = float(m.group(2))
            if 33 <= a <= 39 and 124 <= b <= 132:
                lat, lon = a, b
            elif 33 <= b <= 39 and 124 <= a <= 132:
                lat, lon = b, a

    if lat is None or lon is None:
        m = re.search(r"[?&]c=([0-9]{2,3}\.[0-9]+),([0-9]{2,3}\.[0-9]+)", u)
        if m:
            lon = float(m.group(1))
            lat = float(m.group(2))

    if lat is not None and lon is not None:
        return lat, lon
    return None


def clean_extracted_snippet(s: str) -> str:
    """PDF에서 추출한 텍스트(참고용)를 보기 좋게 정리합니다."""
    if not s:
        return ""
    t = s
    # 공백/탭 정리
    t = re.sub(r"[\t\r]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # 주소/지명 중복 정리(참고용)
    t = normalize_address(t)

    # 자주 반복되는 헤더 토큰 축약(옥션원)
    # 예: '매각물건현황매각물건현황...' → 1회
    for token in ["매각물건현황", "임차인현황", "등기부현황", "매각사례분석"]:
        t = re.sub(rf"(?:{token}){{2,}}", token, t)

    # 일반 반복 토큰 축약(길길길길, 층층층층 등)
    t = re.sub(r"([가-힣0-9]{1,8})\1{1,}", r"\1", t)

    # 가독성 위해 주요 라벨 앞에 줄바꿈 삽입
    for label in ["사건번호", "소 재 지", "새 주 소", "감 정 가", "최 저 가", "매각기일", "말소기준권리", "관련사건"]:
        t = t.replace(label, f"\n{label}")
    t = re.sub(r"\n+", "\n", t).strip()

    # 너무 길면 앞부분만
    if len(t) > 700:
        t = t[:700] + "\n…(생략)"
    return t

def normalize_address(addr: str) -> str:
    """옥션원 PDF 텍스트 추출로 생기는 중복 토큰을 최대한 정리합니다."""
    if not addr:
        return ""
    a = addr
    a = re.sub(r"[\t\r\n]", " ", a)
    a = re.sub(r"\s+", " ", a).strip()

    # 토큰 내부 반복(중랑구중랑구, 비동비동, 5층층층 등)
    a = re.sub(r"([가-힣0-9]{1,8})\1{1,}", r"\1", a)

    # 쉼표/특수기호를 공백으로 통일 후 단어 단위 중복 제거
    tmp = re.sub(r"[，,]", " ", a)
    tmp = re.sub(r"\s+", " ", tmp).strip()
    parts = tmp.split(" ")
    cleaned = []
    prev = None
    for p in parts:
        if not p:
            continue
        # 또 한번 내부 반복 축약
        p2 = re.sub(r"([가-힣0-9]{1,8})\1{1,}", r"\1", p)
        if p2 == prev:
            continue
        cleaned.append(p2)
        prev = p2

    a2 = " ".join(cleaned)
    # 흔한 반복 토큰 추가 정리(필요 시)
    for token in ["서울특별시", "중랑구", "묵동", "현진월드빌", "비동", "동", "층", "호"]:
        a2 = re.sub(rf"(?:{re.escape(token)})\s+(?:{re.escape(token)})", token, a2)

    return a2.strip()


def generate_report_stub(subject: dict, sale_range: dict, outputs: dict, assumptions: dict) -> str:
    """OpenAI API 없이도 '실전형'으로 보이도록 보고서를 구성합니다.
    - 매도가능가(하/중/상) 산정 근거를 표(마크다운 테이블)로 설명
    - 한줄 결론(진행/보류/비추천)을 자동으로 제시
    """
    case_no = subject.get("case_no") or "미추출/수정필요"
    address = subject.get("address") or "미추출/수정필요"
    area = subject.get("area_m2")
    appraisal = subject.get("appraisal")
    min_price = subject.get("min_price")
    auction_date = subject.get("auction_date") or "-"
    base_right = subject.get("base_right") or "-"

    occ = subject.get("occupancy_hint") or "-"
    special = subject.get("special_hint") or "-"
    rights_summary = subject.get("rights_summary") or "-"

    loss0 = outputs.get("loss0_max_bid")
    rec = outputs.get("recommended_bid") or "-"
    loan_amount = outputs.get("loan_amount")

    stats = (sale_range or {}).get("stats") or {}
    delta_used = stats.get("delta_used")
    n = stats.get("n") or ((sale_range or {}).get("n") if isinstance(sale_range, dict) else None)
    outlier_flag = "적용" if stats.get("outlier_filtered") else "미적용"

    # --- 한줄 결론(보수적) ---
    verdict = "보류"
    verdict_reason = []
    if not min_price or int(min_price) <= 0:
        verdict = "보류"
        verdict_reason.append("최저가 확인 필요(0원/미추출)")
    else:
        if loss0 and int(loss0) >= int(min_price):
            verdict = "진행 가능(조건부)"
            verdict_reason.append("손실0 상한이 최저가 이상")
        else:
            verdict = "보류/비추천"
            verdict_reason.append("손실0 상한이 최저가 미만")

        if "제시외" in str(special):
            verdict_reason.append("제시외 건물 리스크")
        if "중복" in str(special):
            verdict_reason.append("중복사건 상태 재확인")

    low = (sale_range or {}).get("low")
    mid = (sale_range or {}).get("mid")
    high = (sale_range or {}).get("high")

    rationale_table = """| 항목 | 값 | 의미 |
|---|---:|---|
"""
    rationale_table += f"| 유사면적 기준 | ±{int(delta_used) if delta_used else '-'}㎡ | 대상면적(전용)과 비슷한 거래만 사용 |\n"
    rationale_table += f"| 표본 수(n) | {n if n else '-'} | 표본이 많을수록 신뢰도 ↑ |\n"
    rationale_table += f"| 하단(25%) | {fmt_money(low)} | **빠른 매도**를 노릴 때 기준 |\n"
    rationale_table += f"| 기준(50%) | {fmt_money(mid)} | **현실 매도**의 중심값(중앙값) |\n"
    rationale_table += f"| 상단(75%) | {fmt_money(high)} | 상품화/시간여유가 있을 때 상단 목표 |\n"
    rationale_table += f"| 이상치 필터 | {outlier_flag} | 중앙값 대비 과도한 값은 제거(왜곡 방지) |\n"

    lines = []
    lines.append(f"# 경매 분석 리포트(자동 · 실전형)")
    lines.append("")
    # ✅ 요약 3줄(맨 위)
    lines.append(f"- **결론:** {verdict}")
    lines.append(f"- **추천 입찰가:** {rec}")
    lines.append(f"- **핵심 리스크:** {special if special!='-' else '특이사항 힌트 없음'} / {occ if occ!='-' else '점유 힌트 없음'}")
    lines.append("")
    lines.append(f"## 결론: **{verdict}**")
    if verdict_reason:
        lines.append(f"- 사유: {' / '.join(verdict_reason)}")
    lines.append("")

    lines.append("## 1) 물건 요약")
    lines.append(f"- 사건번호: **{case_no}**")
    if subject.get("related_case"):
        lines.append(f"- 관련사건(중복): {subject.get('related_case')}")
    lines.append(f"- 주소: **{address}**")
    lines.append(f"- 전용면적: **{area if area is not None else '-'} ㎡**")
    lines.append(f"- 감정가/최저가: **{fmt_money(appraisal)} / {fmt_money(min_price)}**")
    lines.append(f"- 매각기일: **{auction_date}**")
    lines.append(f"- 말소기준: **{base_right}**")
    lines.append("")

    lines.append("## 2) 권리/명도/특이사항 요약")
    lines.append(f"- 점유 힌트: **{occ}**")
    lines.append(f"- 특이사항 힌트: **{special}**")
    lines.append(f"- 등기 요약: **{rights_summary}**")
    lines.append("")

    lines.append("## 3) 매도가능가(실거래 기반) — 근거")
    lines.append(rationale_table)
    lines.append("")
    lines.append("## 4) 손실0 기준 요약(손실 금지 + 6개월 회전)")
    lines.append(f"- 손실0 상한(기준 매도가 기준): **{fmt_money(loss0)}**")
    lines.append(f"- 추천 입찰가(확률형): **{rec}**")
    lines.append(f"- 대출(감정가 60% 가정): **{fmt_money(loan_amount)}**")
    lines.append("")
    lines.append("## 5) 입찰 전 체크리스트(필수)")
    lines.append("- 매각물건명세서/현황조사서 최종 확인(임차인/점유/특별매각조건)")
    lines.append("- 등기부 최신본 재발급(입찰 직전)")
    lines.append("- 전입세대 열람/확정일자(숨은 점유자/임차)")
    lines.append("- 제시외/불법 증·개축 여부 현장 확인")
    lines.append("- 관리비/체납/공과금 확인")
    return "\n".join(lines)

def main():
    st.set_page_config(page_title="부부 전용 경매 분석", layout="wide")
    init_db()
    settings = get_settings()
    cleanup_uploads(settings["delete_after_days"])
    cleanup_old_cases(settings["case_keep_days"])
    cleanup_old_tx_runs(30)

    require_login()

    # 전역 표 스타일: 모든 표 헤더(항목 제목)를 가운데 정렬
    st.markdown(
        """
        <style>
          :root {
            --aa-text: #111827;
            --aa-border: #111827;
            --aa-table-font: 0.68rem;
            --aa-cell-vpad: 6px;
            --aa-cell-hpad: 10px;
            --aa-header-bg: #f3f4f6;
          }
          @media (prefers-color-scheme: dark) {
            :root {
              --aa-text: #f3f4f6;
              --aa-border: #d1d5db;
              --aa-header-bg: #1f2937;
            }
          }

          /* 표 제목(숫자표 탭 포함) 가독성 강화 */
          div[data-testid="stMarkdownContainer"] h3,
          div[data-testid="stMarkdownContainer"] h4 {
            font-weight: 800 !important;
            color: var(--aa-text) !important;
          }

          /* st.table 테두리/구분선 검정 */
          div[data-testid="stTable"] table {
            border-collapse: collapse !important;
            border: 1px solid var(--aa-border) !important;
          }
          div[data-testid="stTable"] th,
          div[data-testid="stTable"] td {
            border: 1px solid var(--aa-border) !important;
            color: var(--aa-text) !important;
            font-size: var(--aa-table-font) !important;
            line-height: 1.0 !important;
            padding-top: var(--aa-cell-vpad) !important;
            padding-bottom: var(--aa-cell-vpad) !important;
            padding-left: var(--aa-cell-hpad) !important;
            padding-right: var(--aa-cell-hpad) !important;
          }

          div[data-testid="stTable"] th {
            text-align: center !important;
            font-weight: 800 !important;
            color: var(--aa-text) !important;
          }

          /* st.dataframe 헤더/셀 정렬 + 검정선 */
          div[data-testid="stDataFrame"] [role="columnheader"] {
            justify-content: center !important;
            text-align: center !important;
            font-weight: 800 !important;
            border-color: var(--aa-border) !important;
            color: var(--aa-text) !important;
            font-size: var(--aa-table-font) !important;
            min-height: calc(var(--aa-cell-vpad) * 2 + 0.9em) !important;
            padding-top: var(--aa-cell-vpad) !important;
            padding-bottom: var(--aa-cell-vpad) !important;
          }
          div[data-testid="stDataFrame"] [role="gridcell"] {
            text-align: center !important;
            border-color: var(--aa-border) !important;
            color: var(--aa-text) !important;
            font-size: var(--aa-table-font) !important;
            line-height: 1.0 !important;
            min-height: calc(var(--aa-cell-vpad) * 2 + 0.9em) !important;
            padding-top: var(--aa-cell-vpad) !important;
            padding-bottom: var(--aa-cell-vpad) !important;
          }
          div[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {
            border-color: var(--aa-border) !important;
          }

          /* 직접 HTML로 그린 표(한눈에보기/링크표)도 동일 폰트 강제 */
          table.aa-uniform-table {
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
          }
          table.aa-uniform-table th,
          table.aa-uniform-table td {
            border: 1px solid var(--aa-border) !important;
            color: var(--aa-text) !important;
            font-size: var(--aa-table-font) !important;
            line-height: 1.0 !important;
            padding-top: var(--aa-cell-vpad) !important;
            padding-bottom: var(--aa-cell-vpad) !important;
            padding-left: var(--aa-cell-hpad) !important;
            padding-right: var(--aa-cell-hpad) !important;
          }
          table.aa-uniform-table th {
            text-align: center !important;
            font-weight: 800 !important;
            background: var(--aa-header-bg) !important;
          }

          /* 인쇄(PDF 저장) 전용 레이아웃 정리 */
          @media print {
            @page {
              size: A4 portrait;
              margin: 10mm;
            }

            /* 좌측 메뉴/헤더/툴바/상태요소 제거 */
            section[data-testid="stSidebar"],
            div[data-testid="stSidebar"],
            header[data-testid="stHeader"],
            div[data-testid="stToolbar"],
            div[data-testid="stDecoration"],
            div[data-testid="stStatusWidget"] {
              display: none !important;
              visibility: hidden !important;
            }

            /* 본문 폭 강제 확장 */
            section.main > div,
            div[data-testid="stMain"],
            div[data-testid="stMainBlockContainer"],
            .block-container {
              max-width: none !important;
              width: 100% !important;
              padding-left: 0 !important;
              padding-right: 0 !important;
              margin: 0 !important;
            }

            /* 컬럼이 너무 좁아지는 현상 방지: 인쇄 시 세로 스택 */
            div[data-testid="column"] {
              width: 100% !important;
              min-width: 100% !important;
              flex: 0 0 100% !important;
            }

            /* 인터랙티브 입력 위젯은 인쇄 제외 (결과 표/텍스트 중심) */
            div[data-testid="stFileUploader"],
            div[data-testid="stButton"],
            div[data-testid="stDownloadButton"],
            div[data-testid="stNumberInput"],
            div[data-testid="stTextInput"],
            div[data-testid="stTextArea"],
            div[data-testid="stSelectbox"],
            div[data-testid="stMultiSelect"],
            div[data-testid="stDateInput"],
            div[data-testid="stTimeInput"],
            div[data-testid="stSlider"] {
              display: none !important;
            }

            /* 탭/표 인쇄 시 잘림 방지 */
            div[data-testid="stTabs"] {
              overflow: visible !important;
            }
            table {
              page-break-inside: avoid !important;
              break-inside: avoid !important;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.sidebar.title("🏠 경매 분석기")
    page = st.sidebar.radio("메뉴", ["새 분석", "분석 리스트", "실거래 조회", "실거래 리스트"], key="menu_radio")

    
    
    if page == "분석 리스트":
        show_detail_from_list = (
            st.session_state.get("page_override") == "result"
            and st.session_state.get("result_from_list") is True
            and bool(st.session_state.get("open_case_id"))
        )
        if show_detail_from_list:
            page = "새 분석"
        else:
            st.session_state["page_override"] = None
            st.session_state["result_from_list"] = False

            st.title("📚 분석 리스트")
            st.caption("저장된 분석 이력을 최신순으로 확인할 수 있습니다. (30일 보관)")
            c_refresh, c_hint = st.columns([1,5])
            if c_refresh.button("🔄 새로고침"):
                st.rerun()
            c_hint.caption("※ 사건번호를 클릭하면 해당 분석 결과로 이동합니다.")

            try:
                cases = list_cases()
            except Exception as e:
                st.error(f"리스트 로드 오류: {e}")
                return

            if not cases:
                st.info("저장된 분석이 없습니다.")
                return

            df = pd.DataFrame(cases)

            if df.empty:
                st.info("저장된 분석 이력이 없습니다.")
                return

            df["분석일자"] = df["created_at"].apply(format_created_at_local)
            df["주소"] = df["address"].fillna("-")
            df["사건번호(클릭)"] = df["case_no"].fillna("-")
            df["매각기일"] = (df["auction_date"].fillna("-") if "auction_date" in df.columns else "-")
            df["추천입찰가"] = df.get("recommended_bid", "-").fillna("-")

            # 테이블 표시 (사건번호는 버튼 컬럼으로 별도 렌더)
            display_df = df[["분석일자", "사건번호(클릭)", "매각기일", "주소", "추천입찰가", "id"]].copy()

            # 헤더
            header = st.columns([1.2, 1.0, 1.0, 3.8, 1.6])
            header[0].markdown("**분석일자**")
            header[1].markdown("**사건번호**")
            header[2].markdown("**매각기일**")
            header[3].markdown("**주소**")
            header[4].markdown("**추천입찰가**")
            st.divider()

            # 행 렌더 (버튼 클릭 시 내부 이동)
            for _, r in display_df.iterrows():
                row = st.columns([1.2, 1.0, 1.0, 3.8, 1.6])
                row[0].write(r["분석일자"])
                if row[1].button(str(r["사건번호(클릭)"]), key=f"hist_open_{r['id']}"):
                    st.session_state["open_case_id"] = r["id"]
                    st.session_state["page_override"] = "result"
                    st.session_state["result_from_list"] = True
                    st.rerun()
                row[2].write(r["매각기일"])
                row[3].write(r["주소"])
                row[4].write(r["추천입찰가"])

            st.divider()
            return

    if page == "실거래 조회":
        st.title("📈 실거래 조회")
        st.caption("API 기반 조회(시/구 입력 → 동/번지 선택)를 기본으로 사용합니다. 필요시 엑셀 조회도 가능합니다.")
        tab_api, tab_excel = st.tabs(["API 조회", "엑셀 조회"])

        with tab_api:
            st.markdown("#### 🔑 API 키 입력")
            k1, k2 = st.columns(2)
            k1.text_input(
                "VWORLD API Key",
                value=st.session_state.get("tx_api_vworld_key", ""),
                type="password",
                key="tx_api_vworld_key",
                help="동/번지 후보 조회에 사용됩니다.",
            )
            k2.text_input(
                "국토부 실거래 API Key (Decoding)",
                value=st.session_state.get("tx_api_molit_key", ""),
                type="password",
                key="tx_api_molit_key",
                help="실거래 조회에 사용됩니다.",
            )

            s1, s2, s3 = st.columns([1.2, 1.2, 1.0])
            property_type = s1.selectbox("건물 유형", ["아파트", "연립다세대(빌라)"], index=0, key="tx_api_property_type")
            sido = s2.selectbox("시/도", ["서울특별시", "경기도"], index=0, key="tx_api_sido")
            months_back = s3.number_input("조회 개월수", min_value=1, max_value=36, value=12, step=1, key="tx_api_months")

            gugun_options = SIDO_GUGUN_OPTIONS.get(sido, [])
            sigungu = st.selectbox("구/군", gugun_options, index=0 if gugun_options else None, key="tx_api_sigungu")

            if st.button("1) 동/번지 후보 불러오기", key="tx_api_load_lot"):
                if not str(sido).strip() or not str(sigungu).strip():
                    st.warning("시/도와 시/군/구를 입력하세요.")
                else:
                    cand, err = fetch_vworld_lot_candidates(str(sido).strip(), str(sigungu).strip())
                    if err:
                        st.error(err)
                    else:
                        st.session_state["tx_api_candidates"] = cand
                        st.success(f"후보 {len(cand)}건 로드")

            cands = st.session_state.get("tx_api_candidates") or []
            if cands:
                dongs = sorted({c["dong"] for c in cands})
                sel_dong = st.selectbox("동 선택", dongs, key="tx_api_sel_dong")
                bunjis = sorted({c["bunji"] for c in cands if c["dong"] == sel_dong})
                sel_bunji = st.selectbox("번지 선택", bunjis, key="tx_api_sel_bunji")

                if st.button("2) 실거래 조회 실행", key="tx_api_fetch_trades"):
                    pick = next((c for c in cands if c["dong"] == sel_dong and c["bunji"] == sel_bunji), None)
                    if not pick:
                        st.error("선택한 동/번지 후보를 찾지 못했습니다.")
                    else:
                        df_api, err = fetch_molit_trades_by_lot(
                            pnu=pick.get("pnu"),
                            dong=sel_dong,
                            bunji=sel_bunji,
                            months_back=int(months_back),
                            property_type=("아파트" if property_type == "아파트" else "연립다세대"),
                        )
                        if err:
                            st.error(err)
                        else:
                            st.session_state["tx_api_view_df"] = df_api
                            st.session_state["tx_api_query"] = f"{sido} {sigungu} {sel_dong} {sel_bunji}"
                            st.success(f"조회 완료: {len(df_api)}건")

            view_api = st.session_state.get("tx_api_view_df")
            if isinstance(view_api, pd.DataFrame):
                df_show = view_api.copy()
                if "전용면적(㎡)" in df_show.columns:
                    df_show["전용면적(㎡)"] = df_show["전용면적(㎡)"].map(lambda v: f"{float(v):.2f}" if pd.notna(v) else v)
                for cnum in ["거래금액", "면적단가"]:
                    if cnum in df_show.columns:
                        df_show[cnum] = df_show[cnum].map(lambda v: f"{int(v):,}" if pd.notna(v) and str(v).strip() not in ("", "nan") else v)
                st.markdown("#### API 조회 결과")
                st.markdown(df_show.to_html(index=False, classes=["aa-uniform-table"], border=0), unsafe_allow_html=True)

                if st.button("💾 API 조회 저장(실거래 리스트 반영)", key="save_tx_api_run_btn"):
                    import uuid
                    run_id = str(uuid.uuid4())
                    run = {
                        "id": run_id,
                        "created_at": now_local_str(),
                        "created_by": st.session_state.get("user_email"),
                        "title": f"API 실거래 조회 {now_local_str()}",
                        "query": st.session_state.get("tx_api_query") or "",
                        "rows": df_show.to_dict(orient="records"),
                    }
                    try:
                        save_tx_run(run)
                        st.success(f"저장 완료: {run_id[:8]} (실거래 리스트에 반영)")
                    except Exception as e:
                        st.error(f"저장 실패: {e}")

        with tab_excel:
            tx_file = st.file_uploader("실거래 엑셀 업로드", type=["xlsx", "xls"], key="tx_only_uploader")
            if tx_file is None:
                st.info("실거래 엑셀 파일을 업로드하세요.")
            else:
                try:
                    tx_df = parse_comps_view_xlsx(tx_file.getvalue())
                except Exception as e:
                    st.error(f"실거래 엑셀 파싱 실패: {e}")
                    return

                c1, c2, c3 = st.columns([2, 1, 1])
                q_text = c1.text_input("검색(시군구/번지/건물명)", value="", key="tx_query_text")
                area_target = c2.number_input("기준 면적(㎡)", min_value=0.0, value=0.0, step=0.01, key="tx_area_target")
                top_n = c3.number_input("표시 개수", min_value=1, max_value=200, value=30, step=1, key="tx_top_n")

                view = tx_df.copy()
                if q_text.strip():
                    q = q_text.strip()
                    mask = pd.Series(False, index=view.index)
                    for col in ["시군구", "번지", "건물명"]:
                        if col in view.columns:
                            mask = mask | view[col].astype(str).str.contains(q, na=False)
                    view = view[mask]
                if area_target > 0 and "전용면적(㎡)" in view.columns:
                    a = pd.to_numeric(view["전용면적(㎡)"], errors="coerce")
                    view = view[a.between(area_target - 10.0, area_target + 10.0)]
                view = view.head(int(top_n))

                fmt_map = {}
                if "거래금액" in view.columns:
                    fmt_map["거래금액"] = lambda v: f"{int(v):,}" if pd.notna(v) and str(v).strip() not in ("", "nan") else v
                if "면적단가" in view.columns:
                    fmt_map["면적단가"] = lambda v: f"{int(v):,}" if pd.notna(v) and str(v).strip() not in ("", "nan") else v
                if "전용면적(㎡)" in view.columns:
                    fmt_map["전용면적(㎡)"] = lambda v: f"{float(v):.2f}" if pd.notna(v) and str(v).strip() not in ("", "nan") else v
                for col, fn in fmt_map.items():
                    if col in view.columns:
                        view[col] = view[col].map(lambda v: fn(v))

                st.markdown("#### 엑셀 조회 결과")
                st.markdown(view.to_html(index=False, classes=["aa-uniform-table"], border=0), unsafe_allow_html=True)

                if st.button("💾 엑셀 조회 저장(실거래 리스트 반영)", key="save_tx_run_btn"):
                    import uuid
                    run_id = str(uuid.uuid4())
                    run = {
                        "id": run_id,
                        "created_at": now_local_str(),
                        "created_by": st.session_state.get("user_email"),
                        "title": f"실거래 조회 {now_local_str()}",
                        "query": q_text.strip(),
                        "rows": view.to_dict(orient="records"),
                    }
                    try:
                        save_tx_run(run)
                        st.success(f"저장 완료: {run_id[:8]} (실거래 리스트에 반영)")
                    except Exception as e:
                        st.error(f"저장 실패: {e}")
        return

    if page == "실거래 리스트":
        st.title("🗂️ 실거래 리스트")
        st.caption("저장된 실거래 조회 이력을 확인할 수 있습니다. (30일 보관)")

        try:
            runs = list_tx_runs()
        except Exception as e:
            st.error(f"리스트 로드 오류: {e}")
            return
        if not runs:
            st.info("저장된 실거래 조회 이력이 없습니다.")
            return

        df_runs = pd.DataFrame(runs)
        df_runs["저장일시"] = df_runs["created_at"].apply(format_created_at_local)
        df_runs["검색어"] = df_runs["query"].fillna("")
        df_runs["건수"] = df_runs["count"].fillna(0)

        q = st.text_input("리스트 찾기(검색어/제목)", value="", key="tx_list_search")
        if q.strip():
            mask = (
                df_runs["title"].astype(str).str.contains(q, na=False)
                | df_runs["검색어"].astype(str).str.contains(q, na=False)
            )
            df_runs = df_runs[mask]
        if df_runs.empty:
            st.info("검색 결과가 없습니다.")
            return

        page_size = 20
        total = len(df_runs)
        pages = max(1, (total + page_size - 1) // page_size)
        p = st.number_input("페이지", min_value=1, max_value=pages, value=1, step=1, key="tx_list_page")
        start = (int(p) - 1) * page_size
        end = start + page_size
        view = df_runs.iloc[start:end]

        h = st.columns([1.8, 3.0, 1.0, 1.2])
        h[0].markdown("**저장일시**")
        h[1].markdown("**제목/검색어**")
        h[2].markdown("**건수**")
        h[3].markdown("**열기**")
        st.divider()

        for _, r in view.iterrows():
            row = st.columns([1.8, 3.0, 1.0, 1.2])
            row[0].write(r["저장일시"])
            row[1].write(f"{r['title']} / {r['검색어']}")
            row[2].write(str(int(r["건수"])))
            if row[3].button("보기", key=f"open_tx_{r['id']}"):
                st.session_state["open_tx_run_id"] = r["id"]
                st.rerun()

        open_tx_id = st.session_state.get("open_tx_run_id")
        if open_tx_id:
            run = get_tx_run(open_tx_id)
            if run:
                st.markdown("---")
                st.subheader(f"📄 {run.get('title')}")
                st.caption(f"검색어: {run.get('query') or '-'}")
                rows = run.get("rows") or []
                if rows:
                    df_show = pd.DataFrame(rows)
                    st.markdown(df_show.to_html(index=False, classes=["aa-uniform-table"], border=0), unsafe_allow_html=True)
                else:
                    st.info("저장된 행 데이터가 없습니다.")
        return
    st.title("🧾 새 경매 물건 분석")
    st.caption("PDF/실거래 엑셀을 올리고, 가정값을 조정한 뒤 [분석 실행]을 누르세요.")

    left, right = st.columns([1,1])
    with left:
        auction_pdf = st.file_uploader("1) 경매 물건 PDF 업로드", type=["pdf"])
        comps_xlsx = st.file_uploader("2) 실거래 엑셀 업로드", type=["xlsx","xls"])
        st.markdown("#### 3) 평면도 업로드(선택)")
        floorplan_img = st.file_uploader("평면도 파일 업로드", type=["png","jpg","jpeg"], help="드래그&드롭 가능")
        st.caption("맥 스크린샷(Shift+Cmd+4) 후 우측 하단 썸네일을 **이 업로드 영역으로 드래그&드롭**하면 매우 빠릅니다. (브라우저 보안상 Ctrl+V 붙여넣기 업로드는 기본 Streamlit만으로 안정적으로 지원되지 않습니다)")
        floorplan_bytes = floorplan_img.getvalue() if floorplan_img is not None else None
        floorplan_name = floorplan_img.name if floorplan_img is not None else None
        if floorplan_bytes:
            st.image(floorplan_bytes, caption="평면도 미리보기(썸네일)", width=260)
        # 링크 입력란 제거(주소 기반 자동 생성)
        links = ""

    with right:
        st.subheader("4) 가정값(수정 가능)")
        interest_rate = st.number_input("금리(연)", min_value=0.0, max_value=50.0, value=5.0, step=0.1) / 100.0
        holding_days = st.number_input("보유기간(일)", min_value=1, max_value=3650, value=90, step=1)
        repair_cost = st.number_input("수리비(원)", min_value=0, value=3_000_000, step=100_000)
        eviction_cost = st.number_input("명도비(원)", min_value=0, value=2_000_000, step=100_000)
        early_repay_fee_rate = st.number_input("중도상환수수료율(%)", min_value=0.0, max_value=10.0, value=1.2, step=0.1) / 100.0
        tax_rate = st.number_input("취득세 등율(%) 가정", min_value=0.0, max_value=10.0, value=1.1, step=0.1) / 100.0
        st.subheader("5) 시나리오 표 설정")
        bid_step = st.selectbox("입찰가 간격", [1_000_000, 2_000_000, 5_000_000], index=0, format_func=lambda x: f"{x//10_000}만원")

    if st.button("📊 분석 실행", type="primary", disabled=(auction_pdf is None or comps_xlsx is None)):
        import uuid
        case_id = str(uuid.uuid4())
        created_at = now_local_str()
        user_email = st.session_state.user_email

        pdf_bytes = auction_pdf.getvalue()
        xlsx_bytes = comps_xlsx.getvalue()

        subject = parse_auction_pdf(pdf_bytes)
        comps = parse_comps_xlsx(xlsx_bytes)
        sale_range = estimate_sale_price_range(comps, subject.get("area_m2"))

        st.session_state["pending"] = {
            "case_id": case_id,
            "created_at": created_at,
            "user_email": user_email,
            "subject": subject,
            "sale_range": sale_range,
            "links": links,
            "assumptions": {
                "interest_rate": interest_rate,
                "holding_days": int(holding_days),
                "repair_cost": int(repair_cost),
                "eviction_cost": int(eviction_cost),
                "early_repay_fee_rate": float(early_repay_fee_rate),
                "tax_rate": float(tax_rate),
                "bid_step": int(bid_step),
            },
            "pdf_bytes": pdf_bytes,
            "xlsx_bytes": xlsx_bytes,
            "pdf_name": auction_pdf.name,
            "xlsx_name": comps_xlsx.name,
            "floorplan_name": floorplan_name,
            "floorplan_bytes": floorplan_bytes,
        }
        st.session_state["page_override"] = "review"
        st.rerun()

    if st.session_state.get("page_override") == "review":
        pending = st.session_state.get("pending")
        if not pending:
            st.warning("대기 중인 분석이 없습니다.")
            return
        st.title("✅ 추출값 검수/수정")
        subj = pending["subject"]

        col1, col2 = st.columns(2)
        with col1:
            case_no = st.text_input("사건번호", value=subj.get("case_no") or "")
            address = st.text_input("주소", value=subj.get("address") or "")
            property_type = st.text_input("물건종별(예: 빌라/아파트)", value="빌라")
            area_m2 = st.number_input("전용면적(㎡)", value=float(subj.get("area_m2") or 0.0), min_value=0.0, step=0.01)
        with col2:
            appraisal = st.number_input("감정가(원)", value=int(subj.get("appraisal") or 0), min_value=0, step=100_000)
            min_price = st.number_input("최저가(원)", value=int(subj.get("min_price") or 0), min_value=0, step=100_000)
            auction_date = st.text_input("매각기일(YYYY.MM.DD)", value=subj.get("auction_date") or "")
            base_right = st.text_input("말소기준(있으면)", value=subj.get("base_right") or "")

        st.caption("PDF에서 자동 추출한 텍스트 일부(참고)")
        st.code(clean_extracted_snippet(subj.get("raw_text_snippet") or ""), language="text")

        if st.button("🚀 최종 분석 생성", type="primary"):
            import uuid
            # 최종 생성 시점마다 새 이력을 남기기 위해 ID를 재발급
            case_id = str(uuid.uuid4())
            finalized_at = now_local_str()

            # 업로드 저장(7일 후 자동 삭제)
            class UF:
                def __init__(self, name, buf): self.name=name; self._buf=buf
                def getbuffer(self): return self._buf
            pdf_path = save_upload(case_id, "auction_pdf", UF(pending["pdf_name"], pending["pdf_bytes"]))
            xlsx_path = save_upload(case_id, "comps_xlsx", UF(pending["xlsx_name"], pending["xlsx_bytes"]))

            
            # 평면도 이미지(선택) 저장
            if pending.get("floorplan_bytes") and pending.get("floorplan_name"):
                floorplan_path = save_upload(case_id, "floorplan_img", UF(pending["floorplan_name"], pending["floorplan_bytes"]))
            else:
                floorplan_path = None
            loan_amount = int(appraisal * 0.60)
            sr = pending["sale_range"].copy()

            # 실거래 표(원본) 샘플을 함께 저장(보기 좋게 출력용)
            comps_raw = pd.read_excel(io.BytesIO(pending["xlsx_bytes"]))
            # 유사면적 ±10㎡ 필터
            try:
                sa = float(area_m2) if area_m2 else None
            except Exception:
                sa = None
            comps_view = comps_raw.copy()
            if sa is not None and "전용면적(㎡)" in comps_view.columns:
                comps_view = comps_view[pd.to_numeric(comps_view["전용면적(㎡)"], errors="coerce").between(sa-10, sa+10)]
            if "층" in comps_view.columns:
                _floor_num = pd.to_numeric(comps_view["층"], errors="coerce")
                comps_view = comps_view[_floor_num.ne(-1) | _floor_num.isna()]
            keep_cols = [c for c in ["계약년월","시군구","번지","건물명","전용면적(㎡)","거래금액","면적단가","층","건축년도"] if c in comps_view.columns]
            comps_view = comps_view[keep_cols].head(30)

            if sr.get("low") is None:
                st.error("실거래 엑셀에서 유사표본 매도가능가를 산출하지 못했습니다. 엑셀 컬럼을 확인해주세요.")
                st.stop()

            sale_prices = [int(sr["low"]), int(sr["mid"]), int(sr["high"])]
            bid_start = int(min_price) if int(min_price) > 0 else int(appraisal * 0.80)
            bid_end = (bid_start + 40_000_000) if int(min_price) <= 0 else int(min_price + 40_000_000)
            bid_step = int(pending["assumptions"]["bid_step"])

            df_matrix, cost_info = build_profit_matrix(
                sale_prices, bid_start, bid_end, bid_step,
                float(pending["assumptions"]["tax_rate"]),
                loan_amount,
                float(pending["assumptions"]["interest_rate"]),
                int(pending["assumptions"]["holding_days"]),
                float(pending["assumptions"]["early_repay_fee_rate"]),
                int(pending["assumptions"]["repair_cost"]),
                int(pending["assumptions"]["eviction_cost"]),
            )

            mid_col = f"매도가 {sale_prices[1]/100_000_000:.2f}억"
            loss0_max_bid = None
            ok = df_matrix[df_matrix[mid_col] >= 0]
            if len(ok) > 0:
                loss0_max_bid = int(ok["입찰가"].max())

            recommended_bid = None
            if loss0_max_bid:
                lo = int(loss0_max_bid * 0.97)
                hi = int(loss0_max_bid * 0.99)
                def round_step(x): return int(round(x / bid_step) * bid_step)
                recommended_bid = f"{round_step(lo):,} ~ {round_step(hi):,}원"

                # 한줄 결론(진행/보류/비추천) - 화면 배너용
                verdict = "보류"
                verdict_reason = []

                if int(min_price) <= 0:
                    verdict = "보류"
                    verdict_reason.append("최저가 미추출(0원) → 최저가 수동 입력 후 재분석 필요")
                else:
                    if loss0_max_bid and loss0_max_bid >= int(min_price):
                        verdict = "진행 가능(조건부)"
                        verdict_reason.append("손실0 상한이 최저가 이상(손실 금지 조건 충족)")
                    else:
                        verdict = "보류/비추천"
                        verdict_reason.append("손실0 상한이 최저가 미만(손실 금지 조건 불충족)")

                    sh = (subj.get("special_hint") or "")
                    if "제시외" in sh:
                        verdict_reason.append("제시외 건물 가능성 → 원상복구/민원 리스크 확인 필요")
                    if "중복" in sh:
                        verdict_reason.append("중복사건(정지) 표기 → 입찰 직전 사건 진행상태 재확인")

            outputs = {
                "sale_range": sr,
                "sale_prices": sale_prices,
                "matrix": df_matrix.to_dict(orient="records"),
                "cost_info": cost_info,
                "loan_amount": loan_amount,
                "loss0_max_bid": loss0_max_bid,
                "verdict": verdict,
                "verdict_reason": verdict_reason,
                "recommended_bid": recommended_bid,
                "matrix_cols": list(df_matrix.columns),
                "bid_range": {"start": bid_start, "end": bid_end, "step": bid_step},
    "comps_sample": comps_view.to_dict(orient="records"),
    "subject_snapshot": {
        "case_no": case_no or None,
        "related_case": subj.get("related_case"),
        "address": address or None,
        "property_type": property_type or None,
        "area_m2": float(area_m2) if area_m2 else None,
        "appraisal": int(appraisal) if appraisal else None,
        "min_price": int(min_price) if min_price else None,
        "min_price_pct": subj.get("min_price_pct"),
        "current_round": subj.get("current_round"),
        "prior_unsold_count": subj.get("prior_unsold_count"),
        "auction_date": auction_date or None,
        "base_right": base_right or None,
        "occupancy_hint": subj.get("occupancy_hint"),
        "special_hint": subj.get("special_hint"),
        "rights_summary": subj.get("rights_summary"),
        "rights_rows": subj.get("rights_rows") or [],
    },
}


            assumptions = pending["assumptions"].copy()
            assumptions.update({"loan_amount": loan_amount, "appraisal": appraisal, "min_price": min_price})

            report_md = generate_report_stub(
                {
                    "case_no": case_no,
                    "related_case": subj.get("related_case"),
                    "address": address,
                    "area_m2": area_m2,
                    "appraisal": appraisal,
                    "min_price": min_price,
                    "auction_date": auction_date or subj.get("auction_date"),
                    "base_right": base_right or subj.get("base_right"),
                    "occupancy_hint": subj.get("occupancy_hint"),
                    "special_hint": subj.get("special_hint"),
                    "rights_rows": subj.get("rights_rows"),
                    "rights_summary": subj.get("rights_summary"),
                },
                sr, outputs, assumptions
            )

            case = {
                "id": case_id,
                "created_at": finalized_at,
                "created_by": pending["user_email"],
                "status": "DONE",
                "case_no": case_no or None,
                "address": address or None,
                "property_type": property_type or None,
                "area_m2": float(area_m2) if area_m2 else None,
                "appraisal": int(appraisal) if appraisal else None,
                "min_price": int(min_price) if min_price else None,
        "min_price_pct": subj.get("min_price_pct"),
        "current_round": subj.get("current_round"),
        "prior_unsold_count": subj.get("prior_unsold_count"),
                "auction_date": auction_date or None,
                "links": {
                    "raw": pending.get("links") or "",
                    "floorplan_path": floorplan_path,
                    "auction_pdf_path": pdf_path,
                    "auction_pdf_name": pending.get("pdf_name") or "auction.pdf",
                },
                "inputs": assumptions,
                "outputs": outputs,
                "report_md": report_md,
            }
            save_case(case)
            saved_case = get_case(case_id)
            if not saved_case:
                st.error("저장은 시도되었지만 DB 재조회 확인에 실패했습니다. 다시 시도해주세요.")
                st.stop()
            st.session_state["open_case_id"] = case_id
            st.session_state["last_saved_case_id"] = case_id
            st.session_state["page_override"] = "result"
            st.session_state["result_from_list"] = False
            st.success(f"분석 결과가 저장되었습니다. (ID: {case_id[:8]})")
            st.rerun()
        st.stop()

    if st.session_state.get("page_override") == "result":
        case_id = st.session_state.get("open_case_id")
        c = get_case(case_id) if case_id else None
        if not c:
            st.warning("결과를 불러올 수 없습니다.")
            return
        st.title("📌 분석 결과")
        if st.session_state.get("result_from_list") is True:
            if st.button("← 분석 리스트로 돌아가기"):
                st.session_state["page_override"] = None
                st.session_state["result_from_list"] = False
                st.rerun()
        # =============================
        # 📌 기본 물건 정보(상단 고정)
        # =============================
        outputs = c.get("outputs") or {}
        snap = outputs.get("subject_snapshot") or {}

        case_no = (snap.get("case_no") or c.get("case_no") or "-")
        auction_date = (snap.get("auction_date") or c.get("auction_date") or "-")
        address = (snap.get("address") or c.get("address") or "-")
        appraisal = snap.get("appraisal") if snap.get("appraisal") is not None else c.get("appraisal")
        min_price = snap.get("min_price") if snap.get("min_price") is not None else c.get("min_price")

        prior_unsold = snap.get("prior_unsold_count")
        try:
            prior_unsold = int(prior_unsold) if prior_unsold is not None else None
        except Exception:
            prior_unsold = None

        min_pct = snap.get("min_price_pct")
        try:
            min_pct = float(min_pct) if min_pct is not None else None
        except Exception:
            min_pct = None
        if min_pct is None and appraisal and min_price:
            try:
                min_pct = round((float(min_price) / float(appraisal)) * 100.0, 1)
            except Exception:
                min_pct = None

        deposit = int(min_price * 0.10) if min_price else None

        r1, r2, r3 = st.columns(3)
        r1.metric("경매번호", case_no)
        r2.metric("매각기일", auction_date)
        delta_txt = []
        if prior_unsold is not None:
            delta_txt.append(f"{prior_unsold}회 유찰")
        if min_pct is not None:
            delta_txt.append(f"감정가 대비 {min_pct}%")
        r3.metric("최저매각가", fmt_money(min_price), delta=(" / ".join(delta_txt) if delta_txt else None))

        r4, r5, r6 = st.columns(3)
        r4.metric("감정가", fmt_money(appraisal))
        r5.metric("입찰보증금(10%)", fmt_money(deposit))
        r6.metric("전용면적", f"{fmt_area(snap.get('area_m2') or c.get('area_m2'))} ㎡")

        st.write(f"**소재지:** {address}")
        st.divider()

        verdict = (c.get("outputs") or {}).get("verdict") or "-"

        reasons = (c.get("outputs") or {}).get("verdict_reason") or []


        # 눈에 잘 들어오는 결론 배너

        if "진행" in verdict:

            st.success(f"✅ 결론: {verdict}")

        elif "비추천" in verdict:

            st.error(f"⛔ 결론: {verdict}")

        else:

            st.warning(f"⚠️ 결론: {verdict}")


        if reasons:

            st.caption("사유: " + " / ".join(reasons))

        # 지도/참고 링크(자동)
        st.subheader("지도/참고 링크")
        addr = (c.get("address") or "").strip()
        links_obj = c.get("links") if isinstance(c.get("links"), dict) else {}
        pdf_path = (links_obj.get("auction_pdf_path") or "").strip() if isinstance(links_obj, dict) else ""
        pdf_name = (links_obj.get("auction_pdf_name") or "auction.pdf").strip() if isinstance(links_obj, dict) else "auction.pdf"
        if (not pdf_path) and c.get("id"):
            # 구버전 호환: uploads 테이블에서 경매 PDF 경로 조회
            try:
                con = sqlite3.connect(DB_PATH)
                cur = con.cursor()
                cur.execute(
                    """
                    SELECT storage_path
                    FROM uploads
                    WHERE case_id=? AND file_type='auction_pdf' AND deleted_at IS NULL
                    ORDER BY uploaded_at DESC
                    LIMIT 1
                    """,
                    (c.get("id"),),
                )
                row = cur.fetchone()
                con.close()
                if row and row[0]:
                    pdf_path = str(row[0]).strip()
            except Exception:
                pass
        if addr:
            # 네이버 지도 검색 링크(주소 기반)
            naver = f"https://map.naver.com/v5/search/{quote(addr)}"
            link_items = [
                ("🔎 네이버 지도에서 위치 보기", naver),
                ("🏢 부동산플래닛 물건 검색", "https://property.bdsplanet.com/main"),
            ]

            # 네이버 매물 정보(동 + 건물명 + 분양) 검색 링크
            dong_m = re.search(r"([가-힣0-9]+동)", addr)
            bld_m = re.search(
                r"([가-힣A-Za-z0-9]+(?:아파트|오피스텔|빌라|주택|타운|캐슬|하우스|맨션|빌|월드빌|스위트빌|파크빌|하이츠))",
                addr,
            )
            naver_terms = []
            if dong_m:
                naver_terms.append(dong_m.group(1))
            if bld_m:
                naver_terms.append(bld_m.group(1))
            naver_terms.append("분양")
            naver_item_q = " ".join([t for t in naver_terms if t]).strip()
            if naver_item_q:
                naver_item = f"https://search.naver.com/search.naver?query={quote(naver_item_q)}"
                link_items.append(("🏠 네이버 매물 정보", naver_item))

            link_items.append(("🏗️ 재개발 검색", "https://jaegebal.com/"))
            if pdf_path and Path(pdf_path).exists():
                link_items.append(("📄 경매 PDF 열기", f"file://{pdf_path}"))

            # 2단 표 형태로 링크 출력
            rows = []
            for i in range(0, len(link_items), 2):
                left = link_items[i]
                right = link_items[i + 1] if i + 1 < len(link_items) else ("", "")
                left_html = f'<a href="{left[1]}" target="_blank">{left[0]}</a>' if left[0] else ""
                right_html = f'<a href="{right[1]}" target="_blank">{right[0]}</a>' if right[0] else ""
                rows.append(
                    "<tr>"
                    f"<td style='width:50%; text-align:center;'>{left_html}</td>"
                    f"<td style='width:50%; text-align:center;'>{right_html}</td>"
                    "</tr>"
                )

            table_html = (
                "<table class='aa-uniform-table'>"
                + "".join(rows)
                + "</table>"
            )
            st.markdown(table_html, unsafe_allow_html=True)
            if pdf_path and Path(pdf_path).exists():
                try:
                    with open(pdf_path, "rb") as f:
                        st.download_button(
                            "📥 경매 PDF 다시 다운로드",
                            data=f.read(),
                            file_name=(pdf_name or "auction.pdf"),
                            mime="application/pdf",
                            key=f"dl_pdf_{c.get('id')}",
                        )
                except Exception:
                    pass
        else:
            st.caption("주소를 추출하지 못해 지도 링크를 생성할 수 없습니다.")

        # (이전 버전 호환) 과거에 저장된 링크가 있으면 함께 표시하고, 좌표가 있으면 지도 표시
        raw_links = (c.get("links") or {}).get("raw") if isinstance(c.get("links"), dict) else None
        links_list = parse_links(raw_links or "")
        if links_list:
            for u in links_list[:5]:
                st.markdown(f"- {u}")
            latlon = None
            for u in links_list:
                latlon = extract_latlon_from_link(u)
                if latlon:
                    break
            if latlon:
                lat, lon = latlon
                st.caption("※ 링크에서 위·경도를 추출해 대략 위치를 표시합니다.")
                st.map(pd.DataFrame([{"lat": lat, "lon": lon}]))

        st.divider()

        tab1, tab2 = st.tabs(["숫자표(엑셀형)", "GPT 의견(리포트)"])
        outputs = c.get("outputs") or {}
        with tab1:
            snap = outputs.get("subject_snapshot") or {}
            st.subheader("한눈에 보기(요약)")

            # =============================

            # 💸 낙찰 비용 요약(간이)

            # =============================

            try:
                dep = int((snap.get("min_price") or 0) * 0.10)
            except Exception:
                dep = None

            summary_left = [
                {"항목": "사건번호", "내용": snap.get("case_no") or "-"},
                {"항목": "감정가", "내용": fmt_money(snap.get("appraisal"))},
                {"항목": "최저가", "내용": fmt_money(snap.get("min_price"))},
                {"항목": "보증금(10%)", "내용": fmt_money(dep)},
                {"항목": "주소", "내용": snap.get("address") or "-"},
            ]
            summary_right = [
                {"항목": "관련사건(중복)", "내용": snap.get("related_case") or "-"},
                {"항목": "전용면적", "내용": f"{fmt_area(snap.get('area_m2'))} ㎡"},
                {"항목": "매각기일", "내용": snap.get("auction_date") or "-"},
                {"항목": "말소기준", "내용": snap.get("base_right") or "-"},
                {"항목": "등기 요약", "내용": snap.get("rights_summary") or "-"},
                {"항목": "점유/임차", "내용": snap.get("occupancy_hint") or "-"},
                {"항목": "특이사항", "내용": snap.get("special_hint") or "-"},
            ]

            def _summary_table_html(rows):
                trs = []
                for row in rows:
                    k = str(row.get("항목", "-"))
                    v = str(row.get("내용", "-"))
                    trs.append(
                        f"<tr><td style='text-align:center;'>{k}</td>"
                        f"<td style='text-align:center; white-space:nowrap;word-break:keep-all;'>{v}</td></tr>"
                    )
                return (
                    "<table class='aa-uniform-table'>"
                    "<colgroup>"
                    "<col style='width:36%;'>"
                    "<col style='width:64%;'>"
                    "</colgroup>"
                    "<thead><tr>"
                    "<th>항목</th>"
                    "<th>내용</th>"
                    "</tr></thead>"
                    f"<tbody>{''.join(trs)}</tbody></table>"
                )

            def _uniform_df_table_html(
                df: pd.DataFrame,
                show_index: bool = False,
                highlight_col: str | None = None,
                highlight_values: set | None = None,
                col_widths: dict | None = None,
                right_align_cols: set | None = None,
                center_align_cols: set | None = None,
                no_wrap_cols: set | None = None,
            ):
                d = df.copy()
                headers = list(d.columns)
                thead = "<tr>"
                if show_index:
                    thead += "<th style='width:44px;'></th>"
                for h in headers:
                    width_css = ""
                    if col_widths and h in col_widths:
                        width_css = f" style='width:{col_widths[h]};'"
                    thead += f"<th{width_css}>{html.escape(str(h))}</th>"
                thead += "</tr>"

                body_rows = []
                for i, (_, row) in enumerate(d.iterrows()):
                    hl = False
                    if highlight_col and highlight_values and highlight_col in d.columns:
                        hl = str(row.get(highlight_col, "")) in highlight_values
                    tr_style = " style='background-color: rgba(255,215,0,0.20); font-weight:700;'" if hl else ""
                    tds = ""
                    if show_index:
                        tds += f"<td>{i}</td>"
                    for col in headers:
                        cell_styles = []
                        if right_align_cols and col in right_align_cols:
                            cell_styles.append("text-align:right")
                        elif center_align_cols and col in center_align_cols:
                            cell_styles.append("text-align:center")
                        if no_wrap_cols and col in no_wrap_cols:
                            cell_styles.append("white-space:nowrap")
                        style_attr = f" style='{'; '.join(cell_styles)}'" if cell_styles else ""
                        tds += f"<td{style_attr}>{html.escape(str(row.get(col, '-')))}</td>"
                    body_rows.append(f"<tr{tr_style}>{tds}</tr>")

                return (
                    "<table class='aa-uniform-table'>"
                    f"<thead>{thead}</thead>"
                    f"<tbody>{''.join(body_rows)}</tbody>"
                    "</table>"
                )

            s1, s2 = st.columns(2)
            with s1:
                st.markdown(_summary_table_html(summary_left), unsafe_allow_html=True)
            with s2:
                st.markdown(_summary_table_html(summary_right), unsafe_allow_html=True)



            st.subheader('🗺️ 평면도')
            fp = (c.get('links') or {}).get('floorplan_path') if isinstance(c.get('links'), dict) else None
            if fp:
                try:
                    with open(fp, 'rb') as _f:
                        _img = _f.read()
                    mime = mimetypes.guess_type(fp)[0] or "image/jpeg"
                    b64 = base64.b64encode(_img).decode("ascii")
                    data_url = f"data:{mime};base64,{b64}"
                    modal_id = f"fp_modal_{str(c.get('id') or 'default').replace('-', '')}"
                    st.markdown(
                        f"""
                        <style>
                          .{modal_id}-overlay {{
                            display: none;
                            position: fixed;
                            inset: 0;
                            background: rgba(0, 0, 0, 0.62);
                            z-index: 99999;
                            align-items: center;
                            justify-content: center;
                            padding: 20px;
                          }}
                          .{modal_id}-overlay:target {{
                            display: flex;
                          }}
                          .{modal_id}-dialog {{
                            width: min(1100px, 94vw);
                            max-height: 92vh;
                            overflow: auto;
                            background: #ffffff;
                            border-radius: 12px;
                            padding: 12px;
                            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.35);
                          }}
                          .{modal_id}-toolbar {{
                            display: flex;
                            justify-content: flex-end;
                            margin-bottom: 8px;
                          }}
                          .{modal_id}-close {{
                            background: #111827;
                            color: #ffffff;
                            text-decoration: none;
                            padding: 6px 12px;
                            border-radius: 8px;
                            font-size: 0.9rem;
                          }}
                          .{modal_id}-img {{
                            width: 100%;
                            border-radius: 8px;
                            display: block;
                          }}
                        </style>
                        <a href="#{modal_id}" style="display:inline-block;">
                          <img src="{data_url}" alt="평면도 썸네일"
                               style="width:260px; border-radius:10px; cursor:zoom-in; display:block;" />
                        </a>
                        <div style="font-size:0.9rem; opacity:0.8; margin-top:4px;">
                          평면도(썸네일 클릭 시 팝업 확대)
                        </div>
                        <div id="{modal_id}" class="{modal_id}-overlay">
                          <div class="{modal_id}-dialog">
                            <div class="{modal_id}-toolbar">
                              <a href="#" class="{modal_id}-close">닫기</a>
                            </div>
                            <img src="{data_url}" alt="평면도 확대" class="{modal_id}-img" />
                          </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                except Exception:
                    st.info('평면도 파일을 불러오지 못했습니다.')
            else:
                st.caption('평면도(선택) 업로드가 없습니다.')


            st.markdown('---')

            st.subheader("💸 낙찰 비용 요약(간이)")
            inp = c.get("inputs") or {}

            rec_default_low = parse_recommended_low(outputs.get("recommended_bid"))
            base_win = int(rec_default_low or snap.get("min_price") or 0) if isinstance(snap, dict) else int(c.get("min_price") or 0)

            win_price = st.number_input(
                "낙찰가(입찰가) 가정(원) - 추천입찰가 하단값 기본",
                min_value=0,
                value=int(base_win),
                step=100_000,
                key="win_price_assumed",
            )

            appraisal_val = int(snap.get("appraisal") or 0) if isinstance(snap, dict) else int(c.get("appraisal") or 0)

            loan_amount = int(appraisal_val * 0.60) if appraisal_val else 0

            round_info = infer_round_and_unsold(appraisal_val, win_price)

            deposit = int(round(win_price * 0.10)) if win_price else 0

            # 대표님 기준: 낙찰 잔금(자기자본) = 낙찰가 - 대출액
            balance = int(max(0, win_price - loan_amount)) if win_price else 0
            # 추가 납부 잔금(보증금 제외) = (낙찰가-대출) - 보증금
            extra_balance = int(max(0, balance - deposit)) if win_price else 0

            taxes = calc_auction_taxes(win_price)

            legal_fee = 1_000_000
            inp = c.get("inputs") or {}
            repair_cost = int(inp.get('repair_cost') or 0)
            eviction_cost = int(inp.get('eviction_cost') or 0)

            inp = c.get("inputs") or {}

            repair_cost = int(inp.get("repair_cost") or 0)

            eviction_cost = int(inp.get("eviction_cost") or 0)

            misc_after = legal_fee + repair_cost + eviction_cost

            total_needed = deposit + extra_balance + taxes["total"] + misc_after

            cash_needed_with_loan = int(balance + taxes["total"] + misc_after)

            # 큰 숫자 metric 대신 표로 요약(가독성)

            summary_rows = [

                {"항목": "입찰 보증금(10%)", "금액": fmt_money(deposit)},

                {"항목": "대출액(감정가 60% 가정)", "금액": fmt_money(loan_amount)},

                {"항목": "추가 납부 잔금(보증금 제외)", "금액": fmt_money(extra_balance)},

                {"항목": "낙찰 세금 합계", "금액": fmt_money(taxes['total'])},

                {"항목": "낙찰 후 경비(법무+수리+명도)", "금액": fmt_money(misc_after)},

                {"항목": "현금 필요액(대출 반영)", "금액": fmt_money(cash_needed_with_loan)},

            ]

            st.markdown(
                _uniform_df_table_html(
                    pd.DataFrame(summary_rows),
                    show_index=True,
                    right_align_cols={"금액"},
                ),
                unsafe_allow_html=True,
            )

            st.caption(f"대출(감정가 60% 가정): {fmt_money(loan_amount)} / 현금 필요액(대출 반영): {fmt_money(cash_needed_with_loan)}")

            st.caption(f"최저매각가 정보(추정): {round_info.get('round') or '-'}차 / 유찰 {round_info.get('unsold') or 0}회 / 감정가 대비 {round_info.get('pct') or '-'}% / 할인 {round_info.get('discount_pct') or '-'}%")

            df_tax = pd.DataFrame([

                {"항목":"취득/등록세(낙찰가 1%)", "금액": taxes["acq_tax"]},

                {"항목":"지방교육세(취득/등록세의 10%)", "금액": taxes["bond_cert"]},

                {"항목":"국민주택채권 할인비용(고정)", "금액": taxes["bond_discount"]},

                {"항목":"등록면허세(고정)", "금액": taxes["reg_license"]},

                {"항목":"합계", "금액": taxes["total"]},

            ])

            df_tax_disp = df_tax.copy()
            df_tax_disp["금액"] = df_tax_disp["금액"].map(lambda x: f"{int(x):,}원")
            df_need = pd.DataFrame([

                {"항목":"입찰 보증금(10%)", "금액": deposit},
                {"항목":"추가 납부 잔금(보증금 제외)", "금액": extra_balance},
                {"항목":"낙찰 세금", "금액": taxes["total"]},
                {"항목":"낙찰 후 기타경비(법무+수리+명도)", "금액": misc_after},
                {"항목":"합계(참고)", "금액": total_needed},

            ])

            df_need_disp = df_need.copy()
            df_need_disp["금액"] = df_need_disp["금액"].map(lambda x: f"{int(x):,}원")
            # 요청사항: 두 표를 직렬이 아닌 병렬 배치
            ct1, ct2 = st.columns(2)
            with ct1:
                st.markdown("#### 낙찰 세금(간이) 상세")
                st.markdown(
                    _uniform_df_table_html(
                        df_tax_disp,
                        show_index=False,
                        right_align_cols={"금액"},
                        highlight_col="항목",
                        highlight_values={"합계", "합계(참고)"},
                    ),
                    unsafe_allow_html=True,
                )
            with ct2:
                st.markdown("#### 취득 필요자금(요약)")
                st.markdown(
                    _uniform_df_table_html(
                        df_need_disp,
                        show_index=False,
                        right_align_cols={"금액"},
                        highlight_col="항목",
                        highlight_values={"합계", "합계(참고)"},
                    ),
                    unsafe_allow_html=True,
                )

            st.markdown("---")
            # =============================
            # 📊 매도 이익 시뮬레이션(3/6개월)
            # =============================
            st.subheader("📊 매도 이익 시뮬레이션(3/6개월, 매매희망가 기준)")
            st.caption("대표님 표 형식으로 3개월/6개월 매도 시 비용과 매도 이익을 비교합니다.")
            
            # 입력값(요청사항): 2단 배치 + 표 연동
            sr = outputs.get('sale_range') or {}
            default_sale = int(sr.get('mid') or 0)
            default_win = int(st.session_state.get("win_price_assumed", int(snap.get("min_price") or 0)))
            default_repair = int(inp.get("repair_cost") or 0)
            default_eviction = int(inp.get("eviction_cost") or 0)

            sim_c1, sim_c2 = st.columns(2)
            with sim_c1:
                win_price = st.number_input(
                    "낙찰가(매입가) 가정(원)",
                    min_value=0,
                    value=int(default_win),
                    step=100_000,
                    key=f"sim_win_price_{c.get('id')}",
                )
                broker_rate = st.number_input(
                    "양도시 부동산중개료율(%)",
                    min_value=0.0,
                    max_value=2.0,
                    value=0.40,
                    step=0.05,
                    key=f"sim_broker_rate_{c.get('id')}",
                ) / 100.0
                repair_cost = st.number_input(
                    "수리비(원)",
                    min_value=0,
                    value=int(default_repair),
                    step=100_000,
                    key=f"sim_repair_cost_{c.get('id')}",
                )
            with sim_c2:
                sale_price = st.number_input(
                    "매도가(매매희망가) 가정(원)",
                    min_value=0,
                    value=int(default_sale),
                    step=100_000,
                    key=f"sim_sale_price_{c.get('id')}",
                )
                cap_tax_rate = st.number_input(
                    "양도세 실효세율(%) 가정",
                    min_value=0.0,
                    max_value=80.0,
                    value=35.0,
                    step=1.0,
                    key=f"sim_cap_tax_rate_{c.get('id')}",
                ) / 100.0
                eviction_cost = st.number_input(
                    "명도비(원)",
                    min_value=0,
                    value=int(default_eviction),
                    step=100_000,
                    key=f"sim_eviction_cost_{c.get('id')}",
                )
            
            taxes_sim = calc_auction_taxes(int(win_price))

            # 낙찰 후 비용 소계 = 낙찰 세금 + (법무+수리+명도)
            post_cost_subtotal = int(taxes_sim['total'] + repair_cost + eviction_cost + legal_fee)
            early_fee = int(round(loan_amount * float(inp.get('early_repay_fee_rate', 0.0))))
            
            def build_sale_table(months: int):
                gross_profit = int(sale_price - win_price)  # 1. 양도 단순이익
                post_cost = int(post_cost_subtotal)         # 2. 낙찰 후 비용 소계
                broker_fee = int(round(sale_price * broker_rate))  # 3. 중개료
                interest = int(round(loan_amount * float(inp.get('interest_rate', 0.0)) * (months/12.0)))  # 4. 대출이자
                taxable_base = max(0, gross_profit - post_cost - broker_fee - interest)  # 엑셀 동일(양도 단순이익 - 낙찰후비용 - 중개료 - 이자)
                cap_tax = int(round(taxable_base * cap_tax_rate))  # 5. 양도세(가정)
                early = int(early_fee)  # 6. 중도상환수수료
                net = int(gross_profit - post_cost - broker_fee - interest - cap_tax - early)  # 매도 이익
                invested = int(max(0, win_price - loan_amount) + post_cost)  # 투자금(자기자본+낙찰후비용)
                roi = (net / invested) if invested > 0 else None
                rows = [
                    {'항목':'양도 단순이익', '금액': gross_profit, '비고':''},
                    {'항목':'낙찰 후 비용 소계', '금액': post_cost, '비고':''},
                            {'항목':'   └ 낙찰 세금(간이)', '금액': taxes_sim['total'], '비고':''},
                            {'항목':'   └ 수리비', '금액': repair_cost, '비고':''},
                            {'항목':'   └ 명도비', '금액': eviction_cost, '비고':''},
                            {'항목':'   └ 법무비(고정)', '금액': legal_fee, '비고':''},
                    {'항목':'양도시 부동산중개료', '금액': broker_fee, '비고': f"{broker_rate*100:.2f}%"},
                    {'항목':'대출이자', '금액': interest, '비고': f"{months}개월"},
                    {'항목':'양도세', '금액': cap_tax, '비고': f"{cap_tax_rate*100:.0f}%"},
                    {'항목':'중도상환수수료', '금액': early, '비고': f"{float(inp.get('early_repay_fee_rate',0.0))*100:.2f}%"},
                    {'항목':'매도 이익', '금액': net, '비고':''},
                    {'항목':'투자 대비 이익률', '금액': roi, '비고':''},
                ]
                df = pd.DataFrame(rows)
                df_disp = df.copy()
                def _fmt(v):
                    if v is None: return '-'
                    if isinstance(v, float) and v < 10: return f"{v*100:.2f}%"
                    return f"{int(round(v)):,}원"
                df_disp['금액'] = df_disp['금액'].apply(_fmt)
                return df_disp
            
            c_m1, c_m2 = st.columns(2)
            with c_m1:
                st.markdown("#### 3개월 이내 매도 시 (매매희망가 기준)")
                df_3 = build_sale_table(3)
                st.markdown(
                    _uniform_df_table_html(
                        df_3,
                        show_index=True,
                        right_align_cols={"금액"},
                        center_align_cols={"비고"},
                        highlight_col="항목",
                        highlight_values={"매도 이익"},
                    ),
                    unsafe_allow_html=True,
                )
            with c_m2:
                st.markdown("#### 6개월 이내 매도 시 (매매희망가 기준)")
                df_6 = build_sale_table(6)
                st.markdown(
                    _uniform_df_table_html(
                        df_6,
                        show_index=True,
                        right_align_cols={"금액"},
                        center_align_cols={"비고"},
                        highlight_col="항목",
                        highlight_values={"매도 이익"},
                    ),
                    unsafe_allow_html=True,
                )
            st.markdown('---')


            rr = snap.get("rights_rows") or []
            if rr:
                st.subheader("등기부현황(파싱)")
                df_rr = pd.DataFrame(rr)
                cols = [c for c in ["date","kind","holder","amount","is_base","status"] if c in df_rr.columns]
                df_rr = df_rr[cols]
                if "amount" in df_rr.columns:
                    df_rr["amount"] = df_rr["amount"].map(lambda v: f"{int(v):,}" if pd.notna(v) else v)
                st.markdown(_uniform_df_table_html(df_rr, show_index=False), unsafe_allow_html=True)

            comps_sample = outputs.get("comps_sample") or []
            if comps_sample:
                st.subheader("실거래(유사면적 ±10㎡, 상위 30)")
                df_c = pd.DataFrame(comps_sample)
                # 지하층(-1) 제외
                if "층" in df_c.columns:
                    _floor_num = pd.to_numeric(df_c["층"], errors="coerce")
                    df_c = df_c[_floor_num.ne(-1) | _floor_num.isna()]
                fmt_map = {}
                if "거래금액" in df_c.columns:
                    fmt_map["거래금액"] = lambda v: f"{int(v):,}" if pd.notna(v) else v
                if "면적단가" in df_c.columns:
                    fmt_map["면적단가"] = lambda v: f"{int(v):,}" if pd.notna(v) else v
                if "전용면적(㎡)" in df_c.columns:
                    fmt_map["전용면적(㎡)"] = lambda v: f"{float(v):.2f}" if pd.notna(v) else v
                if fmt_map:
                    for col, fn in fmt_map.items():
                        if col in df_c.columns:
                            df_c[col] = df_c[col].map(lambda v: fn(v))
                st.markdown(
                    _uniform_df_table_html(
                        df_c,
                        show_index=True,
                        col_widths={
                            "시군구": "220px",
                            "층": "52px",
                        },
                        right_align_cols={"전용면적(㎡)", "거래금액", "면적단가"},
                        center_align_cols={"계약년월", "시군구", "번지", "건물명", "층", "건축년도"},
                        no_wrap_cols={"시군구"},
                    ),
                    unsafe_allow_html=True,
                )

            sr = outputs.get("sale_range") or {}
            st.subheader("매도가능가(실거래 기반)")

            st.write(f"- 기준(중앙값): **{fmt_money(sr.get('mid'))}**")
            st.caption(sr.get("note",""))

            st.subheader("입찰 시나리오(직접 입력)")
            sale_prices = outputs.get("sale_prices") or []
            sale_mid_default = int(sale_prices[1]) if len(sale_prices) >= 2 else int((sr.get("mid") or 0))
            c_in1, c_in2 = st.columns(2)
            with c_in1:
                custom_bid = st.number_input(
                    "입찰가 직접 입력(원)",
                    min_value=0,
                    value=int(win_price),
                    step=100_000,
                    key=f"custom_bid_price_input_{c.get('id')}",
                )
            with c_in2:
                sale_mid = st.number_input(
                    "기준 매도가 직접 입력(원)",
                    min_value=0,
                    value=int(sale_mid_default),
                    step=100_000,
                    key=f"custom_sale_mid_input_{c.get('id')}",
                )
            custom_bid = int(custom_bid)
            sale_mid = int(sale_mid)

            taxes_custom = calc_auction_taxes(custom_bid)
            broker_fee_3m = int(round(sale_mid * broker_rate))
            interest_3m = int(round(loan_amount * float(inp.get("interest_rate", 0.0)) * (3 / 12.0)))
            early_fee_cost = int(round(loan_amount * float(inp.get("early_repay_fee_rate", 0.0))))
            post_cost_3m = int(taxes_custom["total"] + repair_cost + eviction_cost + legal_fee)
            taxable_base_3m = max(0, int(sale_mid - custom_bid - post_cost_3m - broker_fee_3m - interest_3m))
            cap_tax_3m = int(round(taxable_base_3m * cap_tax_rate))
            expected_profit = int(
                sale_mid
                - custom_bid
                - post_cost_3m
                - broker_fee_3m
                - interest_3m
                - cap_tax_3m
                - early_fee_cost
            )
            area_m2_val = float(snap.get("area_m2") or c.get("area_m2") or 0.0)
            unit_price = int(round(sale_mid / area_m2_val)) if area_m2_val > 0 else 0
            df_custom = pd.DataFrame([{
                "입찰가": custom_bid,
                "기준 매도가": sale_mid,
                "예상 이익액": expected_profit,
                "면적단가": unit_price,
            }])
            df_custom_disp = df_custom.copy()
            for k in df_custom_disp.columns:
                df_custom_disp[k] = df_custom_disp[k].map(lambda v: f"{int(v):,}" if pd.notna(v) else v)
            st.markdown(
                _uniform_df_table_html(
                    df_custom_disp,
                    show_index=False,
                    right_align_cols={"입찰가", "기준 매도가", "예상 이익액", "면적단가"},
                ),
                unsafe_allow_html=True,
            )
            st.caption(
                f"반영 항목: 3개월 대출이자 {fmt_money(interest_3m)} / "
                f"중개수수료 {fmt_money(broker_fee_3m)} / "
                f"양도세 {fmt_money(cap_tax_3m)}"
            )

            st.subheader("추천 입찰가(확률형)")
            rec = outputs.get("recommended_bid") or "-"
            loss0 = outputs.get("loss0_max_bid")
            bid_rng = (outputs.get("bid_range") or {})
            step = bid_rng.get("step")
            st.write(rec)

            # 근거 설명
            if loss0:
                lo = int(loss0 * 0.97)
                hi = int(loss0 * 0.99)
                if step:
                    lo = int(round(lo / step) * step)
                    hi = int(round(hi / step) * step)
                st.caption(
                    f"근거: 손실0 상한(기준 매도가 기준) {fmt_money(loss0)}의 97~99% 구간을 '확률형' 추천가로 사용합니다. "
                    f"(입찰 간격 {step//10_000 if step else '-'}만원 단위 반올림)"
                )
                st.caption("의미: 손실0을 지키면서도 낙찰 확률을 조금 끌어올리는 구간입니다. 경쟁이 약하면 97% 근처, 경쟁이 강하면 99% 근처를 사용하세요.")
            else:
                st.caption("근거: 손실0 상한을 산출하지 못해 추천가 근거를 표시할 수 없습니다. (최저가/매도가능가/가정값 확인 필요)")

            st.markdown("#### 직접 메모")
            def _auto_textarea_height(text: str, min_h: int = 180, max_h: int = 680) -> int:
                t = str(text or "")
                # 줄바꿈 + 긴 문장 자동 줄바꿈(대략 90자)까지 반영해 높이 계산
                explicit_lines = t.count("\n") + 1
                wrapped_lines = sum(max(1, (len(line) // 90) + 1) for line in t.split("\n"))
                lines = max(explicit_lines, wrapped_lines)
                return max(min_h, min(max_h, 90 + lines * 22))

            note_key = f"user_note_{c.get('id')}"
            persisted_note = (outputs.get("user_note") or "")
            if note_key not in st.session_state:
                st.session_state[note_key] = persisted_note
            note_h = _auto_textarea_height(st.session_state.get(note_key, persisted_note))
            user_note = st.text_area(
                "추천 입찰가 메모",
                placeholder="예: 3.08억 이하만 입찰 / 임차인 점유 재확인 필요",
                height=note_h,
                key=note_key,
            )

            st.markdown("#### 임장 분석")
            visit_note_key = f"visit_note_{c.get('id')}"
            persisted_visit_note = (outputs.get("visit_note") or outputs.get("site_visit_note") or "")
            if visit_note_key not in st.session_state:
                st.session_state[visit_note_key] = persisted_visit_note
            visit_note_h = _auto_textarea_height(st.session_state.get(visit_note_key, persisted_visit_note))
            visit_note = st.text_area(
                "임장 분석 메모",
                placeholder="예: 채광/소음/주차/동선/누수 흔적/공실률/관리상태 등을 기록",
                height=visit_note_h,
                key=visit_note_key,
            )
            st.caption("메모는 아래 [현재 결과 저장] 버튼으로 저장할 때 함께 기록됩니다.")

            st.markdown("---")
            st.subheader("수동 저장")
            if st.button("💾 현재 결과 저장(리스트 반영)"):
                import uuid
                new_case_id = str(uuid.uuid4())
                new_outputs = dict(outputs or {})
                new_outputs["manual_bid"] = int(custom_bid)
                new_outputs["manual_expected_profit"] = int(expected_profit)
                new_outputs["user_note"] = user_note or ""
                new_outputs["visit_note"] = visit_note or ""
                new_case = {
                    "id": new_case_id,
                    "created_at": now_local_str(),
                    "created_by": c.get("created_by"),
                    "status": "DONE",
                    "case_no": c.get("case_no"),
                    "address": c.get("address"),
                    "property_type": c.get("property_type"),
                    "area_m2": c.get("area_m2"),
                    "appraisal": c.get("appraisal"),
                    "min_price": c.get("min_price"),
                    "auction_date": c.get("auction_date"),
                    "links": c.get("links") or {},
                    "inputs": c.get("inputs") or {},
                    "outputs": new_outputs,
                    "report_md": c.get("report_md") or "",
                }
                try:
                    save_case(new_case)
                    st.session_state["last_saved_case_id"] = new_case_id
                    st.success(f"저장 완료: {new_case_id[:8]} (분석 리스트에 반영)")
                except Exception as e:
                    st.error(f"저장 실패: {e}")


        with tab2:
            st.subheader("입찰 전 체크리스트(표)")
            checklist = [
                "매각물건명세서/현황조사서 최종 확인(임차인/점유/특별매각조건)",
                "등기부 최신본 재발급(입찰 직전)",
                "전입세대 열람/확정일자(숨은 점유자/임차)",
                "제시외/불법 증·개축 여부 현장 확인",
                "관리비/체납/공과금 확인",
            ]
            df_chk = pd.DataFrame({"체크": [False]*len(checklist), "항목": checklist})
            st.data_editor(df_chk, use_container_width=True, hide_index=True)

            st.divider()
            st.subheader("상세 리포트(원문)")
            st.markdown(c.get("report_md") or "(리포트 없음)")
        st.stop()

if __name__ == "__main__":
    main()
