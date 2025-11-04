from __future__ import annotations

import csv
import json
import os
import re
import shlex
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

# ---------- 유틸 ----------

def _compose_query(keyword: str, start: str, end: str, lang_ko: bool = True) -> str:
    """
    snscrape 쿼리 문자열 구성
    - note: snscrape는 until이 배제(exclusive)
    """
    parts = [shlex.quote(keyword), f"since:{start}", f"until:{end}"]
    if lang_ko:
        parts.append("lang:ko")
    return " ".join(parts)

def _run_snscrape(query: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    snscrape CLI 실행 → jsonl 라인 파싱 → dict 목록 리턴
    """
    cmd = ["snscrape", "--jsonl", "--progress", "twitter-search", query]
    if limit is not None:
        cmd.extend(["--max-results", str(limit)])

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    rows: List[Dict[str, Any]] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    proc.stdout.close()
    proc.wait()
    return rows

def _flatten_tweet(j: Dict[str, Any]) -> Dict[str, Any]:
    """
    snscrape 트윗 json → 분석 친화적 납작 구조
    """
    user = j.get("user") or {}
    retweeted = j.get("retweetedTweet")
    quoted = j.get("quotedTweet")
    is_rt = bool(retweeted) or (j.get("rawContent", "").startswith("RT @"))
    return {
        "tweet_id": j.get("id"),
        "text": j.get("rawContent"),
        "created_at": j.get("date"),  # ISO8601(UTC)
        "user_id": user.get("id"),
        "username": user.get("username"),
        "displayname": user.get("displayname"),
        "followers_count": user.get("followersCount"),
        "friends_count": user.get("friendsCount"),
        "statuses_count": user.get("statusesCount"),
        "retweet_count": j.get("retweetCount"),
        "like_count": j.get("likeCount"),
        "reply_count": j.get("replyCount"),
        "quote_count": j.get("quoteCount"),
        "lang": j.get("lang"),
        "is_retweet": is_rt,
        "quoted_tweet_id": quoted.get("id") if isinstance(quoted, dict) else None,
        "source_label": j.get("sourceLabel"),
        "url": j.get("url"),
    }

_HANGUL_RE = re.compile(r"[가-힣]")

def _has_hangul(text: str) -> bool:
    return bool(_HANGUL_RE.search(text or ""))

def _utc_to_kst(iso_utc: str) -> Optional[str]:
    """
    '2024-01-01T08:00:00+00:00' → KST ISO8601
    """
    from dateutil import tz
    KST = tz.gettz("Asia/Seoul")
    if not iso_utc:
        return None
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        return dt.astimezone(KST).isoformat()
    except Exception:
        return None

# 별자리(한국어 표기 + 약칭) 정규식은 configs/project.yaml에서 읽어와 사용
def _extract_zodiac(text: str, patterns: Dict[str, str]) -> Optional[str]:
    if not text:
        return None
    for name, pat in patterns.items():
        if re.search(pat, text, flags=re.IGNORECASE):
            return name
    return None

def _botlike_filter(row: pd.Series) -> bool:
    """
    광고/봇 유사 휴리스틱 필터 (True=유지, False=제외)
    너무 공격적이지 않게 보수적 세팅.
    """
    followers = row.get("followers_count") or 0
    friends = row.get("friends_count") or 0
    text = row.get("text") or ""

    many_links = len(re.findall(r"https?://\S+", text)) >= 2
    repeated_hash = len(set(re.findall(r"#\w+", text))) >= 8
    promo_words = re.search(r"(무료|할인|특가|홍보|광고)", text)

    if followers < 5 and friends > 300 and (many_links or promo_words or repeated_hash):
        return False
    return True

# ---------- 메인 엔트리 ----------

def run_collect(cfg: dict, limit_per_kw: Optional[int] = None) -> pd.DataFrame:
    """
    configs/project.yaml을 읽은 dict(cfg)로 수집 파이프라인 실행.
    - 수집 결과를 CSV/Parquet로 저장하고 DataFrame 반환.
    """
    pj = cfg.get("project", {})
    coll = cfg.get("collect", {})
    zodiac = cfg.get("zodiac_regex", {})

    start = pj.get("start_date")
    end = pj.get("end_date")
    tz_name = pj.get("timezone", "Asia/Seoul")

    keywords = coll.get("keywords", [])
    lang_filter = coll.get("lang_filter", True)
    keep_if_hangul = coll.get("keep_if_hangul", True)
    apply_bot_filter = coll.get("apply_bot_filter", False)
    checkpoint_every = coll.get("checkpoint_every", 5000)
    output_prefix = coll.get("output_prefix", "data/raw/ohaasa")

    # 1) 수집
    all_rows: List[Dict[str, Any]] = []
    for kw in keywords:
        q = _compose_query(kw, start, end, lang_ko=lang_filter)
        js = _run_snscrape(q, limit=limit_per_kw)
        for j in js:
            all_rows.append(_flatten_tweet(j))
        # 간단 체크포인트(옵션)
        if checkpoint_every and len(all_rows) >= checkpoint_every:
            tmp = pd.DataFrame(all_rows).drop_duplicates(subset=["tweet_id"])
            os.makedirs(os.path.dirname(output_prefix), exist_ok=True)
            tmp.to_csv(f"{output_prefix}.checkpoint.csv", index=False, encoding="utf-8-sig")

    if not all_rows:
        print("[WARN] No data collected. Check your query/date range.")
        return pd.DataFrame()

    # 2) DataFrame 구성 + 중복 제거
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["tweet_id"]).reset_index(drop=True)

    # 3) 언어 필터 ko + 한글 fallback
    if lang_filter:
        mask_ko = (df["lang"] == "ko")
        if keep_if_hangul:
            mask_hangul = df["text"].fillna("").apply(_has_hangul)
            df = df[mask_ko | mask_hangul].copy()
        else:
            df = df[mask_ko].copy()

    # 4) 시간대 변환(UTC→KST)
    df["created_at_kst"] = df["created_at"].apply(_utc_to_kst)

    # 5) 별자리 추출
    df["zodiac_sign"] = df["text"].fillna("").apply(lambda t: _extract_zodiac(t, zodiac))

    # 6) is_retweet 보정
    df["is_retweet"] = df["is_retweet"].fillna(False) | df["text"].fillna("").str.startswith("RT @")

    # 7) 봇/광고 휴리스틱 필터(선택)
    if apply_bot_filter:
        before = len(df)
        df = df[df.apply(_botlike_filter, axis=1)].copy()
        print(f"[FILTER] bot/advert heuristic: {before} -> {len(df)}")

    # 8) 저장
    out_csv = f"{output_prefix}.csv"
    out_parquet = f"{output_prefix}.parquet"
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    try:
        df.to_parquet(out_parquet, index=False)
        print(f"[DONE] Saved {len(df)} rows\n - CSV: {out_csv}\n - Parquet: {out_parquet}")
    except Exception as e:
        print(f"[WARN] Parquet save failed: {e}\n - CSV: {out_csv}")

    return df
