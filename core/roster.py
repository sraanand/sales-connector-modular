# core/roster.py
"""
Roster loader + round-robin assignment for Test Drive Reminders.

The Google Sheet layout (one worksheet):
- Column A: associate email (required)
- Column B: associate nickname (display name in SMS)
- Columns C..I: day-of-week availability flags per associate (Mon..Sun)
  Cell values considered "available": "y", "yes", "true", "1" (case/whitespace-insensitive)

We will:
1) Load the sheet as a DataFrame.
2) Normalise headers so we have canonical day names: Mon,Tue,Wed,Thu,Fri,Sat,Sun
3) On a given date, pick associates with a true-ish flag under that day.
4) Assign leads round-robin to those associates.

This module is framework-agnostic; Streamlit is used only for secrets and optional info messages.
"""

from __future__ import annotations
import re
from datetime import date
from itertools import cycle
from typing import List, Dict

import pandas as pd
import streamlit as st

# Google auth / gspread
try:
    import gspread
    from google.oauth2 import service_account
except Exception:
    gspread = None
    service_account = None

# --------- constants ----------
# Canonical weekday abbreviations we’ll use as column names
DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# --------- helpers ----------

def _truthy_cell(val) -> bool:
    """Interpret a cell as boolean availability."""
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in {"y", "yes", "true", "1"}

def _weekday_col_for(d: date) -> str:
    """Return 'Mon'..'Sun' for the given date."""
    return DOW[d.weekday()]  # Monday == 0

def _normalise_headers(cols):
    out, seen_days = [], 0
    for i, c in enumerate(cols):
        s = (c or "").strip()
        sl = s.lower()
        if i == 0: out.append("email"); continue
        if i == 1: out.append("name");  continue
        mapped = None
        for j, day in enumerate(DOW):
            if re.fullmatch(day, s, flags=re.IGNORECASE) or re.fullmatch(day.lower()+"day", sl):
                mapped = DOW[j]; break
        if mapped is None and seen_days < 7:
            mapped = DOW[seen_days]
        if mapped is not None:
            out.append(mapped); seen_days += 1
        else:
            out.append(s or f"Col{i+1}")
    return out


def _get_gspread_client():
    """
    Build a gspread client from Streamlit Secrets service account.
    Return None if gspread or credentials are unavailable.
    """
    if gspread is None or service_account is None:
        st.info("gspread/google-auth not imported; falling back to CSV.")
        return None
    try:
        info = st.secrets.get("gcp_service_account")
        if not isinstance(info, dict):
            return None
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.warning(f"Service account init failed: {e}; falling back to CSV.")
        return None

def load_roster_df(sheet_url: str, worksheet_name: str | None = None) -> pd.DataFrame:
    """
    Tries gspread (private sheet, service account). If that fails, tries CSV
    (requires the sheet/tab to be public/published).
    Always returns a DataFrame with columns: email, name, Mon..Sun (may be empty).
    """
    # 1) Private path via service account
    client = _get_gspread_client()
    if client is not None:
        try:
            sh = client.open_by_url(sheet_url)
            ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1
            raw = ws.get_all_values()
            if not raw:
                st.warning("Roster sheet is empty (no values).")
                return pd.DataFrame(columns=["email","name"] + DOW)
            df = pd.DataFrame(raw[1:], columns=_normalise_headers(raw[0]))
            st.info(f"Roster loaded via gspread: rows={len(df)}")
            return _coerce_roster(df)
        except Exception as e:
            st.warning(f"gspread read failed ({type(e).__name__}): {e}. Trying CSV fallback...")

    # 2) CSV fallback (public or 'publish to web')
    try:
        csv_url = _csv_url_from_share_url(sheet_url)
        st.info(f"CSV fallback URL: {csv_url}")
        df = pd.read_csv(csv_url)
        df.columns = _normalise_headers(list(df.columns))
        st.info(f"Roster loaded via CSV: rows={len(df)}")
        return _coerce_roster(df)
    except Exception as e:
        st.error(f"CSV fallback failed: {e}")
        return pd.DataFrame(columns=["email","name"] + DOW)

def _csv_url_from_share_url(sheet_url: str, gid: int | None = None) -> str:
    base = sheet_url.split("/edit", 1)[0]
    if gid is None: gid = int(st.secrets.get("ROSTER_GID", 0))
    return f"{base}/export?format=csv&gid={gid}"

def _coerce_roster(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the roster DataFrame has expected columns and boolean availability.
    - missing cols are created
    - availability parsed via _truthy_cell
    """
    work = df.copy()
    for c in ["email", "name"] + DOW:
        if c not in work.columns:
            work[c] = ""
    work["email"] = work["email"].astype(str).str.strip()
    work["name"] = work["name"].astype(str).str.strip()
    for d in DOW:
        work[d] = work[d].map(_truthy_cell)
    # Keep only necessary columns in order
    return work[["email", "name"] + DOW]

def available_associates_for_date(roster: pd.DataFrame, d: date) -> List[Dict[str, str]]:
    """
    From the roster DF, return a list of associates available on date 'd':
    [{ 'name': 'Nick', 'email': 'nick@...' }, ...]
    """
    if roster is None or roster.empty:
        return []
    col = _weekday_col_for(d)
    out = []
    for _, r in roster.iterrows():
        if bool(r.get(col, False)) and r.get("email"):
            out.append({"name": str(r.get("name") or "").strip(),
                        "email": str(r.get("email") or "").strip()})
    return out

def round_robin_assign(dedup_df: pd.DataFrame, associates: List[Dict[str, str]], d: date) -> pd.DataFrame:
    """
    Assign each deduped lead to an available associate, round-robin.
    Deterministic per-day by sorting phones and starting at an offset derived from the date
    (so re-runs the same day produce the same assignment).

    Returns a COPY of dedup_df with two new columns:
      - SalesAssociate   (associate name)
      - SalesEmail       (associate email)
    """
    if dedup_df is None or dedup_df.empty or not associates:
        # no allocation possible; return input with empty columns
        work = dedup_df.copy() if isinstance(dedup_df, pd.DataFrame) else pd.DataFrame()
        if not work.empty:
            work["SalesAssociate"] = ""
            work["SalesEmail"] = ""
        return work

    work = dedup_df.copy()

    # Stable order: sort by phone to be deterministic; you can change to index order if preferred
    work = work.sort_values(by=["Phone", "CustomerName"], na_position="last", kind="stable").reset_index(drop=True)

    # Derive a start offset from the date (0..len(associates)-1)
    # Simple, stable: (YYYY*10000 + MM*100 + DD) mod N
    n = len(associates)
    seed = d.year * 10000 + d.month * 100 + d.day
    offset = seed % n

    # Prepare a cyclic iterator starting from the offset
    rotation = associates[offset:] + associates[:offset]
    rr = cycle(rotation)

    names, emails = [], []
    for _idx, _row in work.iterrows():
        a = next(rr)
        names.append(a["name"])
        emails.append(a["email"])

    work["SalesAssociate"] = names
    work["SalesEmail"] = emails
    return work

# core/roster.py (append these helpers)

def _safe_get_sa_email() -> str:
    try:
        info = st.secrets.get("gcp_service_account")
        return (info or {}).get("client_email", "")
    except Exception:
        return ""

def debug_dump_roster(sheet_url: str, roster_df: pd.DataFrame, target_date: date, worksheet_name: str | None = None):
    """
    Streamlit-friendly diagnostics for the roster pipeline.
    Shows:
      - Auth method (service account present / not)
      - Raw header row (if gspread path works)
      - Normalised columns + head()
      - Weekday column chosen for 'target_date' and who is 'True'
    """
    st.markdown("### 🛠 Roster debug")

    # Auth / SA email
    sa_email = _safe_get_sa_email()
    st.write({
        "service_account_present": bool(sa_email),
        "service_account_email": sa_email,
        "gspread_imported": gspread is not None,
    })

    # Try to show raw header row via gspread (if possible)
    try:
        client = _get_gspread_client()
        if client is not None:
            sh = client.open_by_url(sheet_url)
            ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1
            raw = ws.get_all_values()
            raw_header = raw[0] if raw else []
            st.write({"raw_header_row": raw_header})
        else:
            st.info("gspread client not available (using CSV fallback or empty).")
            # FYI CSV URL
            if "/edit" in sheet_url:
                csv_url = sheet_url.split("/edit", 1)[0] + "/export?format=csv"
                st.code(csv_url, language="text")
    except Exception as e:
        st.warning(f"Could not fetch raw header row via gspread: {e}")

    # Normalised DF overview
    st.write({
        "roster_shape": roster_df.shape if roster_df is not None else (0, 0),
        "roster_columns": list(roster_df.columns) if roster_df is not None else [],
    })
    if roster_df is not None and not roster_df.empty:
        st.dataframe(roster_df.head(20), use_container_width=True)

    # Which weekday column for the selected date?
    weekday_col = _weekday_col_for(target_date)
    st.write({"target_date": str(target_date), "weekday_col": weekday_col})

    # Show who is available for that day
    if roster_df is not None and not roster_df.empty:
        if weekday_col in roster_df.columns:
            true_count = int(roster_df[weekday_col].sum())
            st.write({"available_count": true_count})
            st.dataframe(roster_df.loc[roster_df[weekday_col] == True, ["name", "email"]], use_container_width=True)
        else:
            st.error(f"Column for weekday '{weekday_col}' not found in roster. "
                     f"Expected day columns like Mon..Sun in columns C..I.")
