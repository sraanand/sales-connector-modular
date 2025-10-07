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
    """Interpret a cell value as availability True/False."""
    if val is None:
        return False
    return str(val).strip().lower() in {"y", "yes", "true", "1"}

def _normalise_headers(cols: list[str]) -> list[str]:
    """
    Map the sheet’s header row to expected names:
      A → 'email', B → 'name', C..I → Mon..Sun
    We also accept header text like 'Monday', 'mon', etc.
    """
    out = []
    seen_days = 0
    for i, c in enumerate(cols):
        s = (c or "").strip()
        sl = s.lower()
        if i == 0:
            out.append("email")
            continue
        if i == 1:
            out.append("name")
            continue
        mapped = None
        for j, day in enumerate(DOW):
            if re.fullmatch(day, s, flags=re.IGNORECASE) or re.fullmatch(day.lower() + "day", sl):
                mapped = DOW[j]
                break
        if mapped is None and seen_days < 7:
            # If headers are blank in C..I, still map them sequentially Mon..Sun
            mapped = DOW[seen_days]
        if mapped is not None:
            out.append(mapped)
            seen_days += 1
        else:
            out.append(s or f"Col{i+1}")
    return out

def _coerce_roster(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the roster DF has: email, name, Mon..Sun (bool).
    Missing columns are created; day cells are cast to booleans with _truthy_cell.
    """
    work = df.copy()
    for c in ["email", "name"] + DOW:
        if c not in work.columns:
            work[c] = ""
    work["email"] = work["email"].astype(str).str.strip()
    work["name"]  = work["name"].astype(str).str.strip()
    for d in DOW:
        work[d] = work[d].map(_truthy_cell)
    return work[["email", "name"] + DOW]

def _get_gspread_client_strict():
    """
    Build a gspread client from Streamlit secrets.
    Raises a descriptive RuntimeError if anything is missing.
    """
    if gspread is None or service_account is None:
        raise RuntimeError("gspread/google-auth not installed. Add them to requirements.txt and redeploy.")
    try:
        info = st.secrets.get("gcp_service_account")
    except Exception:
        info = None
    if not isinstance(info, dict):
        raise RuntimeError("Missing 'gcp_service_account' in Streamlit secrets (entire SA JSON).")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    try:
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        raise RuntimeError(f"Service account auth failed: {e}")

def load_roster_df(sheet_url: str, worksheet_name: str | None = None) -> pd.DataFrame:
    """
    STRICT loader: uses gspread + service account only.
    - sheet_url: the Google Sheet URL you shared with the service account
    - worksheet_name: optional exact tab name. If None, uses the first sheet.

    Raises RuntimeError with a clear message on failure.
    """
    client = _get_gspread_client_strict()
    try:
        sh = client.open_by_url(sheet_url)   # will throw if the SA doesn't have access
    except Exception as e:
        raise RuntimeError(
            "Cannot open sheet by URL. "
            "Ensure the service account EMAIL in secrets is SHARED on the sheet "
            f"(Viewer or Editor). Underlying error: {e}"
        )

    try:
        ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1
    except Exception as e:
        raise RuntimeError(
            f"Cannot access worksheet '{worksheet_name or 'sheet1'}'. "
            "Check the tab name (case-sensitive) or pass the correct name. "
            f"Underlying error: {e}"
        )

    raw = ws.get_all_values()
    if not raw:
        raise RuntimeError("Roster worksheet appears to be empty (no header/rows).")

    header = _normalise_headers(raw[0])
    df = pd.DataFrame(raw[1:], columns=header)
    return _coerce_roster(df)

def available_associates_for_date(roster: pd.DataFrame, d: date) -> List[Dict[str, str]]:
    """Return [{'name': 'Nick', 'email': 'n@x.com'}, ...] for associates available on date d."""
    if roster is None or roster.empty:
        return []
    col = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][d.weekday()]
    if col not in roster.columns:
        return []
    out = []
    for _, r in roster.iterrows():
        if bool(r.get(col, False)) and r.get("email"):
            out.append({"name": str(r.get("name") or "").strip(),
                        "email": str(r.get("email") or "").strip()})
    return out

def round_robin_assign(dedup_df: pd.DataFrame, associates: List[Dict[str, str]], d: date) -> pd.DataFrame:
    """
    Attach SalesAssociate + SalesEmail to dedup_df by round-robin over associates.
    Deterministic per-day via a stable sort and a date-derived offset.
    """
    if dedup_df is None or dedup_df.empty or not associates:
        work = dedup_df.copy() if isinstance(dedup_df, pd.DataFrame) else pd.DataFrame()
        if not work.empty:
            work["SalesAssociate"] = ""
            work["SalesEmail"] = ""
        return work

    work = dedup_df.copy().sort_values(by=["Phone", "CustomerName"], na_position="last", kind="stable").reset_index(drop=True)
    n = len(associates)
    seed = d.year * 10000 + d.month * 100 + d.day
    offset = seed % n
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

# Optional: UI-first debug block (call from your view if needed)
def debug_roster_connectivity(sheet_url: str, worksheet_name: str | None = None):
    """Print explicit, UI-friendly diagnostics for the roster path."""
    st.markdown("### 🛠 Roster (strict) debug")
    try:
        info = st.secrets.get("gcp_service_account")
        sa_email = (info or {}).get("client_email", None)
        st.write({"has_gspread": gspread is not None,
                  "has_service_account": service_account is not None,
                  "has_secret": isinstance(info, dict),
                  "service_account_email": sa_email})
        client = _get_gspread_client_strict()
        sh = client.open_by_url(sheet_url)
        st.write({"opened_sheet_title": sh.title})
        ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1
        st.write({"worksheet_title": ws.title})
        raw = ws.get_all_values()
        st.write({"rows": len(raw), "first_row": raw[0] if raw else []})
        if raw:
            df = _coerce_roster(pd.DataFrame(raw[1:], columns=_normalise_headers(raw[0])))
            st.dataframe(df.head(20), use_container_width=True)
    except Exception as e:
        st.error(f"Roster connection failed: {e}")
