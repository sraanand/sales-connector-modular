"""General utilities (dates, filters, dedupe) — logic preserved."""
from config import *
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import streamlit as st


# ---- mel_day_bounds_to_epoch_ms ----

def mel_day_bounds_to_epoch_ms(d: date) -> tuple[int, int]:
    start_local = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=MEL_TZ)
    end_local   = start_local + timedelta(days=1) - timedelta(milliseconds=1)
    start_ms    = int(start_local.astimezone(UTC_TZ).timestamp() * 1000)
    end_ms      = int(end_local.astimezone(UTC_TZ).timestamp() * 1000)
    return start_ms, end_ms



# ---- parse_epoch_or_iso_to_local_date ----

def parse_epoch_or_iso_to_local_date(s) -> date | None:
    try:
        if s is None or (isinstance(s, float) and np.isnan(s)): return None
        if isinstance(s, (int, np.integer)) or (isinstance(s, str) and s.isdigit()):
            dt = pd.to_datetime(int(s), unit="ms", utc=True).tz_convert(MEL_TZ)
        else:
            dt = pd.to_datetime(s, utc=True)
            if dt.tzinfo is None: dt = dt.tz_localize("UTC")
            dt = dt.tz_convert(MEL_TZ)
        return dt.date()
    except Exception:
        try: return pd.to_datetime(s).date()
        except Exception: return None

def parse_td_slot_time_prop(val) -> str:
    """Parse HubSpot 'td_booking_slot_time' -> 'HH:MM' local if epoch, or normalize common strings."""
    if val is None or (isinstance(val, float) and np.isnan(val)): return ""
    s = str(val).strip()
    if not s: return ""
    if s.isdigit() and len(s) >= 10:
        try:
            return pd.to_datetime(int(s), unit="ms", utc=True).tz_convert(MEL_TZ).strftime("%H:%M")
        except Exception:
            pass
    for fmt in ["%H:%M", "%I:%M %p", "%H:%M:%S"]:
        try:
            t = datetime.strptime(s, fmt).time()
            return f"{t.hour:02d}:{t.minute:02d}"
        except Exception:
            continue
    try:
        ts = pd.to_datetime(s)
        if isinstance(ts, pd.Timestamp):
            if ts.tzinfo is None: ts = ts.tz_localize("UTC")
            ts = ts.tz_convert(MEL_TZ)
            return ts.strftime("%H:%M")
    except Exception:
        pass
    return s

# ---- normalize_phone ----

def normalize_phone(raw) -> str:
    if pd.isna(raw) or raw is None: return ''
    s = str(raw).strip()
    if s.startswith('+'): digits = '+' + ''.join(ch for ch in s if ch.isdigit())
    else:                 digits = ''.join(ch for ch in s if ch.isdigit())
    if digits.startswith('+61') and len(digits) == 12: return digits
    if digits.startswith('61')  and len(digits) == 11: return '+' + digits
    if digits.startswith('0')   and len(digits) == 10 and digits[1] == '4': return '+61' + digits[1:]
    if digits.startswith('4')   and len(digits) == 9:  return '+61' + digits
    return ''



# ---- format_date_au ----

def format_date_au(d: date) -> str:
    return d.strftime("%d %b %Y") if isinstance(d, date) else ""



# ---- rel_date ----

def rel_date(d: date) -> str:
    if not isinstance(d, date): return ''
    today = datetime.now(MEL_TZ).date()
    diff = (d - today).days
    if diff == 0: return 'today'
    if diff == 1: return 'tomorrow'
    if diff == -1: return 'yesterday'
    if 1 < diff <= 7: return 'in a few days'
    if -7 <= diff < -1: return 'a few days ago'
    if 8 <= diff <= 14: return 'next week'
    if -14 <= diff <= -8: return 'last week'
    return d.strftime('%b %d')



# ---- prepare_deals ----

def prepare_deals(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame): df = pd.DataFrame()
    else: df = df.copy()
    for c in DEAL_PROPS:
        if c not in df.columns: df[c] = pd.Series(dtype="object")
    df["slot_date"]      = df["td_booking_slot"].apply(parse_epoch_or_iso_to_local_date)
    df["slot_time"]      = df["td_booking_slot"].apply(parse_epoch_or_iso_to_local_time)
    df["slot_date_prop"] = df["td_booking_slot_date"].apply(parse_epoch_or_iso_to_local_date)
    df["slot_time_param"]= df["td_booking_slot_time"].apply(parse_td_slot_time_prop)
    df["conducted_date_local"] = df["td_conducted_date"].apply(parse_epoch_or_iso_to_local_date)
    df["conducted_time_local"] = df["td_conducted_date"].apply(parse_epoch_or_iso_to_local_time)
    df["phone_raw"]      = df["mobile"].where(df["mobile"].notna(), df["phone"])
    df["phone_norm"]     = df["phone_raw"].apply(normalize_phone)
    df["email"]          = df["email"].fillna('')
    df["full_name"]      = df["full_name"].fillna('')
    return df



# ---- filter_internal_test_emails ----

def filter_internal_test_emails(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove cars24.com / yopmail.com emails. Return (filtered_df, removed_df[with Reason])."""
    if df is None or df.empty or "email" not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(), pd.DataFrame()
    work = df.copy()
    dom = work["email"].astype(str).str.strip().str.lower().str.split("@").str[-1]
    mask = ~dom.isin({"cars24.com", "yopmail.com"})
    removed = work[~mask].copy()
    if not removed.empty:
        removed["Reason"] = "Internal/test email domain"
    return work[mask].copy(), removed



# ---- filter_sms_already_sent ----

def filter_sms_already_sent(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter out deals where td_reminder_sms_sent is already 'true' or 'Yes'.
    Returns (kept_df, removed_df) where removed_df includes a Reason column.
    """
    if df is None or df.empty or "td_reminder_sms_sent" not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(), pd.DataFrame()
    
    work = df.copy()
    
    # Check if SMS was already sent - check for both "true" (value) and "Yes" (label)
    work["sms_sent"] = work["td_reminder_sms_sent"].apply(
        lambda x: str(x).lower() in ['yes', 'true'] if pd.notna(x) else False
    )
    
    removed = work[work["sms_sent"]].drop(columns=["sms_sent"]).copy()
    kept = work[~work["sms_sent"]].drop(columns=["sms_sent"]).copy()
    
    if not removed.empty:
        removed["Reason"] = "SMS reminder already sent (td_reminder_sms_sent = true)"
    
    return kept, removed



# ---- get_all_deal_ids_for_contacts ----

def get_all_deal_ids_for_contacts(messages_df: pd.DataFrame, deals_df: pd.DataFrame) -> dict[str, list[str]]:
    """
    For each phone number in messages_df, get all associated deal IDs from deals_df.
    Returns a dict mapping phone -> list of deal IDs
    """
    phone_to_deals = {}
    
    if messages_df is None or messages_df.empty or deals_df is None or deals_df.empty:
        return phone_to_deals
    
    # Create a mapping of normalized phone to deal IDs
    for _, msg_row in messages_df.iterrows():
        phone = str(msg_row.get("Phone", "")).strip()
        if not phone:
            continue
        
        # Find all deals with this phone number
        matching_deals = deals_df[deals_df["phone_norm"] == phone]["hs_object_id"].tolist()
        phone_to_deals[phone] = [str(d) for d in matching_deals if d]
    
    return phone_to_deals



# ---- dedupe_users_with_audit ----

def dedupe_users_with_audit(df: pd.DataFrame, *, use_conducted: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Same as dedupe_users, but also returns a DataFrame of deals 'removed' by dedupe
    (i.e., additional deals beyond the first per user_key), with a Reason.
    """
    base = dedupe_users(df, use_conducted=use_conducted)
    if df is None or df.empty:
        return base, pd.DataFrame()
    work = df.copy()
    work["email_l"] = work["email"].astype(str).str.strip().str.lower()
    work["user_key"] = (work["phone_norm"].fillna('') + "|" + work["email_l"].fillna('')).str.strip()
    work = work[work["user_key"].astype(bool)]
    dropped_rows = []
    for _, grp in work.groupby("user_key", sort=False):
        if len(grp) <= 1:
            continue
        representative = grp.iloc[0]
        rep_name  = str(representative.get("full_name") or "").strip()
        rep_phone = str(representative.get("phone_norm") or "").strip()
        rep_email = str(representative.get("email") or "").strip()
        for _, r in grp.iloc[1:].iterrows():
            dropped_rows.append({
                "hs_object_id": r.get("hs_object_id"),
                "full_name": r.get("full_name"),
                "email": r.get("email"),
                "phone_norm": r.get("phone_norm"),
                "vehicle_make": r.get("vehicle_make"),
                "vehicle_model": r.get("vehicle_model"),
                "dealstage": r.get("dealstage"),
                "Reason": f"Deduped under {rep_name or rep_phone or rep_email}"
            })
    dropped_df = pd.DataFrame(dropped_rows)
    return base, dropped_df



# ---- dedupe_users ----

def dedupe_users(df: pd.DataFrame, *, use_conducted: bool) -> pd.DataFrame:
    """Return rows with: CustomerName, Phone, Email, DealsCount, Cars, WhenExact, WhenRel, DealStages, StageHint, VehicleDetails."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["CustomerName","Phone","Email","DealsCount","Cars","WhenExact","WhenRel","DealStages","StageHint","VehicleDetails"])
    work = df.copy()
    work["email_l"] = work["email"].astype(str).str.strip().str.lower()
    work["user_key"] = (work["phone_norm"].fillna('') + "|" + work["email_l"].fillna('')).str.strip()
    work = work[work["user_key"].astype(bool)]
    rows = []
    for _, grp in work.groupby("user_key", sort=False):
        name  = first_nonempty_str(grp["full_name"])
        phone = first_nonempty_str(grp["phone_norm"])
        email = first_nonempty_str(grp["email"])
        cars_list, when_exact_list, when_rel_list, stages_list, video_urls_list = [], [], [], [], []
        vehicle_details_list = []  # NEW: Store detailed vehicle info
        
        for _, r in grp.iterrows():
            # Build basic car description
            car = f"{str(r.get('vehicle_make') or '').strip()} {str(r.get('vehicle_model') or '').strip()}".strip() or "car"
            
            # NEW: Build detailed vehicle info for messaging
            vehicle_year = str(r.get('vehicle_year') or '').strip()
            vehicle_colour = str(r.get('vehicle_colour') or '').strip()
            vehicle_url = str(r.get('vehicle_url') or '').strip()
            simplified_color = simplify_vehicle_color(vehicle_colour)
            stage_id = str(r.get('dealstage') or '').strip()
            
            # Store detailed vehicle info as dict
            vehicle_detail = {
                'make': str(r.get('vehicle_make') or '').strip(),
                'model': str(r.get('vehicle_model') or '').strip(), 
                'year': vehicle_year,
                'color': simplified_color,
                'url': vehicle_url,
                'stage_id': stage_id
            }
            vehicle_details_list.append(vehicle_detail)
            
            if use_conducted:
                d = r.get("conducted_date_local"); t = r.get("conducted_time_local") or ""
            else:
                d = r.get("slot_date_prop") or r.get("slot_date")
                t = r.get("slot_time_param") or r.get("slot_time") or ""
            when_rel = rel_date(d) if isinstance(d, date) else ""
            when_exact = (f"{format_date_au(d)} {t}".strip()).strip()

            # Collect video URL
            video_url = str(r.get("video_url__short_") or "").strip()

            cars_list.append(car)
            when_exact_list.append(when_exact)
            when_rel_list.append(when_rel if t == "" else f"{when_rel} at {t}".strip())
            stages_list.append(str(r.get("dealstage") or ""))
            if video_url:  # Only add non-empty video URLs
                video_urls_list.append(video_url)

        stage_labels = sorted({stage_label(x) for x in stages_list if str(x)})
        if STAGE_CONDUCTED_ID in stages_list: hint = "conducted"
        elif STAGE_BOOKED_ID in stages_list: hint = "booked"
        elif STAGE_ENQUIRY_ID in stages_list: hint = "enquiry"
        else: hint = "unknown"

        # Get unique video URLs
        unique_video_urls = list(set([url for url in video_urls_list if url]))

        rows.append({
            "CustomerName": name, "Phone": phone, "Email": email,
            "DealsCount": len([c for c in cars_list if c]),
            "Cars": "; ".join([c for c in cars_list if c]),
            "WhenExact": "; ".join([w for w in when_exact_list if w]),
            "WhenRel": "; ".join([w for w in when_rel_list if w]),
            "DealStages": "; ".join(stage_labels) if stage_labels else "",
            "StageHint": hint,
            "VideoURLs": "; ".join(unique_video_urls) if unique_video_urls else "",
            "VehicleDetails": vehicle_details_list  # NEW: Detailed vehicle info
        })
    out = pd.DataFrame(rows)
    want = ["CustomerName","Phone","Email","DealsCount","Cars","WhenExact","WhenRel","DealStages","StageHint","VideoURLs","VehicleDetails"]
    return out[want] if not out.empty else out


def _coerce_to_utc_datetime(value):
    """
    Best-effort parser that accepts:
      - epoch milliseconds (13 digits) or seconds (10 digits)
      - ISO 8601 strings (e.g. '2024-08-01T03:45:00Z' or without 'Z')
    Returns an aware datetime in UTC, or None if parsing fails.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None

    # Try numeric (epoch seconds / ms)
    try:
        # allow strings like "1690896300000" too
        s = str(value).strip()
        if s.replace(".", "", 1).isdigit():
            num = float(s)
            # Heuristic: >= 1e12 ⇒ milliseconds
            if num >= 1e12:
                ts = num / 1000.0
            else:
                ts = num
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass

    # Try ISO via pandas (handles many formats)
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if isinstance(ts, pd.Timestamp) and pd.notna(ts):
            return ts.to_pydatetime()  # already UTC from utc=True
    except Exception:
        pass

    return None


def parse_epoch_or_iso_to_local_time(value, tz=None, *, as_str=True, fmt="%I:%M %p"):
    """
    Convert epoch ms/seconds or ISO string to a Melbourne-local time-of-day.
    - tz: optional ZoneInfo; defaults to Australia/Melbourne
    - as_str=True: returns a string like '09:30 AM'
      as_str=False: returns a timezone-aware datetime (local) for further use
    """
    if tz is None:
        try:
            tz = MEL_TZ  # provided by config.py in this project
        except NameError:
            tz = ZoneInfo("Australia/Melbourne")

    dt_utc = _coerce_to_utc_datetime(value)
    if not dt_utc:
        return "" if as_str else None

    local_dt = dt_utc.astimezone(tz)
    return local_dt.strftime(fmt) if as_str else local_dt


def parse_epoch_or_iso_to_local_datetime(value, tz=None, *, as_str=False, fmt="%d/%m/%Y %I:%M %p"):
    """
    Like the function above, but returns the full local datetime.
    - as_str=False: returns timezone-aware datetime
    - as_str=True:  returns formatted 'DD/MM/YYYY HH:MM AM/PM'
    """
    if tz is None:
        try:
            tz = MEL_TZ
        except NameError:
            tz = ZoneInfo("Australia/Melbourne")

    dt_utc = _coerce_to_utc_datetime(value)
    if not dt_utc:
        return "" if as_str else None

    local_dt = dt_utc.astimezone(tz)
    return local_dt.strftime(fmt) if as_str else local_dt


def _search_once(payload: dict, total_cap: int) -> pd.DataFrame:
    """
    Run a single HubSpot CRM search request with built-in pagination until either:
      1) We exhaust HubSpot's 'paging.next.after' cursor, or
      2) We collect 'total_cap' rows, whichever comes first, or
      3) We hit an HTTP/network error.

    Why this exists
    ---------------
    HubSpot's CRM Search API returns results in pages and provides a "paging.next.after"
    cursor to fetch the next page. This helper loops through pages for you, gathering
    the 'properties' of each deal into a list, then returns a pandas DataFrame.

    Important dependencies (globals this function expects exist)
    ------------------------------------------------------------
    - HS_SEARCH_URL: str
        The full HubSpot Search endpoint for deals, e.g.
        "https://api.hubapi.com/crm/v3/objects/deals/search"
    - hs_headers(): callable -> dict
        Function that returns HTTP headers containing Authorization and Content-Type.
    - DEAL_PROPS: list[str]
        A list of column names expected downstream when no results are found.
        If we get zero rows back, we return an empty DataFrame with these columns.
    - requests, time, pandas as pd, and Streamlit as st are imported at module level.

    Parameters
    ----------
    payload : dict
        The exact request body you would POST to HubSpot's search endpoint.
        This function *mutates* `payload` by injecting/overwriting the "after" cursor
        between pages (same as the original implementation). If you need to preserve
        the original dict, pass a copy.
    total_cap : int
        Hard upper bound on how many rows we will collect before stopping.
        Prevents runaway pagination when you only need a sample/bounded count.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with one row per returned deal (flattened 'properties').
        - If at least one row is fetched: columns come from the deal properties HubSpot returned.
        - If zero rows are fetched: returns `pd.DataFrame(columns=DEAL_PROPS)` so downstream code
          that relies on specific columns does not break.

    Error handling & UX
    -------------------
    - If HubSpot returns a non-200 status, we try to show a meaningful error payload in Streamlit
      (parsed JSON if possible, else raw text), then stop the loop gracefully.
    - If any exception occurs (network, JSON parsing, etc.), we display a Streamlit error and stop.
    - We use a small `time.sleep(0.08)` between pages as a courtesy throttle to avoid hammering the API.

    Notes on pagination and limits
    ------------------------------
    - HubSpot's response JSON looks like:
        {
          "results": [ { "properties": {...}, "id": "123" }, ... ],
          "paging": { "next": { "after": "cursor-token" } }
        }
      We keep requesting until 'paging.next.after' is absent (no more pages) or `total_cap` reached.
    - We increment `fetched` for every item appended so the cap is respected even across partial pages.
    """
    results, fetched, after = [], 0, None

    while True:
        try:
            # If we have a next-page cursor from the previous response, attach it to the payload.
            # NOTE: This mutates the caller's dict (kept to match original behaviour).
            if after:
                payload["after"] = after

            # POST the search request. The payload is expected to contain filterGroups, properties, etc.
            r = requests.post(HS_SEARCH_URL, headers=hs_headers(), json=payload, timeout=25)

            # Original logic: do not raise; instead, branch on status_code so we can show a nicer message.
            if r.status_code != 200:
                # Try to parse HubSpot's error as JSON; fall back to raw text if that fails.
                try:
                    msg = r.json()
                except Exception:
                    msg = {"error": r.text}
                st.error(f"HubSpot search error {r.status_code}: {msg}")
                break  # Stop the loop on HTTP error.

            # Convert the page into JSON.
            data = r.json()

            # Extract results. HubSpot places each row under "results" as an object with "properties".
            # We only keep the 'properties' dict (flat feature set for DataFrame).
            for item in data.get("results", []):
                results.append(item.get("properties", {}) or {})
                fetched += 1
                # Respect the hard cap even if there are more results in the current page.
                if fetched >= total_cap:
                    break

            # If we hit the cap inside the page, stop the outer loop as well.
            if fetched >= total_cap:
                break

            # If there is a "next" cursor, keep going; otherwise we are done paginating.
            after = (data.get("paging") or {}).get("next", {}).get("after")
            if not after:
                break

            # Courtesy throttle to avoid rate limits / spikes when fetching many pages.
            time.sleep(0.08)

        except Exception as e:
            # Any network, parsing, or unexpected error: report and abort the loop.
            st.error(f"Network/search error: {e}")
            break

    # If we fetched at least one row, build a DataFrame from the collected property dicts.
    # Otherwise, return an empty DataFrame with the expected columns so downstream code does not crash.
    return pd.DataFrame(results) if results else pd.DataFrame(columns=DEAL_PROPS)

def analyze_with_chatgpt(notes_text, customer_name="Customer", vehicle="Vehicle"):
    """Analyze customer notes using ChatGPT with enhanced debugging"""
    if not notes_text or notes_text == "No notes":
        return {
            "summary": "No notes available for analysis",
            "category": "No clear reason documented",
            "next_steps": "Contact customer to understand their experience"
        }
    
    system_prompt = """You are analyzing customer interaction notes from a car dealership to understand why customers didn't pay a deposit after test drives.

CRITICAL: You must respond with ONLY valid JSON in exactly this format - no extra text, no explanations, just the JSON:

{
  "summary": "1-2 line summary of what specifically happened during customer interaction and why deposit was not paid",
  "category": "choose one category from the list below", 
  "next_steps": "specific actionable next step for the sales team to re-engage this customer"
}

Categories (choose exactly one):
- Price/Finance Issues
- Vehicle Condition/Quality  
- Customer Not Ready
- Comparison Shopping
- Feature/Specification Issues
- Trust/Service Issues
- External Factors
- Already Purchased Elsewhere
- Changed Mind/Lost Interest
- No clear reason documented

Rules:
- Response must be valid JSON only
- Keep summary under 150 characters
- Keep next_steps under 100 characters
- Use only the categories listed above exactly as written"""

    user_prompt = f"""Customer: {customer_name}
Vehicle: {vehicle}

Customer interaction notes from dealership:
{notes_text}

Analyze why this customer didn't pay a deposit after their test drive and what the sales team should do next."""

    try:
        import openai
        
        # Set API key
        openai_api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY", "")
        if not openai_api_key:
            return {
                "summary": "OpenAI API key not configured",
                "category": "Analysis failed",
                "next_steps": "Configure OpenAI API key in secrets"
            }

        openai.api_key = openai_api_key
        
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=250
        )
        
        response_text = response.choices[0].message.content.strip()
        
        # Try to parse JSON
        try:
            result = json.loads(response_text)
        except json.JSONDecodeError as json_err:
            # Try to fix common JSON issues
            fixed_response = fix_json_response(response_text)
            if fixed_response:
                try:
                    result = json.loads(fixed_response)
                except:
                    return create_fallback_analysis(response_text, customer_name)
            else:
                return create_fallback_analysis(response_text, customer_name)
        
        return {
            "summary": result.get("summary", "Analysis incomplete"),
            "category": result.get("category", "No clear reason documented"),
            "next_steps": result.get("next_steps", "Review customer interaction manually")
        }
        
    except Exception as e:
        return {
            "summary": f"ChatGPT analysis failed: {str(e)[:50]}...",
            "category": "Analysis failed",
            "next_steps": "Review notes manually and contact customer"
        }

def build_pairs_text(cars: str, when_rel: str) -> str:
    """
    Combine two semicolon-separated lists — car names and relative time phrases —
    into a single, aligned, human-readable string.

    WHAT IT DOES
    ------------
    - Splits both inputs on ';' (semicolon), trims whitespace, and discards empty items.
    - Zips them by index so that:
        cars[i] pairs with when_rel[i]
      If one list is longer than the other, the missing side is treated as "" (empty).
    - For each pair, joins the non-empty pieces with a single space, e.g. "CX-5 tomorrow".
    - Returns all pairs joined by "; " (semicolon + space).

    WHY
    ---
    Downstream SMS copy often needs something like:
        "CX-5 today; Cerato tomorrow"
    given two separate sources (e.g., "CX-5; Cerato" and "today; tomorrow").
    This helper aligns them safely even when lengths differ.

    PARAMETERS
    ----------
    cars : str
        A single string of car identifiers or names separated by semicolons,
        e.g. "CX-5; Cerato ;  Ranger".
        Whitespace around each item is trimmed; blank items are ignored.
    when_rel : str
        A single string of relative time phrases separated by semicolons,
        e.g. "today; tomorrow".
        Same cleaning rules as 'cars'.

    RETURNS
    -------
    str
        A string like "CX-5 today; Cerato tomorrow" (or "" if both inputs are empty
        after cleaning).

    EXAMPLES
    --------
    build_pairs_text("CX-5; Cerato", "today; tomorrow")
      -> "CX-5 today; Cerato tomorrow"

    build_pairs_text("CX-5; Cerato; Ranger", "today")
      -> "CX-5 today; Cerato; Ranger"
         (second and third pairs use empty time -> item alone)

    build_pairs_text("", "today; tomorrow")
      -> "today; tomorrow"
         (car side empty -> just times)

    build_pairs_text("CX-5; ; Ranger", " ; tomorrow ")
      -> "CX-5; Ranger tomorrow"
         (empty/whitespace items are dropped before pairing)
    """
    # Split both inputs on ';', trim whitespace, and drop empty pieces.
    # Using (cars or "") ensures we handle None safely.
    c_list = [c.strip() for c in (cars or "").split(";") if c.strip()]
    w_list = [w.strip() for w in (when_rel or "").split(";") if w.strip()]

    pairs = []

    # We need to walk as far as the longer list so nothing gets lost.
    # If one list runs out, we substitute "" for that side.
    max_len = max(len(c_list), len(w_list))
    for i in range(max_len):
        c = c_list[i] if i < len(c_list) else ""
        w = w_list[i] if i < len(w_list) else ""

        # Join the two parts with a space, but then .strip() to avoid leading/trailing
        # spaces when one side is empty.
        # Examples:
        #   c="CX-5", w="today"   -> "CX-5 today"
        #   c="Cerato", w=""      -> "Cerato"
        #   c="", w="tomorrow"    -> "tomorrow"
        pairs.append(f"{c} {w}".strip())

    # Remove any fully empty pairs (can happen if both sides were missing at a position),
    # then join with "; " for the final human-readable string.
    return "; ".join([p for p in pairs if p])


def create_fallback_analysis(raw_response, customer_name):
    """
    Build a *safe, structured* analysis dictionary when parsing an LLM/JSON
    response fails.

    WHY THIS EXISTS
    ---------------
    - Sometimes an upstream model returns malformed JSON (extra text, stray commas).
    - When parsing fails, we still want to return a consistent structure so the UI
      can render something useful instead of crashing.

    BEHAVIOUR (UNCHANGED)
    ---------------------
    1) Start from conservative defaults:
       - summary   → "Analysis incomplete due to formatting issues"
       - category  → "No clear reason documented"
       - next_steps→ "Review notes manually and contact customer"
    2) Make a *best-effort* attempt to extract a short summary line from the raw text:
       - Split text by lines
       - Scan for the first line containing any of the hints:
         "summary", "what happened", "customer" (case-insensitive)
       - If that line has >10 chars, use its first 100 chars as the summary
    3) Always include a trimmed copy of the original text (first 200 chars) in
       `raw_response` so humans can inspect what went wrong.

    PARAMETERS
    ----------
    raw_response : str
        The original, unparsed text returned by the model (or any source that failed).
    customer_name : str
        Currently unused in the fallback, but kept for signature compatibility.
        (You can embed it into the summary in future without breaking callers.)

    RETURNS
    -------
    dict with keys:
      - "summary": str        (short, human-readable)
      - "category": str       (coarse reason label; conservative default)
      - "next_steps": str     (actionable next step; conservative default)
      - "raw_response": str   (first 200 chars of the original, for forensic review)

    EXAMPLE
    -------
    create_fallback_analysis("SUMMARY: Missed call; Customer said they will call back", "Alex")
      -> {
           "summary": "SUMMARY: Missed call; Customer said they will call back",
           "category": "No clear reason documented",
           "next_steps": "Review notes manually and contact customer",
           "raw_response": "SUMMARY: Missed call; Customer said they will call back"
         }
    """
    # Break the raw text into lines so we can scan them for a plausible summary.
    lines = raw_response.split("\n")

    # Conservative defaults (used if we cannot extract anything meaningful).
    summary = "Analysis incomplete due to formatting issues"
    category = "No clear reason documented"
    next_steps = "Review notes manually and contact customer"

    # Best-effort extraction:
    # Look for the first line that *looks like* a summary cue. We use a small set
    # of keywords and keep it case-insensitive. If a matching line is long enough,
    # we take up to 100 characters as the summary.
    for line in lines:
        lower = line.lower()
        if any(word in lower for word in ["summary", "what happened", "customer"]):
            if len(line.strip()) > 10:
                summary = line.strip()[:100]  # keep summaries short for UI readability
                break

    # Always return a compact copy of the raw response for human review.
    # Trim at 200 characters to avoid dumping large payloads into the UI.
    trimmed_raw = raw_response[:200] + "..." if len(raw_response) > 200 else raw_response

    return {
        "summary": summary,
        "category": category,
        "next_steps": next_steps,
        "raw_response": trimmed_raw,
    }

def first_nonempty_str(series: pd.Series) -> str:
    """
    Return the *first* non-empty, non-'nan' (case-insensitive) string from a pandas Series.

    WHY
    ---
    When building messages, you often have multiple candidate fields for a name
    (e.g., firstname, fullname, nickname). This helper picks the first one that
    actually contains a usable value after cleaning.

    BEHAVIOUR (unchanged)
    ---------------------
    1) If the input is None → return "".
    2) Convert every value to a string (so numbers/None/NaN become strings), then:
       - fill NaN with "" to avoid "nan" strings from pandas
       - strip surrounding whitespace
    3) Filter out:
       - empty strings ("")
       - the literal string "nan" (case-insensitive), which can still appear when the
         original value stringified to "nan".
    4) If any values remain, return the *first*; else return "".

    PARAMETERS
    ----------
    series : pd.Series
        Any Series of candidate values (mixed types allowed).

    RETURNS
    -------
    str
        The first usable string, or "" if none found.

    EXAMPLES
    --------
    - ["", None, "  Alice  "]       → "Alice"
    - [np.nan, "nan", "Bob"]        → "Bob"
    - [None, "   ", "\t"]           → ""
    """
    # Short-circuit: no data to search.
    if series is None:
        return ""

    # 1) Coerce *all* values to strings so downstream string ops are safe.
    # 2) Turn real NaNs into "" so they do not become the literal "nan" later.
    # 3) Trim surrounding whitespace from each string.
    s = series.astype(str).fillna("").map(lambda x: x.strip())

    # Keep only strings that are:
    #   - truthy after stripping (not empty),
    #   - not the literal "nan" (case-insensitive) which can arise from prior conversions.
    s = s[(s.astype(bool)) & (s.str.lower() != "nan")]

    # Return the first surviving value, or "" if we filtered everything out.
    return s.iloc[0] if not s.empty else ""

def fix_json_response(response_text):
    """
    Attempt to salvage a JSON string from a noisy LLM response.

    WHY THIS EXISTS
    ---------------
    LLMs sometimes wrap valid JSON with extra narration (before/after), or include
    raw newlines/tabs that break strict JSON parsing. This helper tries a *minimal*
    cleanup so the JSON can be parsed without changing the actual JSON content.

    WHAT IT DOES (UNCHANGED BEHAVIOUR)
    ----------------------------------
    1) Trim leading junk: find the first '{' and drop everything before it.
    2) Trim trailing junk: find the last '}' and drop everything after it.
    3) Escape control chars: replace literal newlines/tabs with '\\n' and '\\t'.
    4) Validate: try json.loads() on the cleaned string.
       - If parsing works → return the cleaned JSON string (not a dict).
       - If parsing fails → return None.

    NOTES / LIMITATIONS
    -------------------
    - This is intentionally conservative:
      * It does not try to balance braces or fix quotes/commas.
      * It assumes a single top-level JSON object (between the first '{' and last '}').
    - Caller can then safely do: json.loads(fix_json_response(...)) if not None.

    Parameters
    ----------
    response_text : str
        The raw text returned by the model (may contain extra prose around JSON).

    Returns
    -------
    str | None
        A JSON-parseable string if salvage succeeded, else None.
    """
    try:
        # 1) Remove any text before the FIRST '{'
        #    Rationale: LLMs often prefix with explanations (e.g., "Here is your JSON:")
        start_idx = response_text.find('{')
        if start_idx > 0:
            response_text = response_text[start_idx:]

        # 2) Remove any text after the LAST '}'
        #    Rationale: LLMs often append commentary (e.g., "Hope this helps!")
        end_idx = response_text.rfind('}')
        if end_idx > 0:
            response_text = response_text[:end_idx + 1]

        # 3) Escape common control characters that break JSON parsing when unescaped.
        #    This does NOT alter content; it just makes the string JSON-valid.
        response_text = response_text.replace('\n', '\\n').replace('\t', '\\t')

        # 4) Validate: only return the string if json.loads() accepts it.
        json.loads(response_text)
        return response_text

    except Exception:
        # Any failure (no braces, invalid JSON, non-string input, etc.) → signal unsalvageable.
        return None


def mel_range_bounds_to_epoch_ms(d1: date, d2: date) -> tuple[int, int]:
    if d2 < d1: d1, d2 = d2, d1
    s,_ = mel_day_bounds_to_epoch_ms(d1)
    _,e = mel_day_bounds_to_epoch_ms(d2)
    return s,e

def simplify_vehicle_color(color_name: str) -> str:
    """Simplify complex manufacturer color names to basic colors for SMS messages"""
    if not color_name or pd.isna(color_name):
        return ""
    
    color = str(color_name).lower().strip()
    
    # Red variations
    if any(word in color for word in ['red', 'crimson', 'scarlet', 'burgundy', 'ruby', 'cherry', 'rose']):
        return "Red"
    # Blue variations  
    if any(word in color for word in ['blue', 'navy', 'azure', 'cobalt', 'sapphire', 'indigo', 'teal']):
        return "Blue"
    # White variations
    if any(word in color for word in ['white', 'pearl', 'ivory', 'cream', 'snow', 'frost']):
        return "White"
    # Black variations
    if any(word in color for word in ['black', 'ebony', 'coal', 'charcoal', 'onyx', 'midnight']):
        return "Black"
    # Silver/Grey variations
    if any(word in color for word in ['silver', 'grey', 'gray', 'platinum', 'steel', 'graphite', 'titanium']):
        return "Silver"
    # Green variations
    if any(word in color for word in ['green', 'emerald', 'forest', 'sage', 'olive', 'lime']):
        return "Green"
    # Yellow/Gold variations
    if any(word in color for word in ['yellow', 'gold', 'amber', 'champagne', 'bronze']):
        return "Gold"
    # Orange variations
    if any(word in color for word in ['orange', 'copper', 'sunset', 'rust']):
        return "Orange"
    # Purple variations
    if any(word in color for word in ['purple', 'violet', 'magenta', 'plum']):
        return "Purple"
    # Brown variations
    if any(word in color for word in ['brown', 'tan', 'beige', 'mocha', 'coffee', 'chocolate']):
        return "Brown"
    
    # If no match found, try to extract the first recognizable color word
    color_words = color.split()
    for word in color_words:
        if word in ['red', 'blue', 'white', 'black', 'silver', 'green', 'yellow', 'orange', 'purple', 'brown']:
            return word.capitalize()
    
    return ""
