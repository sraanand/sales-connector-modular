"""HubSpot HTTP helpers — logic copied from original app.py."""
from config import *
import requests
import pandas as pd
import streamlit as st
import os

# ---- hs_headers ----

def hs_headers() -> dict:
    return {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

def _hs_token() -> str:
    """
    Resolve a HubSpot private app token from config or environment.
    We try a few common names so you do not have to touch code:
      - HUBSPOT_PRIVATE_APP_TOKEN
      - HUBSPOT_API_KEY
      - HUBSPOT_TOKEN
      - HS_TOKEN
    Put one of these in Streamlit Secrets.
    """
    for key in ("HUBSPOT_PRIVATE_APP_TOKEN", "HUBSPOT_API_KEY", "HUBSPOT_TOKEN", "HS_TOKEN"):
        val = globals().get(key) or os.getenv(key)
        if val:
            return val
    raise RuntimeError(
        "HubSpot token not configured. Set HUBSPOT_PRIVATE_APP_TOKEN (or HUBSPOT_API_KEY) "
        "in Streamlit Cloud → Settings → Secrets."
    )

def _hs_headers() -> dict:
    """Standard JSON + Bearer auth headers for HubSpot HTTP calls."""
    return {
        "Authorization": f"Bearer {_hs_token()}",
        "Content-Type": "application/json",
    }

def _hs_get(path: str, params: dict | None = None) -> dict:
    """Low-level GET wrapper for HubSpot."""
    base = "https://api.hubapi.com"
    url = f"{base}{path}"
    r = requests.get(url, headers=_hs_headers(), params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()

def _hs_post(path: str, payload: dict) -> dict:
    """Low-level POST wrapper for HubSpot."""
    base = "https://api.hubapi.com"
    url = f"{base}{path}"
    r = requests.post(url, headers=_hs_headers(), json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def _hs_patch(path: str, payload: dict) -> dict:
    """Low-level PATCH wrapper for HubSpot."""
    base = "https://api.hubapi.com"
    url = f"{base}{path}"
    r = requests.patch(url, headers=_hs_headers(), json=payload, timeout=60)
    r.raise_for_status()
    return r.json()
# ---- hs_get_owner_info ----

def hs_get_owner_info(owner_id):
    """Get owner information by ID"""
    try:
        url = f"{HS_ROOT}/crm/v3/owners/{owner_id}"
        response = requests.get(url, headers=hs_headers(), timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception:
        return None

def _search_once(
    payload: dict,
    *,
    total_cap: int = 1000,
    endpoint: str = "/crm/v3/objects/deals/search"
) -> pd.DataFrame:
    """
    Execute a HubSpot CRM search with safe pagination and return a flat DataFrame
    of 'properties' plus 'id'. This mirrors the monolith's private helper.
    """
    out = []
    after = None
    fetched = 0
    while True:
        body = dict(payload)  # shallow copy so we do not mutate the caller's payload
        # Respect a caller-provided 'limit' but never exceed total_cap
        limit = int(body.get("limit", 100))
        limit = min(limit, max(0, total_cap - fetched))
        if limit <= 0:
            break
        body["limit"] = limit
        if after is not None:
            body["after"] = after

        j = _hs_post(endpoint, body)
        results = j.get("results", [])
        out.extend(results)
        fetched += len(results)

        after = j.get("paging", {}).get("next", {}).get("after")
        if not after or fetched >= total_cap:
            break

    # Flatten to DataFrame (properties + id)
    rows = []
    for r in out:
        props = r.get("properties", {}) or {}
        props["id"] = r.get("id")
        rows.append(props)
    return pd.DataFrame(rows)

# ---- hs_get_deal_property_options ----

def hs_get_deal_property_options(property_name: str) -> list[dict]:
    """
    Fetch selectable options for a HubSpot *deal* property, and return them as
    a list of dicts shaped like: [{"label": "...", "value": "..."}, ...].

    Typical use:
      - For a dropdown in Streamlit where the choices should mirror HubSpot's
        property options (e.g., a "State" or "Source" field on a deal).

    Parameters
    ----------
    property_name : str
        The internal name of the HubSpot deal property whose options we want.
        Example: "customer_state" or "hs_deal_stage" or your custom property.

    Returns
    -------
    list[dict]
        A list of {"label": str, "value": str} dictionaries.
        - "label" is what you would display to the user.
        - "value" is the underlying option value stored in HubSpot.

        If the live call fails (network error or unexpected payload), a
        sensible fallback list of Australian state/territory codes is returned:
        [{"label":"VIC","value":"VIC"}, ..., {"label":"ACT","value":"ACT"}]

    External dependencies / config
    ------------------------------
    - HS_PROP_URL: base URL for HubSpot property metadata, e.g.
        "https://api.hubapi.com/crm/v3/properties/deals"
      This function appends "/{property_name}" to it.

    - hs_headers(): function returning the HTTP headers including
      Authorization (Bearer token) and Content-Type. It centralises auth so
      we do not duplicate token handling here.

    - requests: HTTP client library used to call HubSpot.

    - st (Streamlit): used for user-friendly info messages when we fall back.

    Why the fallback?
    -----------------
    - UX resilience: if HubSpot is down or the token is wrong, the app still
      renders a drop-down with a default set of states rather than crashing.
    - In your original app, this property was often a "state" selector; returning
      the AU states is a reasonable last-resort default.

    Error handling strategy
    -----------------------
    - r.raise_for_status(): raises on 4xx/5xx to quickly enter the except path.
    - requests.exceptions.RequestException: captures all network/HTTP issues.
    - Broad Exception: a final safety net for any JSON/shape issues.
    - In both failure cases, we display a gentle Streamlit message and return
      the fallback list.
    """

    # Pre-built fallback list if the HubSpot call fails or returns nothing.
    # Shape matches what Streamlit select components typically expect.
    fallback_states = [{"label": s, "value": s} for s in ["VIC","NSW","QLD","SA","WA","TAS","NT","ACT"]]

    try:
        # Construct the HubSpot API endpoint for this specific deal property.
        # Example final URL:
        #   https://api.hubapi.com/crm/v3/properties/deals/customer_state
        url = f"{HS_PROP_URL}/{property_name}"

        # Perform a GET with standard auth headers.
        # `archived=false` ensures we only receive currently-active options.
        # Short timeout keeps the UI responsive on network issues.
        r = requests.get(url, headers=hs_headers(), params={"archived": "false"}, timeout=8)

        # Raise an HTTPError if HubSpot returns 4xx/5xx.
        # This sends us to the RequestException handler below.
        r.raise_for_status()

        # Parse the JSON payload.
        data = r.json()

        # HubSpot responds with an "options" array for enum/selection properties.
        # If "options" is absent or None, we coerce to [] to simplify handling.
        options = data.get("options", []) or []

        out = []
        for opt in options:
            # Each option typically has:
            # - "value": machine value written to the property
            # - "label": human-friendly label (sometimes "displayValue" instead)
            value = str(opt.get("value") or "").strip()
            label = str(opt.get("label") or opt.get("displayValue") or value).strip()

            # Only keep options with a non-empty value.
            # If label is empty, fall back to value so the UI still shows something.
            if value:
                out.append({"label": label or value, "value": value})

        # If HubSpot returned no usable options, fall back to AU states.
        return out or fallback_states

    except requests.exceptions.RequestException:
        # Any network/HTTP-specific problem (timeouts, 401/403/404, etc.).
        # We show a soft info message to the user and use the fallback list.
        st.info("Network issue while fetching state options. Using default states.")
        return fallback_states

    except Exception:
        # Any other unexpected issue (e.g., JSON format changes).
        # Keep the app functional with a gentle message + fallback.
        st.info("Unexpected issue while fetching state options. Using default states.")
        return fallback_states



# ---- hs_search_deals_by_date_property ----

def hs_search_deals_by_date_property(*,
    pipeline_id: str, stage_id: str, state_value: str,
    date_property: str, date_eq_ms: int | None,
    date_start_ms: int | None, date_end_ms: int | None,
    total_cap: int = HS_TOTAL_CAP
) -> pd.DataFrame:
    filters = [
        {"propertyName": "pipeline", "operator": "EQ", "value": pipeline_id},
        {"propertyName": "dealstage", "operator": "EQ", "value": stage_id},
        {"propertyName": "car_location_at_time_of_sale", "operator": "EQ", "value": state_value},
    ]
    if date_eq_ms is not None:
        filters.append({"propertyName": date_property, "operator": "EQ", "value": int(date_eq_ms)})
    else:
        if date_start_ms is not None:
            filters.append({"propertyName": date_property, "operator": "GTE", "value": int(date_start_ms)})
        if date_end_ms is not None:
            filters.append({"propertyName": date_property, "operator": "LTE", "value": int(date_end_ms)})
    payload = {"filterGroups": [{"filters": filters}], "properties": DEAL_PROPS, "limit": HS_PAGE_LIMIT}
    df = _search_once(payload, total_cap=total_cap)
    if df.empty and date_eq_ms is not None:
        widen = 12 * 3600 * 1000
        filters[-1] = {"propertyName": date_property, "operator": "GTE", "value": int(date_eq_ms - widen)}
        filters.append({"propertyName": date_property, "operator": "LTE", "value": int(date_eq_ms + widen)})
        payload = {"filterGroups": [{"filters": filters}], "properties": DEAL_PROPS, "limit": HS_PAGE_LIMIT}
        df = _search_once(payload, total_cap=total_cap)
    return df



# ---- hs_search_deals_by_appointment_and_stages ----

def hs_search_deals_by_appointment_and_stages(appointment_id: str, pipeline_id: str, stage_ids: set[str]) -> pd.DataFrame:
    filters = [
        {"propertyName": "pipeline", "operator": "EQ", "value": pipeline_id},
        {"propertyName": "appointment_id", "operator": "EQ", "value": str(appointment_id).strip()},
        {"propertyName": "dealstage", "operator": "IN", "values": list(stage_ids)},
    ]
    payload = {"filterGroups": [{"filters": filters}], "properties": DEAL_PROPS, "limit": HS_PAGE_LIMIT}
    return _search_once(payload, total_cap=HS_TOTAL_CAP)



# ---- hs_deals_to_contacts_map ----

def hs_deals_to_contacts_map(deal_ids: list[str]) -> dict[str, list[str]]:
    out = {str(d): [] for d in deal_ids}
    if not deal_ids: return out
    url = f"{HS_ROOT}/crm/v4/objects/deals/batch/read"
    payload = {"properties": [], "inputs": [{"id": str(d)} for d in deal_ids], "associations": ["contacts"]}
    try:
        r = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
        r.raise_for_status()
        for item in r.json().get("results", []):
            did = str(item.get("id"))
            contacts = [a.get("id") for a in item.get("associations", {}).get("contacts", [])]
            out[did] = [str(x) for x in contacts if x]
    except Exception as e:
        st.warning(f"Could not read deal→contacts associations: {e}")
    return out



# ---- hs_contacts_to_deals_map ----

def hs_contacts_to_deals_map(contact_ids: list[str]) -> dict[str, list[str]]:
    out = {str(c): [] for c in contact_ids}
    if not contact_ids: return out
    url = f"{HS_ROOT}/crm/v4/objects/contacts/batch/read"
    payload = {"properties": [], "inputs": [{"id": str(c)} for c in contact_ids], "associations": ["deals"]}
    try:
        r = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
        r.raise_for_status()
        for item in r.json().get("results", []):
            cid = str(item.get("id"))
            deals = [a.get("id") for a in item.get("associations", {}).get("deals", [])]
            out[cid] = [str(x) for x in deals if x]
    except Exception as e:
        st.warning(f"Could not read contact→deals associations: {e}")
    return out



# ---- hs_batch_read_deals ----

def hs_batch_read_deals(deal_ids: list[str], props: list[str]) -> dict[str, dict]:
    out = {}
    if not deal_ids: return out
    url = f"{HS_ROOT}/crm/v3/objects/deals/batch/read"
    for i in range(0, len(deal_ids), 100):
        chunk = deal_ids[i:i+100]
        payload = {"properties": props, "inputs": [{"id": str(d)} for d in chunk]}
        try:
            r = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
            r.raise_for_status()
            for item in r.json().get("results", []):
                out[str(item.get("id"))] = item.get("properties", {}) or {}
        except Exception as e:
            st.warning(f"Could not batch read deals (props={props}): {e}")
    return out

# ============ Aircall ============



# ---- update_deals_sms_sent ----

def update_deals_sms_sent(deal_ids: list[str]) -> tuple[int, int]:
    """
    Update the td_reminder_sms_sent property to 'true' for the given deal IDs.
    Returns (success_count, failure_count)
    """
    if not deal_ids:
        return 0, 0
    
    success_count = 0
    failure_count = 0
    
    url = f"{HS_ROOT}/crm/v3/objects/deals/batch/update"
    
    # Process in batches of 100 (HubSpot limit)
    for i in range(0, len(deal_ids), 100):
        batch = deal_ids[i:i+100]
        
        inputs = []
        for deal_id in batch:
            inputs.append({
                "id": str(deal_id),
                "properties": {
                    "td_reminder_sms_sent": "true"  # CHANGED FROM "Yes" to "true"
                }
            })
        
        payload = {"inputs": inputs}
        
        try:
            response = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
            if response.status_code == 200:
                success_count += len(batch)
            else:
                failure_count += len(batch)
                st.warning(f"Failed to update batch: {response.text[:200]}")
        except Exception as e:
            failure_count += len(batch)
            st.warning(f"Error updating deals: {str(e)}")
    
    return success_count, failure_count



# ---- export_sms_update_list ----

def export_sms_update_list(phone_to_deals: dict, sent_phones: list) -> pd.DataFrame:
    """
    Create a DataFrame with deal IDs that need to be updated after SMS send.
    """
    update_records = []
    for phone in sent_phones:
        if phone in phone_to_deals:
            for deal_id in phone_to_deals[phone]:
                update_records.append({
                    "Deal ID": deal_id,
                    "td_reminder_sms_sent": "true",
                    "Phone": phone,
                    "Update Time": datetime.now(MEL_TZ).strftime("%Y-%m-%d %H:%M:%S")
                })
    
    return pd.DataFrame(update_records)



# ---- get_contact_ids_for_deal ----

def get_contact_ids_for_deal(deal_id):
    """Get contact IDs associated with a deal"""
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}  
    try:
        url = f"{HS_ROOT}/crm/v3/objects/deals/{deal_id}/associations/contacts"
        response = requests.get(url, headers=headers, timeout=25)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            return [result.get("toObjectId") or result.get("id") for result in results]
        else:
            return []
    except Exception:
        return []



# ---- get_consolidated_notes_for_deal ----

def get_consolidated_notes_for_deal(deal_id):
    """Get all consolidated notes for a deal"""
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    
    contact_ids = get_contact_ids_for_deal(deal_id)
    
    if not contact_ids:
        return "No notes"
    
    all_formatted_notes = []
    
    for contact_id in contact_ids:
        note_ids = get_contact_note_ids(contact_id)
        
        if note_ids:
            notes = get_notes_content(note_ids)
            
            for note in notes:
                props = note.get("properties", {})
                body = props.get("hs_note_body", "")
                timestamp = props.get("hs_timestamp") or props.get("hs_createdate", "")
                owner_id = props.get("hubspot_owner_id")
                
                if body and body.strip():
                    # Clean HTML from body
                    import re
                    clean_body = re.sub(r'<[^>]+>', '', body).strip()
                    clean_body = clean_body.replace('&nbsp;', ' ').replace('&amp;', '&')
                    
                    if clean_body:
                        # Format timestamp
                        date_str = "Unknown Date"
                        if timestamp:
                            try:
                                from datetime import datetime
                                if len(str(timestamp)) > 10:  # milliseconds
                                    dt = datetime.fromtimestamp(int(timestamp) / 1000)
                                else:  # seconds
                                    dt = datetime.fromtimestamp(int(timestamp))
                                date_str = dt.strftime("%Y-%m-%d %H:%M")
                            except:
                                date_str = str(timestamp)
                        
                        # Get owner name
                        owner_name = get_owner_name(owner_id)
                        
                        formatted_note = f"[{date_str}] ({owner_name}) {clean_body}"
                        all_formatted_notes.append(formatted_note)
    
    if all_formatted_notes:
        return "\n\n".join(all_formatted_notes)
    else:
        return "No notes"



# ---- get_deals_by_owner_and_daterange ----

def get_deals_by_owner_and_daterange(start_date, end_date, state_val, selected_owners):
    """Simplified version - get deals by date range and state only"""
    try:
        start_ms, _ = mel_day_bounds_to_epoch_ms(start_date)
        _, end_ms = mel_day_bounds_to_epoch_ms(end_date)
        
        raw_deals = hs_search_deals_by_date_property(
            pipeline_id=PIPELINE_ID,
            stage_id="1119198253",
            state_value=state_val,
            date_property="td_conducted_date",
            date_eq_ms=None,
            date_start_ms=start_ms,
            date_end_ms=end_ms,
            total_cap=HS_TOTAL_CAP
        )
        
        if raw_deals is None or (isinstance(raw_deals, list) and len(raw_deals) == 0) or (isinstance(raw_deals, pd.DataFrame) and raw_deals.empty):
            return pd.DataFrame()
            
        return prepare_deals(raw_deals)
        
    except Exception as e:
        st.error(f"Error fetching deals: {str(e)}")
        return pd.DataFrame()


# ============ NEW: Appointment ID based car filtering ============



# ---- get_deals_by_appointment_id ----

def get_deals_by_appointment_id(appointment_id: str) -> list[str]:
    """Get all deal IDs that have the given appointment_id"""
    if not appointment_id:
        return []
    
    try:
        url = f"{HS_ROOT}/crm/v3/objects/deals/search"
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "appointment_id",
                            "operator": "EQ",
                            "value": str(appointment_id).strip()
                        }
                    ]
                }
            ],
            "properties": ["hs_object_id", "dealstage", "appointment_id"],
            "limit": 100
        }
        response = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
        
        if response.status_code == 200:
            data = response.json()
            deals = data.get("results", [])
            return [deal["properties"]["hs_object_id"] for deal in deals]
        else:
            st.warning(f"Error searching deals by appointment_id: {response.text}")
            return []
            
    except Exception as e:
        st.warning(f"Exception searching deals by appointment_id: {e}")
        return []



# ---- hs_deals_to_contacts_map ----

def hs_deals_to_contacts_map(deal_ids: list[str]) -> dict[str, list[str]]:
    out = {str(d): [] for d in deal_ids}
    if not deal_ids: return out
    url = f"{HS_ROOT}/crm/v4/objects/deals/batch/read"
    payload = {"properties": [], "inputs": [{"id": str(d)} for d in deal_ids], "associations": ["contacts"]}
    try:
        r = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
        r.raise_for_status()
        for item in r.json().get("results", []):
            did = str(item.get("id"))
            contacts = [a.get("id") for a in item.get("associations", {}).get("contacts", [])]
            out[did] = [str(x) for x in contacts if x]
    except Exception as e:
        st.warning(f"Could not read deal→contacts associations: {e}")
    return out



# ---- hs_contacts_to_deals_map ----

def hs_contacts_to_deals_map(contact_ids: list[str]) -> dict[str, list[str]]:
    out = {str(c): [] for c in contact_ids}
    if not contact_ids: return out
    url = f"{HS_ROOT}/crm/v4/objects/contacts/batch/read"
    payload = {"properties": [], "inputs": [{"id": str(c)} for c in contact_ids], "associations": ["deals"]}
    try:
        r = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
        r.raise_for_status()
        for item in r.json().get("results", []):
            cid = str(item.get("id"))
            deals = [a.get("id") for a in item.get("associations", {}).get("deals", [])]
            out[cid] = [str(x) for x in deals if x]
    except Exception as e:
        st.warning(f"Could not read contact→deals associations: {e}")
    return out



# ---- hs_batch_read_deals ----

def hs_batch_read_deals(deal_ids: list[str], props: list[str]) -> dict[str, dict]:
    out = {}
    if not deal_ids: return out
    url = f"{HS_ROOT}/crm/v3/objects/deals/batch/read"
    for i in range(0, len(deal_ids), 100):
        chunk = deal_ids[i:i+100]
        payload = {"properties": props, "inputs": [{"id": str(d)} for d in chunk]}
        try:
            r = requests.post(url, headers=hs_headers(), json=payload, timeout=25)
            r.raise_for_status()
            for item in r.json().get("results", []):
                out[str(item.get("id"))] = item.get("properties", {}) or {}
        except Exception as e:
            st.warning(f"Could not batch read deals (props={props}): {e}")
    return out

# ============ Aircall ============


def filter_deals_by_appointment_id_car_active_purchases(deals_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter out deals where other deals with the same appointment_id have active purchase stages.
    This works by:
    1. Getting the appointment_id for each deal
    2. Finding all other deals with the same appointment_id  
    3. Checking if any of those other deals have active purchase stages
    4. Excluding the original deal if so
    """
    if deals_df is None or deals_df.empty:
        return deals_df.copy() if isinstance(deals_df, pd.DataFrame) else pd.DataFrame(), pd.DataFrame()
    
    # Get deal IDs
    deal_ids = deals_df.get("hs_object_id", pd.Series(dtype=str)).dropna().astype(str).tolist()
    if not deal_ids:
        return deals_df.copy(), pd.DataFrame()
    
    # Get appointment_id for each deal
    deal_appointment_map = {}
    deal_data = hs_batch_read_deals(deal_ids, props=["appointment_id"])
    
    for deal_id in deal_ids:
        props = deal_data.get(deal_id, {})
        appointment_id = props.get("appointment_id")
        if appointment_id:
            deal_appointment_map[deal_id] = str(appointment_id).strip()
    
    if not deal_appointment_map:
        return deals_df.copy(), pd.DataFrame()
    
    # For each unique appointment_id, find all deals with that appointment_id
    appointment_ids = set(deal_appointment_map.values())
    
    # Get all deals for each appointment_id and check their stages
    deals_to_exclude = set()
    
    for appointment_id in appointment_ids:
        # Get all deals with this appointment_id
        all_deals_for_appointment = get_deals_by_appointment_id(appointment_id)
        
        # Get stages for all these deals
        if all_deals_for_appointment:
            stage_data = hs_batch_read_deals(all_deals_for_appointment, props=["dealstage"])
            
            # Check if any deal (other than our original ones) has active purchase stage
            has_active_purchase = False
            for check_deal_id in all_deals_for_appointment:
                if check_deal_id in deal_ids:
                    continue  # Skip our original deals
                
                stage = (stage_data.get(check_deal_id, {}) or {}).get("dealstage")
                
                if stage and str(stage) in ACTIVE_PURCHASE_STAGE_IDS:
                    has_active_purchase = True
                    break
            
            # If any other deal has active purchase stage, exclude all our original deals with this appointment_id
            if has_active_purchase:
                for deal_id, deal_appointment in deal_appointment_map.items():
                    if deal_appointment == appointment_id:
                        deals_to_exclude.add(deal_id)
    
    # Filter the dataframe
    work = deals_df.copy()
    work["__keep"] = work["hs_object_id"].apply(lambda x: str(x) not in deals_to_exclude)
    
    dropped = work[~work["__keep"]].drop(columns=["__keep"]).copy()
    kept = work[work["__keep"]].drop(columns=["__keep"]).copy()
    
    if not dropped.empty:
        dropped["Reason"] = "Car (via appointment_id) has another deal in active purchase stage"
    
    return kept, dropped

def get_contact_note_ids(contact_id):
    """
    Fetch the list of HubSpot Note IDs that are *associated with a given contact*.

    Behaviour (unchanged)
    ---------------------
    - Builds a direct HTTP GET to HubSpot's associations endpoint for notes.
    - Uses a simple Bearer token header taken from HUBSPOT_TOKEN.
    - If HTTP 200: parses the JSON and extracts each associated note's ID
      (prefers 'toObjectId', falls back to 'id'), coerces to string.
    - If HTTP status != 200 or any exception: returns [] (silent failure by design).

    Why return [] on failure?
    -------------------------
    This mirrors the original function's "soft-fail" UX: upstream code can treat
    a missing list as "no notes" without having to catch exceptions at every call site.

    External dependencies expected in module scope
    ----------------------------------------------
    - HUBSPOT_TOKEN : str   (HubSpot private app token)
    - HS_ROOT       : str   (e.g. "https://api.hubapi.com")
    - requests      : module
    - Optional: Streamlit/logging if you want to add diagnostics (kept out here).

    Parameters
    ----------
    contact_id : str | int
        HubSpot Contact ID whose associated Notes we want.

    Returns
    -------
    list[str]
        List of note IDs associated to this contact. Empty list on error or if none.

    Endpoint details
    ----------------
    GET {HS_ROOT}/crm/v3/objects/contacts/{contact_id}/associations/notes
      - Returns a JSON like:
        {
          "results": [
            {"toObjectId": 12345, "type": "...", ...},
            ...
          ]
        }
      - Some payloads may use "id" instead of "toObjectId"; we try both.
    """
    # Build standard Bearer token headers (no Content-Type needed for GET).
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

    try:
        # Construct the associations endpoint for contact -> notes
        url = f"{HS_ROOT}/crm/v3/objects/contacts/{contact_id}/associations/notes"

        # Keep a practical timeout so the UI does not hang indefinitely.
        response = requests.get(url, headers=headers, timeout=25)

        # Soft-check status: do NOT raise; mirror original behaviour of returning [] on non-200.
        if response.status_code == 200:
            data = response.json() or {}
            results = data.get("results", []) or []

            # Extract an ID from each association object.
            # Prefer 'toObjectId'; some shapes might carry 'id' instead.
            note_ids = [res.get("toObjectId") or res.get("id") for res in results]

            # Coerce to strings and drop falsy values.
            return [str(nid) for nid in note_ids if nid]
        else:
            # Non-200 → treat as "no notes" per original contract.
            return []

    except Exception:
        # Network error, JSON decode error, etc. → silent soft-fail to [].
        return []


def get_notes_content(note_ids):
    """Get note content"""
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    if not note_ids:
        return []
    
    try:
        url = f"{HS_ROOT}/crm/v3/objects/notes/batch/read"
        payload = {
            "properties": ["hs_note_body", "hs_timestamp", "hs_createdate", "hubspot_owner_id"],
            "inputs": [{"id": str(note_id)} for note_id in note_ids]
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=25)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        else:
            return []
    except:
        return []


def get_owner_name(owner_id):
    """Get owner name"""
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    if not owner_id:
        return "Unknown User"
    
    try:
        url = f"{HS_ROOT}/crm/v3/owners/{owner_id}"
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            first_name = data.get("firstName", "")
            last_name = data.get("lastName", "")
            if first_name or last_name:
                return f"{first_name} {last_name}".strip()
            else:
                return data.get("email", f"User {owner_id}")
        else:
            return f"User {owner_id}"
    except:
        return f"User {owner_id}"
