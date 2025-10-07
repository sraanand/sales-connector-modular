# workflows/reminders.py
"""
Reminders workflow:
- Add a multi-select to pick available Sales Associates (from core.roster).
- Round-robin assign the deduped customer list to the selected associates.
- Build SMS using draft_sms_reminder_associate (signed by associate).
- Show 'Sales Associate' column in Message Preview.
- Everything else (filters, HubSpot search, etc.) stays as before.
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import time
import pandas as pd
import streamlit as st


# --- App / Core imports (existing modules in your repo) ---
from config import *
from core.utils import *
from clients.hubspot_client import *
from clients.aircall_client import *
from core.drafting import *
from core.roster import *

MEL_TZ = ZoneInfo("Australia/Melbourne")


# -------------------------------------------
# Helper: show rows we excluded (audit table)
# -------------------------------------------
def _show_removed_table(df: pd.DataFrame, title: str):
    """Small helper to render a table of removed rows with Reason."""
    if df is None or df.empty:
        return
    cols = [
        c for c in [
            "full_name","email","phone_norm","vehicle_make","vehicle_model",
            "dealstage","hs_object_id","Reason"
        ] if c in df.columns
    ]
    st.markdown(f"**{title}** ({len(df)})")
    st.dataframe(
        df[cols].rename(columns={
            "full_name":"Customer","phone_norm":"Phone",
            "vehicle_make":"Make","vehicle_model":"Model",
            "dealstage":"Stage"
        }),
        use_container_width=True
    )


# ---------------------------------------------------------
# Message builder: specifically for Reminders + Associates
# ---------------------------------------------------------
def _build_messages_for_reminders_with_associates(dedup_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construct the final message table for the Reminders flow.
    We expect dedup_df to ALREADY contain 'SalesAssociate' and 'SalesEmail'
    (added via round_robin_assign). If those are blank (no selection),
    we fall back to generic associate name "" and still generate a message.

    Output columns (note SalesAssociate before SMS draft, as requested):
      ["CustomerName","Phone","Email","SalesAssociate","SalesEmail",
       "Cars","WhenExact","WhenRel","DealStages","Message"]
    """
    if dedup_df is None or dedup_df.empty:
        return pd.DataFrame(columns=[
            "CustomerName","Phone","Email","SalesAssociate","SalesEmail",
            "Cars","WhenExact","WhenRel","DealStages","Message"
        ])

    out_rows = []
    for _, row in dedup_df.iterrows():
        phone = str(row.get("Phone") or "").strip()
        if not phone:
            # We exclude rows without a phone here; they’ll be captured in “skipped”.
            continue

        name         = str(row.get("CustomerName") or "").strip()
        cars         = str(row.get("Cars") or "").strip()
        when_rel     = str(row.get("WhenRel") or "").strip()
        video_urls   = str(row.get("VideoURLs") or "").strip()
        associate    = str(row.get("SalesAssociate") or "").strip()
        associate_em = str(row.get("SalesEmail") or "").strip()

        # Build “car + relative time” pairs for the prompt, e.g. “Mazda 3 tomorrow; Kia Cerato today at 13:00”
        pairs_text = build_pairs_text(cars, when_rel)

        # Use associate-personalised reminder
        msg = draft_sms_reminder_associate(
            name=name,
            pairs_text=pairs_text,
            associate_name=associate,        # signed by the associate (not “–Cars24 Laverton”)
            video_urls=video_urls            # keep the video URL rules as-is
        )

        out_rows.append({
            "CustomerName": name,
            "Phone": phone,
            "Email": str(row.get("Email") or "").strip(),
            "SalesAssociate": associate,
            "SalesEmail": associate_em,
            "Cars": cars,
            "WhenExact": str(row.get("WhenExact") or ""),
            "WhenRel": when_rel,
            "DealStages": str(row.get("DealStages") or ""),
            "Message": msg
        })

    return pd.DataFrame(out_rows, columns=[
        "CustomerName","Phone","Email","SalesAssociate","SalesEmail",
        "Cars","WhenExact","WhenRel","DealStages","Message"
    ])


# -----------------------------------
# Main entry: Test Drive Reminders UI
# -----------------------------------
def view_reminders():
    st.subheader("🛣️  Test Drive Reminders")

    # ----------------------------
    # 1) Top form: date + state + associates
    # ----------------------------
    with st.form("reminders_form"):
        st.markdown('<div class="form-row">', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([2, 2, 1])

        # Date of the booking we’re reminding for
        with c1:
            rem_date = st.date_input("TD booking date", value=datetime.now(MEL_TZ).date())

        # State selector (HubSpot property options)
        state_options = hs_get_deal_property_options("car_location_at_time_of_sale")
        values = [o["value"] for o in state_options] if state_options else []
        labels = [o["label"] for o in state_options] if state_options else []
        def_val = "VIC" if "VIC" in values else (values[0] if values else "")

        with c2:
            if labels:
                chosen_label = st.selectbox(
                    "Vehicle state", labels,
                    index=(values.index("VIC") if "VIC" in values else 0)
                )
                label_to_val = {o["label"]: o["value"] for o in state_options}
                rem_state_val = label_to_val.get(chosen_label, def_val)
            else:
                rem_state_val = st.text_input("Vehicle state", value=def_val)

        # Associate multi-select: user chooses who’s working today
        with c3:
            all_names = list_associate_names()
            chosen_names = st.multiselect(
                "Available associates", all_names, default=all_names
            )
        st.markdown("</div>", unsafe_allow_html=True)

        go = st.form_submit_button("Fetch deals", use_container_width=True)

    # ----------------------------
    # 2) On submit: fetch + filter
    # ----------------------------
    if go:
        st.markdown(
            "<span style='background:#4436F5;color:#FFFFFF;padding:4px 8px;border-radius:6px;'>Searching HubSpot…</span>",
            unsafe_allow_html=True
        )

        # Search deals in HubSpot for the selected booking date
        eq_ms, _ = mel_day_bounds_to_epoch_ms(rem_date)
        raw = hs_search_deals_by_date_property(
            pipeline_id=PIPELINE_ID,
            stage_id=STAGE_BOOKED_ID,
            state_value=rem_state_val,
            date_property="td_booking_slot_date",
            date_eq_ms=eq_ms,
            date_start_ms=None, date_end_ms=None,
            total_cap=HS_TOTAL_CAP
        )
        deals = prepare_deals(raw)

        # A) Removed because SMS already sent
        deals_not_sent, removed_sms_sent = filter_sms_already_sent(deals)

        # B) Removed because another deal with same car (via appointment_id) is in active purchase
        deals_car_filtered, dropped_car_purchases = filter_deals_by_appointment_id_car_active_purchases(deals_not_sent)

        # C) Removed because internal/test domains
        deals_f, removed_internal = filter_internal_test_emails(deals_car_filtered)

        # D) Dedup (and keep an audit list of what was collapsed)
        dedup, dedupe_dropped = dedupe_users_with_audit(deals_f, use_conducted=False)

        # E) NEW: Round-robin assignment to associates the user selected
        selected_associates = get_associates_by_names(chosen_names)
        if selected_associates:
            dedup = round_robin_assign(dedup, selected_associates, seed_date=rem_date)
        else:
            # If none selected, we still proceed with blank associate columns
            dedup["SalesAssociate"] = ""
            dedup["SalesEmail"] = ""

        # F) Build messages using associate-personalised drafts
        msgs = _build_messages_for_reminders_with_associates(dedup)

        # Persist for the rest of the page (if you use these later)
        st.session_state["reminders_deals"] = deals_f
        st.session_state["reminders_removed_sms_sent"] = removed_sms_sent
        st.session_state["reminders_dropped_car_purchases"] = dropped_car_purchases
        st.session_state["reminders_removed_internal"] = removed_internal
        st.session_state["reminders_dedup"] = dedup
        st.session_state["reminders_dedupe_dropped"] = dedupe_dropped
        st.session_state["reminders_msgs"] = msgs

    # ----------------------------
    # 3) Render from session
    # ----------------------------
    deals_f      = st.session_state.get("reminders_deals")
    removed_sms  = st.session_state.get("reminders_removed_sms_sent")
    dropped_car  = st.session_state.get("reminders_dropped_car_purchases")
    removed_int  = st.session_state.get("reminders_removed_internal")
    dedup        = st.session_state.get("reminders_dedup")
    dedupe_drop  = st.session_state.get("reminders_dedupe_dropped")
    msgs         = st.session_state.get("reminders_msgs")

    # Store phone-to-deals mapping for later update
    st.session_state["reminders_phone_to_deals"] = get_all_deal_ids_for_contacts(msgs, deals_f) 


    # Show trimmed-out rows FIRST, with reasons
    if isinstance(removed_sms, pd.DataFrame) and not removed_sms.empty:
        st.warning(f"⚠️ {len(removed_sms)} deals excluded — SMS already sent")
        _show_removed_table(removed_sms, "Removed (SMS reminder already sent)")
    if isinstance(dropped_car, pd.DataFrame) and not dropped_car.empty:
        _show_removed_table(dropped_car, "Removed (car has another active purchase deal via appointment_id)")
    if isinstance(removed_int, pd.DataFrame) and not removed_int.empty:
        _show_removed_table(removed_int, "Removed by domain filter (cars24.com / yopmail.com)")

    # Show filtered (kept) trimmed table
    if isinstance(deals_f, pd.DataFrame) and not deals_f.empty:
        disp = deals_f.copy()
        if "dealstage" in disp.columns and "Stage" not in disp.columns:
            disp["Stage"] = disp["dealstage"].apply(stage_label)
        st.markdown("#### Filtered deals (trimmed)")
        st.dataframe(
            disp[[
                c for c in [
                    "hs_object_id","appointment_id","full_name","email","phone_norm",
                    "vehicle_make","vehicle_model","slot_date_prop","slot_time_param",
                    "video_url__short_","td_reminder_sms_sent","Stage"
                ] if c in disp.columns
            ]].rename(columns={
                "hs_object_id":"Deal ID",
                "appointment_id":"Appointment ID",
                "full_name":"Customer",
                "phone_norm":"Phone",
                "vehicle_make":"Make",
                "vehicle_model":"Model",
                "slot_date_prop":"TD booking date",
                "slot_time_param":"Time",
                "video_url__short_":"Video URL"
            }),
            use_container_width=True, height=380
        )

    # Show dedup results
    if isinstance(dedupe_drop, pd.DataFrame) and not dedupe_drop.empty:
        _show_removed_table(dedupe_drop, "Collapsed during dedupe (duplicates)")

    if isinstance(dedup, pd.DataFrame) and not dedup.empty:
        st.markdown("#### Deduped list (by mobile|email)")
        st.dataframe(
            dedup[[
                c for c in [
                    "CustomerName","Phone","Email","DealsCount",
                    "Cars","WhenExact","DealStages","SalesAssociate","SalesEmail","VideoURLs"
                ] if c in dedup.columns
            ]].rename(columns={"WhenExact":"When (exact)","DealStages":"Stage(s)"}),
            use_container_width=True
        )

    # Show Message Preview with Sales Associate column
    edited = pd.DataFrame()
    if isinstance(msgs, pd.DataFrame) and not msgs.empty:
        st.markdown("#### Message Preview (Reminders)")
        # We render the preview inline to ensure SalesAssociate is shown between Phone and SMS
        view_df = msgs[[
            "CustomerName","Phone","SalesAssociate","Message"
        ]].rename(columns={
            "CustomerName":"Customer",
            "Message":"SMS draft"
        }).copy()

        if "Send" not in view_df.columns:
            view_df.insert(0, "Send", False)

        edited = st.data_editor(
            view_df,
            key="editor_reminders",
            use_container_width=True,
            height=420,
            column_config={
                "Send": st.column_config.CheckboxColumn("Send", help="Tick to send", default=False, width="small"),
                "Customer": st.column_config.TextColumn("Customer", width=160),
                "Phone": st.column_config.TextColumn("Phone", width=140),
                "SalesAssociate": st.column_config.TextColumn("Sales Associate", width=140),
                "SMS draft": st.column_config.TextColumn("SMS draft", width=520,
                    help="You can edit the text before sending")
            },
            hide_index=True,
        )
    else:
        st.info("No messages to preview.")


# MODIFIED: Send SMS button with deal update functionality
    if not edited.empty and st.button("Send SMS"):
        to_send = edited[edited["Send"]]
        if to_send.empty:
            st.warning("No rows selected.")
        elif not (AIRCALL_ID and AIRCALL_TOKEN and AIRCALL_NUMBER_ID):
            st.error("Missing Aircall credentials in .env.")
        else:
            st.info("Sending messages…")
            sent, failed = 0, 0
            sent_phones = []  # Track which phones were sent successfully
            
            for _, r in to_send.iterrows():
                ok, msg = send_sms_via_aircall(r["Phone"], r["SMS draft"], AIRCALL_NUMBER_ID)
                if ok: 
                    sent += 1
                    sent_phones.append(r["Phone"])
                    st.success(f"✅ Sent to {r['Phone']}")
                else:  
                    failed += 1
                    st.error(f"❌ Failed for {r['Phone']}: {msg}")
                time.sleep(1)
            
            # NEW: Update deals in HubSpot after successful sends
            if sent_phones and st.session_state.get("reminders_phone_to_deals"):
                st.info("Updating HubSpot deals...")
                phone_to_deals = st.session_state["reminders_phone_to_deals"]
                
                all_deal_ids = []
                for phone in sent_phones:
                    if phone in phone_to_deals:
                        all_deal_ids.extend(phone_to_deals[phone])
                
                if all_deal_ids:
                    # Build mapping deal_id -> associate_email
                    deal_to_email = {}
                    for _, r in to_send.iterrows():
                        phone = r["Phone"]
                        associate_email = r.get("SalesEmail", "")
                        user_id = r.get("SalesUserId")
                        if phone in phone_to_deals:
                            for deal_id in phone_to_deals[phone]:
                                deal_to_email[deal_id] = associate_email
                    update_success, update_fail = update_deals_sms_sent(deal_to_email)
                    if update_success > 0:
                        st.success(f"✅ Updated {update_success} deals with SMS sent status")
                    if update_fail > 0:
                        st.warning(f"⚠️ Failed to update {update_fail} deals")
            
            if sent: st.balloons()
            st.success(f"🎉 Done! SMS Sent: {sent} | Failed: {failed}") 
