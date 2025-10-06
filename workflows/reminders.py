"""Workflow view: Test Drive Reminders — extracted from original app.py."""


from config import *
from clients.hubspot_client import *
from clients.aircall_client import *
from core.utils import *
from core.drafting import *
from core.roster import *
from ui.components import *
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def view_reminders():
    st.subheader("🛣️  Test Drive Reminders")
    with st.form("reminders_form"):
        st.markdown('<div class="form-row">', unsafe_allow_html=True)
        c1,c2,c3 = st.columns([2,2,1])
        with c1: rem_date = st.date_input("TD booking date", value=datetime.now(MEL_TZ).date())
        state_options = hs_get_deal_property_options("car_location_at_time_of_sale")
        values = [o["value"] for o in state_options] if state_options else []
        labels = [o["label"] for o in state_options] if state_options else []
        def_val = "VIC" if "VIC" in values else (values[0] if values else "")
        with c2:
            if labels:
                chosen_label = st.selectbox("Vehicle state", labels, index=(values.index("VIC") if "VIC" in values else 0))
                label_to_val = {o["label"]:o["value"] for o in state_options}
                rem_state_val = label_to_val.get(chosen_label, def_val)
            else:
                rem_state_val = st.text_input("Vehicle state", value=def_val)
        with c3: go = st.form_submit_button("Fetch deals", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    if go:
        st.markdown("<span style='background:#4736FE;color:#FFFFFF;padding:4px 8px;border-radius:6px;'>Searching HubSpot…</span>", unsafe_allow_html=True)
        eq_ms, _ = mel_day_bounds_to_epoch_ms(rem_date)
        raw = hs_search_deals_by_date_property(
            pipeline_id=PIPELINE_ID, stage_id=STAGE_BOOKED_ID, state_value=rem_state_val,
            date_property="td_booking_slot_date", date_eq_ms=eq_ms,
            date_start_ms=None, date_end_ms=None, total_cap=HS_TOTAL_CAP
        )
        deals = prepare_deals(raw)

        # 1) NEW: Filter out deals where SMS was already sent
        deals_not_sent, removed_sms_sent = filter_sms_already_sent(deals)

        # 2) Filter by appointment_id car active purchases
        deals_car_filtered, dropped_car_purchases = filter_deals_by_appointment_id_car_active_purchases(deals_not_sent)

        # 3) Filter internal/test emails
        deals_f, removed_internal = filter_internal_test_emails(deals_car_filtered)

        # 4) Audit dedupe
        dedup, dedupe_dropped = dedupe_users_with_audit(deals_f, use_conducted=False)

        # --- ROSTER: load, filter by availability, assign round-robin ---

        # a) Load the roster (one-time per run; cache if you want with st.cache_data)
        ROSTER_URL = "https://docs.google.com/spreadsheets/d/1-9Ax-7GUymChhKaRXAyCxBG7oDNJ9wqBlxcZivzz8Ic/edit?usp=sharing"
        roster_df = load_roster_df(ROSTER_URL)  # uses Service Account secrets or CSV fallback

        # b) Determine the target date for reminders (the form's rem_date)
        today_mel = datetime.now(MEL_TZ).date()  # or use 'rem_date' if that’s the send date
        target_date = rem_date if 'rem_date' in locals() else today_mel

        # c) Pick associates available on target_date
        avail = available_associates_for_date(roster_df, target_date)

        if not avail:
            st.warning("No sales associates marked available for the selected date. SMS will be generic.")
            # Proceed without assignment: dedup remains unchanged; message drafts will fall back to your existing function.
        else:
            # d) Assign round-robin and attach SalesAssociate / SalesEmail
            dedup = round_robin_assign(dedup, avail, target_date)

        # 5) Build messages with audit
        # --- Build messages with associate personalisation (Reminders only) ---
        if dedup is None or dedup.empty:
            msgs = pd.DataFrame(columns=["CustomerName","Phone","Email","SalesAssociate","Cars","WhenExact","WhenRel","DealStages","Message"])
            skipped_msgs = pd.DataFrame(columns=["Customer","Email","Cars","Reason"])
        else:
            out_rows = []
            skipped = []
            for _, row in dedup.iterrows():
                phone = str(row.get("Phone") or "").strip()
                if not phone:
                    skipped.append({
                        "Customer": str(row.get("CustomerName") or ""),
                        "Email": str(row.get("Email") or ""),
                        "Cars": str(row.get("Cars") or ""),
                        "Reason": "Missing/invalid phone"
                    })
                    continue

                name  = str(row.get("CustomerName") or "").strip()
                cars  = str(row.get("Cars") or "").strip()
                when_rel = str(row.get("WhenRel") or "").strip()
                pairs_text = build_pairs_text(cars, when_rel)

                video_urls = str(row.get("VideoURLs") or "").strip()
                associate_name = str(row.get("SalesAssociate") or "").strip()

                if associate_name:
                    # Personalised, associate-signed reminder
                    msg = draft_sms_reminder_associate(name, pairs_text, associate_name, video_urls)
                else:
                    # Fallback to your original generic reminder function (if no associates available)
                    msg = draft_sms_reminder(name, pairs_text, video_urls)

                out_rows.append({
                    "CustomerName": name,
                    "Phone": phone,
                    "Email": str(row.get("Email") or "").strip(),
                    "SalesAssociate": associate_name,   # NEW
                    "Cars": cars,
                    "WhenExact": str(row.get("WhenExact") or ""),
                    "WhenRel": when_rel,
                    "DealStages": str(row.get("DealStages") or ""),
                    "Message": msg
                })

            msgs = pd.DataFrame(out_rows, columns=["CustomerName","Phone","Email","SalesAssociate","Cars","WhenExact","WhenRel","DealStages","Message"])
            skipped_msgs = pd.DataFrame(skipped, columns=["Customer","Email","Cars","Reason"])


        # Store all artifacts
        st.session_state["reminders_deals"] = deals_f
        st.session_state["reminders_removed_sms_sent"] = removed_sms_sent
        st.session_state["reminders_dropped_car_purchases"] = dropped_car_purchases
        st.session_state["reminders_removed_internal"] = removed_internal
        st.session_state["reminders_dedup"] = dedup
        st.session_state["reminders_dedupe_dropped"] = dedupe_dropped
        st.session_state["reminders_msgs"]  = msgs
        st.session_state["reminders_skipped_msgs"] = skipped_msgs
        
        # Store phone-to-deals mapping for later update
        st.session_state["reminders_phone_to_deals"] = get_all_deal_ids_for_contacts(msgs, deals_f)

    # ---- Render from persisted state ----
    deals_f      = st.session_state.get("reminders_deals")
    removed_sms  = st.session_state.get("reminders_removed_sms_sent")  # NEW
    dropped_car  = st.session_state.get("reminders_dropped_car_purchases")
    removed_int  = st.session_state.get("reminders_removed_internal")
    dedup        = st.session_state.get("reminders_dedup")
    dedupe_drop  = st.session_state.get("reminders_dedupe_dropped")
    msgs         = st.session_state.get("reminders_msgs")
    skipped_msgs = st.session_state.get("reminders_skipped_msgs")


    # NEW: Show SMS already sent filter results FIRST
    if isinstance(removed_sms, pd.DataFrame) and not removed_sms.empty:
        st.warning(f"⚠️ {len(removed_sms)} deals excluded - SMS reminders already sent")
        show_removed_table(removed_sms, "Removed (SMS reminder already sent)")

    # Show car purchase filter results
    if isinstance(dropped_car, pd.DataFrame) and not dropped_car.empty:
        show_removed_table(dropped_car, "Removed (car has active purchase deal via appointment_id)")

    if isinstance(removed_int, pd.DataFrame) and not removed_int.empty:
        show_removed_table(removed_int, "Removed by domain filter (cars24.com / yopmail.com)")

    if isinstance(deals_f, pd.DataFrame) and not deals_f.empty:
        # Update column display to include SMS status
        render_trimmed(deals_f, "Filtered deals (trimmed)", [
            ("hs_object_id","Deal ID"), 
            ("appointment_id","Appointment ID"), 
            ("full_name","Customer"), 
            ("email","Email"), 
            ("phone_norm","Phone"),
            ("vehicle_make","Make"), 
            ("vehicle_model","Model"),
            ("slot_date_prop","TD booking date"), 
            ("slot_time_param","Time"),
            ("video_url__short_","Video URL"),
            ("td_reminder_sms_sent","SMS Sent"),  # NEW
            ("Stage","Stage"),
        ])

    if isinstance(dedupe_drop, pd.DataFrame) and not dedupe_drop.empty:
        show_removed_table(dedupe_drop, "Collapsed during dedupe (duplicates)")

    if isinstance(dedup, pd.DataFrame) and not dedup.empty:
        st.markdown("#### <span style='color:#000000;'>Deduped list (by mobile|email)</span>", unsafe_allow_html=True)
        st.dataframe(dedup[["CustomerName","Phone","Email","DealsCount","Cars","WhenExact","DealStages","VideoURLs"]]
                     .rename(columns={"WhenExact":"When (exact)","DealStages":"Stage(s)"}),
                     use_container_width=True)

    if isinstance(msgs, pd.DataFrame) and not msgs.empty:
        st.markdown("#### <span style='color:#000000;'>Message Preview (Reminders)</span>", unsafe_allow_html=True)
        edited = render_selectable_messages(msgs, key="reminders")
        if isinstance(skipped_msgs, pd.DataFrame) and not skipped_msgs.empty:
            st.markdown("**Skipped while creating SMS**")
            st.dataframe(skipped_msgs, use_container_width=True)

    # --- SEND: SMS + update HubSpot ticket_owner ---
    # NOTE: change the button CTA label as requested
    if not edited.empty and st.button("Send SMS and update HS"):
        to_send = edited[edited["Send"]]

        if to_send.empty:
            st.warning("No rows selected.")
        elif not (AIRCALL_ID and AIRCALL_TOKEN and AIRCALL_NUMBER_ID):
            st.error("Missing Aircall credentials in .env.")
        else:
            # We will:
            # 1) send SMS per selected row
            # 2) collect phones that were actually sent
            # 3) map those phones -> deal_ids (from session)
            # 4) map each deal_id -> the associate's SalesEmail from msgs
            # 5) batch update 'ticket_owner' in HubSpot

            st.info("Sending messages…")
            sent_phones: list[str] = []
            sent, failed = 0, 0

            # Build a quick lookup from phone -> SalesEmail from the 'msgs' DF
            # (Assumes you included 'SalesEmail' in msgs when you built it.)
            phone_to_sales_email = {}
            if "SalesEmail" in msgs.columns:
                # If there could be duplicates, the first is fine; all rows for that phone
                # should have the same SalesEmail because of round-robin assignment.
                grouper = msgs.groupby("Phone", dropna=False)["SalesEmail"].first()
                phone_to_sales_email = grouper.to_dict()

            for _, r in to_send.iterrows():
                phone = str(r["Phone"]).strip()

                # Body is editable in the table, so we use the edited text
                body = str(r["SMS draft"]).strip()

                ok, msg = send_sms_via_aircall(phone, body, AIRCALL_NUMBER_ID)
                if ok:
                    sent += 1
                    sent_phones.append(phone)
                    st.success(f"✅ Sent to {phone}")
                else:
                    failed += 1
                    st.error(f"❌ Failed for {phone}: {msg}")

                time.sleep(1)

            # === HubSpot ticket_owner update (ONLY for phones that actually got an SMS) ===
            # Build deal_id -> associate_email map.
            # We use the phone -> [deal_ids] mapping persisted earlier in session state.
            phone_to_deals = st.session_state.get("reminders_phone_to_deals") or {}
            deal_to_email: dict[str, str] = {}

            # You told us the sheet layout is strict:
            #  A: email (we already carried this as SalesEmail)
            #  B: nickname (displayed as SalesAssociate)
            #  C..I: Mon..Sun availability
            # Given that, we trust 'SalesEmail' in msgs for the correct HubSpot owner value.
            for phone in sent_phones:
                sales_email = phone_to_sales_email.get(phone, "").strip()
                if not sales_email:
                    # If for any reason SalesEmail didn't come through, skip updating HS for this phone.
                    # (Optionally warn so you can fix roster mapping.)
                    st.warning(f"⚠️ No SalesEmail found for {phone}; skipping HubSpot owner update.")
                    continue

                # Update ALL deals tied to that phone (you created this map earlier)
                for did in phone_to_deals.get(phone, []):
                    deal_to_email[str(did)] = sales_email

            # Perform one or more batch updates (100 per chunk handled inside helper)
            if deal_to_email:
                st.info("Updating HubSpot ticket owners…")
                u_ok, u_fail = hs_update_ticket_owner_map(deal_to_email)
                if u_ok:
                    st.success(f"✅ Updated 'ticket_owner' on {u_ok} deal(s)")
                if u_fail:
                    st.warning(f"⚠️ Failed to update 'ticket_owner' on {u_fail} deal(s)")

            if sent:
                st.balloons()
            st.success(f"🎉 Done! SMS Sent: {sent} | Failed: {failed}")
