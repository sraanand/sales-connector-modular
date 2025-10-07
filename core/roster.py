# core/roster.py
"""
Simple, sheet-free roster utilities.

We deliberately DROP Google Sheets and keep a fixed list of associates here.
Reminders workflow will let the user pick from this list via a multi-select.
Those selected will be assigned to the deduped customer list round-robin.
"""

from __future__ import annotations
from datetime import date
from itertools import cycle
from typing import List, Dict
import pandas as pd


# -----------------------------
# 1) Static associate directory
# -----------------------------
# Keep ALL truth (email + nickname) here. You can extend this list later.
ASSOCIATES: List[Dict[str, str]] = [
    {"name": "Thomas", "email": "thomas.trindall@cars24.com", "internal_id":118915857},
    {"name": "Ian",    "email": "zhan.hung@cars24.com", "internal_id":81519037},
    {"name": "Aanand", "email": "sr.aanand@cars24.com", "internal_id":176927767},
]



def list_associate_names() -> List[str]:
    return [a["name"] for a in ASSOCIATES]

def list_associate_email() -> List[str]:
    return [a["email"] for a in ASSOCIATES]

def get_associates_by_names(selected_names: List[str]) -> List[Dict[str, str]]:
    names = set(n.strip() for n in (selected_names or []))
    return [ {"name": a["name"], "email": a["email"], "internal_id": a["internal_id"]}
             for a in ASSOCIATES if a["name"] in names ]

def round_robin_assign(dedup_df: pd.DataFrame, associates: List[Dict[str, str]], *, seed_date: date|None=None) -> pd.DataFrame:
    if dedup_df is None or dedup_df.empty:
        out = dedup_df.copy() if isinstance(dedup_df, pd.DataFrame) else pd.DataFrame()
        if isinstance(out, pd.DataFrame) and not out.empty:
            out["SalesAssociate"] = ""
            out["SalesEmail"] = ""
        return out
    if not associates:
        out = dedup_df.copy()
        out["SalesAssociate"] = ""
        out["SalesEmail"] = ""
        return out

    work = dedup_df.copy().sort_values(by=["Phone","CustomerName"], na_position="last", kind="stable").reset_index(drop=True)
    roster = associates[:]
    if seed_date:
        n = len(roster)
        offset = (seed_date.year * 10000 + seed_date.month * 100 + seed_date.day) % n
        roster = roster[offset:] + roster[:offset]
    rr = cycle(roster)

    names, emails, ids = [], [], []
    for _i, _r in work.iterrows():
        a = next(rr)
        names.append(a["name"])
        emails.append(a["email"])
        ids.append(a["internal_id"])
    work["SalesAssociate"] = names
    work["SalesEmail"] = emails
    work["SalesUserIds"] = ids
    return work
