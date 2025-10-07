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
    {"name": "Thomas", "email": "thomas.trindall@cars24.com"},
    {"name": "Ian",    "email": "81519037"},
    {"name": "Aanand", "email": "sr.aanand@cars24.com"},
]


def list_associate_names() -> List[str]:
    """Return the list of nicknames (for display in the multi-select)."""
    return [a["name"] for a in ASSOCIATES]


def get_associates_by_names(selected_names: List[str]) -> List[Dict[str, str]]:
    """
    Given a list of nicknames (as selected in the multi-select),
    return [{"name": "...", "email": "..."}, ...] in the same order.
    """
    selected = []
    names_set = set(n.strip() for n in selected_names or [])
    for a in ASSOCIATES:
        if a["name"] in names_set:
            selected.append({"name": a["name"], "email": a["email"]})
    return selected


# -------------------------------------------------
# 2) Round-robin assignment to a deduped lead list
# -------------------------------------------------
def round_robin_assign(
    dedup_df: pd.DataFrame,
    associates: List[Dict[str, str]],
    *,
    seed_date: date | None = None,
) -> pd.DataFrame:
    """
    Attach SalesAssociate + SalesEmail to each row of dedup_df by cycling
    through the selected associates.

    Parameters
    ----------
    dedup_df : DataFrame
        Expected columns (among others): ["CustomerName","Phone",...].
        We will not mutate the input; a copy is returned.
    associates : list of dict
        [{"name":"Thomas","email":"..."}, ...] — must be non-empty.
    seed_date : date | None
        Optional date to create a deterministic offset so that the first
        associate alternates daily. If None, no offset is applied.

    Returns
    -------
    DataFrame
        Same rows + two new columns: ["SalesAssociate","SalesEmail"].
        If associates is empty or dedup_df is empty, we add empty columns.
    """
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

    # Stable order so assignments are repeatable within a run
    work = dedup_df.copy().sort_values(
        by=["Phone", "CustomerName"], na_position="last", kind="stable"
    ).reset_index(drop=True)

    # Optional deterministic offset by date so the first associate rotates
    roster = associates[:]
    if seed_date:
        n = len(roster)
        offset = (seed_date.year * 10000 + seed_date.month * 100 + seed_date.day) % n
        roster = roster[offset:] + roster[:offset]

    rr = cycle(roster)

    names, emails = [], []
    for _idx, _row in work.iterrows():
        a = next(rr)
        names.append(a["name"])
        emails.append(a["email"])

    work["SalesAssociate"] = names
    work["SalesEmail"] = emails
    return work
