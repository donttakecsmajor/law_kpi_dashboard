import streamlit as st
import pandas as pd
from datetime import date
import calendar

from db import init_db, execute, query_df, get_setting, set_setting

# ------------------ CONFIG ------------------
st.set_page_config(page_title="Denmon MVP Dashboards", layout="wide")

# ------------------ SIDEBAR: DB RECONNECT + INIT ------------------
st.sidebar.title("Denmon MVP")

if st.sidebar.button("Reconnect DB"):
    st.session_state.pop("db_inited", None)
    st.rerun()

# Run DB init only once per user session (avoids repeated DDL on reruns)
if "db_inited" not in st.session_state:
    init_db()
    st.session_state["db_inited"] = True

PEOPLE = ["Jackelin", "Emma", "Alejandra", "David", "Caroline"]

MONTHS = [
    (1, "Jan"), (2, "Feb"), (3, "Mar"), (4, "Apr"),
    (5, "May"), (6, "Jun"), (7, "Jul"), (8, "Aug"),
    (9, "Sep"), (10, "Oct"), (11, "Nov"), (12, "Dec"),
]

# ------------------ HELPERS ------------------
def currency(x) -> str:
    try:
        return f"${float(x):,.2f}"
    except:
        return "$0.00"

def safe_float(x, default=0.0):
    try:
        return float(x)
    except:
        return default

def yyyymm_from_year_month(year: int, month: int) -> str:
    return f"{year}-{month:02d}"

def start_end_for_month(year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)

def start_end_for_ytd(year: int):
    start = date(year, 1, 1)
    today = date.today()
    end = today if year == today.year else date(year, 12, 31)
    return start, end

def dash(val):
    return "—" if val is None else val

# ------------------ NAV ------------------
page = st.sidebar.radio(
    "Go to",
    [
        "Data Entry — Settlements",
        "Data Entry — Pre-Suit KPIs",
        "Goals / Settings",
        "Dashboard — Firmwide",
        "Dashboard — Pre-Suit",
    ],
)

# =========================================================
# PAGE 1: DATA ENTRY — SETTLEMENTS
# =========================================================
if page == "Data Entry — Settlements":
    st.title("Data Entry — Settlements")
    ##st.caption("CM/PARA | CLIENT | SETTLEMENT AMOUNT | POLICY LIMITS | FEE EARNED | DATE OF SETTLEMENT | TOD")

    with st.form("settlement_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)

        with c1:
            person = st.selectbox("CM/PARA", PEOPLE)
            client = st.text_input("CLIENT")
            settlement_date = st.date_input("DATE OF SETTLEMENT", value=date.today())

        with c2:
            settlement_amount = st.number_input("SETTLEMENT AMOUNT", min_value=0.0, step=1000.0)
            fee_earned = st.number_input("FEE EARNED", min_value=0.0, step=100.0)

        with c3:
            policy_limits = st.number_input("POLICY LIMITS", min_value=0.0, step=1000.0)
            tod = st.text_input("TOD (optional)")
            track = st.selectbox("Track (for % split)", ["unknown", "pre_suit", "litigation"])

        submitted = st.form_submit_button("Save", use_container_width=True)
        if submitted:
            if not client.strip():
                st.error("CLIENT is required.")
            else:
                execute(
                    """
                    INSERT INTO settlements
                    (person_name, client_name, settlement_amount, policy_limits, fee_earned, settlement_date, tod, track)
                    VALUES (:person_name, :client_name, :settlement_amount, :policy_limits, :fee_earned, :settlement_date, :tod, :track)
                    """,
                    {
                        "person_name": person,
                        "client_name": client.strip(),
                        "settlement_amount": float(settlement_amount),
                        "policy_limits": float(policy_limits),
                        "fee_earned": float(fee_earned),
                        "settlement_date": settlement_date.isoformat(),
                        "tod": tod.strip() if tod else None,
                        "track": track,
                    },
                )
                st.success("Saved settlement row.")

    st.divider()
    st.subheader("Recent entries")
    df = query_df(
        """
        SELECT person_name AS "CM/PARA",
               client_name AS "CLIENT",
               settlement_amount AS "SETTLEMENT AMOUNT",
               policy_limits AS "POLICY LIMITS",
               fee_earned AS "FEE EARNED",
               settlement_date AS "DATE OF SETTLEMENT",
               tod AS "TOD",
               track AS "TRACK"
        FROM settlements
        ORDER BY settlement_date DESC, id DESC
        LIMIT 200
        """
    )
    st.dataframe(df, use_container_width=True, hide_index=True)

# =========================================================
# PAGE 2: DATA ENTRY — PRE-SUIT KPIs
# =========================================================
elif page == "Data Entry — Pre-Suit KPIs":
    st.title("Data Entry — Pre-Suit KPIs")
    ##st.caption("# DEMANDS SENT | SETTLEMENTS $ | AVERAGE LIEN RESOLUTION | NO. OF FILES W/OUT 14 DAY CONTACT | NPS SCORE")

    today = date.today()
    default_month = yyyymm_from_year_month(today.year, today.month)

    with st.form("pre_suit_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)

        with c1:
            person = st.selectbox("Person", PEOPLE)
            month = st.text_input("Month (YYYY-MM)", value=default_month)

        with c2:
            demands_sent = st.number_input("# DEMANDS SENT", min_value=0, step=1)
            settlements_kpi = st.number_input("SETTLEMENTS $", min_value=0.0, step=1000.0)

        with c3:
            lien_days = st.number_input("AVERAGE LIEN RESOLUTION (days)", min_value=0.0, step=1.0)
            no_contact = st.number_input("NO. OF FILES W/OUT 14 DAY CONTACT", min_value=0, step=1)
            nps = st.number_input("NPS SCORE (0-5)", min_value=0.0, max_value=5.0, step=0.5)

        submitted = st.form_submit_button("Save / Update Month", use_container_width=True)
        if submitted:
            execute(
                """
                INSERT INTO pre_suit_kpis
                (person_name, month, demands_sent, settlements_amount, avg_lien_resolution_days,
                 files_without_14_day_contact, nps_score)
                VALUES (:person_name, :month, :demands_sent, :settlements_amount, :avg_lien_resolution_days,
                        :files_without_14_day_contact, :nps_score)
                ON CONFLICT (person_name, month)
                DO UPDATE SET
                    demands_sent = EXCLUDED.demands_sent,
                    settlements_amount = EXCLUDED.settlements_amount,
                    avg_lien_resolution_days = EXCLUDED.avg_lien_resolution_days,
                    files_without_14_day_contact = EXCLUDED.files_without_14_day_contact,
                    nps_score = EXCLUDED.nps_score
                """,
                {
                    "person_name": person,
                    "month": month.strip(),
                    "demands_sent": int(demands_sent),
                    "settlements_amount": float(settlements_kpi),
                    "avg_lien_resolution_days": float(lien_days),
                    "files_without_14_day_contact": int(no_contact),
                    "nps_score": float(nps),
                },
            )
            st.success("Saved / Updated KPI row.")

    st.divider()
    st.subheader("KPI rows")
    df = query_df(
        """
        SELECT person_name AS "PERSON",
               month AS "MONTH",
               demands_sent AS "# DEMANDS SENT",
               settlements_amount AS "SETTLEMENTS $",
               avg_lien_resolution_days AS "AVG LIEN (days)",
               files_without_14_day_contact AS "FILES W/OUT 14D CONTACT",
               nps_score AS "NPS"
        FROM pre_suit_kpis
        ORDER BY month DESC, person_name ASC
        LIMIT 200
        """
    )
    st.dataframe(df, use_container_width=True, hide_index=True)

# =========================================================
# PAGE 3: GOALS / SETTINGS (YEAR-AWARE)
# =========================================================
elif page == "Goals / Settings":
    st.title("Goals / Settings")
    st.caption("Revenue goal is stored per-year (default: 2026). Google reviews are global.")

    year_options = list(range(2024, 2031))
    default_year_idx = year_options.index(2026) if 2026 in year_options else 0

    c0, c1, c2, c3 = st.columns([1.0, 1.3, 1.3, 1.3])

    with c0:
        goal_year = st.selectbox("Goal Year", year_options, index=default_year_idx)

    revenue_key = f"revenue_goal_{goal_year}"
    fallback_2026 = get_setting("revenue_goal_2026", "0")

    with c1:
        revenue_goal = st.number_input(
            f"{goal_year} Revenue Goal (Fees Earned)",
            min_value=0.0,
            step=10000.0,
            value=safe_float(get_setting(revenue_key, fallback_2026), 0.0),
        )

    with c2:
        google_base = st.number_input(
            "Google Reviews Baseline ",
            min_value=0,
            step=1,
            value=int(safe_float(get_setting("google_reviews_baseline", "221"), 221)),
        )

    with c3:
        google_current = st.number_input(
            "Google Reviews Current",
            min_value=0,
            step=1,
            value=int(safe_float(get_setting("google_reviews_current", "221"), 221)),
        )

    if st.button("Save Settings", use_container_width=True):
        set_setting(revenue_key, str(float(revenue_goal)))
        set_setting("google_reviews_baseline", str(int(google_base)))
        set_setting("google_reviews_current", str(int(google_current)))
        st.success(f"Saved. Revenue goal stored as: {revenue_key}")

# =========================================================
# PAGE 4: DASHBOARD — FIRMWIDE
# =========================================================
elif page == "Dashboard — Firmwide":
    today = date.today()
    year_options = list(range(2024, 2031))
    default_year_idx = year_options.index(2026) if 2026 in year_options else year_options.index(today.year)

    c1, c2, c3 = st.columns([1.2, 1.5, 2.3])
    with c1:
        view_mode = st.selectbox("View", ["YTD", "Monthly", "Custom"])
    with c2:
        year_sel = st.selectbox("Year", year_options, index=default_year_idx)
    with c3:
        month_sel = st.selectbox("Month", [m[1] for m in MONTHS], index=0)

    st.title(f'{year_sel} FIRMWIDE DASHBOARD — DENMON "D2" LAW')

    if view_mode == "YTD":
        start, end = start_end_for_ytd(int(year_sel))
        header_range = f"YTD {year_sel} ({start.isoformat()} → {end.isoformat()})"
    elif view_mode == "Monthly":
        month_num = [m[0] for m in MONTHS if m[1] == month_sel][0]
        start, end = start_end_for_month(int(year_sel), int(month_num))
        header_range = f"{month_sel}-{str(year_sel)[-2:]} ({start.isoformat()} → {end.isoformat()})"
    else:
        c4, c5 = st.columns(2)
        with c4:
            start = st.date_input("Custom start", value=date(int(year_sel), 1, 1))
        with c5:
            end = st.date_input("Custom end", value=date.today())
        header_range = f"Custom ({start.isoformat()} → {end.isoformat()})"

    st.subheader(header_range)

    df = query_df(
        """
        SELECT person_name, client_name, settlement_amount, policy_limits, fee_earned, settlement_date, tod, track
        FROM settlements
        WHERE settlement_date BETWEEN :start AND :end
        """,
        {"start": start.isoformat(), "end": end.isoformat()},
    )

    if not df.empty and "settlement_date" in df.columns:
        df["settlement_date"] = pd.to_datetime(df["settlement_date"]).dt.date.astype(str)

    total_settlement = float(df["settlement_amount"].sum()) if not df.empty else 0.0
    total_fees = float(df["fee_earned"].sum()) if not df.empty else 0.0
    num_cases = int(len(df)) if not df.empty else 0
    avg_settlement = float(df["settlement_amount"].mean()) if num_cases else 0.0
    avg_fee = float(df["fee_earned"].mean()) if num_cases else 0.0

    pre_fee = float(df.loc[df["track"] == "pre_suit", "fee_earned"].sum()) if not df.empty else 0.0
    lit_fee = float(df.loc[df["track"] == "litigation", "fee_earned"].sum()) if not df.empty else 0.0
    pre_pct = (pre_fee / total_fees * 100.0) if total_fees else 0.0
    lit_pct = (lit_fee / total_fees * 100.0) if total_fees else 0.0

    revenue_goal = safe_float(get_setting(f"revenue_goal_{year_sel}", get_setting("revenue_goal_2026", "0")), 0.0)
    progress = (total_fees / revenue_goal * 100.0) if revenue_goal else 0.0
    google_current = int(safe_float(get_setting("google_reviews_current", "221"), 221))

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Settlements", currency(total_settlement))
    m2.metric("Total Fees Earned", currency(total_fees))
    m3.metric("No. of Cases Settled", f"{num_cases}")
    m4.metric("Avg Settlement", currency(avg_settlement))
    m5.metric("Avg Fee Earned", currency(avg_fee))
    m6.metric("No. of Google Reviews", f"{google_current}")

    st.divider()

    g1, g2, g3 = st.columns([1.2, 1.2, 2.6])
    g1.metric(f"{year_sel} Revenue Goal", currency(revenue_goal))
    g2.metric("Goal Progress", f"{progress:,.1f}%")
    with g3:
        st.progress(min(max(progress / 100.0, 0.0), 1.0))

    st.markdown("### Revenue Split (Fees Earned)")
    s1, s2, s3 = st.columns(3)
    s1.metric("Pre-Suit", f"{pre_pct:,.1f}%")
    s2.metric("Litigation", f"{lit_pct:,.1f}%")
    s3.metric("Unknown", f"{max(0.0, 100.0 - pre_pct - lit_pct):,.1f}%")

    st.divider()

    st.markdown("## CM/PARA Performance Boxes")
    st.caption("Totals + that person’s transactions in the selected period.")

    if df.empty:
        st.info("No settlement rows found in this selected period.")
    else:
        for person in PEOPLE:
            person_df = df[df["person_name"] == person].copy()

            p_cases = int(len(person_df)) if not person_df.empty else 0
            p_settle_total = float(person_df["settlement_amount"].sum()) if not person_df.empty else 0.0
            p_fee_total = float(person_df["fee_earned"].sum()) if not person_df.empty else 0.0
            p_last_date = person_df["settlement_date"].max() if not person_df.empty else None

            st.markdown(f"### {person}")
           ## st.caption("CLIENT | SETTLEMENT AMOUNT | POLICY LIMITS | FEE EARNED | DATE OF SETTLEMENT | TOD")

            cA, cB, cC = st.columns(3)
            cA.metric("Total Settlement Amount", currency(p_settle_total))
            cB.metric("Total Fee Earned", currency(p_fee_total))
            cC.metric("Cases Settled", f"{p_cases}")

            if p_last_date:
                st.caption(f"Latest settlement date: {p_last_date}")

            with st.expander(f"Transactions — {person}", expanded=True if p_cases > 0 else False):
                if person_df.empty:
                    st.info("No transactions for this person in the selected period.")
                else:
                    view_cols = person_df.rename(columns={
                        "client_name": "CLIENT",
                        "settlement_amount": "SETTLEMENT AMOUNT",
                        "policy_limits": "POLICY LIMITS",
                        "fee_earned": "FEE EARNED",
                        "settlement_date": "DATE OF SETTLEMENT",
                        "tod": "TOD",
                        "track": "TRACK"
                    })[["CLIENT", "SETTLEMENT AMOUNT", "POLICY LIMITS", "FEE EARNED", "DATE OF SETTLEMENT", "TOD", "TRACK"]]

                    st.dataframe(
                        view_cols.sort_values("DATE OF SETTLEMENT", ascending=False),
                        use_container_width=True,
                        hide_index=True
                    )

            st.divider()

# =========================================================
# PAGE 5: DASHBOARD — PRE SUIT
# =========================================================
elif page == "Dashboard — Pre-Suit":
    st.title("PRE SUIT DASHBOARD 2026")

    kpi_df = query_df(
        """
        SELECT person_name, month,
               demands_sent, settlements_amount,
               avg_lien_resolution_days, files_without_14_day_contact, nps_score
        FROM pre_suit_kpis
        """
    )

    ps_df = query_df(
        """
        SELECT person_name, client_name, settlement_amount, fee_earned, settlement_date, tod
        FROM settlements
        WHERE track = 'pre_suit'
        """
    )

    if not ps_df.empty:
        ps_df["settlement_date"] = pd.to_datetime(ps_df["settlement_date"]).dt.date.astype(str)
        ps_df["month"] = pd.to_datetime(ps_df["settlement_date"]).dt.strftime("%Y-%m")

    month_set = set()
    if not kpi_df.empty:
        month_set.update(kpi_df["month"].dropna().unique().tolist())
    if not ps_df.empty and "month" in ps_df.columns:
        month_set.update(ps_df["month"].dropna().unique().tolist())

    months = sorted(list(month_set), reverse=True)

    topbar = st.columns([1.2, 2.8])
    with topbar[0]:
        month_sel = st.selectbox("Month", ["All Months"] + months, index=0)

    with topbar[1]:
        compare_people = st.multiselect(
            "Compare people (top summary table)",
            options=PEOPLE,
            default=PEOPLE
        )

    if ps_df.empty:
        ps_view = pd.DataFrame()
    else:
        ps_view = ps_df.copy() if month_sel == "All Months" else ps_df[ps_df["month"] == month_sel].copy()

    st.divider()
    st.markdown("## Summary (Computed from Pre-Suit Settlements)")

    if not compare_people:
        st.info("Pick at least one person in Compare people.")
    else:
        if ps_view.empty:
            summary = pd.DataFrame({"PERSON": compare_people})
            summary["Cases Settled"] = 0
            summary["Total Settlements"] = 0.0
            summary["Fees Earned"] = 0.0
        else:
            summary = ps_view.groupby("person_name", as_index=False).agg(
                **{
                    "Cases Settled": ("settlement_amount", "count"),
                    "Total Settlements": ("settlement_amount", "sum"),
                    "Fees Earned": ("fee_earned", "sum"),
                }
            ).rename(columns={"person_name": "PERSON"})

        summary = summary.set_index("PERSON").reindex(compare_people).fillna(0)

        pivot = pd.DataFrame({
            p: {
                "Cases Settled": int(summary.loc[p, "Cases Settled"]) if p in summary.index else 0,
                "Total Settlements": currency(summary.loc[p, "Total Settlements"]) if p in summary.index else currency(0),
                "Fees Earned": currency(summary.loc[p, "Fees Earned"]) if p in summary.index else currency(0),
            }
            for p in compare_people
        })
        st.dataframe(pivot, use_container_width=True)

    st.divider()
    st.markdown("## Person Boxes (KPIs + Transactions)")

    for person in PEOPLE:
        if kpi_df.empty:
            kpi_person = pd.DataFrame()
        else:
            if month_sel == "All Months":
                kpi_person = kpi_df[kpi_df["person_name"] == person].copy()
            else:
                kpi_person = kpi_df[(kpi_df["person_name"] == person) & (kpi_df["month"] == month_sel)].copy()

        if kpi_person.empty:
            kpi_month_label = month_sel
            dem = None
            kpi_settle_amt = None
            lien = None
            no_contact = None
            nps = None
        else:
            kpi_month_label = "All Months" if month_sel == "All Months" else month_sel
            dem = int(kpi_person["demands_sent"].sum())
            kpi_settle_amt = float(kpi_person["settlements_amount"].sum())
            lien = float(kpi_person["avg_lien_resolution_days"].mean())
            no_contact = int(kpi_person["files_without_14_day_contact"].sum())
            nps = float(kpi_person["nps_score"].mean())

        if ps_df.empty:
            ps_person = pd.DataFrame()
        else:
            if month_sel == "All Months":
                ps_person = ps_df[ps_df["person_name"] == person].copy()
            else:
                ps_person = ps_df[(ps_df["person_name"] == person) & (ps_df["month"] == month_sel)].copy()

        txn_count = int(len(ps_person)) if not ps_person.empty else 0
        last_date = ps_person["settlement_date"].max() if not ps_person.empty else None

        st.markdown(f"### {person}")

        kpi_row = pd.DataFrame([{
            "Person": person,
            "Month": kpi_month_label,
            "# Demands Sent": dash(dem),
            "Settlements $ (KPI)": dash(currency(kpi_settle_amt) if kpi_settle_amt is not None else None),
            "Avg Lien (days)": dash(f"{lien:,.1f}" if lien is not None else None),
            "Files w/out 14D Contact": dash(no_contact),
            "NPS": dash(f"{nps:,.1f}" if nps is not None else None),
        }])
        st.dataframe(kpi_row, use_container_width=True, hide_index=True)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Sum Demands Sent", str(dem) if dem is not None else "—")
        c2.metric("Sum Settlements $ (KPI)", currency(kpi_settle_amt) if kpi_settle_amt is not None else "—")
        c3.metric("Avg Lien (days)", f"{lien:,.1f}" if lien is not None else "—")
        c4.metric("Files w/out 14D contact", str(no_contact) if no_contact is not None else "—")
        c5.metric("NPS", f"{nps:,.1f}/5" if nps is not None else "—")

        if last_date:
            st.caption(f"Latest settlement date: {last_date}")

        with st.expander(f"Pre-Suit Transactions — {person}", expanded=True if txn_count > 0 else False):
            if ps_person.empty:
                st.info("No Pre-Suit settlement transactions for this person in the selected period.")
            else:
                view = ps_person.rename(columns={
                    "client_name": "CLIENT",
                    "settlement_amount": "SETTLEMENT AMOUNT",
                    "fee_earned": "FEE EARNED",
                    "settlement_date": "DATE OF SETTLEMENT",
                    "tod": "TOD",
                })[["CLIENT", "SETTLEMENT AMOUNT", "FEE EARNED", "DATE OF SETTLEMENT", "TOD"]].copy()

                view["SETTLEMENT AMOUNT"] = view["SETTLEMENT AMOUNT"].apply(currency)
                view["FEE EARNED"] = view["FEE EARNED"].apply(currency)

                st.dataframe(
                    view.sort_values("DATE OF SETTLEMENT", ascending=False),
                    use_container_width=True,
                    hide_index=True
                )

        st.divider()
