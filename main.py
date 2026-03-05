import streamlit as st
import pandas as pd
import datetime as dt
from collections import defaultdict
import smartsheet
import firebase_admin
from firebase_admin import credentials, firestore

# ==========================
# CONSTANTS
# ==========================

APP_NAME = 'Service Scheduler'
APP_ICON = '🗓️'
APP_DESCRIPTION = 'Schedule service tasks based on booking data and staffing constraints.'

OCCUPANCY_PRIORITY = {
    "VACANT":    1,
    "HOLD":      2,
    "DEPARTURE": 3,
    "ARRIVAL":   4,
    "B2B":       5,
    "OCCUPIED":  6
}

OCCUPANCY_OPTIONS = list(OCCUPANCY_PRIORITY.keys())

# ==========================
# FIREBASE INIT
# ==========================

@st.cache_resource
def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(dict(st.secrets["firebase"]))
        firebase_admin.initialize_app(cred)
    return firestore.client()

db = init_firebase()

# ==========================
# SMARTSHEET PULL
# ==========================

def pull_smartsheet_geo():
    ss       = smartsheet.Smartsheet(st.secrets["smartsheet"]["access_token"])
    sheet_id = st.secrets["smartsheet"]["sheet_id"]
    sheet    = ss.Sheets.get_sheet(sheet_id)

    rows     = []
    col_map  = {col.id: col.title for col in sheet.columns}

    for row in sheet.rows:
        row_data = {}
        for cell in row.cells:
            row_data[col_map[cell.column_id]] = cell.value
        rows.append(row_data)

    df = pd.DataFrame(rows)

    # Keep only required columns
    df = df[["Unit_Code", "Order"]]

    # Convert Order safely
    df["Order"] = pd.to_numeric(df["Order"], errors="coerce")

    # Replace NaN Orders with very large number (so they sort last)
    df["Order"] = df["Order"].fillna(999999)

    # Convert to int safely
    df["Order"] = df["Order"].astype(int)

    # Drop rows without Unit_Code
    df = df.dropna(subset=["Unit_Code"])

    return df.sort_values("Order")

# ==========================
# BOOKING PARSER
# ==========================

def build_occupancy_map(df):
    occupancy = defaultdict(lambda: defaultdict(set))

    for _, row in df.iterrows():
        unit      = row["Unit_Code"]
        booking   = row["Booking_Number"]
        first     = pd.to_datetime(row["First_Night"]).date()
        last      = pd.to_datetime(row["Last_Night"]).date()
        is_hold   = str(booking).startswith("HLD")
        departure = last + dt.timedelta(days=1)

        if is_hold:
            for d in pd.date_range(first, departure):
                occupancy[unit][d.date()].add("HOLD")
        else:
            occupancy[unit][first].add("ARRIVAL")
            occupancy[unit][departure].add("DEPARTURE")

            for d in pd.date_range(first + dt.timedelta(days=1), last):
                occupancy[unit][d.date()].add("OCCUPIED")

    # Detect B2B
    for unit in occupancy:
        for date in list(occupancy[unit].keys()):
            if "ARRIVAL" in occupancy[unit][date] and "DEPARTURE" in occupancy[unit][date]:
                occupancy[unit][date] = {"B2B"}

    return occupancy

# ==========================
# DATE HELPERS
# ==========================

def default_week():
    today = dt.date.today()
    start = today - dt.timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
    end = start + dt.timedelta(days=6)
    return start, end

# ==========================
# SCHEDULING ENGINE
# ==========================

def schedule_units(
    occupancy_map,
    geo_df,
    start_date,
    end_date,
    workable_types,
    capacity_map,
    ignore_units
):
    scheduled   = []
    unscheduled = []

    date_range  = pd.date_range(start_date, end_date)
    date_range  = [d.date() for d in date_range]

    units       = sorted(occupancy_map.keys())

    geo_map     = dict(zip(geo_df["Unit_Code"], geo_df["Order"]))

    # Sort geographically first
    units.sort(key=lambda x: geo_map.get(x, 999999))

    for unit in units:

        # ==========================
        # NEW: Previously Scheduled Check
        # ==========================
        if unit in ignore_units:
            unscheduled.append((unit, "previously_scheduled"))
            continue

        workable_dates = []

        for date in date_range:
            occ_types = occupancy_map[unit].get(date, {"VACANT"})

            # HOLD conflict rule
            if "HOLD" in occ_types and len(occ_types) > 1:
                occ_types = {t for t in occ_types if t != "HOLD"}

            best_type = min(occ_types, key=lambda x: OCCUPANCY_PRIORITY[x])

            if best_type in workable_types:
                workable_dates.append((date, best_type))

        if not workable_dates:
            unscheduled.append((unit, "no_workable_occupancy_type"))
            continue

        # Sort by priority, then earliest date
        workable_dates.sort(key=lambda x: (OCCUPANCY_PRIORITY[x[1]],x[0]))

        placed = False

        for date, occ in workable_dates:
            if capacity_map[date] > 0:
                scheduled.append({"Unit_Code": unit, "Scheduled_Date": date, "Occupancy_Type": occ})
                capacity_map[date] -= 1
                placed = True
                break

        if not placed:
            unscheduled.append((unit, "capacity_met"))

    return (
        pd.DataFrame(scheduled),
        pd.DataFrame(unscheduled, columns=["Unit_Code", "Reason"])
    )

# ==========================
# STREAMLIT UI
# ==========================

st.set_page_config(page_title=APP_NAME, page_icon=APP_ICON, layout='centered')
st.image(st.secrets['images']["rr_logo"], width=100)
st.title(APP_NAME)
st.info(APP_DESCRIPTION)

project_id_input = st.text_input("Project ID", placeholder="Leave blank if new project. One will be provided after scheduling. Save it for next time.")
support_email    = st.secrets["support"]["email"]
ignore_units     = set()
project_ref      = None

if project_id_input:

    project_ref = db.collection("projects").document(project_id_input)
    doc         = project_ref.get()

    if doc.exists:

        ignore_units = set(doc.to_dict().get("scheduled_unit_codes", []))
        st.success(f"Loaded {len(ignore_units)} previously scheduled units.")

    else:

        st.error(f"Project ID not found. Contact {support_email}")
        st.stop()

st.subheader("Occupancy")

uploaded = st.file_uploader("**Booking Summary Report** | Booking Existing Between", type="xlsx")

if uploaded:

    booking_df     = pd.read_excel(uploaded, sheet_name="Sheet 1")
    occupancy_map  = build_occupancy_map(booking_df)
    workable_types = st.multiselect("What occupancy types are workable for these tasks?", OCCUPANCY_OPTIONS, default=["VACANT"])

    st.subheader("Staffing")

    default_start, default_end = default_week()
    l, r                       = st.columns(2)
    start_date                 = l.date_input("Start Date", default_start)
    end_date                   = r.date_input("End Date", default_end)

    if end_date < start_date:
        st.error("End Date must be >= Start Date")
        st.stop()


    date_range       = pd.date_range(start_date, end_date)
    capacity_map     = {}
    tasks_per_person = st.number_input("How many of these tasks can one person handle per day?", min_value=1, value=4)

    st.divider()

    for d in date_range:
        persons = st.number_input(f"Persons on **{d.strftime('%A')}, {d.strftime('%m/%d/%Y')}**", min_value=0, value=0, key=str(d))
        capacity_map[d.date()] = persons * tasks_per_person

    st.divider()

    st.warning(f'Please review the above information carefully before building the schedule.\n\nContact {support_email} for support.')

    if st.button("Build Schedule", type="primary", width='stretch'):

        geo_df = pull_smartsheet_geo()

        scheduled_df, unscheduled_df = schedule_units(
            occupancy_map  = occupancy_map,
            geo_df         = geo_df,
            start_date     = start_date,
            end_date       = end_date,
            workable_types = workable_types,
            capacity_map   = capacity_map,
            ignore_units   = ignore_units
        )

        if scheduled_df.empty:
            st.warning('No units scheduled.')
        else:
            st.success(f'**{len(scheduled_df)}** units scheduled successfully.')
            st.dataframe(scheduled_df)

            csv = scheduled_df.to_csv(index=False).encode("utf-8")

            st.download_button(label="Download Schedule",data=csv,file_name="schedule.csv",mime="text/csv",width='stretch')

            # Persist to Firebase
            scheduled_units = scheduled_df["Unit_Code"].tolist()

            if not project_id_input:

                project_ref      = db.collection("projects").document()
                project_id_input = project_ref.id

            existing = project_ref.get().to_dict() or {}
            combined = list(set(existing.get("scheduled_unit_codes", []) + scheduled_units))

            project_ref.set({
                "scheduled_unit_codes": combined,
                "created_at": existing.get("created_at", firestore.SERVER_TIMESTAMP),
                "last_updated": firestore.SERVER_TIMESTAMP
            })

            st.info(f'**Project ID**: {project_id_input}')

        if not unscheduled_df.empty:
            st.subheader("Unscheduled Units")
            st.dataframe(unscheduled_df)