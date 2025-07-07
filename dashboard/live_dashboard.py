# dashboard/live_dashboard.py
import streamlit as st
import redis
import pandas as pd
import time
from datetime import datetime

# --- CONFIGURATION ---
REDIS_HOST = "localhost" 
REDIS_PORT = 6379

# A list of all possible store names to query from Redis.
STORE_NAMES = [
    "Cairo Festival City", "Mall of Arabia", "City Stars",
    "San Stefano Grand Plaza", "Mall of Egypt", "Point 90",
    "Downtown Katameya", "City Centre Almaza"
]

# --- STREAMLIT PAGE SETUP ---
st.set_page_config(
    page_title="Real-Time Sales Dashboard", 
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Sales Dashboard")
st.markdown("This dashboard retrieves live metrics from Redis and refreshes every 60 seconds.")

# --- CONNECT TO REDIS ---
@st.cache_resource
def get_redis_connection():
    """Establishes and returns a connection to Redis."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        return r
    except redis.exceptions.ConnectionError as e:
        st.error(f"Failed to connect to Redis: {e}")
        st.info("Please ensure the Redis container is running and port 6379 is accessible.")
        return None

redis_conn = get_redis_connection()

if not redis_conn:
    st.stop()

# --- LAYOUT PLACEHOLDERS ---
st.subheader("Performance Summary (Last Minute)")
kpi1, kpi2, kpi3 = st.columns(3)

st.subheader("Sales by Store (Last Minute)")
chart_placeholder = st.empty()

# --- DATA FETCHING AND DISPLAY LOOP ---
while True:
    try:
        all_metrics = []
        # Iterate through all store names to fetch their latest metrics
        for store_name in STORE_NAMES:
            redis_key = f"live_metrics:store:{store_name}" 
            data = redis_conn.hgetall(redis_key)
            
            if data and "total_sales_last_minute" in data:
                all_metrics.append({
                    "Store Name": store_name,
                    "Total Sales": float(data["total_sales_last_minute"]),
                    "Last Updated": data.get("last_updated", "N/A")
                })

        if not all_metrics:
            with chart_placeholder.container():
                st.warning("Waiting for data from Spark... Make sure the Streaming job and Producer are running.")
            time.sleep(10) # Wait a bit longer if no data is found
            continue 

        df = pd.DataFrame(all_metrics)

        # --- UPDATE KPIs (Key Performance Indicators) ---
        total_sales = df['Total Sales'].sum()
        active_stores_count = df['Store Name'].nunique()
        
        # Safely get the latest update time
        try:
            latest_update_time_str = df['Last Updated'].max()
            latest_update_time = datetime.strptime(latest_update_time_str, '%Y-%m-%d %H:%M:%S').strftime('%H:%M:%S')
        except (ValueError, TypeError):
            latest_update_time = "N/A"

        kpi1.metric(label="Total Sales (EGP)", value=f"{total_sales:,.2f}")
        kpi2.metric(label="Active Stores", value=f"{active_stores_count}")
        kpi3.metric(label="Last Data Update", value=f"{latest_update_time}")
        
        # --- UPDATE CHART ---
        df_sorted = df.sort_values(by="Total Sales", ascending=False).set_index("Store Name")

        with chart_placeholder.container():
            st.bar_chart(df_sorted['Total Sales'])

        time.sleep(10)

    except Exception as e:
        st.error(f"An error occurred while updating the dashboard: {e}")
        time.sleep(60)