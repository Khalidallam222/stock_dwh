"""
dashboard.py

Streamlit dashboard for the Stock Market Analytics Warehouse.
Reads from the RDS Star Schema through a least-privilege (SELECT-only)
database role. Deploy on Streamlit Community Cloud (share.streamlit.io)
for a free, public URL.

Local testing requires a .streamlit/secrets.toml file (see the deployment
guide, Section 19, Step 2) -- never commit that file to git.
"""


import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Stock Market DWH", page_icon="📈", layout="wide")

st.title("📈 Stock Market Analytics Warehouse")
st.caption("AWS EC2 → S3 → RDS Star Schema, orchestrated by Airflow · refreshed daily")

# st.connection reads [connections.postgresql] from secrets.toml, pools the
# connection, and caches query results via the ttl= argument on .query().
conn = st.connection("postgresql", type="sql")

companies = conn.query(
    "SELECT symbol, company_name, sector FROM dim_company ORDER BY symbol",
    ttl="1h",
)

st.sidebar.header("Filters")
all_symbols = companies["symbol"].tolist()
default_selection = all_symbols[:5] if len(all_symbols) >= 5 else all_symbols
selected = st.sidebar.multiselect("Symbols", all_symbols, default=default_selection)

if not selected:
    st.warning("Pick at least one symbol from the sidebar to see data.")
    st.stop()


# Safe to string-join here: values can only come from the dropdown above,
# which is itself populated from dim_company -- never from free-text input.

symbol_list = "', '".join(selected)

prices = conn.query(
   f"""
   SELECT d.date, c.symbol, f.close_price, f.volume, f.price_change, f.daily_range
   FROM fact_stock_prices f
   JOIN dim_company c ON f.company_id = c.company_id
   JOIN dim_date   d ON f.date_id    = d.date_id
   WHERE c.symbol IN ('{symbol_list}')
   ORDER BY d.date
  """,
   ttl="1h",
)

col1, col2, col3 = st.columns(3)
col1.metric("Symbols selected", len(selected))
col2.metric("Rows loaded", f"{len(prices):,}")
col3.metric("Latest trading day", str(prices["date"].max()) if not prices.empty else "—")

PALETTE = ["#D4AF37", "#2DD4BF", "#EF6461", "#6699CC", "#9B7EDE", "#4C9F70"]

st.subheader("Closing price trend")
if prices.empty:
    st.info("No data yet for this selection — has the pipeline run yet?")
else:
    fig = px.line(
        prices,
        x="date",
        y="close_price",
        color='symbol',
        color_discrete_sequence=PALETTE,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)



st.subheader("Sector performance this year")
sector_perf = conn.query(
    """
    SELECT c.sector,
           ROUND(AVG(f.price_change), 4) AS avg_daily_change,
           ROUND(SUM(f.volume) / 1e6, 2)  AS total_volume_millions
    FROM fact_stock_prices f
    JOIN dim_company c ON f.company_id = c.company_id
    JOIN dim_date   d ON f.date_id    = d.date_id
    WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY c.sector
    ORDER BY avg_daily_change DESC
    """,
    ttl="1h",
)
if not sector_perf.empty:
    fig2 = px.bar(
        sector_perf, x="sector", y="avg_daily_change",
        color_discrete_sequence=[PALETTE[0]],
    )
    fig2.update_layout(margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No rows for the current calendar year yet.")



st.subheader("Most volatile stocks")
volatility = conn.query(
    """
    SELECT c.symbol, c.sector,
           ROUND(AVG(f.daily_range), 2) AS avg_daily_range,
           ROUND(MAX(f.daily_range), 2) AS max_daily_range
    FROM fact_stock_prices f
    JOIN dim_company c ON f.company_id = c.company_id
    GROUP BY c.symbol, c.sector
    ORDER BY avg_daily_range DESC
    """,
    ttl="1h",
)
st.dataframe(volatility, use_container_width=True, hide_index=True)

st.caption("Data refreshes daily via the Airflow-orchestrated ETL pipeline · cached up to 1 hour.")