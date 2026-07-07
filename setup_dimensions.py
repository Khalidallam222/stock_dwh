import os 
import time 
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('AV_API_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')

SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
           'TSLA', 'NVDA', 'JPM',  'JNJ',  'WMT']

RDS_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

engine = create_engine(RDS_URL)

# ==========================================
# PHASE 1: POPULATE dim_date (2020–2030)
# ==========================================
print("--- 1. Generating Date Dimension (2020-2030) ---")

date_sql = """
    INSERT INTO dim_date (
        date,
        year,
        month,
        month_name,
        quarter,
        quarter_name,
        day_of_week,
        day_of_week_name,
        is_weekend
    )
    SELECT 
        datum AS date,
        EXTRACT(YEAR FROM datum)::INT AS year,
        EXTRACT(MONTH FROM datum)::INT AS month,
        TO_CHAR(datum, 'Month') AS month_name,
        EXTRACT(QUARTER FROM datum)::INT AS quarter,
        'Q' || EXTRACT(QUARTER FROM datum)::INT AS quarter_name,
        EXTRACT(ISODOW FROM datum)::INT AS day_of_week,
        TO_CHAR(datum, 'Day') AS day_of_week_name,
        EXTRACT(ISODOW FROM datum)::INT IN (6, 7) AS is_weekend

    FROM generate_series('2020-01-01'::date,'2030-12-31'::date,'1 day'::interval) AS datum
    ON CONFLICT (date) DO NOTHING;
"""

with engine.begin() as conn:
    conn.execute(text(date_sql))

print("✅ dim_date populated (4,018 dates).")

# ==========================================
# PHASE 2: POPULATE dim_company FROM API
# ==========================================
print("\n--- 2. Fetching Company Overviews from Alpha Vantage ---")
print("(This uses 10 API calls — ~2.5 minutes due to rate limiting)\n")

companies = []
for symbol in SYMBOLS:
    print(f"  Fetching {symbol}...")
    url = (
        f"https://www.alphavantage.co/query?"
        f"function=OVERVIEW&symbol={symbol}&apikey={API_KEY}"
    )
    response = requests.get(url)
    data = response.json()
    
    if 'Symbol' not in data:
        print(f"  ⚠️  No data returned for {symbol}. Check API key or rate limit.")
        time.sleep(15)
        continue

    companies.append({
        'symbol': data.get("Symbol"),
        'company_name': data.get("Name"),
        'sector': data.get("Sector"),
        'industry': data.get("Industry"),
        'exchange': data.get("Exchange"),
        'country': data.get("Country"),
        'currency': data.get("Currency")}
    )
    print(f"  ✅ {symbol} — {data.get('Name')} ({data.get('Sector')})")
    time.sleep(15)

df_companies = pd.DataFrame(companies)
df_companies.to_sql('dim_company', engine, if_exists='append', index=False)

print(f"\n✅ {len(df_companies)} companies loaded into dim_company.")
print("Dimension setup complete. You're ready to run the daily pipeline.")