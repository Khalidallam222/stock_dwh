import os 
import boto3
import pandas as pd
from io import StringIO
from datetime import date
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()

REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')

RDS_URL = (f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
          f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

s3 = boto3.client('s3', region_name=REGION)

engine = create_engine(RDS_URL)

today = date.today().isoformat()

processed_key = f"processed/{today}/prices.csv"

try:
    obj = s3.get_object(Bucket=S3_BUCKET,Key=processed_key)
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
except Exception as e:
    print(f"❌ Could not read s3://{S3_BUCKET}/{processed_key}")
    print(f"   Error: {e}")
    print("   Did you run transform.py first?")
    exit(1)

df['sale_date'] = pd.to_datetime(df['sale_date']).dt.date
print(f"Total records available from S3: {len(df)}")

# ==========================================
# PHASE 2: INCREMENTAL FILTER
# The core concept: only load what isn't already in the warehouse.
# ==========================================
print("\nChecking watermark — last loaded date in warehouse...")

last_loaded = pd.read_sql("""
    SELECT MAX(d.date) AS last_date
    FROM fact_stock_prices f
    JOIN dim_date d ON f.date_id = d.date_id
""", engine).iloc[0, 0]

if last_loaded is not None:
    last_loaded = pd.to_datetime(last_loaded).date()
    df = df[df['sale_date'] > last_loaded]
    print(f"  Watermark found: {last_loaded}")
    print(f"  Filtering to records after {last_loaded}...")
else:
    print("  No existing data found — performing full historical load.")

print(f"  Records to insert after filter: {len(df)}")

if len(df) == 0:
    print("\n✅ Warehouse is already up to date. Nothing to load.")
    exit(0)


# ==========================================
# PHASE 3: READ DIMENSION TABLES FROM RDS
# ==========================================
print("\nReading dimension tables from RDS for foreign key mapping...")
db_dates = pd.read_sql("SELECT date_id, date AS sale_date FROM dim_date;", engine)
db_companies = pd.read_sql("SELECT company_id, symbol FROM dim_company;", engine)

db_dates['sale_date'] = pd.to_datetime(db_dates['sale_date']).dt.date

print(f"  dim_date    → {len(db_dates)} rows")
print(f"  dim_company → {len(db_companies)} rows")

# ==========================================
# PHASE 4: MAP FOREIGN KEYS (TEXT → IDs)
# ==========================================
print("Mapping foreign keys...")

before = len(df)
df = df.merge(db_dates, on='sale_date', how='inner')
df = df.merge(db_companies, on='symbol', how='inner')
after = len(df)

if before != after:
        print(f"  ⚠️  {before - after} rows dropped during merge (dates/symbols not in dimensions).")

# ==========================================
# PHASE 5: BUILD AND LOAD FACT TABLE
# ==========================================

fact = df[[
    'date_id', 'company_id',
    'open_price', 'high_price', 'low_price', 'close_price',
    'volume', 'price_change', 'daily_range'
]]

print(f"\nLoading {len(fact)} new rows into fact_stock_prices...")

fact.to_sql(
    'fact_stock_prices',
    engine,
    if_exists='append',
    index=False,
    chunksize=5000,
    method='multi'

)

print(f"\n✅ {len(fact)} new records loaded into fact_stock_prices.")
print(f"   Warehouse is now current up to {df['sale_date'].max()}.")
print("Incremental load complete.")