import os
import json
import boto3
import pandas as pd
from io import StringIO
from datetime import date
from dotenv import load_dotenv


load_dotenv()

AV_API_KEY = os.getenv("AV_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
REGION  = os.getenv("AWS_REGION")

s3 = boto3.client('s3', region_name=REGION)

today = date.today().isoformat()

print(f"Starting transformation for {today}...")

# List all raw JSON files for today's date
prefix = f"raw/{today}/"
response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

if 'Contents' not in response:
    print(f"❌ No raw files found at s3://{S3_BUCKET}/{prefix}")
    print("   Did you run extract.py first?")
    exit(1)

files = [obj['Key'] for obj in response['Contents']]
print(f"Found {len(files)} raw files to transform.\n")

all_records = []

for key in files:
    symbol = key.split('/')[-1].replace('.json', '')
    print(f"  Transforming {symbol}...")
    

    # Read raw JSON from S3
    raw = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(raw['Body'].read().decode('utf-8'))

    time_series = data.get('Time Series (Daily)', {})

    for date_str, values in time_series.items():
        try:
            all_records.append({
                'symbol':      symbol,
                'sale_date':   date_str,
                'open_price':  float(values['1. open']),
                'high_price':  float(values['2. high']),
                'low_price':   float(values['3. low']),
                'close_price': float(values['4. close']),
                'volume':      int(values['5. volume'])
            })
        except (KeyError, ValueError) as e:
            print(f"    ⚠️  Skipping malformed row for {symbol} on {date_str}: {e}")


print(f"\nRaw record count before cleaning: {len(all_records)}")
df = pd.DataFrame(all_records)

# ==========================================
# TRANSFORMATIONS
# ==========================================

# Convert to Pandas datetime (Keep it as a Pandas datetime for now!)
df['sale_date'] = pd.to_datetime(df['sale_date'])

# Remove weekends (This works perfectly because it's still a Pandas datetime)
df = df[df['sale_date'].dt.dayofweek < 5]

# Drop rows missing critical price data
df = df.dropna(subset=['open_price','close_price','volume'])

# Drop rows with zero or negative prices (data quality guard)
df = df[(df['open_price'] > 0) & (df['close_price'] > 0) & (df['volume'] > 0)]

# Calculate derived metrics
df['price_change'] = (df['close_price'] - df['open_price']).round(2)
df['daily_range'] = (df['high_price'] - df['low_price']).round(2)

# Remove duplicates (same symbol + date)
df = df.drop_duplicates(subset=['symbol', 'sale_date'])


print(f"Clean record count after transformation: {len(df)}")

# ==========================================
# SAVE PROCESSED CSV TO S3
# ==========================================
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
processed_key = f"processed/{today}/prices.csv"

s3.put_object(
    Bucket=S3_BUCKET,
    Key=processed_key,
    Body=csv_buffer.getvalue(),
    ContentType='text/csv'
)

print(f"\n✅ Processed data saved to s3://{S3_BUCKET}/{processed_key}")