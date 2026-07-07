import os
import json
import time 
import boto3
import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()

AV_API_KEY = os.getenv("AV_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
REGION  = os.getenv("AWS_REGION")

SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
           'TSLA', 'NVDA', 'JPM',  'JNJ',  'WMT']

s3 = boto3.client('s3', region_name=REGION)
today = date.today().isoformat()

print(f"Starting extraction for {today}...")
print(f"Fetching {len(SYMBOLS)} symbols (~{len(SYMBOLS) * 15}s due to rate limiting)\n")


success_count = 0

for symbol in SYMBOLS:
    print(f"  Pulling {symbol}...")
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}"
        f"&outputsize=compact"
        f"&apikey={AV_API_KEY}"
    )

    try:
        response = requests.get(url, timeout=30)
        data = response.json()
        if 'Time Series (Daily)' not in data:
            print(f"  ⚠️  No time series data for {symbol}.")
            print(f"      API response: {list(data.keys())}")
            continue
        s3_key = f"raw/{today}/{symbol}.json"
        s3.put_object(
            Bucket=S3_BUCKET, 
            Key=s3_key, 
            Body=json.dumps(data),
            ContentType='application/json'
        )
        trading_days = len(data['Time Series (Daily)'])
        print(f"  ✅ {symbol} — {trading_days} trading days → s3://{S3_BUCKET}/{s3_key}")
    
        success_count+=1
    
    except Exception as e:
            print(f"  ❌ Error fetching {symbol}: {e}")
    
    time.sleep(15)


print(f"\n✅ Extraction complete: {success_count}/{len(SYMBOLS)} symbols saved.")
print(f"   Raw data location: s3://{S3_BUCKET}/raw/{today}/")
