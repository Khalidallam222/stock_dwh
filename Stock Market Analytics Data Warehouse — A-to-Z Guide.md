

> **CV Title:** "Cloud Data Pipeline: Stock Market Analytics Warehouse (AWS EC2 + S3 + RDS)"
> 
> Real-world data engineering project. Extracts live stock market data from the Alpha Vantage API, stages it in Amazon S3, transforms it with Python on EC2, and loads it into a PostgreSQL Star Schema on Amazon RDS for analytics.

---

## Final Architecture

```
Alpha Vantage API  (live stock prices + company data)
         ↓
      EC2 Python   (extract.py — pulls JSON from API)
         ↓
     S3 raw/       (raw JSON, partitioned by date)
         ↓
      EC2 Python   (transform.py — cleans and flattens)
         ↓
  S3 processed/    (clean CSV, ready to load)
         ↓
      EC2 Python   (load.py — maps foreign keys, inserts)
         ↓
  RDS PostgreSQL   (Star Schema Data Warehouse)
         ↓
      analysis.py  (analytics queries)
```

---

## Star Schema

```
dim_date               fact_stock_prices          dim_company
─────────              ─────────────────          ───────────
date_id (PK) ◄──────── date_id (FK)               company_id (PK)
date                   company_id (FK) ──────────► symbol
year                   open_price                  company_name
month                  high_price                  sector
month_name             low_price                   industry
quarter                close_price                 exchange
quarter_name           volume                      country
day_of_week            price_change                currency
day_of_week_name       daily_range
is_weekend             loaded_at
```

---

## Table of Contents

1. [Get Your Free API Key](#1-get-your-free-api-key)
2. [AWS Setup — S3 Bucket](#2-aws-setup--s3-bucket)
3. [AWS Setup — RDS PostgreSQL](#3-aws-setup--rds-postgresql)
4. [AWS Setup — IAM Role for EC2](#4-aws-setup--iam-role-for-ec2)
5. [EC2 Environment Setup](#5-ec2-environment-setup)
6. [Project Structure & Config](#6-project-structure--config)
7. [Create the RDS Schema](#7-create-the-rds-schema)
8. [Setup Script — Dimensions](#8-setup-script--dimensions)
9. [Extract — API to S3](#9-extract--api-to-s3)
10. [Transform — S3 Raw to S3 Processed](#10-transform--s3-raw-to-s3-processed)
11. [Load — S3 to RDS](#11-load--s3-to-rds)
12. [Pipeline Orchestrator](#12-pipeline-orchestrator)
13. [Optimize the Warehouse](#13-optimize-the-warehouse)
14. [Test & Validate](#14-test--validate)
15. [Analytics Queries](#15-analytics-queries)
16. [Orchestration with Apache Airflow](#16-orchestration-with-apache-airflow)
17. [A Public Dashboard with Streamlit](#17-a-public-dashboard-with-streamlit)
18. [Building the Dashboard with Streamlit & st.connection](#18-building-the-dashboard-with-streamlit--stconnection)
19. [Updated Full Execution Order](#19-updated-full-execution-order)
20. [Updated CV Description](#20-updated-cv-description)

---

## 1. Get Your Free API Key

1. Go to **https://www.alphavantage.co/support/#api-key**
2. Enter your email and click **GET FREE API KEY**
3. Copy the key — you'll add it to your `.env` file in Step 6

**Free tier limits:**

- 25 API requests per day
- 5 requests per minute

With 10 stocks tracked, the daily pipeline uses exactly 10 calls — well within the limit. The one-time setup script uses another 10 calls for company overviews.

---

## 2. AWS Setup — S3 Bucket

S3 is your **data lake** — the staging area between the API and the warehouse.

### Create the bucket

1. AWS Console → **S3** → **Create bucket**
2. **Bucket name:** `stock-market-dwh-khalid3llam` (must be globally unique)
3. **Region:** same region as your EC2 (e.g. `us-east-1`)
4. Leave **Block all public access** ON (default — this is correct)
5. Click **Create bucket**

### Bucket structure (created automatically by scripts)

```
stock-market-dwh-yourname/
├── raw/
│   └── 2024-01-15/
│       ├── AAPL.json
│       ├── MSFT.json
│       └── ...
└── processed/
    └── 2024-01-15/
        └── prices.csv
```

No need to create these folders manually — the scripts create them on first write.

---

## 3. AWS Setup — RDS PostgreSQL

### Launch the RDS instance

1. AWS Console → **RDS** → **Create database**
2. Choose **Standard create**
3. Engine: **PostgreSQL** → version **15**
4. Template: **Free tier**
5. Settings:
    - DB instance identifier: `stock-dw`
    - Master username: `postgres`
    - Master password: choose a strong password and save it 
6. Instance: **db.t3.micro** (free tier)
7. Storage: **20 GB** (free tier max)
8. **Connectivity:**
    - VPC: default VPC
    - **Public access: YES** (so your EC2 can reach it by hostname)
    - VPC security group: create new → name it `rds-stock-dw-sg`
9. Click **Create database**

Wait ~5 minutes for it to become available.

### Get the RDS endpoint

RDS → your database → **Connectivity & security** tab → copy the **Endpoint**. It looks like: `stock-dw.xxxxxxxxx.us-east-1.rds.amazonaws.com`

### Open port 5432 to EC2

1. RDS → your database → **Security** → click the VPC security group
2. **Inbound rules** → **Edit inbound rules** → **Add rule**:
    - Type: `PostgreSQL`
    - Port: `5432`
    - Source: choose **My IP** for testing, or the private IP of your EC2 instance for production
3. Save rules

### Create the database inside RDS

SSH into your EC2, then:

```bash
psql -h RDS-host-name -U postgres -p 5432
```

Enter your password, then:

```sql
CREATE DATABASE stock_dw;
\q
```

---

## 4. AWS Setup — IAM Role for EC2

This lets your EC2 instance read and write S3 **without storing any AWS credentials on disk**. boto3 automatically picks up the role — no access keys in your code.

### Create the role

1. AWS Console → **IAM** → **Roles** → **Create role**
2. Trusted entity: **AWS service** → **EC2**
3. Attach permissions policy: search for and select **AmazonS3FullAccess**
4. Role name: `ec2-s3-stock-dw-role`
5. **Create role**

### Attach the role to your EC2 instance

1. EC2 → **Instances** → select your instance
2. **Actions** → **Security** → **Modify IAM role**
3. Select `ec2-s3-stock-dw-role` → **Update IAM role**

From now on, any boto3 call on that EC2 instance automatically has S3 access. No `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` needed anywhere.

---

## 5. EC2 Environment Setup

SSH into your EC2 instance:

```bash
ssh -i your-key.pem ubuntu@YOUR-EC2-IP
```

### Install dependencies

```bash
sudo apt update && sudo apt install python3 python3-pip python3-venv git -y
```

### Create the project folder and virtual environment

```bash
mkdir ~/stock_dwh && cd ~/stock_dwh
python3 -m venv venv
source venv/bin/activate
```

### Install Python libraries

```bash
pip install pandas sqlalchemy psycopg2-binary boto3 requests python-dotenv
```

> Re-activate the virtual environment with `source ~/stock_dwh/venv/bin/activate` every time you open a new terminal session.

---

## 6. Project Structure & Config

### Folder structure

```
~/stock_dwh/
├── venv/                    ← virtual environment (never push to git)
├── .env                     ← secrets (never push to git)
├── .env.example             ← template for others (push to git)
├── .gitignore
├── setup_rds.py             ← run once: create tables
├── setup_dimensions.py      ← run once: populate dim_date + dim_company
├── extract.py               ← daily: API → S3 raw
├── transform.py             ← daily: S3 raw → S3 processed
├── load.py                  ← daily: S3 processed → RDS
├── pipeline.py              ← daily: runs extract → transform → load
└── analysis.py              ← analytics queries
```

### Create the `.env` file

```bash
nano .env
```

Paste and fill in your values:

```
# Alpha Vantage
AV_API_KEY=YOUR_API_KEY_HERE

# AWS
S3_BUCKET=stock-market-dwh-yourname
AWS_REGION=us-east-1

# RDS
RDS_HOST=stock-dw.xxxxxxxxx.us-east-1.rds.amazonaws.com
RDS_PORT=5432
RDS_DB=stock_dw
RDS_USER=postgres
RDS_PASSWORD=YOUR_RDS_PASSWORD_HERE
```

Save: `Ctrl+O` → Enter → `Ctrl+X`

### Create the `.env.example` (safe to push)

```bash
nano .env.example
```

```
AV_API_KEY=your_alpha_vantage_key_here
S3_BUCKET=your-s3-bucket-name
AWS_REGION=us-east-1
RDS_HOST=your-rds-endpoint.rds.amazonaws.com
RDS_PORT=5432
RDS_DB=stock_dw
RDS_USER=postgres
RDS_PASSWORD=your_rds_password_here
```

### Create the `.gitignore`

```bash
nano .gitignore
```

```
.env
venv/
__pycache__/
*.pyc
*.csv
```

---

## 7. Create the RDS Schema

**File: `setup_rds.py`** — run once to create all tables.

```python
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

RDS_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
    f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

engine = create_engine(RDS_URL)

schema_sql = """
-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id          SERIAL PRIMARY KEY,
    date             DATE UNIQUE NOT NULL,
    year             INT NOT NULL,
    month            INT NOT NULL,
    month_name       VARCHAR(20) NOT NULL,
    quarter          INT NOT NULL,
    quarter_name     VARCHAR(20) NOT NULL,
    day_of_week      INT NOT NULL,
    day_of_week_name VARCHAR(20) NOT NULL,
    is_weekend       BOOLEAN NOT NULL
);

-- Dimension: Company
CREATE TABLE IF NOT EXISTS dim_company (
    company_id   SERIAL PRIMARY KEY,
    symbol       VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(200),
    sector       VARCHAR(100),
    industry     VARCHAR(100),
    exchange     VARCHAR(50),
    country      VARCHAR(50),
    currency     VARCHAR(10)
);

-- Fact: Daily Stock Prices
CREATE TABLE IF NOT EXISTS fact_stock_prices (
    price_id     SERIAL PRIMARY KEY,
    date_id      INT NOT NULL REFERENCES dim_date(date_id),
    company_id   INT NOT NULL REFERENCES dim_company(company_id),
    open_price   DECIMAL(10,2),
    high_price   DECIMAL(10,2),
    low_price    DECIMAL(10,2),
    close_price  DECIMAL(10,2),
    volume       BIGINT,
    price_change DECIMAL(10,2),
    daily_range  DECIMAL(10,2),
    loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_id, company_id)
);
"""

with engine.begin() as conn:
    conn.execute(text(schema_sql))

print("✅ RDS schema created successfully.")
print("Tables created: dim_date, dim_company, fact_stock_prices")
```

Run it:

```bash
python3 setup_rds.py
```

### Instructions

#### **Step 1: Imports and Environment Setup**

**Objective:** Bring in the necessary libraries and load your secret `.env` variables into the script.

- **Instruction 1:** Import the built-in `os` module (used for reading environment variables).
    
- **Instruction 2:** From the `sqlalchemy` library, import two specific functions: `create_engine` and `text`.
    
- **Instruction 3:** From the `dotenv` library, import the `load_dotenv` function.
    
- **Instruction 4:** Execute the function to load your environment variables into memory.
    

> **💡 Hints for Step 1:**
> 
> - _Function to run:_ `load_dotenv()` requires no arguments. Just call it directly.
>     

#### **Step 2: Build the Connection String & Engine**

**Objective:** Construct the exact URL required to connect to PostgreSQL, and initialize the SQLAlchemy database engine.

- **Instruction 1:** Create a variable named `RDS_URL`. Use a Python formatted string (f-string) to construct the standard PostgreSQL connection URI.
    
- **Instruction 2:** Create a variable named `engine`. Initialize it using the SQLAlchemy function you imported, passing the URL you just built.
    

> **💡 Hints for Step 2:**
> 
> - _Method to fetch variables:_ Use `os.getenv('YOUR_VAR_NAME')`. You will need `RDS_USER`, `RDS_PASSWORD`, `RDS_HOST`, `RDS_PORT`, and `RDS_DB`.
>     
> - _URI Format:_ The strict format for Postgres is: `postgresql://username:password@hostname:port/database_name`
>     
> - _Function to use:_ `create_engine(RDS_URL)` creates the core interface between Python and the database.
>     

#### **Step 3: Draft the SQL Schema (DDL)**

**Objective:** Write the raw SQL commands to create your Star Schema.

- **Instruction 1:** Create a variable named `schema_sql` and assign it a multi-line string (using `"""`).
    
- **Instruction 2:** Inside the string, write the SQL to create `dim_date`.
    
    - _Columns:_ `date_id` (auto-incrementing PK), `date` (Date, unique, not null), `year` (Int, not null), `month` (Int, not null), `month_name` (String up to 20 chars, not null), `quarter` (Int, not null), `quarter_name` (String up to 20 chars, not null), `day_of_week` (Int, not null), `day_of_week_name` (String up to 20 chars, not null), `is_weekend` (Boolean, not null).
        
- **Instruction 3:** Write the SQL to create `dim_company`.
    
    - _Columns:_ `company_id` (auto-incrementing PK), `symbol` (String up to 10 chars, unique, not null), `company_name` (String up to 200 chars), `sector` (String up to 100 chars), `industry` (String up to 100 chars), `exchange` (String up to 50 chars), `country` (String up to 50 chars), `currency` (String up to 10 chars).
        
- **Instruction 4:** Write the SQL to create `fact_stock_prices`.
    
    - _Columns:_ `price_id` (auto-incrementing PK), `date_id` (Int, not null, must reference `dim_date`), `company_id` (Int, not null, must reference `dim_company`), `open_price`, `high_price`, `low_price`, `close_price`, `price_change`, `daily_range` (all should be Decimals allowing 10 total digits and 2 decimal places), `volume` (Large Integer), `loaded_at` (Timestamp, defaulting to the current time).
        
    - _Constraint:_ Add a rule at the bottom of this table ensuring the combination of `date_id` and `company_id` is totally unique.
        

> **💡 Hints for Step 3:**
> 
> - _Table Safety:_ Always start with `CREATE TABLE IF NOT EXISTS table_name (` so your script doesn't crash if run twice.
>     
> - _SQL Data Types:_ Use `SERIAL` for auto-incrementing primary keys. Use `VARCHAR(n)` for strings. Use `DECIMAL(10,2)` for the financial prices. Use `BIGINT` for volume. Use `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` for the loaded_at column.
>     
> - _Foreign Keys:_ The syntax is `column_name INT NOT NULL REFERENCES other_table(primary_key_column)`.
>     
> - _Composite Unique:_ At the end of the fact table column list, add `UNIQUE(col1, col2)`.
>     

#### **Step 4: Execute the Transaction**

**Objective:** Safely send the SQL string to the database and execute it as a single transaction.

- **Instruction 1:** Open a context manager (`with` statement) to establish a connection using the engine. Assign this connection to the alias `conn`.
    
- **Instruction 2:** Using the connection, execute your `schema_sql` string.
    
- **Instruction 3:** Print a success message (e.g., "✅ RDS schema created successfully.") so you know it finished.
    

> **💡 Hints for Step 4:**
> 
> - _Context Manager Method:_ Use `with engine.begin() as conn:`. Using `.begin()` is crucial because it automatically handles the database _transaction_. If the SQL succeeds, it automatically runs a `COMMIT`. If there is an error, it automatically runs a `ROLLBACK`.
>     
> - _Execution Method:_ You cannot just pass the string directly in modern SQLAlchemy. You must wrap it in the `text()` function you imported earlier. Use `conn.execute(text(schema_sql))`.
>     


---

## 8. Setup Script — Dimensions

**File: `setup_dimensions.py`** — run **once** to populate `dim_date` and `dim_company`.

This uses 10 of your 25 daily API calls (one OVERVIEW call per stock symbol). Run it on a separate day from your first pipeline run, or early in the day.

```python
import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

API_KEY  = os.getenv('AV_API_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')

SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
           'TSLA', 'NVDA', 'JPM',  'JNJ',  'WMT']

RDS_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
    f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)
engine = create_engine(RDS_URL)

# ==========================================
# PHASE 1: POPULATE dim_date (2020–2030)
# ==========================================
print("--- 1. Generating Date Dimension (2020-2030) ---")

date_sql = """
INSERT INTO dim_date (
    date, year, month, month_name,
    quarter, quarter_name, day_of_week, day_of_week_name, is_weekend
)
SELECT
    datum                                    AS date,
    EXTRACT(YEAR    FROM datum)::INT         AS year,
    EXTRACT(MONTH   FROM datum)::INT         AS month,
    TO_CHAR(datum, 'Month')                  AS month_name,
    EXTRACT(QUARTER FROM datum)::INT         AS quarter,
    'Q' || EXTRACT(QUARTER FROM datum)::INT  AS quarter_name,
    EXTRACT(ISODOW  FROM datum)::INT         AS day_of_week,
    TO_CHAR(datum, 'Day')                    AS day_of_week_name,
    EXTRACT(ISODOW  FROM datum) IN (6, 7)    AS is_weekend
FROM generate_series(
    '2020-01-01'::date,
    '2030-12-31'::date,
    '1 day'::interval
) AS datum
ON CONFLICT (date) DO NOTHING;
"""

with engine.begin() as conn:
    conn.execute(text(date_sql))
print("✅ dim_date populated (3,653 dates).")

# ==========================================
# PHASE 2: POPULATE dim_company FROM API
# ==========================================
print("\n--- 2. Fetching Company Overviews from Alpha Vantage ---")
print("(This uses 10 API calls — ~2.5 minutes due to rate limiting)\n")

companies = []

for symbol in SYMBOLS:
    print(f"  Fetching {symbol}...")
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=OVERVIEW&symbol={symbol}&apikey={API_KEY}"
    )
    response = requests.get(url)
    data = response.json()

    if 'Symbol' not in data:
        print(f"  ⚠️  No data returned for {symbol}. Check API key or rate limit.")
        time.sleep(15)
        continue

    companies.append({
        'symbol':       data.get('Symbol'),
        'company_name': data.get('Name'),
        'sector':       data.get('Sector'),
        'industry':     data.get('Industry'),
        'exchange':     data.get('Exchange'),
        'country':      data.get('Country'),
        'currency':     data.get('Currency')
    })
    print(f"  ✅ {symbol} — {data.get('Name')} ({data.get('Sector')})")
    time.sleep(15)  # 5 calls/min limit = 1 call per 12s; 15s is safe

df_companies = pd.DataFrame(companies)
df_companies.to_sql('dim_company', engine, if_exists='append', index=False)

print(f"\n✅ {len(df_companies)} companies loaded into dim_company.")
print("Dimension setup complete. You're ready to run the daily pipeline.")
```

Run it:

```bash
python3 setup_dimensions.py
```

Expected output:

```
✅ dim_date populated (4,018 dates).
  ✅ AAPL — Apple Inc (TECHNOLOGY)
  ✅ MSFT — Microsoft Corporation (TECHNOLOGY)
  ...
✅ 10 companies loaded into dim_company.
```


### **Exercise: Populate the Dimension Tables**

**Objective:** Write a Python script that pre-loads 10 years of calendar dates using PostgreSQL's generation engine, and then extracts company profiles from a live REST API to load into your database using Pandas.

#### **Task 1: Imports and Environment Setup**

**Objective:** Bring in the required libraries and establish the database connection.

- **Instruction 1:** Import `os`, `time`, `requests`, `pandas as pd`, `create_engine`, `text` (from sqlalchemy), and `load_dotenv`.
    
- **Instruction 2:** Load your `.env` variables.
    
- **Instruction 3:** Fetch `AV_API_KEY` and construct your `RDS_URL` just like in the previous script. Create the SQLAlchemy `engine`.
    
- **Instruction 4:** Create a Python list named `SYMBOLS` containing a few stock tickers (e.g., `['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']`).
    

#### **Task 2: Populate `dim_date` (The SQL Way)**

**Objective:** Write and execute an `INSERT` statement that uses PostgreSQL's built-in date generator to create 3,653 rows instantly.

- **Instruction 1:** Create a string variable named `date_sql`.
    
- **Instruction 2:** Write an `INSERT INTO dim_date (...)` statement. List all the columns except `date_id` (since it auto-increments).
    
- **Instruction 3:** Instead of `VALUES`, use a `SELECT` statement that pulls from `generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day'::interval) AS datum`.
    
- **Instruction 4:** Use PostgreSQL date functions to extract the needed parts from `datum`.
    
    - _Year/Month/Quarter/Day of week:_ Use `EXTRACT(YEAR FROM datum)::INT`.
        
    - _Names:_ Use `TO_CHAR(datum, 'Month')` and `TO_CHAR(datum, 'Day')`.
        
    - _Is Weekend:_ Check if `EXTRACT(ISODOW FROM datum)` is `IN (6, 7)`.
        
- **Instruction 5:** Add `ON CONFLICT (date) DO NOTHING;` to the very end of the SQL string.
    
- **Instruction 6:** Execute this string using `with engine.begin() as conn:` and `conn.execute(text(date_sql))`.
    

> **💡 Hints for Task 2:**
> 
> - _Why `generate_series`?_ It allows the database engine to generate rows in milliseconds without Python having to write a slow "for-loop" for 3,650 days.
>     
> - _Why `ON CONFLICT`?_ This makes your script **idempotent**. If you accidentally run the script twice, it won't crash or create duplicate dates; it will just silently skip them. (Note: this relies on the `UNIQUE` constraint you put on the `date` column in the previous exercise!).
>     

#### **Task 3: Populate `dim_company` (The Python/API Way)**

**Objective:** Loop through your stock symbols, fetch their metadata from Alpha Vantage, and store the results in a list.

- **Instruction 1:** Create an empty list called `companies`.
    
- **Instruction 2:** Start a `for` loop iterating over your `SYMBOLS` list.
    
- **Instruction 3:** Inside the loop, construct the API URL. The Alpha Vantage documentation states the endpoint is: `https://www.alphavantage.co/query?function=OVERVIEW&symbol={YOUR_SYMBOL}&apikey={YOUR_API_KEY}`.
    
- **Instruction 4:** Use the `requests` library to fetch the URL and parse the response into a JSON dictionary.
    
- **Instruction 5:** Write an `if` statement to check if the API actually returned data (e.g., `if 'Symbol' not in data:`). If it failed, print a warning, sleep for 15 seconds, and `continue` to the next symbol.
    
- **Instruction 6:** Append a new dictionary to your `companies` list. Map your database column names to the API's JSON keys: `Symbol`, `Name`, `Sector`, `Industry`, `Exchange`, `Country`, `Currency`. (e.g., `'company_name': data.get('Name')`).
    
- **Instruction 7:** Add a `time.sleep(15)` at the bottom of the loop.
    

> **💡 Hints for Task 3:**
> 
> - _Parsing JSON:_ Use `response = requests.get(url)` followed by `data = response.json()`.
>     
> - _Why use `.get()`?_ Using `data.get('Sector')` instead of `data['Sector']` is safer. If the API is missing the sector, `.get()` safely returns `None` (which becomes a `NULL` in SQL) rather than crashing your whole script with a `KeyError`.
>     
> - _Why sleep?_ The free Alpha Vantage API strictly limits you to 5 calls per minute. Sleeping for 15 seconds after every call ensures you only make 4 calls per minute, keeping you safely under the limit.
>     

#### **Task 4: Load the Companies via Pandas**

**Objective:** Convert your Python list of dictionaries into a Pandas DataFrame and push it to the database.

- **Instruction 1:** Outside/below the `for` loop, convert your `companies` list into a DataFrame using `pd.DataFrame(companies)`.
    
- **Instruction 2:** Use the Pandas `.to_sql()` method to load the DataFrame into the `dim_company` table.
    
- **Instruction 3:** Print a success message.
    

> **💡 Hints for Task 4:**
> 
> - _The `to_sql` parameters:_ `df.to_sql('dim_company', engine, if_exists='append', index=False)`.
>     
> - _Why `append`?_ Because `dim_company` already exists (you created it in `setup_rds.py`). You just want to add rows to it, not replace it.
>     
> - _Why `index=False`?_ Pandas dataframes have a built-in row number index (0, 1, 2...). You don't want to push this meaningless index into your database.
>     

### **How to check your work:**

Run your script in the terminal:

Bash

```
python3 setup_dimensions.py
```

Because of the `time.sleep(15)`, it should take a little over a minute to run if you used 5 symbols. Once it finishes, log into your database with `psql` and run:

`SELECT COUNT(*) FROM dim_date;` (Should be **4,018**)

`SELECT symbol, company_name, sector FROM dim_company;` (Should list the companies you fetched).

---

## 9. Extract — API to S3

**File: `extract.py`** — run daily. Pulls stock prices from Alpha Vantage and saves raw JSON to S3.

```python
import os
import json
import time
import boto3
import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()

API_KEY   = os.getenv('AV_API_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')

SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
           'TSLA', 'NVDA', 'JPM',  'JNJ',  'WMT']

s3    = boto3.client('s3', region_name=os.getenv('AWS_REGION'))
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
        f"&outputsize=compact"   # 'compact' = last 100 trading days
        f"&apikey={API_KEY}"
    )

    try:
        response = requests.get(url, timeout=30)
        data = response.json()

        if 'Time Series (Daily)' not in data:
            print(f"  ⚠️  No time series data for {symbol}.")
            print(f"      API response: {list(data.keys())}")
            time.sleep(15)
            continue

        # Save raw JSON to S3: raw/YYYY-MM-DD/SYMBOL.json
        s3_key = f"raw/{today}/{symbol}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        trading_days = len(data['Time Series (Daily)'])
        print(f"  ✅ {symbol} — {trading_days} trading days → s3://{S3_BUCKET}/{s3_key}")
        success_count += 1

    except Exception as e:
        print(f"  ❌ Error fetching {symbol}: {e}")

    time.sleep(15)  # Respect rate limit: 5 calls/min on free tier

print(f"\n✅ Extraction complete: {success_count}/{len(SYMBOLS)} symbols saved.")
print(f"   Raw data location: s3://{S3_BUCKET}/raw/{today}/")
```

Here is your next exercise! This script is the true beginning of your daily ETL pipeline. You are now stepping into Cloud Data Engineering by interacting directly with **Amazon S3** (your Data Lake).

Open a new, blank file named `extract.py`. Do not look at the solution! Follow these guided steps to build the extractor.

### **Exercise: Extract — API to S3 (The Data Lake)**

**Objective:** Write a Python script that fetches daily stock prices from the Alpha Vantage API and saves the raw, untouched JSON responses directly into an Amazon S3 bucket, organized by date.

#### **Task 1: Imports and AWS Setup**

**Objective:** Load your environment variables and initialize your connection to AWS.

- **Instruction 1:** Import `os`, `json`, `time`, `boto3`, `requests`. Also import `date` from the `datetime` module, and `load_dotenv` from `dotenv`.
    
- **Instruction 2:** Load your `.env` variables and retrieve your `AV_API_KEY` and `S3_BUCKET`.
    
- **Instruction 3:** Create a list named `SYMBOLS` containing your stock tickers.
    
- **Instruction 4:** Initialize your AWS S3 client using `boto3`. You need to pass the service name (`'s3'`) and your AWS region.
    
- **Instruction 5:** Get today's date formatted as a standard string (YYYY-MM-DD) and store it in a variable named `today`.
    

> **💡 Hints for Task 1:**
> 
> - _Boto3 Initialization:_ `s3 = boto3.client('s3', region_name=os.getenv('AWS_REGION'))`. Notice that you don't need to pass an AWS Access Key! Because you attached an **IAM Role** to your EC2 instance in Step 4, `boto3` securely gets temporary permissions automatically in the background.
>     
> - _Date String:_ Use `today = date.today().isoformat()`.
>     

#### **Task 2: The Extraction Loop & URL Construction**

**Objective:** Loop through your companies and build the specific API request URL for each one.

- **Instruction 1:** Create a variable called `success_count` and set it to 0.
    
- **Instruction 2:** Start a `for` loop iterating over your `SYMBOLS` list.
    
- **Instruction 3:** Inside the loop, construct the Alpha Vantage URL using an f-string.
    
    - The base URL is `https://www.alphavantage.co/query`
        
    - You need 4 parameters added to the URL: `function=TIME_SERIES_DAILY`, `symbol={symbol}`, `outputsize=compact`, and `apikey={API_KEY}`.
        

> **💡 Hints for Task 2:**
> 
> - _Output Size:_ Alpha Vantage offers `full` (20 years of data) or `compact` (the last 100 trading days). We use `compact` here to keep the API fast and the files small.
>     

#### **Task 3: Fetching and Validating (Defensive Programming)**

**Objective:** Safely request the data and handle API errors or rate limits.

- **Instruction 1:** Still inside the loop, open a `try:` block. Whenever you make a network request over the internet, you should wrap it in a try/except block so your pipeline doesn't crash if the WiFi blips.
    
- **Instruction 2:** Use `requests.get()` to hit the URL. Add a `timeout=30` parameter so it doesn't hang forever if the API is slow. Convert the response to JSON.
    
- **Instruction 3:** Validate the data. Alpha Vantage returns a key called `'Time Series (Daily)'` if successful. Write an `if` statement to check if this key is NOT in your data.
    
    - _If it's missing:_ Print a warning, print the keys that _did_ return (often an error message about rate limits), sleep for 15 seconds, and use `continue` to skip to the next symbol.
        

> **💡 Hints for Task 3:**
> 
> - _Validation logic:_ `if 'Time Series (Daily)' not in data:`
>     
> - _Printing keys:_ `list(data.keys())` is great for debugging what the API actually sent back when it fails.
>     

#### **Task 4: Saving to Amazon S3**

**Objective:** Push the raw data directly into your cloud storage bucket.

- **Instruction 1:** (Still inside the `try` block, below the validation). Construct the S3 file path (the "Key"). We want to organize our data lake by date. Set `s3_key = f"raw/{today}/{symbol}.json"`.
    
- **Instruction 2:** Use the `s3.put_object()` method to upload the file. You need to pass the `Bucket` name, the `Key` (file path), the `Body` (the actual data), and the `ContentType`.
    
- **Instruction 3:** Increment your `success_count` by 1.
    
- **Instruction 4:** Create the `except Exception as e:` block to catch and print any network errors.
    
- **Instruction 5:** Add a `time.sleep(15)` at the very bottom of the loop (outside the try/except blocks) to respect the 5 calls/minute rate limit.
    
- **Instruction 6:** Outside the loop, print a final summary of how many files were saved.
    

> **💡 Hints for Task 4:**
> 
> - _The Body parameter:_ `requests.json()` gave you a Python dictionary. S3 doesn't know what a Python dictionary is; it only accepts text or bytes. You must use `json.dumps(data)` to convert the dictionary back into a raw JSON string before sending it to S3.
>     
> - _Content Type:_ Set `ContentType='application/json'` in the `put_object` call. This tells S3 (and anyone downloading the file later) exactly what kind of data it is.
>     

### **How to check your work:**

Run your script in the terminal:

Bash

```
python3 extract.py
```

It should take about 2.5 minutes. Once it prints the success message, go to your **AWS Console** in your browser:

1. Open **S3**.
    
2. Click on your bucket (`stock-market-dwh-...`).
    
3. You should automatically see a new folder named `raw/`. Click into it, then click into the folder with today's date.
    
4. You should see 10 `.json` files sitting perfectly in your data lake!

---

## 10. Transform — S3 Raw to S3 Processed

**File: `transform.py`** — run daily after extract. Flattens JSON, cleans data, saves clean CSV to S3.

```python
import os
import json
import boto3
import pandas as pd
from io import StringIO
from datetime import date
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET')
s3    = boto3.client('s3', region_name=os.getenv('AWS_REGION'))
today = date.today().isoformat()

print(f"Starting transformation for {today}...")

# List all raw JSON files for today's date
prefix   = f"raw/{today}/"
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
    raw  = s3.get_object(Bucket=S3_BUCKET, Key=key)
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

# Type casting
df['sale_date'] = pd.to_datetime(df['sale_date']).dt.date

# Remove weekends (market is closed — defensive check)
df = df[pd.to_datetime(df['sale_date']).dt.dayofweek < 5]

# Drop rows missing critical price data
df = df.dropna(subset=['open_price', 'close_price', 'volume'])

# Drop rows with zero or negative prices (data quality guard)
df = df[(df['open_price'] > 0) & (df['close_price'] > 0) & (df['volume'] > 0)]

# Calculate derived metrics
df['price_change'] = (df['close_price'] - df['open_price']).round(2)
df['daily_range']  = (df['high_price']  - df['low_price']).round(2)

# Remove duplicates (same symbol + date)
df = df.drop_duplicates(subset=['symbol', 'sale_date'])

print(f"Clean record count after transformation: {len(df)}")

# ==========================================
# SAVE PROCESSED CSV TO S3
# ==========================================
csv_buffer    = StringIO()
df.to_csv(csv_buffer, index=False)
processed_key = f"processed/{today}/prices.csv"

s3.put_object(
    Bucket=S3_BUCKET,
    Key=processed_key,
    Body=csv_buffer.getvalue(),
    ContentType='text/csv'
)

print(f"\n✅ Processed data saved to s3://{S3_BUCKET}/{processed_key}")
```


Here is your next exercise! You have reached the **"T" (Transform)** in your ETL pipeline.

This is where Data Engineers earn their paychecks. You are going to take the messy, nested JSON data sitting in your S3 Data Lake, flatten it, clean out the garbage (like weekend data or missing prices), calculate business metrics, and save it as a clean CSV ready for the database.

Open a new, blank file named `transform.py`. Follow these guided steps to build your transformer!

### **Exercise: Transform — S3 Raw to S3 Processed**

**Objective:** Write a Python script using `boto3` and `pandas` to read multiple JSON files from S3, flatten the hierarchical data into a tabular format, clean it, and upload a single CSV file back to S3.

#### **Task 1: Setup and S3 Discovery**

**Objective:** Connect to S3 and find the files you downloaded today.

- **Instruction 1:** Import the necessary modules: `os`, `json`, `boto3`, `pandas as pd`, `StringIO` from `io`, `date` from `datetime`, and `load_dotenv` from `dotenv`.
    
- **Instruction 2:** Load your environment variables. Initialize your `s3` client and store today's date in a variable (just like in the `extract.py` script).
    
- **Instruction 3:** Set a variable `prefix` to the folder path where today's raw data is stored: `f"raw/{today}/"`.
    
- **Instruction 4:** Use `s3.list_objects_v2()` to get a list of everything inside that specific S3 folder.
    
- **Instruction 5:** Write an `if` statement to check if the key `'Contents'` is NOT in the S3 response. If it's missing, print an error and `exit(1)` (this means `extract.py` failed or hasn't run yet).
    
- **Instruction 6:** Use list comprehension to extract just the file paths (the `'Key'`) from the `'Contents'` list into a variable named `files`.
    

> **💡 Hints for Task 1:**
> 
> - _Listing objects:_ `response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)`
>     
> - _List Comprehension:_ `files = [obj['Key'] for obj in response['Contents']]`
>     

#### **Task 2: Flattening the JSON**

**Objective:** Read the nested JSON files directly from the cloud and flatten them into a simple Python list.

- **Instruction 1:** Create an empty list called `all_records`.
    
- **Instruction 2:** Start a `for` loop to iterate over your `files` list.
    
- **Instruction 3:** Inside the loop, extract the stock symbol from the S3 file path (e.g., turn `"raw/2026-07-03/AAPL.json"` into `"AAPL"`).
    
- **Instruction 4:** Use `s3.get_object()` to download the file into memory. Read the body and parse it using `json.loads()`.
    
- **Instruction 5:** Extract the `'Time Series (Daily)'` dictionary from the parsed JSON.
    
- **Instruction 6:** Start a second (nested) `for` loop to iterate through the dates and values: `for date_str, values in time_series.items():`
    
- **Instruction 7:** Inside a `try/except (KeyError, ValueError)` block, append a new dictionary to `all_records`. Map the JSON fields to your database columns: `'symbol'`, `'sale_date'`, `'open_price'` (convert to float), `'high_price'` (float), `'low_price'` (float), `'close_price'` (float), and `'volume'` (int). The JSON keys are weird (e.g., `'1. open'`), so map them carefully!
    

> **💡 Hints for Task 2:**
> 
> - _String manipulation:_ `symbol = key.split('/')[-1].replace('.json', '')`
>     
> - _Reading S3 data into memory:_ `raw = s3.get_object(Bucket=S3_BUCKET, Key=key)`
>     
> - _Decoding S3 bytes:_ `data = json.loads(raw['Body'].read().decode('utf-8'))`
>     

#### **Task 3: Pandas Data Cleaning (The Core Transformation)**

**Objective:** Convert your flat list into a DataFrame and apply business rules.

- **Instruction 1:** Convert `all_records` into a pandas DataFrame: `df = pd.DataFrame(all_records)`.
    
- **Instruction 2:** Convert the `sale_date` column into actual Python date objects using `pd.to_datetime().dt.date`.
    
- **Instruction 3:** Filter the DataFrame to _remove_ weekends. (If the API accidentally returns weekend data, it will corrupt our analytics).
    
- **Instruction 4:** Drop any rows that have missing values (`NaN`) in `'open_price'`, `'close_price'`, or `'volume'`.
    
- **Instruction 5:** Filter the DataFrame to ensure prices and volumes are greater than 0 (Data Quality Guard).
    
- **Instruction 6:** Create a new column named `price_change`. Set it to `close_price` minus `open_price`, and use `.round(2)` to round it to two decimal places.
    
- **Instruction 7:** Create a new column named `daily_range`. Set it to `high_price` minus `low_price`, and round it to two decimals.
    
- **Instruction 8:** Finally, drop any duplicate rows based on a subset of `'symbol'` and `'sale_date'`.
    

> **💡 Hints for Task 3:**
> 
> - _Weekend Filter:_ `df = df[pd.to_datetime(df['sale_date']).dt.dayofweek < 5]` (Monday=0, Friday=4, so anything `< 5` is a weekday).
>     
> - _Dropping nulls:_ `df = df.dropna(subset=['open_price', 'close_price', 'volume'])`
>     
> - _Deduplication:_ `df = df.drop_duplicates(subset=['symbol', 'sale_date'])`
>     

#### **Task 4: Save the CSV to the Data Lake**

**Objective:** Write the cleaned DataFrame back to S3 as a CSV without saving it to your EC2's hard drive.

- **Instruction 1:** Create an in-memory string buffer using `csv_buffer = StringIO()`.
    
- **Instruction 2:** Convert the DataFrame to a CSV and write it into the buffer using `df.to_csv(csv_buffer, index=False)`.
    
- **Instruction 3:** Construct the target S3 path: `processed_key = f"processed/{today}/prices.csv"`.
    
- **Instruction 4:** Use `s3.put_object()` to upload the CSV. Pass the Bucket, Key, Body (`csv_buffer.getvalue()`), and ContentType (`'text/csv'`).
    
- **Instruction 5:** Print a success message!
    

> **💡 Hints for Task 4:**
> 
> - _Why `StringIO`?_ Normally, `df.to_csv("file.csv")` saves to your local hard drive. Because we want to send the file directly to the cloud over the internet, we save it into an imaginary "text file" in our RAM using `StringIO`, and then upload that text.
>     

### **How to check your work:**

Run your script in the terminal:

Bash

```
python3 transform.py
```

If it runs successfully, go to your **AWS Console**:

1. Open **S3** and click your bucket.
    
2. You should now see a `processed/` folder! Click into it, then into today's date.
    
3. You will see a single file named `prices.csv`. This file contains the combined, cleaned, and calculated data for all 10 stocks.
    

You are now ready for the final step: Loading the data into PostgreSQL!
---

## 11. Load — S3 to RDS (Incremental)

**File: `load.py`** — run daily after transform. Reads clean CSV from S3, filters to only new records not yet in the warehouse, maps foreign keys, and inserts into RDS.

### Why incremental loading matters

`extract.py` always pulls the last 100 trading days from Alpha Vantage — it has no way to ask for "just today." Without incremental loading, every pipeline run would attempt to process and insert all 100 days, wasting compute and time.

The fix: before inserting anything, ask RDS what the latest date already loaded is, then discard everything older than that. On the very first run it loads all 100 days of history. On every run after that it loads only the 1 new day.

```
Run 1 (first time):  100 days available → last_loaded = None  → insert 100 rows
Run 2 (next day):    100 days available → last_loaded = today-1 → insert 1 row
Run 3 (day after):   100 days available → last_loaded = today-1 → insert 1 row
```

```python
import os
import boto3
import pandas as pd
from io import StringIO
from datetime import date
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET')
RDS_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
    f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

s3     = boto3.client('s3', region_name=os.getenv('AWS_REGION'))
engine = create_engine(RDS_URL)
today  = date.today().isoformat()

print(f"Starting incremental load for {today}...")

# ==========================================
# PHASE 1: READ PROCESSED CSV FROM S3
# ==========================================
processed_key = f"processed/{today}/prices.csv"

try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=processed_key)
    df  = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
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
db_dates     = pd.read_sql("SELECT date_id, date AS sale_date FROM dim_date",    engine)
db_companies = pd.read_sql("SELECT company_id, symbol         FROM dim_company", engine)

db_dates['sale_date'] = pd.to_datetime(db_dates['sale_date']).dt.date

print(f"  dim_date    → {len(db_dates)} rows")
print(f"  dim_company → {len(db_companies)} rows")

# ==========================================
# PHASE 4: MAP FOREIGN KEYS (TEXT → IDs)
# ==========================================
print("Mapping foreign keys...")

before = len(df)
df = df.merge(db_dates,     on='sale_date', how='inner')
df = df.merge(db_companies, on='symbol',    how='inner')
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
```

---
Here is your next exercise! You have reached the final **"L" (Load)** in your ETL pipeline.

So far, you’ve extracted raw JSON from Alpha Vantage and transformed it into a clean, analyst‑ready CSV. Now, you are going to take that CSV and load it into your PostgreSQL data warehouse — but not blindly. You will implement an **incremental load pattern** so that only new records are inserted each day, even though your source files always contain the last 100 trading days. This is a hallmark of professional data engineering: never reprocess what you already have.

Open a new, blank file named `load.py`. Follow these guided steps to build your loader!

---

### **Why incremental loading matters**

`extract.py` always pulls the last 100 trading days from Alpha Vantage — it has no way to ask for "just today." Without incremental loading, every pipeline run would attempt to process and insert all 100 days, wasting compute and time.

**The fix:** before inserting anything, ask RDS what the latest date already loaded is, then discard everything older than that. On the very first run it loads all 100 days of history. On every run after that it loads only the 1 new day.

```
Run 1 (first time):  100 days available → last_loaded = None  → insert 100 rows
Run 2 (next day):    100 days available → last_loaded = today-1 → insert 1 row
Run 3 (day after):   100 days available → last_loaded = today-1 → insert 1 row
```

---

### **Exercise: Load — S3 to RDS (Incremental)**

**Objective:** Write a Python script that reads a clean CSV from S3, determines which records are new by checking a *watermark* in the PostgreSQL warehouse, maps business keys to foreign keys using dimension tables, and inserts only the fresh rows into `fact_stock_prices`.

---

#### **Task 1: Setup and Reading Processed CSV from S3**

**Objective:** Connect to both S3 and RDS, then grab the CSV that `transform.py` created today.

* **Instruction 1:** Import `os`, `boto3`, `pandas as pd`, `StringIO` from `io`, `date` from `datetime`, `create_engine` from `sqlalchemy`, and `load_dotenv` from `dotenv`.
* **Instruction 2:** Load your environment variables. Build the RDS connection string using `f"postgresql://..."` and store it as `RDS_URL`. Create your `s3` client and an SQLAlchemy `engine`.
* **Instruction 3:** Define `today` exactly as in the previous scripts.
* **Instruction 4:** Build the S3 key for the processed file: `f"processed/{today}/prices.csv"`.
* **Instruction 5:** Use a `try/except` block to read that CSV directly from S3 into a pandas DataFrame. If the file isn’t found, print an error and exit.
* **Instruction 6:** Convert the `sale_date` column in your DataFrame to Python date objects.

> **💡 Hints for Task 1:**
> * *Building RDS URL:* `RDS_URL = f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"`
> * *Reading CSV from S3:* `obj = s3.get_object(Bucket=S3_BUCKET, Key=processed_key)`; then `df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))`

---

#### **Task 2: Incremental Filter (The Watermark)**

**Objective:** Query the data warehouse to find the most recent date already loaded, and then keep only records newer than that date.

* **Instruction 1:** Write a SQL query to find the maximum `date` from the fact table by joining `fact_stock_prices` with `dim_date` on `date_id`. Use `pd.read_sql()` to execute it.
* **Instruction 2:** Extract the single value from the result (hint: `.iloc[0, 0]`). This is your watermark: `last_loaded`.
* **Instruction 3:** If `last_loaded` is not `None`, convert it to a date object and filter your DataFrame: `df = df[df['sale_date'] > last_loaded]`.
* **Instruction 4:** If `last_loaded` is `None` (meaning the table is empty), print a message indicating a full historical load is being performed.
* **Instruction 5:** After filtering, check if the DataFrame is empty. If it is, print that the warehouse is already up to date and gracefully exit.

> **💡 Hints for Task 2:**
> * *Watermark query:* `SELECT MAX(d.date) AS last_date FROM fact_stock_prices f JOIN dim_date d ON f.date_id = d.date_id`
> * *Filter syntax:* `df = df[df['sale_date'] > last_loaded]`
> * *Exiting early:* use `exit(0)`

---

#### **Task 3: Reading Dimension Tables from RDS**

**Objective:** Pull the `dim_date` and `dim_company` tables from PostgreSQL so you can map textual dates and symbols to surrogate IDs.

* **Instruction 1:** Read the entire `dim_date` table into a DataFrame: `db_dates`. Keep only `date_id` and the date column (rename it to `sale_date` for merging).
* **Instruction 2:** Read the entire `dim_company` table into a DataFrame: `db_companies`. Keep only `company_id` and `symbol`.
* **Instruction 3:** Convert the `sale_date` column in `db_dates` to actual date objects (matching the type in your main DataFrame).

> **💡 Hints for Task 3:**
> * *Reading dim_date:* `pd.read_sql("SELECT date_id, date AS sale_date FROM dim_date", engine)`
> * *Reading dim_company:* `pd.read_sql("SELECT company_id, symbol FROM dim_company", engine)`

---

#### **Task 4: Mapping Foreign Keys**

**Objective:** Replace human‑readable `sale_date` and `symbol` with the surrogate keys `date_id` and `company_id`.

* **Instruction 1:** Merge your filtered DataFrame with `db_dates` on the column `sale_date`. Use an `inner` join. If any dates are missing from the dimension table, they will be dropped.
* **Instruction 2:** Merge the result with `db_companies` on the column `symbol` (again `inner`).
* **Instruction 3:** Keep track of the row count before and after the merges. If any rows were lost, print a warning – it usually means you forgot to run your dimension‑seed script.

> **💡 Hints for Task 4:**
> * *First merge:* `df = df.merge(db_dates, on='sale_date', how='inner')`
> * *Second merge:* `df = df.merge(db_companies, on='symbol', how='inner')`

---

#### **Task 5: Building and Inserting into the Fact Table**

**Objective:** Select only the columns needed for `fact_stock_prices` and load them into the database.

* **Instruction 1:** Create a new DataFrame `fact` that includes only these columns: `'date_id'`, `'company_id'`, `'open_price'`, `'high_price'`, `'low_price'`, `'close_price'`, `'volume'`, `'price_change'`, `'daily_range'`.
* **Instruction 2:** Use `fact.to_sql()` to append the rows to the `fact_stock_prices` table. Important settings: `if_exists='append'`, `index=False`, `chunksize=5000`, `method='multi'`.
* **Instruction 3:** Print a final success message showing how many records were loaded and the maximum date now present in the warehouse.

> **💡 Hints for Task 5:**
> * *Column selection:* `fact = df[['date_id', 'company_id', ...]]`
> * *Insert syntax:* `fact.to_sql('fact_stock_prices', engine, if_exists='append', index=False, chunksize=5000, method='multi')`

---

### **How to check your work:**

Run your script in the terminal:

```bash
python3 load.py
```

If it runs successfully:

1. Log into your PostgreSQL database using your preferred client (or `psql` on the EC2).
2. Run a quick query:
   ```sql
   SELECT COUNT(*) FROM fact_stock_prices;
   SELECT MAX(d.date) FROM fact_stock_prices f JOIN dim_date d ON f.date_id = d.date_id;
   ```
3. The first time you run `load.py`, you should see 100 rows (or whatever the number of trading days extracted was). The next day, running it again should add exactly 10 new rows (one per symbol) – the incremental logic at work!

## 12. Pipeline Orchestrator

**File: `pipeline.py`** — runs the full daily pipeline in one command.

```python
import subprocess
import sys
from datetime import date

print(f"{'='*55}")
print(f"  Stock Market DWH Pipeline — {date.today().isoformat()}")
print(f"{'='*55}")

scripts = [
    ('extract.py',   'Extract: Alpha Vantage API → S3 raw'),
    ('transform.py', 'Transform: S3 raw → S3 processed'),
    ('load.py',      'Load: S3 processed → RDS'),
]

for script, description in scripts:
    print(f"\n[{description}]")
    print('-' * 45)
    result = subprocess.run([sys.executable, script], capture_output=False)

    if result.returncode != 0:
        print(f"\n❌ {script} failed. Pipeline aborted.")
        sys.exit(1)

print(f"\n{'='*55}")
print("  🎉 Pipeline completed successfully.")
print(f"{'='*55}")
```

---
You're absolutely right — understanding *why* the code works is far more important than just copying it. I’ll expand the orchestration exercise with a thorough explanation of every concept, so you walk away knowing exactly what each line does and how to reason about it.

Below is the full practice material, now with a **“Concept Deep Dive”** section that breaks down the provided `pipeline.py` line by line. Use this to learn the code, not just write it.

---



**Objective:** Learn how to chain multiple Python scripts together using `subprocess`, understand exit codes, and build a fail‑fast orchestrator that stops immediately if any step breaks.

---

### Task 1: Imports and the Starting Banner
Same as before, but let's understand the modules:

- `subprocess` – lets Python spawn new processes, run shell commands or other scripts, and wait for them to finish. It’s the bridge between your orchestrator and the individual ETL scripts.
- `sys` – gives access to the Python interpreter. `sys.executable` is the path to the Python binary that’s currently running your script. This guarantees you call `extract.py` with the same Python version, avoiding “command not found” or version conflicts.
- `datetime.date` – just to display today’s date in the header.

```python
import subprocess
import sys
from datetime import date

print(f"{'='*55}")
print(f"  Stock Market DWH Pipeline — {date.today().isoformat()}")
print(f"{'='*55}")
```

**Why the fancy header?**  
When you run this daily, the banner clearly marks the beginning of a new pipeline run in the logs. If something goes wrong, you can scroll back and find exactly where the failure happened.

---

### Task 2: Defining the Script Execution Order

We store the scripts and their human‑readable descriptions in a list of tuples.  
Each tuple is `(filename, description)`.

```python
scripts = [
    ('extract.py',   'Extract: Alpha Vantage API → S3 raw'),
    ('transform.py', 'Transform: S3 raw → S3 processed'),
    ('load.py',      'Load: S3 processed → RDS'),
]
```

**Why a list of tuples?**
- **Order matters** – the list preserves the sequence. `extract` must run before `transform`, and `transform` before `load`.
- **Description is for you** – it prints what’s happening so you can see progress in the terminal.
- **Easy to modify** – if you add a new step (e.g., data validation), just append another tuple.

---

### Task 3: Looping Through and Executing Scripts

This is the heart of the orchestrator. Let’s walk through every line.

```python
for script, description in scripts:
    print(f"\n[{description}]")
    print('-' * 45)
    result = subprocess.run([sys.executable, script], capture_output=False)

    if result.returncode != 0:
        print(f"\n❌ {script} failed. Pipeline aborted.")
        sys.exit(1)
```

#### Breaking it down:

1. **`for script, description in scripts:`**  
   Unpacks each tuple. On the first iteration, `script = 'extract.py'`, `description = 'Extract: Alpha Vantage API → S3 raw'`.

2. **Printing the step header**  
   `[Extract: Alpha Vantage API → S3 raw]` and a dashed line visually separate the output of each script.

3. **`subprocess.run(...)`** – the key call  
   - It launches a **new child process**.  
   - The command is `[sys.executable, script]`.  
     - `sys.executable` is something like `/usr/bin/python3` (or wherever Python is installed).  
     - `script` is `'extract.py'`.  
     - Together they become the equivalent of typing `python3 extract.py` in the terminal.  
   - `capture_output=False` means **the child process’s output goes directly to the same terminal window**. You see all prints, errors, and progress in real time. If you set it to `True`, the output would be hidden and stored in a variable instead – not what we want during monitoring.

4. **`result.returncode`**  
   Every process returns an **exit code** when it finishes.  
   - `0` → success (no errors).  
   - Any non‑zero value → failure (script crashed, exited with `exit(1)`, or raised an uncaught exception).  

   The orchestrator checks this immediately after the child process finishes.

5. **Fail‑fast logic**  
   ```python
   if result.returncode != 0:
       print(f"\n❌ {script} failed. Pipeline aborted.")
       sys.exit(1)
   ```
   If `extract.py` fails, there’s no point running `transform.py` – you’d be transforming nothing, or worse, transforming stale data. This “fail‑fast” pattern stops the pipeline instantly and returns a non‑zero exit code from the orchestrator itself (`sys.exit(1)`), which is important for monitoring tools (cron, Airflow) to detect failure.

---

### Task 4: Success Banner

Only if the loop finishes without hitting a `sys.exit(1)` do we reach this code:

```python
print(f"\n{'='*55}")
print("  🎉 Pipeline completed successfully.")
print(f"{'='*55}")
```

This tells you at a glance that all three scripts completed without errors. The pipeline is done.

---

### Concept Deep Dive – Why This Works

#### The Orchestrator Pattern
In data engineering, you never run raw scripts by hand in production. Instead, a single “orchestrator” script is scheduled to run automatically (via cron, Airflow, etc.). The orchestrator is responsible for:
- **Sequencing** – ensuring scripts run in the correct order.
- **Error handling** – stopping the entire run if something breaks.
- **Logging** – providing a clear, unified log of what happened.

This simple `pipeline.py` does all three.

#### `subprocess.run` vs. `import`
Why not just `import extract`, `import transform`, and call functions?  
- **Isolation** – each script runs in its own process, with its own memory space. If `extract.py` crashes, it won’t corrupt the orchestrator’s state.  
- **Simplicity** – you don’t need to refactor existing scripts into functions; they remain standalone, testable scripts.  
- **Error detection** – exit codes provide a universal way to detect success or failure, regardless of the script’s internal structure.

#### Exit Codes: The Universal Language
Every command‑line tool returns an exit code. Python scripts implicitly return `0` on success. If you call `sys.exit(1)` (as we did in earlier exercises when a file wasn’t found), the exit code becomes `1`. An unhandled exception also results in a non‑zero exit code. The orchestrator simply checks this number – if it’s not zero, the pipeline fails.

#### Real‑time Output (`capture_output=False`)
When `capture_output=False`, the child process inherits the parent’s standard output and error streams. This means:
- You see `print()` statements from `extract.py` immediately.
- Errors (tracebacks) appear in the same terminal, in order.
- The orchestrator waits until the script finishes before moving on – it’s synchronous.

If you set `capture_output=True`, you would need to print `result.stdout` and `result.stderr` manually, and you’d lose the real‑time feel. For a daily monitoring script, real‑time output is usually preferred.

#### `sys.executable` – Avoid “python not found” Problems
On some systems, `python` might point to Python 2, while `python3` is the correct version. By using `sys.executable`, you are guaranteed to use the exact same interpreter that’s running the orchestrator. This eliminates version mismatches and missing package errors (since you installed `boto3`, `pandas`, etc. for that interpreter).

---

### Additional Learning Exercises

1. **Test failure handling** – temporarily rename `transform.py` and run `pipeline.py`. Watch how it prints the error and stops before attempting `load.py`.
2. **Manually set a non‑zero exit code** – add `sys.exit(2)` at the top of `extract.py`. Observe that the orchestrator exits with `sys.exit(1)` (its own error exit code).
3. **Try `capture_output=True`** – change it to `True` and print `result.stdout` after the run. Notice that you no longer see live output; everything appears only after the script finishes.

---

### How to check your work:

Run your orchestrator in the terminal:

```bash
python3 pipeline.py
```

You should see the header, then each script’s output in sequence, and finally the success banner. If any script fails, the pipeline aborts immediately, and you’ll see a clear “❌” error message.

You’ve now built a professional‑grade orchestrator. This single file is ready to be scheduled daily with `cron` or any workflow manager. Congratulations on completing the entire ETL pipeline!

## 13. Optimize the Warehouse

Connect to RDS:

```bash
psql -h YOUR-RDS-ENDPOINT -U postgres -d stock_dw
```

### Build indexes

```sql
-- Index foreign keys in the fact table (speeds up all JOINs)
CREATE INDEX idx_fact_date    ON fact_stock_prices(date_id);
CREATE INDEX idx_fact_company ON fact_stock_prices(company_id);

-- Index dimension columns analysts filter by most often
CREATE INDEX idx_dim_date_year     ON dim_date(year);
CREATE INDEX idx_dim_date_month    ON dim_date(month);
CREATE INDEX idx_dim_company_sector ON dim_company(sector);
CREATE INDEX idx_dim_company_symbol ON dim_company(symbol);
```

### Run ANALYZE

Tells the PostgreSQL query planner the indexes exist and are populated:

```sql
ANALYZE VERBOSE fact_stock_prices;
ANALYZE VERBOSE dim_date;
ANALYZE VERBOSE dim_company;
```

---
Here is your next exercise! You’ve built a fully automated pipeline that extracts, transforms, and loads data every day. But a warehouse isn’t finished just because data is in it — you need to make it **fast**. Analysts will run queries that JOIN multiple tables, filter by date or sector, and aggregate thousands of rows. Without proper indexing, those queries can take minutes instead of milliseconds, and your database will slow down as the fact table grows.

This step is pure SQL. You’ll connect directly to your RDS PostgreSQL instance and apply the final performance tuning that separates a demo database from a production‑ready one.

---

### **Exercise: Optimize the Warehouse**

**Objective:** Create database indexes on high‑traffic columns and update the query planner statistics so PostgreSQL can execute JOINs and filters efficiently.

---

#### **Why optimization matters**

Right now, your `fact_stock_prices` table has foreign keys `date_id` and `company_id`. When an analyst writes:

```sql
SELECT c.symbol, d.date, f.close_price
FROM fact_stock_prices f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_company c ON f.company_id = c.company_id
WHERE c.sector = 'Technology' AND d.year = 2026;
```

PostgreSQL has to scan the *entire* fact table, check every row, and match it to the dimension tables. That’s fine with 100 rows — but after a year (36,500 rows) or a decade, it becomes unusable. **Indexes** act like a book’s index: instead of flipping through every page, the database goes directly to the rows it needs.

---

#### **Task 1: Connect to Your RDS Database**

Open a terminal and connect to your warehouse using `psql`:

```bash
psql -h YOUR-RDS-ENDPOINT -U postgres -d stock_dw
```

Replace `YOUR-RDS-ENDPOINT` with the actual endpoint from AWS (you can find it in the RDS console). The default username is `postgres`, and the database is `stock_dw` (as configured in earlier steps). You’ll be prompted for the password you set during RDS creation.

Once connected, you’ll see the `stock_dw=>` prompt. All remaining commands will be entered there, or you can save them in a `.sql` file and run them with `psql -f`.

---

#### **Task 2: Build Indexes on High‑Traffic Columns**

**Objective:** Create B‑tree indexes on foreign keys and frequently filtered dimension columns.

Run each `CREATE INDEX` command one by one. They will speed up JOINs, WHERE clauses, and GROUP BY operations.

##### **Indexes on Fact Table Foreign Keys**

These are **critical** — every query that joins `fact_stock_prices` to a dimension table will use these.

```sql
CREATE INDEX idx_fact_date    ON fact_stock_prices(date_id);
CREATE INDEX idx_fact_company ON fact_stock_prices(company_id);
```

##### **Indexes on Dimension Tables**

Analysts often filter by year, month, sector, and symbol. Adding indexes here makes those filters nearly instantaneous.

```sql
CREATE INDEX idx_dim_date_year     ON dim_date(year);
CREATE INDEX idx_dim_date_month    ON dim_date(month);
CREATE INDEX idx_dim_company_sector ON dim_company(sector);
CREATE INDEX idx_dim_company_symbol ON dim_company(symbol);
```

**Why these particular columns?**
- `dim_date.year` / `month`: Queries often request “all data for 2025” or “monthly aggregates”.
- `dim_company.sector`: Sectors like “Technology” are common grouping criteria.
- `dim_company.symbol`: You’ll frequently look up a specific stock by its ticker.

> **💡 Hint:** Indexing *every* column is wasteful — it slows down inserts and takes disk space. Only index columns used in WHERE, JOIN, or ORDER BY.

---

#### **Task 3: Run ANALYZE**

**Objective:** Update table statistics so the PostgreSQL query planner knows how to best use your new indexes.

PostgreSQL doesn’t automatically know an index exists until it’s been told about it, or until the table’s statistics are refreshed. `ANALYZE` scans the table and records metadata like the number of distinct values, the distribution of data, and the size of the table. The query planner uses this information to decide whether to use an index or to just scan the whole table (a full scan is sometimes faster for very small tables).

Run these:

```sql
ANALYZE VERBOSE fact_stock_prices;
ANALYZE VERBOSE dim_date;
ANALYZE VERBOSE dim_company;
```

`VERBOSE` isn’t required, but it shows you progress and confirms the command ran. After `ANALYZE`, PostgreSQL will immediately start using the indexes when they’re beneficial.

---

### **Concept Deep Dive – How Indexes and ANALYZE Work**

#### **What is a B‑tree Index?**
A B‑tree index is a self‑balancing tree structure. It stores column values in sorted order and allows PostgreSQL to find matching rows in O(log n) time instead of O(n) scanning. For a 1‑million‑row table, a full scan reads all 1M rows; an index lookup might read only 2–3 tree levels, dramatically reducing disk I/O.

#### **Why Index Foreign Keys?**
When you `JOIN fact_stock_prices f ON f.date_id = d.date_id`, PostgreSQL must match every row in the fact table with its corresponding row in `dim_date`. Without an index on `fact_stock_prices.date_id`, it scans the fact table once for each row in `dim_date` (a nested loop). With the index, it can directly look up the matching rows — a huge speed boost.

#### **The Role of ANALYZE**
PostgreSQL maintains a **statistics collector** that tracks what portion of a column’s values are unique, how many rows there are, etc. `ANALYZE` updates these statistics. Why is this important?

- **Selectivity estimation** — If a query filters `WHERE sector = 'Technology'` and only 5% of companies are in tech, the planner will use the index on `sector`. If all companies were tech (100% selectivity), the planner might ignore the index and do a full scan, because reading the index and then the table would be slower. The planner needs accurate statistics to make that decision.
- **Fresh data** — After a bulk load (like your pipeline’s first 100 rows), statistics are stale. `ANALYZE` ensures the planner knows exactly what the tables look like *now*.

> In a production environment, PostgreSQL automatically runs `ANALYZE` after a significant portion of a table has changed, but manual `ANALYZE` is a best practice after initial data load or heavy modifications.

---

### **How to check your work**

1. **List the indexes**  
   In `psql`, run:  
   ```sql
   \di
   ```
   You should see all six new indexes with their table names.

2. **Test query performance**  
   Run the sample query from the introduction. Before indexes, PostgreSQL might use `Seq Scan` (sequential scan). After indexes, use `EXPLAIN` to see the execution plan:
   ```sql
   EXPLAIN SELECT c.symbol, d.date, f.close_price
   FROM fact_stock_prices f
   JOIN dim_date d ON f.date_id = d.date_id
   JOIN dim_company c ON f.company_id = c.company_id
   WHERE c.sector = 'Technology' AND d.year = 2026;
   ```
   Look for lines like `Index Scan` or `Bitmap Index Scan` — they show the indexes are being used. If you see `Seq Scan on fact_stock_prices`, your indexes might not be created or the table is so small that the planner intentionally ignored them (this is normal with only 100 rows). As more data is loaded, indexes will automatically kick in.

3. **Repeat the pipeline**  
   Run `python3 pipeline.py` again tomorrow. The `load.py` script will insert new rows, and the indexes will immediately speed up all future queries — no further tuning needed.

---

You’ve now transformed a slow, unscaled collection of tables into a **fully optimized, production‑ready data warehouse**. Every piece of your pipeline — extraction, transformation, loading, orchestration, and performance tuning — is in place. Congratulations!


## 14. Test & Validate

### 14.1 Row count check

```sql
SELECT
    (SELECT COUNT(*) FROM fact_stock_prices) AS fact_rows,
    (SELECT COUNT(*) FROM dim_company)        AS companies,
    (SELECT COUNT(*) FROM dim_date)           AS date_rows;
```

Expected: `fact_rows` grows each day (~1,000 rows per daily run × 100 trading days backfill = ~10,000 rows after first load), `companies = 10`, `date_rows = 3653`.

### 14.2 Orphan check — no ghost records

```sql
-- No fact rows pointing to a missing date
SELECT COUNT(*) AS orphaned_dates
FROM fact_stock_prices f
LEFT JOIN dim_date d ON f.date_id = d.date_id
WHERE d.date_id IS NULL;
-- Must return: 0

-- No fact rows pointing to a missing company
SELECT COUNT(*) AS orphaned_companies
FROM fact_stock_prices f
LEFT JOIN dim_company c ON f.company_id = c.company_id
WHERE c.company_id IS NULL;
-- Must return: 0
```

### 14.3 Data integrity — no impossible prices

```sql
SELECT COUNT(*) AS invalid_rows
FROM fact_stock_prices
WHERE open_price <= 0
   OR close_price <= 0
   OR volume <= 0
   OR low_price > high_price;
-- Must return: 0
```

### 14.4 Prove the indexes are being used

```sql
EXPLAIN ANALYZE
SELECT c.symbol, SUM(f.volume) AS total_volume
FROM fact_stock_prices f
JOIN dim_company c ON f.company_id = c.company_id
GROUP BY c.symbol
ORDER BY total_volume DESC;
```

Look for `Index Scan` (not `Seq Scan`) in the output — this confirms your indexes are working.

---

## 15. Analytics Queries

**File: `analysis.py`** — run any time to query the warehouse.

```python
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

RDS_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
    f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)
engine = create_engine(RDS_URL)

queries = {

    "Best Performing Stocks This Month": """
        SELECT
            c.symbol,
            c.company_name,
            c.sector,
            ROUND(AVG(f.price_change), 2)  AS avg_daily_change,
            ROUND(SUM(f.price_change), 2)  AS total_month_change
        FROM fact_stock_prices f
        JOIN dim_company c ON f.company_id = c.company_id
        JOIN dim_date    d ON f.date_id    = d.date_id
        WHERE d.year  = EXTRACT(YEAR  FROM CURRENT_DATE)
          AND d.month = EXTRACT(MONTH FROM CURRENT_DATE)
        GROUP BY c.symbol, c.company_name, c.sector
        ORDER BY total_month_change DESC;
    """,

    "Most Volatile Stocks (Largest Daily Swing)": """
        SELECT
            c.symbol,
            c.sector,
            ROUND(AVG(f.daily_range), 2) AS avg_daily_range,
            ROUND(MAX(f.daily_range), 2) AS max_single_day_range
        FROM fact_stock_prices f
        JOIN dim_company c ON f.company_id = c.company_id
        GROUP BY c.symbol, c.sector
        ORDER BY avg_daily_range DESC;
    """,

    "Sector Performance This Year": """
        SELECT
            c.sector,
            ROUND(AVG(f.price_change), 4) AS avg_daily_price_change,
            ROUND(SUM(f.volume) / 1e6, 2) AS total_volume_millions
        FROM fact_stock_prices f
        JOIN dim_company c ON f.company_id = c.company_id
        JOIN dim_date    d ON f.date_id    = d.date_id
        WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)
        GROUP BY c.sector
        ORDER BY avg_daily_price_change DESC;
    """,

    "Volume Trends by Quarter": """
        SELECT
            d.year,
            d.quarter_name,
            c.symbol,
            ROUND(SUM(f.volume) / 1e6, 2) AS total_volume_millions
        FROM fact_stock_prices f
        JOIN dim_company c ON f.company_id = c.company_id
        JOIN dim_date    d ON f.date_id    = d.date_id
        GROUP BY d.year, d.quarter_name, d.quarter, c.symbol
        ORDER BY d.year, d.quarter, c.symbol;
    """,

    "Top 5 Highest Closing Prices Ever Recorded": """
        SELECT
            c.symbol,
            c.company_name,
            d.date,
            f.close_price
        FROM fact_stock_prices f
        JOIN dim_company c ON f.company_id = c.company_id
        JOIN dim_date    d ON f.date_id    = d.date_id
        ORDER BY f.close_price DESC
        LIMIT 5;
    """
}

for title, query in queries.items():
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)
    df = pd.read_sql(query, engine)
    print(df.to_string(index=False))

print("\n✅ Analysis complete.")
```

Run it:

```bash
python3 analysis.py
```

---
You’ve built the warehouse, optimized it, and automated the pipeline. Now comes the reward: **querying your data** to extract insights. Analysts don’t write raw ETL code — they write SQL queries that answer business questions. This exercise turns you into the analyst, running pre‑built queries that your pipeline made possible.

We’ll write a Python script, `analysis.py`, that connects to the warehouse, executes a set of analytic queries, and prints the results. You’ll learn how to use `pandas.read_sql` to bridge the gap between SQL and your Python environment, and you’ll see how the dimensional model you designed makes joins and aggregations natural and efficient.

---

### **Exercise: Analytics Queries — Deriving Insights from Your Warehouse**

**Objective:** Build a script that connects to your RDS data warehouse, runs multiple SQL queries, and displays the results in a clean, tabular format. Along the way, understand how each query translates a business question into SQL, and how `pandas` makes it easy to work with the returned data.

---

#### **Task 1: Imports, Environment, and Database Connection**

**Objective:** Set up the same connection to PostgreSQL you used in the `load.py` script, but this time for reading data, not writing.

* **Instruction 1:** Import the necessary modules: `os`, `pandas as pd`, `create_engine` from `sqlalchemy`, and `load_dotenv` from `dotenv`.
* **Instruction 2:** Load your environment variables.
* **Instruction 3:** Construct the `RDS_URL` exactly as you did in the load script, using the `RDS_USER`, `RDS_PASSWORD`, `RDS_HOST`, `RDS_PORT`, and `RDS_DB` values from your `.env` file.
* **Instruction 4:** Create an SQLAlchemy `engine` object by calling `create_engine(RDS_URL)`.

> **💡 Hints for Task 1:**
> * *URL format:* `f"postgresql://{user}:{password}@{host}:{port}/{db}"`
> * *Engine creation:* `engine = create_engine(RDS_URL)`
> * The engine is your permanent connection factory — it doesn’t open a connection until you use it, but it manages pooling and reuse automatically.

---

#### **Task 2: Writing Business‑Oriented SQL Queries**

**Objective:** Define a dictionary of queries where each key is a human‑readable title and each value is a SQL string that answers a real business question. You’ll use the same SQL patterns that analysts use every day: `JOIN`, `GROUP BY`, `AVG`, `SUM`, `ORDER BY`, `LIMIT`.

Create a dictionary called `queries` with the following five entries (you can copy the SQL exactly, but read the explanations to understand *why* each clause is there):

##### **Query 1: Best Performing Stocks This Month**

```sql
SELECT
    c.symbol,
    c.company_name,
    c.sector,
    ROUND(AVG(f.price_change), 2)  AS avg_daily_change,
    ROUND(SUM(f.price_change), 2)  AS total_month_change
FROM fact_stock_prices f
JOIN dim_company c ON f.company_id = c.company_id
JOIN dim_date    d ON f.date_id    = d.date_id
WHERE d.year  = EXTRACT(YEAR  FROM CURRENT_DATE)
  AND d.month = EXTRACT(MONTH FROM CURRENT_DATE)
GROUP BY c.symbol, c.company_name, c.sector
ORDER BY total_month_change DESC;
```

**What it does:**  
- Filters to the current month and year using the handy `EXTRACT` function (so the query automatically stays up‑to‑date).  
- Joins the fact table to both dimension tables to get the symbol, company name, and sector.  
- `AVG(price_change)` shows the average daily price movement; `SUM(price_change)` gives the total net change for the month (how much the stock gained or lost overall).  
- Orders by total month change descending so the biggest winners appear first.

##### **Query 2: Most Volatile Stocks (Largest Daily Swing)**

```sql
SELECT
    c.symbol,
    c.sector,
    ROUND(AVG(f.daily_range), 2) AS avg_daily_range,
    ROUND(MAX(f.daily_range), 2) AS max_single_day_range
FROM fact_stock_prices f
JOIN dim_company c ON f.company_id = c.company_id
GROUP BY c.symbol, c.sector
ORDER BY avg_daily_range DESC;
```

**What it does:**  
- `daily_range` (high – low) is a metric you calculated in the transform step. It’s already pre‑computed, making this query lightning fast.  
- `AVG` shows the typical intraday price swing; `MAX` reveals the most extreme day.  
- Ordered by average volatility to highlight the most turbulent stocks.

##### **Query 3: Sector Performance This Year**

```sql
SELECT
    c.sector,
    ROUND(AVG(f.price_change), 4) AS avg_daily_price_change,
    ROUND(SUM(f.volume) / 1e6, 2) AS total_volume_millions
FROM fact_stock_prices f
JOIN dim_company c ON f.company_id = c.company_id
JOIN dim_date    d ON f.date_id    = d.date_id
WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY c.sector
ORDER BY avg_daily_price_change DESC;
```

**What it does:**  
- Groups all stocks by their sector.  
- `AVG(price_change)` tells you which sector’s stocks are, on average, rising or falling each day.  
- `SUM(volume) / 1e6` converts raw volume to millions, making it readable.  
- The `WHERE` clause restricts to the current calendar year.

##### **Query 4: Volume Trends by Quarter**

```sql
SELECT
    d.year,
    d.quarter_name,
    c.symbol,
    ROUND(SUM(f.volume) / 1e6, 2) AS total_volume_millions
FROM fact_stock_prices f
JOIN dim_company c ON f.company_id = c.company_id
JOIN dim_date    d ON f.date_id    = d.date_id
GROUP BY d.year, d.quarter_name, d.quarter, c.symbol
ORDER BY d.year, d.quarter, c.symbol;
```

**What it does:**  
- `dim_date.quarter_name` is a pre‑calculated attribute like “Q1”, “Q2”.  
- Sums the volume per stock per quarter — useful for spotting seasonal trading patterns.  
- `ORDER BY d.quarter` ensures chronological ordering (since `quarter_name` is a string, we also group by the numeric `quarter` to keep the order correct).

##### **Query 5: Top 5 Highest Closing Prices Ever Recorded**

```sql
SELECT
    c.symbol,
    c.company_name,
    d.date,
    f.close_price
FROM fact_stock_prices f
JOIN dim_company c ON f.company_id = c.company_id
JOIN dim_date    d ON f.date_id    = d.date_id
ORDER BY f.close_price DESC
LIMIT 5;
```

**What it does:**  
- A simple “leaderboard” query.  
- Sorts every closing price in history and picks the top 5.  
- Useful for checking data sanity (are there any unrealistic $999,999.99 prices?).

---

#### **Task 3: Running the Queries and Printing Results**

**Objective:** Loop over the `queries` dictionary, execute each one using `pandas.read_sql()`, and display the DataFrame without the default row index.

* **Instruction 1:** Start a `for` loop that iterates over `queries.items()`, which returns `(title, query)` pairs.
* **Instruction 2:** Inside the loop, print a header containing the title, surrounded by lines of `=` signs for readability.
* **Instruction 3:** Use `pd.read_sql(query, engine)` to run the SQL and capture the result in a DataFrame. (This function sends the SQL to the database and converts the returned rows into a pandas DataFrame.)
* **Instruction 4:** Print the DataFrame using `df.to_string(index=False)`. The `index=False` hides the pandas row numbers, so only your data appears.
* **Instruction 5:** After the loop, print a final message indicating the analysis is complete.

> **💡 Hints for Task 3:**
> - *Reading SQL:* `df = pd.read_sql(query, engine)`
> - *Printing nicely:* `print(df.to_string(index=False))`
> - *Looping over dictionary:* `for title, query in queries.items():`

---

#### **Task 4: Run the Script and Observe**

Save your file as `analysis.py` and execute it:

```bash
python3 analysis.py
```

You’ll see each query’s title, then a clean table of results. If any query returns no rows (e.g., no data for the current month yet), the DataFrame will be empty, and you’ll see only the headers.

---

### **Concept Deep Dive: How `pd.read_sql` and the Engine Work Together**

`pd.read_sql` is a convenience function that:

1. Opens a connection to the database using the `engine` (or a raw connection if you pass one).
2. Sends the SQL string to PostgreSQL.
3. PostgreSQL executes the query and returns the results in a binary protocol.
4. `pandas` converts those results into a DataFrame, automatically assigning column names and data types based on the query output.
5. The connection is then returned to the connection pool (or closed) automatically.

Because you’re using SQLAlchemy, the engine manages connection pooling for you. Even if you run 100 queries in a loop, `read_sql` will reuse connections efficiently, preventing the overhead of repeatedly opening and closing new database connections.

#### **Why These Queries Are Fast**
- You built indexes on `date_id`, `company_id`, `year`, `month`, `sector`, and `symbol`. Those indexes are now silently used by the PostgreSQL query planner to speed up every `JOIN` and `WHERE` clause.
- The `WHERE d.year = EXTRACT(...)` filter in Query 1 allows an index‑based scan on `dim_date(year)`, quickly narrowing down the rows before joining.
- Pre‑calculated columns like `price_change` and `daily_range` avoid computing them on the fly.

#### **Extending the Analysis**
Once you’re comfortable, try modifying the queries or adding new ones. For example:
- Find the closing price 7 days ago vs. today for each stock.
- Calculate the correlation between volume and price change.
- Identify days where a stock’s price dropped more than 2% (a crude anomaly detector).

The warehouse is yours — explore it!

---

### **How to Check Your Work**

1. **Run the script** – you should see five neatly formatted tables.
2. **Verify results manually** – pick one symbol and check a few numbers. For instance, run `SELECT * FROM fact_stock_prices WHERE company_id = ...` and confirm that the averages you see match.
3. **Test with no data** – If you run `analysis.py` before running `pipeline.py` today, queries using `CURRENT_DATE` might return empty results (since the most recent data is from yesterday). That’s fine — the script handles it gracefully.

Congratulations! You’ve gone from raw API data all the way to actionable business insights, all automated and optimized. You’ve built a complete, production‑grade data engineering project.



---

## Quick Reset (Start Over)

```bash
# Drop and recreate the RDS database
psql -h YOUR-RDS-ENDPOINT -U postgres
DROP DATABASE stock_dw;
CREATE DATABASE stock_dw;
\q

# Clear S3 bucket contents (AWS CLI)
aws s3 rm s3://your-bucket-name --recursive

# Re-run setup
python3 setup_rds.py
python3 setup_dimensions.py
python3 pipeline.py
```

---

## Cheat Sheet

|Concept|One-line summary|
|---|---|
|S3 raw/|Your data lake — stores original API responses untouched|
|S3 processed/|Clean, flattened CSV ready to load into the warehouse|
|dim_date|Every possible calendar date 2020–2030, pre-generated|
|dim_company|One row per stock ticker — name, sector, exchange|
|fact_stock_prices|One row per company per trading day — OHLCV + metrics|
|IAM role on EC2|Grants S3 access without any hardcoded AWS credentials|
|`outputsize=compact`|Alpha Vantage returns last 100 trading days — enough for analytics|
|`time.sleep(15)`|Rate limit guard — Alpha Vantage free tier caps at 5 calls/min|
|`ON CONFLICT DO NOTHING`|Idempotent insert — safe to re-run without duplicate dates|
|EXPLAIN ANALYZE|Prints query execution plan — confirms indexes are being used|
|Incremental loading|Query `MAX(date)` from warehouse first; only load records newer than that — saves compute on every run after the first|
|Watermark|The `MAX(date)` value marking the boundary between already-loaded and new data — the core of any incremental pipeline|
|Full load vs incremental|Full load: process everything every run. Incremental: `WHERE date > last_loaded_date`. Always prefer incremental at scale.|

---

_Stack: Python 3 · PostgreSQL 15 · Amazon RDS · Amazon S3 · Amazon EC2 · boto3 · SQLAlchemy · pandas · Alpha Vantage API_



# Stock Market DWH — Deployment Addendum

### Sections 18–21: Orchestration with Airflow, a Public Dashboard with Streamlit

This picks up exactly where the original guide left off (Section 17). It adds two pieces to the CV project:

1. **Apache Airflow** replaces the cron + `pipeline.py` orchestrator with real scheduling, per-task retries, and a run history you can screenshot for an interview.
2. **Streamlit**, deployed on **Streamlit Community Cloud** (free), turns `analysis.py` into a public, filterable dashboard you can link from a CV.

**Two architecture decisions made up front, so you can course-correct if you'd rather do it differently:**

- Airflow runs **on the same EC2 instance** you already have, in its own virtual environment, using `airflow standalone`. No new AWS resources, no new bill.
- The dashboard deploys to **Streamlit Community Cloud**, not self-hosted on EC2, because a `your-project.streamlit.app` link is a much better thing to put on a CV than an EC2 IP address. This does mean opening RDS to the public internet — Section 19, Step 5 explains exactly why that's the standard approach and how to do it without handing anyone your data.

### Updated architecture

```
Alpha Vantage API
       │
       ▼
┌───────────────────────────────────────────────────┐
│  EC2 (same instance as before)                     │
│                                                     │
│  Airflow — systemd service, always on              │
│    "airflow standalone" (scheduler+UI+triggerer)   │
│    triggers: extract → transform → load            │
│    Mon–Fri 18:00 UTC (replaces the old crontab)    │
│                                                     │
│  ~/stock_dwh/venv/  — ETL scripts, UNCHANGED       │
│    extract.py → transform.py → load.py             │
└──────────────────────┬──────────────────────────────┘
                        │ writes to
                        ▼
              S3 raw/  →  S3 processed/
                        │
                        ▼
              RDS PostgreSQL (Star Schema)
                 │                    │
                 │ postgres (admin)   │ dashboard_reader (SELECT only)
                 ▼                    ▼
         analysis.py (CLI,     Streamlit Community Cloud
          unchanged)            dashboard.py → public URL
```

### ⚠️ Quick gut-check on AWS costs before you spin anything else up

AWS changed its Free Tier on July 15, 2025. If your account predates that, you're on the legacy model: 750 hrs/month of EC2 + RDS micro instances, good for 12 months from account creation — running Airflow on the _same_ EC2 box costs nothing extra, since it's the same instance-hours the pipeline is already using. If your account is newer, you're on the credit system instead (roughly $100–200 in credits with a hard 6-month expiry, regardless of remaining balance). Either way, nothing in this addendum spins up new billable AWS resources — same EC2 box, same RDS instance — but it's worth knowing which clock you're on.

---

# 16. Orchestration with Apache Airflow

**Objective:** Replace `cron` + `pipeline.py` with an Airflow DAG that runs the same three scripts, unchanged, but gains automatic retries, a visual run history, and the ability to re-run just the failed step instead of the whole pipeline.

### Why bother — cron already works

It does, until it doesn't. With cron, if `extract.py` times out at 6:01pm because Alpha Vantage had a slow minute, the whole pipeline dies silently and you find out the next morning when the dashboard looks stale — if you check. Airflow gives you:

- **Per-task retries** — a flaky API call gets retried automatically instead of failing the whole run
- **A DAG graph** — see at a glance which of extract/transform/load succeeded, failed, or is still running
- **Selective re-runs** — if only `load` fails, re-run just `load`, not `extract` again (which would burn API calls you don't have to spare on the free tier)
- **History** — every run, logged, timestamped, searchable

### ⚠️ Airflow 3.x heads-up

Airflow crossed a major version boundary (3.0) that changes a few things you'll see if you cross-reference other tutorials online. As of this writing, current stable is **3.3.0**. The two changes that matter here:

- `BashOperator` and `PythonOperator` no longer ship inside core `apache-airflow` — they live in a separate `apache-airflow-providers-standard` package. Install with the `[standard]` extra or you'll get a working Airflow with nothing to actually run tasks with.
- The import path changed: `from airflow.providers.standard.operators.bash import BashOperator`, not the old `from airflow.operators.bash import BashOperator`.

Older Airflow 2.x tutorials you find elsewhere will use the old import — that's not wrong for 2.x, just outdated for what you're installing here.

### Step 0 (recommended): give the instance some breathing room

Airflow's own docs recommend more memory than a free-tier `t2.micro`/`t3.micro` (1 GB RAM) has to offer. Rather than upgrading the instance size (and leaving free tier), add 2 GB of swap — it costs nothing and is the difference between a scheduler that survives a bad night and one that gets OOM-killed at 2am:

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile swap swap defaults 0 0' | sudo tee -a /etc/fstab
free -h   # confirm the swap row now shows 2.0G
```

### Step 1: Install Airflow in its own virtual environment

Keep this **separate** from `~/stock_dwh/venv`. Airflow pins tight dependency ranges via a constraints file that can collide with the pandas/boto3/SQLAlchemy versions your ETL scripts already rely on — isolating them means an Airflow upgrade can never break `extract.py`, and vice versa.

```bash
mkdir -p ~/airflow && cd ~/airflow
python3 -m venv venv
source venv/bin/activate
export AIRFLOW_HOME=~/airflow

# Always grabs the current stable version and matches it to your Python version automatically
AIRFLOW_VERSION=$(curl -s https://pypi.org/pypi/apache-airflow/json | python3 -c "import json,sys;print(json.load(sys.stdin)['info']['version'])")
PYTHON_VERSION="$(python3 --version | cut -d ' ' -f2 | cut -d '.' -f1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[standard]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

### Step 2: Add the DAG

```bash
mkdir -p ~/airflow/dags
```

Copy `stock_dwh_dag.py` (attached alongside this guide) into `~/airflow/dags/stock_dwh_dag.py`. The important bit of the file:

```python
PROJECT_DIR = "/home/ubuntu/stock_dwh"
PYTHON_BIN = f"{PROJECT_DIR}/venv/bin/python3"

extract = BashOperator(
    task_id="extract",
    bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} extract.py",
)
```

Notice this calls your **existing scripts, unchanged**, using the exact same "spawn a subprocess with the right Python" pattern `pipeline.py` already used with `subprocess.run([sys.executable, script])` — Airflow's `BashOperator` is doing conceptually the same thing, just now each call is a monitored, retriable _task_ instead of one step in a linear script. Calling the venv's `python3` binary directly (instead of `source venv/bin/activate`) is deliberate — `source` doesn't reliably work inside the non-interactive shell Airflow spawns for each task.

> If your EC2 user isn't `ubuntu` (e.g. `ec2-user` on Amazon Linux), update `PROJECT_DIR` to match.

### stock_dwh_dag.py

```python
"""
stock_dwh_dag.py

Airflow 3.x DAG for the Stock Market Analytics Warehouse.
Orchestrates the existing extract.py -> transform.py -> load.py scripts,
UNCHANGED, via BashOperator -- same idea as pipeline.py's
subprocess.run([sys.executable, script]) pattern, now with per-task
retries, a run history, and a UI.

Place this file in ~/airflow/dags/stock_dwh_dag.py
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Adjust if your EC2 user or project path differ (e.g. Amazon Linux uses
# /home/ec2-user instead of /home/ubuntu).
PROJECT_DIR = "/home/ubuntu/stock_dwh"
PYTHON_BIN = f"{PROJECT_DIR}/venv/bin/python3"  # call the venv's python directly;
                                                  # `source venv/bin/activate` doesn't
                                                  # reliably work in Airflow's non-interactive shell

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_dwh_pipeline",
    description="Daily ETL: Alpha Vantage API -> S3 -> RDS Star Schema",
    default_args=default_args,
    schedule="0 18 * * 1-5",   # 6pm UTC, weekdays -- same schedule as the crontab entry it replaces
    start_date=datetime(2026, 1, 1),
    catchup=False,             # don't backfill months of runs against a 25-request/day API key
    max_active_runs=1,         # never let two days' runs overlap against the same RDS/S3 targets
    tags=["stock-dwh", "etl"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} extract.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} transform.py",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} load.py",
    )

    extract >> transform >> load
```


### Exercise: Build the DAG Yourself

`stock_dwh_dag.py` from this guide is your answer key. You'll get much more out of this if you write it from a blank file first and only check it against the finished version afterward.

**Objective:** Write an Airflow DAG that runs `extract.py`, `transform.py`, and `load.py` in order, unchanged, on the same schedule as the old crontab entry, with automatic retries.

#### Task 1: Imports and Path Constants

Objective: Bring in what the DAG needs and define where your project actually lives.

Instruction 1: Import `datetime` and `timedelta` from the `datetime` module. Instruction 2: Import `DAG` — but not from the place a pre-2026 tutorial would tell you to. Instruction 3: Import `BashOperator` — same caveat. Instruction 4: Define a constant `PROJECT_DIR` pointing at `/home/ubuntu/stock_dwh` (or wherever yours lives). Instruction 5: Define a constant `PYTHON_BIN`, built from `PROJECT_DIR`, that points at the _venv's_ `python3` binary specifically — not the system one.

💡 Hints for Task 1:

- As of Airflow 3.0, `DAG` lives in `airflow.sdk`, and `BashOperator` lives in a separately-installed package: `airflow.providers.standard.operators.bash`. The old `from airflow import DAG` / `from airflow.operators.bash import BashOperator` paths are Airflow 2.x.
- `PYTHON_BIN`: an f-string, `f"{PROJECT_DIR}/venv/bin/python3"`. Pointing directly at the venv's interpreter is what lets a task run with all your project's installed packages _without_ needing to `source activate` first — which doesn't reliably work in the non-interactive shell Airflow spawns per task.

#### Task 2: Retry Behavior

Objective: Define how the DAG should react when a task fails.

**Instruction 1:** Create a dictionary named `default_args`. 
**Instruction 2:** Give it an `"owner"` key with any string value. 
**Instruction 3:** Give it a `"retries"` key — how many times should a failed task retry before giving up? 
**Instruction 4:** Give it a `"retry_delay"` key, using `timedelta`, for how long Airflow should wait between attempts.

💡 Hints for Task 2:

- `"retries": 2` is reasonable for a script calling a rate-limited external API — enough to survive a transient timeout, not so many that a genuinely broken script retries forever.
- `timedelta(minutes=5)` — five minutes gives a flaky API time to recover without making you wait an hour to find out the pipeline is actually broken.

#### Task 3: Defining the DAG

Objective: Use the `DAG` class as a context manager to declare the pipeline's identity and schedule.

**Instruction 1:** Open a `with DAG(...) as dag:` block. 
**Instruction 2:** Give it a `dag_id` of `"stock_dwh_pipeline"`. 
**Instruction 3:** Pass in the `default_args` dictionary from Task 2. 
**Instruction 4:** Set `schedule` to a cron string matching the old crontab entry — 6pm UTC, Monday through Friday. 
**Instruction 5:** Set `start_date` to a fixed date in the past.
**Instruction 6:** Set `catchup=False`. 
**Instruction 7:** Set `max_active_runs=1`.

💡 Hints for Task 3:

- Cron string: `"0 18 * * 1-5"` — minute 0, hour 18, any day-of-month, any month, weekdays 1–5.
- `catchup=False` is not optional here: leave it out (or set it `True`) and the moment you unpause this DAG, Airflow will try to backfill every scheduled run between `start_date` and today — against a 25-request/day API key that has no idea it's about to be asked for months of history at once.
- `max_active_runs=1` stops a slow Tuesday run from still being in progress when Wednesday's scheduled run fires.

#### Task 4: The Three Tasks

Objective: Create one `BashOperator` per script.

**Instruction 1:** Create a `BashOperator` with `task_id="extract"`. 
**Instruction 2:** Its `bash_command` should `cd` into `PROJECT_DIR`, then run `PYTHON_BIN` against `extract.py`. 
**Instruction 3:** Repeat for `transform.py` and `load.py`.

💡 Hints for Task 4:

- `bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} extract.py"` — the `cd` matters: your scripts call `load_dotenv()` with no arguments, which looks for `.env` in the current directory, so the task's working directory has to actually be your project folder.

#### Task 5: Setting the Order

Objective: Tell Airflow these three tasks aren't independent — they must run in sequence.

Instruction 1: Below the three `BashOperator` definitions (still inside the `with DAG(...)` block), chain them so `extract` runs first, then `transform`, then `load`.

💡 Hints for Task 5:

- `extract >> transform >> load` — Airflow overloads `>>` to mean "set as downstream of." Read it left to right exactly as written: extract, then transform, then load.

**How to check your work:**

1. Save the file as `~/airflow/dags/stock_dwh_dag.py`.
2. Run `airflow dags list-import-errors` — an empty result means it parsed cleanly. If your DAG doesn't show up in the UI at all, this is the first thing to check.
3. In the Airflow UI, find `stock_dwh_pipeline` in the DAG list and unpause it.
4. Sanity-check it without waiting for the schedule: `airflow dags test stock_dwh_pipeline 2026-07-08` — runs all three tasks locally, in order, and prints their output right in your terminal.
5. Trigger a real run from the UI (▶) and confirm the Graph view shows extract → transform → load turning green in sequence, not all at once.


### Step 3: Run Airflow as a systemd service

You want this running before you SSH in and after you log out — a systemd service handles both, plus auto-restart if it ever crashes.

```ini
# /etc/systemd/system/airflow-standalone.service
[Unit]
Description=Airflow standalone (webserver + scheduler + triggerer)
After=network.target

[Service]
User=ubuntu
Environment="AIRFLOW_HOME=/home/ubuntu/airflow"
Environment="PATH=/home/ubuntu/airflow/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
WorkingDirectory=/home/ubuntu/airflow
ExecStart=/home/ubuntu/airflow/venv/bin/airflow standalone
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-standalone
sudo systemctl status airflow-standalone
```

On first boot, Airflow generates an admin login and prints it to the log:

```bash
sudo journalctl -u airflow-standalone | grep -A2 -i "password"
```

Save that username/password — you'll use it to log into the UI.

### Step 4: Open the UI — carefully

Same security group you already have, one more rule:

1. EC2 → your instance → Security tab → click the security group
2. Inbound rules → Edit inbound rules → Add rule
3. Type: Custom TCP · Port: `8080` · Source: **My IP** (not `0.0.0.0/0` — this is a login screen into your infrastructure, not a public asset)
4. Visit `http://YOUR-EC2-IP:8080`

or you can **Forward the port in VS Code**

5. Look at the bottom panel in VS Code (the same area where your Terminal is).
6. Click on the **PORTS** tab. _(If you don't see it, look for an icon that looks like a plug, or go to the top menu: View → Ports)._
7. Click the **Forward a Port** button.
8. Type `8080` and press **Enter**.
9. VS Code will add `localhost:8080` to the list.

### Concept Deep Dive — what you actually gained

**`catchup=False`** — with a `start_date` in the past, Airflow's default instinct is to backfill _every_ scheduled interval between `start_date` and now. `catchup=False` turns that off. Skip this and the moment you unpause the DAG, Airflow tries to run months of backfilled extracts against your 25-request/day Alpha Vantage key — not a mistake you want to make twice.

**`max_active_runs=1`** — guards against a slow Tuesday run still executing when Wednesday's scheduled run fires, which would otherwise hit RDS and S3 concurrently for no benefit.

**`retries=2, retry_delay=5 min`** — if `extract.py` fails on a transient Alpha Vantage timeout, Airflow retries it twice, five minutes apart, automatically. The old `pipeline.py` gave up on the first failure and needed a human to notice and re-run it.

### How to check your work

- The Airflow UI shows a `stock_dwh_pipeline` DAG — unpause it (toggle on the left)
- Trigger a manual run (▶ button) and watch `extract → transform → load` turn green in sequence in the Graph view
- Deliberately break something once (rename `.env` for a minute) to see a task go red, retry automatically, and only _then_ fail — confirms the retry logic is real, not just configured

---

# 17. A Public Dashboard with Streamlit

**Objective:** Turn `analysis.py`'s five SQL queries into an interactive, filterable dashboard, deployed for free with a public URL.

### Why not just keep using `analysis.py`?

`analysis.py` is great at a terminal. It's not something you can put a link to on a CV or send a recruiter. A deployed Streamlit app is visual, filterable by whoever's looking at it, and — on Streamlit Community Cloud — free, with a real `your-project.streamlit.app` URL.

### Step 1: Create a least-privilege database user first

The dashboard is about to talk to your database from the public internet. Before writing a line of dashboard code, give it a user that can only ever `SELECT`, never touch schema or data:

```sql
-- Connect as postgres (psql -h YOUR-RDS-ENDPOINT -U postgres -d stock_dw), then:
CREATE USER dashboard_reader WITH PASSWORD 'choose-a-strong-password-here';
GRANT CONNECT ON DATABASE stock_dw TO dashboard_reader;
GRANT USAGE ON SCHEMA public TO dashboard_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dashboard_reader;
```

This is the credential that ends up in a cloud secrets box outside your VPC — it should never be the `postgres` superuser password.

```SQL
CREATE USER dashboard_reader WITH PASSWORD '...';
-- Creates a brand new login for your dashboard. 
GRANT CONNECT ON DATABASE stock_dw TO dashboard_reader;
-- By default, new users can't even open the door to the database. This gives them the key to enter the stock_dw building.
GRANT USAGE ON SCHEMA public TO dashboard_reader;
-- Inside the database, tables are organized into "schemas" (folders). This allows the user to see inside the public folder.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_reader;
-- This is the most important line. It gives the user SELECT (read-only) access to all the tables that currently exist. They cannot INSERT, UPDATE, or DELETE data.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dashboard_reader;
-- This is a pro-tip line. If your Airflow pipeline creates a new table next month, this rule automatically gives the dashboard read access to it. Without this line, the dashboard would crash the next time you add a new table to your pipeline.
```
### Step 2: Build and test `dashboard.py` locally

The full file is attached alongside this guide (`dashboard.py`). It uses Streamlit's built-in `st.connection`, which is the current recommended way to talk to SQL databases from Streamlit — it reads connection details from a secrets file, pools the connection, and caches query results for you via a `ttl` argument, so you don't hand-roll `st.cache_data` + a manual SQLAlchemy engine the way an older tutorial might show.

Create a **local-only** secrets file (never commit this):

```toml
# .streamlit/secrets.toml
[connections.postgresql]
dialect = "postgresql"
host = "your-rds-endpoint.rds.amazonaws.com"
port = "5432"
database = "stock_dw"
username = "dashboard_reader"
password = "your-strong-password"
query = { sslmode = "require" }
```

Test it runs:

```bash
pip install streamlit plotly sqlalchemy psycopg2-binary --break-system-packages
streamlit run dashboard.py
```


---

# 18. Building the Dashboard with Streamlit & `st.connection`

**Full code you will build:**

```python
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

# st.connection reads [connections.postgresql] from secrets.toml, pools the
# connection, and caches query results via the ttl= argument on .query().
conn = st.connection("postgresql", type="sql")

# A palette grounded in the subject (ticker-tape gold / navy) rather than a
# generic dark-dashboard default -- see config.toml in the deployment guide.
PALETTE = ["#D4AF37", "#2DD4BF", "#EF6461", "#6699CC", "#9B7EDE", "#4C9F70"]

st.title("📈 Stock Market Analytics Warehouse")
st.caption("AWS EC2 → S3 → RDS Star Schema, orchestrated by Airflow · refreshed daily")

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
#symbol_list = "', '".join(selected)

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

st.subheader("Closing price trend")
if prices.empty:
    st.info("No data yet for this selection — has the pipeline run yet?")
else:
    fig = px.line(
        prices, x="date", y="close_price", color="symbol",
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
```

---

### Exercise: Build the Interactive Dashboard Step by Step

**Objective:** Create a professional Streamlit dashboard that connects to your RDS data warehouse, gives users interactive filters, and displays KPIs, time‑series charts, and data tables – all without a single `.env` file or raw `create_engine`.

---

#### Task 1: Setup and Page Configuration

- Import `streamlit as st` and `plotly.express as px`.  
- Call `st.set_page_config()` with a page title, an emoji icon (`📈`), and `layout="wide"`.  
- Write a title and a caption describing the data source and refresh schedule.

```python
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Stock Market DWH", page_icon="📈", layout="wide")

st.title("📈 Stock Market Analytics Warehouse")
st.caption("AWS EC2 → S3 → RDS Star Schema, orchestrated by Airflow · refreshed daily")
```

> **💡 Hint:** The wide layout gives your charts more horizontal space. The caption is a thin, gray subtitle.

---

#### Task 2: Connect to PostgreSQL Using `st.connection`

Replace the old `create_engine` + `load_dotenv` pattern with one line:

```python
conn = st.connection("postgresql", type="sql")
```

This reads credentials from `[connections.postgresql]` in your `secrets.toml` file. When you deploy to Streamlit Cloud, the same configuration is provided via the Secrets UI.

> **💡 Hint:** `st.connection` automatically manages a connection pool and returns a `sqlalchemy.engine.Engine` behind the scenes. When you call `.query()`, it opens a connection, runs the SQL, and closes the connection – all transparently.

---

#### Task 3: Load the Company List

Use `conn.query()` to fetch all symbols, company names, and sectors from `dim_company`. Add a `ttl` (time‑to‑live) of `"1h"` to cache this list for one hour. Store the result in `companies`.

```python
companies = conn.query(
    "SELECT symbol, company_name, sector FROM dim_company ORDER BY symbol",
    ttl="1h",
)
```

**Why a TTL?**  
The company list rarely changes. Caching it for an hour saves unnecessary database hits. If you add a new stock, it will appear after the cache expires or when you manually clear it.

---

#### Task 4: Build the Sidebar with a Multiselect

- Create a sidebar header "Filters".  
- Convert the `symbol` column to a plain Python list: `all_symbols = companies["symbol"].tolist()`.  
- Define a default selection of the first 5 symbols (or all if fewer than 5).  
- Use `st.sidebar.multiselect()` to let the user pick one or more stocks.

```python
st.sidebar.header("Filters")
all_symbols = companies["symbol"].tolist()
default_selection = all_symbols[:5] if len(all_symbols) >= 5 else all_symbols
selected = st.sidebar.multiselect("Symbols", all_symbols, default=default_selection)
```

- Add a guard clause: if the user clears the selection, show a warning and stop the script with `st.stop()`.

```python
if not selected:
    st.warning("Pick at least one symbol from the sidebar to see data.")
    st.stop()
```

> **💡 Hint:** `st.stop()` halts execution of the script below it, so nothing else is rendered. This prevents SQL errors from empty selections.

---

#### Task 5: Load Filtered Price Data

- Convert the selected symbols into a SQL‑safe comma‑separated list for an `IN` clause. Because the values come from the database itself, not from free text, this is safe:  
  `symbol_list = "', '".join(selected)`
- Write a dynamic SQL query that joins `fact_stock_prices` with `dim_company` and `dim_date`, filters on the selected symbols, and orders by date.  
- Execute with `conn.query()` using a `ttl="1h"` cache. Store the result in `prices`.

```python
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
```

> **💡 Hint:** The `f"""..."""` f‑string interpolates the symbol list directly. Again, since the possible values are controlled by the company list, this is safe from injection. For user‑typed input, you’d always use parameterized queries.

---

#### Task 6: Display KPI Metrics in Columns

- Create three columns with `col1, col2, col3 = st.columns(3)`.  
- In `col1`, show the number of selected symbols with `st.metric("Symbols selected", len(selected))`.  
- In `col2`, show the total number of price rows loaded, formatted with a thousands separator: `f"{len(prices):,}"`.  
- In `col3`, show the latest trading day from the `date` column. If `prices` is empty, show an em dash (`—`).

```python
col1, col2, col3 = st.columns(3)
col1.metric("Symbols selected", len(selected))
col2.metric("Rows loaded", f"{len(prices):,}")
col3.metric("Latest trading day", str(prices["date"].max()) if not prices.empty else "—")
```

---

#### Task 7: Plot the Closing Price Trend

- Add a subheader "Closing price trend".  
- Check if `prices` is empty. If so, show an informational message.  
- If not, create a Plotly Express line chart:  
  `px.line(prices, x="date", y="close_price", color="symbol", color_discrete_sequence=PALETTE)`  
  (Define `PALETTE` as a list of hex colors near the top of the script.)
- Clean up the chart margins with `fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))`.  
- Render with `st.plotly_chart(fig, use_container_width=True)`.

```python
PALETTE = ["#D4AF37", "#2DD4BF", "#EF6461", "#6699CC", "#9B7EDE", "#4C9F70"]

st.subheader("Closing price trend")
if prices.empty:
    st.info("No data yet for this selection — has the pipeline run yet?")
else:
    fig = px.line(
        prices, x="date", y="close_price", color="symbol",
        color_discrete_sequence=PALETTE,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)
```

> **💡 Hint:** `use_container_width=True` makes the chart fill the column width, giving it a modern, responsive feel.

---

#### Task 8: Show Sector Performance This Year

- Query the sector‑level average daily price change and total volume (in millions) for the current year.  
- Use `ROUND()` and `EXTRACT(YEAR FROM CURRENT_DATE)` directly in SQL.  
- Cache the result for 1 hour.  
- If the result is not empty, create a bar chart with `px.bar()`. Use the first color from your palette as a single color.  
- Again, update layout margins and render with `st.plotly_chart()`.

```python
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
```

---

#### Task 9: Display Most Volatile Stocks as a Table

- Write a query that groups by symbol and sector, computes the average and maximum daily range, and orders by average range descending.  
- Display the result using `st.dataframe()` with `use_container_width=True` and `hide_index=True`.

```python
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
```

---

#### Task 10: Add Final Caption and Run

- End the script with a caption explaining the data refresh frequency and caching.  
- Save the file and test it locally: `streamlit run dashboard.py` (ensure `.streamlit/secrets.toml` exists with your RDS credentials).  
- Commit to GitHub (without the secrets file) and deploy to Streamlit Community Cloud.

```python
st.caption("Data refreshes daily via the Airflow-orchestrated ETL pipeline · cached up to 1 hour.")
```

---

### Concept Deep Dive: How `st.connection` Streamlines Everything

- **No explicit engine management:** `st.connection` creates the SQLAlchemy engine internally using the `secrets.toml` configuration. You no longer need `create_engine` or `load_dotenv`.  
- **Built‑in caching:** The `ttl` argument on `.query()` automatically stores the DataFrame in Streamlit’s cache. When the same query is executed again before the TTL expires, the cached result is returned instantly – no database round‑trip.  
- **Automatic reconnection:** If the database connection drops (e.g., after a period of inactivity), `st.connection` automatically reconnects.  
- **Separation of configuration from code:** All credentials live in `secrets.toml` (local) or Streamlit Cloud Secrets (production). This makes your app more secure and easier to deploy.

### Why This Dashboard Is Production‑Ready

- **Performance:** Queries are cached for an hour, so returning visitors see pages instantly. The database is only hit when the cache expires or a different symbol selection is made (which changes the query).  
- **Security:** The app connects to RDS with a read‑only user if you create one. The secrets are never exposed to the frontend.  
- **Maintainability:** The entire dashboard is one script, easy to version control. Changes pushed to GitHub automatically redeploy on Streamlit Cloud (if you enable auto‑redeploy).

### How to Check Your Work

1. Run `streamlit run dashboard.py` locally. You should see all three sections populated with data from your RDS warehouse.  
2. Change the symbol selection in the sidebar – the charts and KPIs should update immediately.  
3. Check the Streamlit Cloud deployment by visiting your public URL. It should look identical to the local version.

---

Congratulations! You now understand every piece of a modern, cloud‑native Streamlit dashboard built on top of your data warehouse. This is the final visual layer of your entire data engineering project – from raw API to interactive analytics.
### Step 3: Push to GitHub

Streamlit Community Cloud deploys from a GitHub repo. A clean layout keeps this in the same repo as the ETL project, so one link covers the whole CV project:

```
stock_dwh/
├── ... (extract.py, transform.py, load.py — unchanged)
└── dashboard/
    ├── dashboard.py
    └── requirements.txt
```

Add to `.gitignore` (alongside the existing `.env` entry):

```
.streamlit/secrets.toml
```

```bash
git add dashboard/
git commit -m "Add Streamlit dashboard"
git push
```

### Step 4: Deploy on Streamlit Community Cloud

1. Go to **share.streamlit.io** → sign in with GitHub → **New app**
2. Pick your repo and branch; set **main file path** to `dashboard/dashboard.py`
3. Before deploying, open **Advanced settings → Secrets** and paste the same TOML from Step 2 (with the real endpoint and password — this box is the Community Cloud equivalent of your EC2 `.env` file, and it's the only copy that matters once deployed)
4. Click **Deploy** — you'll have a live `your-app-name.streamlit.app` URL within a few minutes

### Step 5: Open RDS to the dashboard — the trade-off, stated plainly

Streamlit Community Cloud does not publish a fixed, allowlist-able IP range — this has been asked about repeatedly in their own community forum, and their old "stable outbound IP" docs page has been retired without replacement. In practice, this is exactly why almost every public tutorial connecting Streamlit Cloud to Postgres, Cloud SQL, or Azure SQL ends up doing the same thing:

**Option A — open the port (what this guide recommends for a portfolio project):** RDS → Security group → Inbound rules → Add rule → PostgreSQL, port `5432`, source `0.0.0.0/0`.

This is reasonable _specifically because_ you paired it with three things already in place: a read-only user (Step 1), a password that only lives in `secrets.toml`/Community Cloud's secrets box and never in git, and `sslmode=require` enforcing an encrypted connection. It would not be reasonable if this were the `postgres` superuser.

**Option B — keep RDS private, self-host the dashboard on the EC2 instance instead.** Run `streamlit run dashboard.py --server.port 8501` under its own systemd service, open port 8501 to "My IP" only (like the Airflow UI), and connect to RDS the same way your ETL scripts already do — no internet-facing database at all. Trade-off: the link is `http://your-ec2-ip:8501`, not a polished public URL, and it's only reachable from wherever you allow. If you want zero open inbound ports at all, a tool like Cloudflare Tunnel can front either the dashboard or the Airflow UI without opening the security group — worth a look if this dashboard is heading anywhere more serious than a CV link.

### Step 6: A theme that doesn't look like a template

Streamlit's theming is limited to a handful of color slots plus a font family — but even within that, "dark background, default blue" is the generic choice every dashboard tutorial reaches for. Since the subject here is literally stock market data, a palette that nods to a ticker tape (deep navy, warm gold) reads more intentional than another neon-on-black dashboard, and it stays out of the way of the red/green price-move colors your charts will already be using:

```toml
# .streamlit/config.toml
[theme]
base = "dark"
primaryColor = "#D4AF37"
backgroundColor = "#0B1220"
secondaryBackgroundColor = "#141B2D"
textColor = "#E8EAF0"
font = "sans serif"
```

### Concept Deep Dive

**`st.connection("postgresql", type="sql")`** reads the `[connections.postgresql]` block from `secrets.toml`, builds and pools a SQLAlchemy engine behind the scenes, and gives you `.query(sql, ttl="1h")` — one call that both runs the query and caches the result. This is the current idiomatic pattern; older Streamlit code you'll find elsewhere manually builds an engine and wraps `pandas.read_sql` in `st.cache_data` to get the same effect.

**`secrets.toml` vs `.env`** — same purpose (keep credentials out of git), different mechanism, because a Streamlit Community Cloud container never sees your EC2's `.env` file. You end up maintaining the same values in two places on purpose: a local `secrets.toml` for testing, and Community Cloud's Secrets box for the deployed app.

**Why `ttl="1h"` matters here specifically** — if this dashboard's link ends up on a CV or LinkedIn post, you don't control how many people load it in the same hour. Caching means fifty simultaneous viewers hit RDS once, not fifty times, which is the difference between "fine" and "noticeable" on a `db.t3.micro`.

### How to check your work

- `streamlit run dashboard.py` locally shows real data and the filters work
- The deployed URL loads in an incognito window (confirms it's genuinely public, not just working because you're signed into GitHub)
- Load it on a phone over cellular data, not wifi — rules out any lingering VPC-only assumption
- Wait for tomorrow's Airflow-triggered run, then reload (respecting the 1-hour cache) and confirm the new trading day's row shows up

---

# 19. Updated Full Execution Order

```
ONE-TIME SETUP
──────────────────────────────────────────
STEP 1 → python3 setup_rds.py
STEP 2 → python3 setup_dimensions.py
STEP 3 → Install + configure Airflow                    (Section 18, Steps 0–3)
STEP 4 → Create dashboard_reader DB user                (Section 19, Step 1)
STEP 5 → Deploy dashboard.py to Streamlit Community Cloud (Section 19, Steps 2–5)

DAILY PIPELINE — now fully automated, no manual trigger
──────────────────────────────────────────
Airflow scheduler → stock_dwh_pipeline DAG → extract → transform → load
  Mon–Fri, 18:00 UTC, with automatic retries
  (the old crontab entry from Section 16 is retired — see Section 18, Step 5)

ALWAYS ON
──────────────────────────────────────────
Airflow UI  → http://YOUR-EC2-IP:8080        (restricted to your IP)
Dashboard   → https://your-app.streamlit.app  (public)

OPTIMIZE / VALIDATE / ANALYZE — unchanged from Sections 13–15
──────────────────────────────────────────
analysis.py, EXPLAIN ANALYZE, and the integrity checks all still work exactly
as written. The dashboard doesn't replace them — it's a visual layer on the
same warehouse, reading through the same indexes you already built.
```

---

# 20. Updated CV Description

**One-line version**

Built a cloud-native stock market analytics pipeline on AWS (EC2 · S3 · RDS), orchestrated with Apache Airflow, ingesting live Alpha Vantage API data into a PostgreSQL Star Schema and surfacing it through a public Streamlit dashboard.

**Additional bullet points**

- Orchestrated the daily ETL pipeline with Apache Airflow (DAGs, per-task retries, run history), replacing a cron-triggered script with automatic failure recovery and visual monitoring
- Designed and deployed a public analytics dashboard in Streamlit, connected to the warehouse through a least-privilege database role over an SSL-enforced connection, hosted free on Streamlit Community Cloud
- Hardened the deployment with a dedicated read-only database role, enforced TLS on all external connections, and environment-specific secrets management (EC2 `.env` vs. Streamlit secrets)

**Updated tech stack for CV**

Python · PostgreSQL · Amazon RDS · Amazon S3 · Amazon EC2 · Apache Airflow · Streamlit · boto3 · SQLAlchemy · pandas · Plotly · REST APIs · ETL · Data Warehousing · Star Schema

---

# Addendum Cheat Sheet

|Concept|One-line summary|
|---|---|
|DAG|Airflow's word for "pipeline definition" — same idea as the scripts list in `pipeline.py`, plus dependency and retry metadata|
|BashOperator|An Airflow task that runs a shell command — used here to call your existing `.py` scripts unchanged|
|`apache-airflow-providers-standard`|Where `BashOperator`/`PythonOperator` actually live as of Airflow 3.0 — install with the `[standard]` extra|
|`airflow standalone`|One command that bootstraps the metadata DB, scheduler, and UI together — right-sized for one DAG on one box|
|`catchup=False`|Stops Airflow from backfilling every missed schedule between `start_date` and now|
|`st.connection`|Streamlit's built-in SQL connection helper — reads `secrets.toml`, pools connections, caches results via `ttl`|
|`secrets.toml`|Streamlit's equivalent of `.env` — local per-project, pasted into Community Cloud's Secrets box, never committed|
|Read-only DB role|A Postgres user granted `SELECT` only — what the public dashboard connects as, so a leaked password can't touch your data|
|systemd service|Keeps a long-running process (Airflow) alive across reboots and SSH logouts, restarting it automatically if it crashes|

### Going further (optional, not required for the CV version)

- **Alerts**: add `on_failure_callback` (Slack webhook or SMTP) to the DAG's `default_args` so a failed run pings you instead of waiting to be noticed in the UI
- **Tighter Airflow integration**: refactor `extract.py`/`transform.py`/`load.py` into functions and swap `BashOperator` for the TaskFlow API, so tasks can pass row counts to each other via XComs instead of only exit codes
- **CI/CD for the DAG**: a GitHub Actions workflow that `rsync`s `dags/*.py` to EC2 over SSH on every push to `main` — editing the DAG becomes `git push`, not `scp`
- **Zero open ports**: front the Airflow UI and/or the self-hosted dashboard option with Cloudflare Tunnel, so nothing needs an inbound security group rule at all