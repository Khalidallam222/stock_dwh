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