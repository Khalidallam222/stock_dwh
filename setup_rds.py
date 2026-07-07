import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

RDS_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

engine = create_engine(RDS_URL)

schema_sql = """
    -- Dimension: Date
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id             SERIAL      PRIMARY KEY,
        date                DATE        UNIQUE      NOT NULL,
        year                INT                     NOT NULL,
        month               INT                     NOT NULL,
        month_name          VARCHAR(20)             NOT NULL,
        quarter             INT                     NOT NULL,
        quarter_name        VARCHAR(20)             NOT NULL,
        day_of_week         INT                     NOT NULL,
        day_of_week_name    VARCHAR(20)             NOT NULL,
        is_weekend          BOOLEAN                 NOT NULL
    
    );

    -- Dimension: Company
    CREATE TABLE IF NOT EXISTS dim_company (
        company_id          SERIAL      PRIMARY KEY,
        symbol              VARCHAR(10) UNIQUE NOT NULL,
        company_name        VARCHAR(200),
        sector              VARCHAR(100),
        industry            VARCHAR(100),
        exchange            VARCHAR(50),
        country             VARCHAR(50),
        currency            VARCHAR(10)
    
    );
    
    -- Fact: Daily Stock Prices
    CREATE TABLE IF NOT EXISTS fact_stock_prices (
        price_id            SERIAL      PRIMARY KEY,
        date_id             INT NOT NULL REFERENCES dim_date(date_id),
        company_id          INT NOT NULL REFERENCES dim_company(company_id),
        open_price          DECIMAL(10, 2),
        high_price          DECIMAL(10, 2),
        low_price           DECIMAL(10, 2),
        close_price         DECIMAL(10, 2),
        volume              BIGINT,
        price_change        DECIMAL(10, 2),
        daily_range        DECIMAL(10, 2),
        loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date_id, company_id)
    
    );
"""

with engine.begin() as conn:
    conn.execute(text(schema_sql))

print("✅ RDS schema created successfully.")
print("Tables created: dim_date, dim_company, fact_stock_prices")