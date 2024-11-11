import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

try:
    cur.execute("SELECT COUNT(*) FROM stock_prices")
    stock_prices_count = cur.fetchone()[0]
    print(f"Number of records in stock_prices: {stock_prices_count}")

    cur.execute("SELECT COUNT(*) FROM stock_metrics")
    stock_metrics_count = cur.fetchone()[0]
    print(f"Number of records in stock_metrics: {stock_metrics_count}")

    if stock_prices_count > 0:
        cur.execute("SELECT symbol, date, closing_price FROM stock_prices LIMIT 5")
        print("\nSample data from stock_prices:")
        for row in cur.fetchall():
            print(row)

    if stock_metrics_count > 0:
        cur.execute("SELECT symbol, date FROM stock_metrics LIMIT 5")
        print("\nSample data from stock_metrics:")
        for row in cur.fetchall():
            print(row)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    cur.close()
    conn.close()