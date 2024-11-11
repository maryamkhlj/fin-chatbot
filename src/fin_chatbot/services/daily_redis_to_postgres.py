import os
import sys
import fcntl
from datetime import datetime, date
import logging
from dotenv import load_dotenv
from redis_manager import RedisManager
from postgres_manager import PostgresManager
import psycopg2

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lock file path
LOCK_FILE = '/tmp/daily_redis_to_postgres.lock'

def acquire_lock():
    global lock_file
    lock_file = open(LOCK_FILE, 'w')
    try:
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except IOError:
        return False

def release_lock():
    global lock_file
    fcntl.lockf(lock_file, fcntl.LOCK_UN)
    lock_file.close()
    os.unlink(LOCK_FILE)

def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        logger.info("Successfully connected to PostgreSQL")
        cur = conn.cursor()
        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        logger.info(f"PostgreSQL database version: {db_version[0]}")
        cur.close()
        conn.close()
    except (Exception, psycopg2.Error) as error:
        logger.error("Error while connecting to PostgreSQL", error)

def transfer_to_postgres(redis_manager, postgres_manager):
    logger.info("Transferring today's latest data from Redis to PostgreSQL...")
    symbols = redis_manager.get_all_symbols()
    today = date.today()
    transferred_count = 0

    for symbol in symbols:
        prices = redis_manager.get_stock_prices(symbol, limit=1)  # Get only the latest price
        if prices:
            price, timestamp = prices[0]
            price_date = datetime.fromtimestamp(timestamp).date()
            if price_date == today:
                try:
                    postgres_manager.batch_insert_stock_prices([(symbol, price_date, price)])
                    transferred_count += 1
                    logger.info(f"Transferred {symbol}: {price} on {price_date}")
                except Exception as e:
                    logger.error(f"Error inserting data for {symbol} on {price_date}: {e}")

    logger.info(f"Transferred {transferred_count} records to PostgreSQL")

def verify_postgres_data(postgres_manager):
    logger.info("Verifying data in PostgreSQL...")
    with postgres_manager.get_cursor(commit=False) as cursor:
        cursor.execute("SELECT COUNT(*) FROM stock_prices")
        count = cursor.fetchone()[0]
        logger.info(f"Total records in stock_prices table: {count}")

        cursor.execute("SELECT symbol, date, closing_price FROM stock_prices ORDER BY date DESC LIMIT 5")
        samples = cursor.fetchall()
        logger.info("Sample data from stock_prices table:")
        for sample in samples:
            logger.info(f"  {sample[0]}: {sample[2]} on {sample[1]}")

def main():
    if not acquire_lock():
        logger.info("Another instance is already running. Exiting.")
        sys.exit(0)

    try:
        test_postgres_connection()

        redis_manager = RedisManager()
        postgres_manager = PostgresManager()

        logger.info("Initializing database")
        postgres_manager.initialize_database()
        
        logger.info("Starting data transfer")
        transfer_to_postgres(redis_manager, postgres_manager)
        
        logger.info("Updating metrics")
        postgres_manager.update_all_metrics()
        
        logger.info("Verifying transferred data")
        verify_postgres_data(postgres_manager)
        
        logger.info("Daily transfer completed successfully")
    except Exception as e:
        logger.error(f"Failed to transfer data to PostgreSQL: {e}", exc_info=True)
    finally:
        if 'postgres_manager' in locals():
            postgres_manager.close_pool()
        release_lock()

if __name__ == "__main__":
    main()