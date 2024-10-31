import asyncio
import time
import logging
import os
import sys
from dotenv import load_dotenv
from redis_manager import RedisManager
from redis_processor import RedisProcessor
from prometheus_client import start_http_server, Counter, Gauge
from common_stocks import COMMON_STOCKS

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define Prometheus metrics
STOCKS_FETCHED = Counter('stocks_fetched_total', 'Number of stocks fetched')
STOCKS_FAILED = Counter('stocks_failed_total', 'Number of stocks failed to fetch')
STOCKS_STORED = Gauge('stocks_stored_total', 'Total number of stocks stored in Redis')

FETCH_ALL_STOCKS = os.getenv('FETCH_ALL_STOCKS', 'False').lower() == 'true'
METRICS_PORT = int(os.getenv('METRICS_PORT', 8000))
FETCH_INTERVAL_HOURS = int(os.getenv('FETCH_INTERVAL_HOURS', 4))

async def fetch_stocks(redis_processor):
    """Fetch stock data and store in Redis"""
    total, fetched, failed = await redis_processor.process_stocks_async(COMMON_STOCKS)
    stored = redis_processor.redis_manager.get_total_stored_stocks()
    
    # Update Prometheus metrics
    STOCKS_FETCHED.inc(fetched)
    STOCKS_FAILED.inc(failed)
    STOCKS_STORED.set(stored)
    
    logger.info(f"Fetch completed - Total: {total}, Fetched: {fetched}, Failed: {failed}, Stored: {stored}")
    return total, fetched, failed, stored

async def main():
    redis_manager = RedisManager()
    
    # Try to acquire lock
    if not redis_manager.acquire_lock('stock_fetcher'):
        logger.info("Another instance is already running. Exiting.")
        sys.exit(0)

    try:
        # Start Prometheus metrics server
        start_http_server(METRICS_PORT)
        logger.info(f"Metrics server started on port {METRICS_PORT}")

        redis_processor = RedisProcessor(redis_manager, verify_ssl=False)

        while True:
            start_time = time.time()
            logger.info(f"Starting scheduled fetch (interval: {FETCH_INTERVAL_HOURS}h)")

            try:
                await fetch_stocks(redis_processor)
                
                if FETCH_ALL_STOCKS:
                    batch = redis_processor.get_next_batch_of_stocks()
                    if batch:
                        await redis_processor.process_stocks_async(batch)

            except Exception as e:
                logger.error(f"Fetch error: {e}", exc_info=True)

            elapsed_time = time.time() - start_time
            sleep_time = max(0, FETCH_INTERVAL_HOURS * 3600 - elapsed_time)
            logger.info(f"Sleeping for {sleep_time/3600:.2f}h")
            await asyncio.sleep(sleep_time)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        redis_manager.release_lock('stock_fetcher')

if __name__ == "__main__":
    asyncio.run(main())