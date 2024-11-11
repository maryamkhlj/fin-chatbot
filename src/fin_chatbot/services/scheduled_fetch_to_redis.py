import asyncio
import time
import logging
from datetime import datetime
import os
import sys
from dotenv import load_dotenv
from redis_manager import RedisManager
from redis_processor import RedisProcessor
from prometheus_client import start_http_server, Counter, Gauge
import fcntl

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define Prometheus metrics
STOCKS_FETCHED = Counter('stocks_fetched_total', 'Number of stocks fetched')
STOCKS_FAILED = Counter('stocks_failed_total', 'Number of stocks failed to fetch')
STOCKS_STORED = Gauge('stocks_stored_total', 'Total number of stocks stored in Redis')

LOCK_FILE = '/tmp/scheduled_fetch_to_redis.lock'
FETCH_ALL_STOCKS = os.getenv('FETCH_ALL_STOCKS', 'False').lower() == 'true'
METRICS_PORT = int(os.getenv('METRICS_PORT', 8000))
API_CALLS_PER_MINUTE = 60  # Finnhub free tier limit
FETCH_INTERVAL_HOURS = int(os.getenv('FETCH_INTERVAL_HOURS', 4))

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

async def fetch_stocks(redis_processor, max_calls):
    common_total, common_fetched, common_failed, common_stored = await redis_processor.process_common_stocks(max_calls=max_calls)
    
    if FETCH_ALL_STOCKS and max_calls > common_fetched + common_failed:
        batch = redis_processor.get_next_batch_of_stocks()
        all_total, all_fetched, all_failed = await redis_processor.process_stocks_async(
            batch,
            max_calls=max_calls - (common_fetched + common_failed)
        )
    else:
        all_total, all_fetched, all_failed = 0, 0, 0
    
    return common_total, common_fetched, common_failed, common_stored, all_total, all_fetched, all_failed

async def main():
    if not acquire_lock():
        logger.info("Another instance is already running. Exiting.")
        sys.exit(0)

    try:
        # Start Prometheus metrics server
        start_http_server(METRICS_PORT)
        logger.info(f"Starting metrics server on port {METRICS_PORT}")

        redis_manager = RedisManager()
        redis_processor = RedisProcessor(redis_manager, verify_ssl=False)

        while True:
            start_time = time.time()
            logger.info(f"Starting scheduled fetch (every {FETCH_INTERVAL_HOURS} hours)")

            api_calls_this_session = 0
            session_start_time = time.time()

            try:
                while api_calls_this_session < API_CALLS_PER_MINUTE * 60 * FETCH_INTERVAL_HOURS:
                    current_time = time.time()
                    if current_time - session_start_time >= 60:
                        api_calls_this_session = 0
                        session_start_time = current_time

                    common_total, common_fetched, common_failed, common_stored, all_total, all_fetched, all_failed = await fetch_stocks(
                        redis_processor, 
                        API_CALLS_PER_MINUTE - api_calls_this_session
                    )
                    
                    api_calls_this_session += common_fetched + common_failed + all_fetched + all_failed
                    all_stored = redis_manager.get_total_stored_stocks()

                    # Update Prometheus metrics
                    STOCKS_FETCHED.inc(common_fetched + all_fetched)
                    STOCKS_FAILED.inc(common_failed + all_failed)
                    STOCKS_STORED.set(all_stored)

                    logger.info("Fetch Summary:")
                    logger.info(f"Common Stocks - Total: {common_total}, Fetched: {common_fetched}, Failed: {common_failed}, Stored: {common_stored}")
                    if FETCH_ALL_STOCKS:
                        logger.info(f"All Stocks Batch - Total: {all_total}, Fetched: {all_fetched}, Failed: {all_failed}")
                    logger.info(f"Total stocks stored in Redis: {all_stored}")
                    logger.info(f"API calls this session: {api_calls_this_session}")

                    # Sleep for a minute before the next fetch attempt
                    await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"An error occurred during the fetch process: {e}", exc_info=True)

            # Calculate sleep time to respect the 4-hour interval
            elapsed_time = time.time() - start_time
            sleep_time = max(0, FETCH_INTERVAL_HOURS * 3600 - elapsed_time)
            logger.info(f"Sleeping for {sleep_time / 3600:.2f} hours until next fetch session")
            await asyncio.sleep(sleep_time)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        release_lock()

if __name__ == "__main__":
    asyncio.run(main())