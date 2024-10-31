import asyncio
import aiohttp
import logging
import os
from dotenv import load_dotenv
from redis_manager import RedisManager
from common_stocks import COMMON_STOCKS

load_dotenv()

logger = logging.getLogger(__name__)

class RedisProcessor:
    def __init__(self, redis_manager, verify_ssl=True):
        self.redis_manager = redis_manager
        self.api_key = os.getenv('FINNHUB_API_KEY')
        self.verify_ssl = verify_ssl
        self.semaphore = asyncio.Semaphore(30)  # Limit concurrent requests to 30
        self.max_retries = int(os.getenv('MAX_RETRIES', 5))
        self.batch_size = int(os.getenv('BATCH_SIZE', 50))
        self.max_calls_per_minute = int(os.getenv('MAX_CALLS_PER_MINUTE', 60))
        self.sleep_after_calls = int(os.getenv('SLEEP_AFTER_CALLS', 60))

    def get_ssl_context(self):
        if self.verify_ssl:
            return None
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    async def fetch_stock_data_async(self, session, symbol):
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={self.api_key}"
        logger.debug(f"Fetching data for symbol: {symbol}")  # Changed to debug level
        
        async with self.semaphore:
            try:
                async with session.get(url, ssl=self.get_ssl_context()) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = data.get('c')
                        if price is not None:
                            if self.redis_manager.set_stock_price(symbol, price):
                                logger.info(f"Successfully stored data for {symbol}: {price}")
                                return True
                    elif response.status == 429:
                        logger.warning(f"Rate limit hit for {symbol}")
                        await asyncio.sleep(self.sleep_after_calls)
                    else:
                        logger.error(f"Error fetching {symbol}: HTTP {response.status}")
                        return False
            except Exception as e:
                logger.error(f"Error fetching {symbol}: {str(e)}")
                return False
        return False

    async def process_stocks_async(self, stocks):
        total_stocks = len(stocks)
        fetched_count = 0
        failed_count = 0
        
        async with aiohttp.ClientSession() as session:
            for i, stock in enumerate(stocks, 1):
                if i % self.max_calls_per_minute == 0:
                    await asyncio.sleep(self.sleep_after_calls)
                
                success = await self.fetch_stock_data_async(session, stock)
                if success:
                    fetched_count += 1
                else:
                    failed_count += 1
                    
        return total_stocks, fetched_count, failed_count

    def get_next_batch_of_stocks(self):
        try:
            all_stocks = self.redis_manager.get_all_symbols()
            if not all_stocks:
                logger.warning("No stocks found in Redis")
                return []

            last_fetched_index_str = self.redis_manager.redis.get('last_fetched_index')
            
            try:
                last_fetched_index = int(last_fetched_index_str) if last_fetched_index_str else 0
            except (ValueError, TypeError):
                logger.warning(f"Invalid last_fetched_index value: {last_fetched_index_str}. Resetting to 0.")
                last_fetched_index = 0

            batch = all_stocks[last_fetched_index:last_fetched_index + self.batch_size]
            next_index = (last_fetched_index + self.batch_size) % len(all_stocks)
            
            try:
                self.redis_manager.redis.set('last_fetched_index', str(next_index))
            except redis.RedisError as e:
                logger.error(f"Failed to update last_fetched_index: {e}")

            return batch
        except Exception as e:
            logger.error(f"Error in get_next_batch_of_stocks: {e}")
            return []

    async def test_fetch_single_stock(self):
        async with aiohttp.ClientSession() as session:
            success = await self.fetch_stock_data_async(session, 'AAPL')
            if success:
                logger.info("Test fetch successful")
                price, timestamp = self.redis_manager.get_latest_stock_price('AAPL')
                logger.info(f"Fetched AAPL: {price}, Timestamp: {timestamp}")
            else:
                logger.error("Test fetch failed")
            return success

    def run_test_fetch(self):
        return asyncio.run(self.test_fetch_single_stock())