import asyncio
import aiohttp
import logging
import time
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
        self.retry_delay = 1  # Start with 1 second delay
        self.max_retries = 5
        self.fetch_interval_minutes = float(os.getenv('FETCH_INTERVAL_MINUTES', 15))
        self.batch_size = int(os.getenv('BATCH_SIZE', 50))

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
        logger.info(f"Fetching data for symbol: {symbol}")
        for attempt in range(self.max_retries):
            async with self.semaphore:
                try:
                    async with session.get(url, ssl=self.get_ssl_context()) as response:
                        if response.status == 200:
                            data = await response.json()
                            price = data.get('c')
                            if price is not None:
                                self.redis_manager.set_stock_price(symbol, price)
                                logger.info(f"Successfully fetched and stored data for symbol: {symbol}, price: {price}")
                                return True
                            else:
                                logger.warning(f"No price data available for symbol: {symbol}. Response: {data}")
                                return False
                        elif response.status == 429:
                            logger.warning(f"Rate limit hit for {symbol}. Retrying after delay. Attempt {attempt + 1}/{self.max_retries}")
                            await asyncio.sleep(self.retry_delay)
                            self.retry_delay *= 2  # Exponential backoff
                        else:
                            logger.error(f"Error fetching data for {symbol}: HTTP {response.status}. Response: {await response.text()}")
                            return False
                except aiohttp.ClientError as e:
                    logger.error(f"Network error fetching data for {symbol}: {str(e)}")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Unexpected error fetching data for {symbol}: {str(e)}")
                    await asyncio.sleep(1)
        logger.error(f"Failed to fetch data for {symbol} after {self.max_retries} attempts")
        return False

    async def process_stocks_async(self, stocks, max_calls=None):
        total_stocks = len(stocks)
        fetched_count = 0
        failed_count = 0
        async with aiohttp.ClientSession() as session:
            for stock in stocks:
                if max_calls is not None and fetched_count + failed_count >= max_calls:
                    break
                success = await self.fetch_stock_data_async(session, stock)
                if success:
                    fetched_count += 1
                else:
                    failed_count += 1
        return total_stocks, fetched_count, failed_count

    async def process_common_stocks(self, max_calls=None):
        logger.info(f"Processing {len(COMMON_STOCKS)} common stocks...")
        total_stocks, fetched_count, failed_count = await self.process_stocks_async(COMMON_STOCKS, max_calls)
        stored_count = self.redis_manager.get_total_stored_stocks()
        logger.info(f"Processed common stocks - Total: {total_stocks}, Fetched: {fetched_count}, Failed: {failed_count}, Stored: {stored_count}")
        return total_stocks, fetched_count, failed_count, stored_count

    def get_next_batch_of_stocks(self):
        all_stocks = self.get_all_available_symbols()
        last_fetched_index_str = self.redis_manager.get('last_fetched_index')
        
        try:
            last_fetched_index = int(last_fetched_index_str) if last_fetched_index_str else 0
        except ValueError:
            logger.warning(f"Invalid last_fetched_index value: {last_fetched_index_str}. Resetting to 0.")
            last_fetched_index = 0

        batch = all_stocks[last_fetched_index:last_fetched_index + self.batch_size]
        next_index = (last_fetched_index + self.batch_size) % len(all_stocks)
        
        self.redis_manager.set('last_fetched_index', str(next_index))
        return batch

    def get_common_symbols(self):
        return COMMON_STOCKS, len(COMMON_STOCKS)

    def get_all_available_symbols(self):
        return self.redis_manager.get_all_symbols()

    def get_stock_data(self, symbol, start_time='-inf', end_time='+inf', limit=10):
        return self.redis_manager.get_stock_prices(symbol, start_time, end_time, limit)

    def get_latest_stock_data(self, symbol):
        return self.redis_manager.get_latest_stock_price(symbol)

    async def test_fetch_single_stock(self):
        async with aiohttp.ClientSession() as session:
            success = await self.fetch_stock_data_async(session, 'AAPL')
            if success:
                logger.info("Test fetch successful. API and Redis connection working.")
                price, timestamp = self.get_latest_stock_data('AAPL')
                logger.info(f"Fetched price for AAPL: {price}, Timestamp: {timestamp}")
            else:
                logger.error("Test fetch failed. Check API key and connections.")
            return success

    def run_test_fetch(self):
        return asyncio.run(self.test_fetch_single_stock())