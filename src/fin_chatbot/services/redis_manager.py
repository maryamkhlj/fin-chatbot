import redis
import json
import time
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        self.redis = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            password=os.getenv('REDIS_PASSWORD')
        )

    def validate_stock_data(self, symbol, price, timestamp=None):
        if not isinstance(symbol, str) or len(symbol) > 10:
            raise ValueError("Invalid symbol")
        
        if not isinstance(price, (int, float)) or price < 0:
            raise ValueError("Invalid price")
        
        if timestamp is not None and not isinstance(timestamp, (int, float)):
            raise ValueError("Invalid timestamp")
        
        return True
    
    def set_stock_price(self, symbol, price, timestamp=None):
        try:
            if self.validate_stock_data(symbol, price, timestamp):
                if timestamp is None:
                    timestamp = time.time()
                self.redis.hset(f"stock:{symbol}", mapping={
                    "price": price,
                    "timestamp": timestamp
                })
                logger.info(f"Stock price set for {symbol}: {price} at {timestamp}")
        except ValueError as e:
            logger.error(f"Data validation failed for {symbol}: {e}")
        except redis.RedisError as e:
            logger.error(f"Redis error setting stock price for {symbol}: {e}")

    def get_stock_prices(self, symbol, limit=10):
        try:
            data = self.redis.hgetall(f"stock:{symbol}")
            if data:
                price = float(data[b'price'])
                timestamp = float(data[b'timestamp'])
                return [(price, timestamp)]
            return []
        except redis.RedisError as e:
            logger.error(f"Redis error getting stock prices for {symbol}: {e}")
            return []

    def get_latest_stock_price(self, symbol):
        try:
            data = self.redis.hgetall(f"stock:{symbol}")
            if data:
                price = float(data[b'price'])
                timestamp = float(data[b'timestamp'])
                return (price, timestamp)
            return (None, None)
        except redis.RedisError as e:
            logger.error(f"Redis error getting latest stock price for {symbol}: {e}")
            return (None, None)

    def get_all_symbols(self):
        try:
            keys = self.redis.keys("stock:*")
            return [key.decode().split(':')[1] for key in keys]
        except redis.RedisError as e:
            logger.error(f"Redis error getting all symbols: {e}")
            return []

    def get_total_stored_stocks(self):
        return len(self.get_all_symbols())

    def delete_stock_data(self, symbol):
        try:
            deleted = self.redis.delete(f"stock:{symbol}")
            return deleted > 0
        except Exception as e:
            logger.error(f"Error deleting data for {symbol}: {e}")
            return False