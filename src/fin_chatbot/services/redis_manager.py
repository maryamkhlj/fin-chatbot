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
        try:
            self.redis = redis.Redis(
                host=os.getenv('REDIS_HOST', 'redis'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                decode_responses=True
            )
            # Test connection
            self.redis.ping()
            logger.info("Redis connection established successfully")
            
            # Set lock timeout to match fetch interval plus some buffer
            self.lock_timeout = int(os.getenv('FETCH_INTERVAL_HOURS', 4)) * 3600 + 300  # 4 hours + 5 minutes buffer
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def acquire_lock(self, lock_name):
        """Acquire a Redis lock with timeout"""
        try:
            # Set lock with expiry time matching fetch interval plus buffer
            lock_timeout = int(os.getenv('FETCH_INTERVAL_HOURS', 4)) * 3600 + 300  # 4 hours + 5 min buffer
            
            # First, clear any stale lock
            current_lock = self.redis.get(f"lock:{lock_name}")
            if current_lock:
                # Check if lock is older than timeout
                try:
                    lock_time = float(current_lock)
                    if time.time() - lock_time > lock_timeout:
                        self.redis.delete(f"lock:{lock_name}")
                        logger.info(f"Cleared stale lock for {lock_name}")
                except (ValueError, TypeError):
                    self.redis.delete(f"lock:{lock_name}")
            
            # Try to acquire lock with expiry
            acquired = self.redis.set(
                f"lock:{lock_name}",
                str(time.time()),
                ex=lock_timeout,  # Auto-expire after timeout
                nx=True    # Only set if not exists
            )
            return bool(acquired)
        except redis.RedisError as e:
            logger.error(f"Error acquiring lock {lock_name}: {e}")
            return False

    def release_lock(self, lock_name):
        """Release a Redis lock"""
        try:
            return self.redis.delete(f"lock:{lock_name}")
        except redis.RedisError as e:
            logger.error(f"Error releasing lock {lock_name}: {e}")
            return False

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
                # Just call hset without checking its return value
                self.redis.hset(f"stock:{symbol}", mapping={
                    "price": price,
                    "timestamp": timestamp
                })
                logger.info(f"Stock price set for {symbol}: {price} at {timestamp}")
                return True
        except ValueError as e:
            logger.error(f"Data validation failed for {symbol}: {e}")
            return False
        except redis.RedisError as e:
            logger.error(f"Redis error setting stock price for {symbol}: {e}")
            return False

    def get_stock_prices(self, symbol, limit=10):
        try:
            data = self.redis.hgetall(f"stock:{symbol}")
            if data:
                price = float(data['price'])  # No b'price' needed
                timestamp = float(data['timestamp'])
                return [(price, timestamp)]
            return []
        except redis.RedisError as e:
            logger.error(f"Redis error getting stock prices for {symbol}: {e}")
            return []

    def get_latest_stock_price(self, symbol):
        try:
            data = self.redis.hgetall(f"stock:{symbol}")
            if data:
                price = float(data['price'])  # No b'price' needed
                timestamp = float(data['timestamp'])
                return (price, timestamp)
            return (None, None)
        except redis.RedisError as e:
            logger.error(f"Redis error getting latest stock price for {symbol}: {e}")
            return (None, None)

    def get_all_symbols(self):
        try:
            keys = self.redis.keys("stock:*")
            return [key.split(':')[1] for key in keys]  # No decode() needed
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