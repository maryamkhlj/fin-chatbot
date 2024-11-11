import redis
import os
from dotenv import load_dotenv

load_dotenv()

redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=int(os.getenv('REDIS_DB', 0)),
    password=os.getenv('REDIS_PASSWORD')
)

def get_all_symbols():
    return [key.decode().split(':')[1] for key in redis_client.keys("stock:*")]

def get_stock_data(symbol):
    key = f"stock:{symbol}"
    key_type = redis_client.type(key).decode()
    
    if key_type == 'hash':
        return redis_client.hgetall(key)
    elif key_type == 'string':
        return redis_client.get(key)
    elif key_type == 'zset':
        return redis_client.zrevrange(key, 0, -1, withscores=True)
    else:
        return f"Unexpected data type: {key_type}"

symbols = get_all_symbols()
print(f"Total symbols in Redis: {len(symbols)}")

print("\nSample data from Redis:")
for symbol in symbols[:5]:  # Print data for first 5 symbols
    data = get_stock_data(symbol)
    print(f"{symbol}: {data}")


# docker cp check_redis_data.py fin_chatbot:/fin_chatbot/
# docker exec -it fin_chatbot python /fin_chatbot/check_redis_data.py