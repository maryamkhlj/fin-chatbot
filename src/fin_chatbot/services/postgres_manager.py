import os
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import execute_batch
from contextlib import contextmanager
import logging
from datetime import date, timedelta
import numpy as np
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresManager:
    def __init__(self):
        self.pool = None
        self.db_params = {
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT')
        }

    def create_pool(self, minconn=1, maxconn=10):
        try:
            self.pool = pool.SimpleConnectionPool(minconn, maxconn, **self.db_params)
            logger.info("Connection pool created successfully")
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error creating connection pool: {error}")
            raise

    @contextmanager
    def get_connection(self):
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    @contextmanager
    def get_cursor(self, commit=True):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                if commit:
                    conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

    def close_pool(self):
        if self.pool:
            self.pool.closeall()
            logger.info("All database connections closed")

    def create_database(self):
        conn = psycopg2.connect(
            dbname='postgres',
            user=self.db_params['user'],
            password=self.db_params['password'],
            host=self.db_params['host'],
            port=self.db_params['port']
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (self.db_params['dbname'],))
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_params['dbname'])))
                logger.info(f"Database '{self.db_params['dbname']}' created successfully")
            else:
                logger.info(f"Database '{self.db_params['dbname']}' already exists")
        conn.close()

    def create_tables(self):
        with self.get_cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    closing_price DECIMAL(10, 2) NOT NULL,
                    UNIQUE (symbol, date)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_metrics (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    cagr_1y DECIMAL(10, 4),
                    cagr_3y DECIMAL(10, 4),
                    cagr_5y DECIMAL(10, 4),
                    volatility_1y DECIMAL(10, 4),
                    ma_50 DECIMAL(10, 2),
                    ma_200 DECIMAL(10, 2),
                    rsi_14 DECIMAL(10, 2),
                    beta_1y DECIMAL(10, 4),
                    sharpe_ratio_1y DECIMAL(10, 4),
                    max_drawdown_1y DECIMAL(10, 4),
                    UNIQUE (symbol, date)
                )
            ''')
        logger.info("Tables created successfully")

    def create_indexes(self):
        with self.get_cursor() as cursor:
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices (symbol, date);
                CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices (date);
                CREATE INDEX IF NOT EXISTS idx_stock_metrics_symbol_date ON stock_metrics (symbol, date);
            ''')
        logger.info("Indexes created successfully")

    def batch_insert_stock_prices(self, data):
        with self.get_cursor() as cursor:
            execute_batch(cursor, '''
                INSERT INTO stock_prices (symbol, date, closing_price)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE
                SET closing_price = EXCLUDED.closing_price
            ''', data)
        logger.info(f"Batch inserted {len(data)} stock prices")

    def get_stock_prices(self, symbol, start_date, end_date):
        with self.get_cursor(commit=False) as cursor:
            cursor.execute('''
                SELECT date, closing_price
                FROM stock_prices
                WHERE symbol = %s AND date BETWEEN %s AND %s
                ORDER BY date
            ''', (symbol, start_date, end_date))
            return cursor.fetchall()

    def get_latest_stock_price(self, symbol):
        with self.get_cursor(commit=False) as cursor:
            cursor.execute('''
                SELECT date, closing_price
                FROM stock_prices
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT 1
            ''', (symbol,))
            result = cursor.fetchone()
            return result if result else (None, None)

    def get_inserted_count(self, date):
        with self.get_cursor(commit=False) as cursor:
            cursor.execute("SELECT COUNT(*) FROM stock_prices WHERE date = %s", (date,))
            return cursor.fetchone()[0]

    def get_total_records(self):
        with self.get_cursor(commit=False) as cursor:
            cursor.execute("SELECT COUNT(*) FROM stock_prices")
            return cursor.fetchone()[0]

    def cleanup_old_data(self, days=5*365):
        cutoff_date = date.today() - timedelta(days=days)
        with self.get_cursor() as cursor:
            cursor.execute("DELETE FROM stock_prices WHERE date < %s", (cutoff_date,))
            deleted_count = cursor.rowcount
        logger.info(f"Deleted {deleted_count} records older than {days} days")
        return deleted_count


    def calculate_metrics(self, symbol, end_date):
        try:
            five_years_ago = end_date - timedelta(days=5*365)
            prices = self.get_stock_prices(symbol, five_years_ago, end_date)
            if not prices:
                logger.warning(f"No price data available for {symbol}")
                return None

            dates, close_prices = zip(*prices)
            close_prices = np.array(close_prices)

            return {
                'date': end_date,
                'cagr_1y': self._calculate_cagr(close_prices[-252:]),
                'cagr_3y': self._calculate_cagr(close_prices[-756:]),
                'cagr_5y': self._calculate_cagr(close_prices),
                'volatility_1y': self._calculate_volatility(close_prices[-252:]),
                'ma_50': np.mean(close_prices[-50:]),
                'ma_200': np.mean(close_prices[-200:]),
                'rsi_14': self._calculate_rsi(close_prices, 14),
                'beta_1y': self._calculate_beta(symbol, dates[-252:], close_prices[-252:]),
                'sharpe_ratio_1y': self._calculate_sharpe_ratio(close_prices[-252:]),
                'max_drawdown_1y': self._calculate_max_drawdown(close_prices[-252:])
            }
        except Exception as e:
            logger.error(f"Error calculating metrics for {symbol}: {e}")
            return None

    def _calculate_volatility(self, prices):
        returns = np.diff(np.log(prices))
        return np.std(returns) * np.sqrt(252)

    def _calculate_rsi(self, prices, window):
        deltas = np.diff(prices)
        seed = deltas[:window+1]
        up = seed[seed >= 0].sum()/window
        down = -seed[seed < 0].sum()/window
        rs = up/down
        rsi = np.zeros_like(prices)
        rsi[:window] = 100. - 100./(1. + rs)

        for i in range(window, len(prices)):
            delta = deltas[i - 1]
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta

            up = (up*(window - 1) + upval)/window
            down = (down*(window - 1) + downval)/window
            rs = up/down
            rsi[i] = 100. - 100./(1. + rs)

        return rsi[-1]

    def _calculate_beta(self, symbol, dates, prices):
        # Improved beta calculation (placeholder - needs market data)
        market_prices = self._get_market_prices(dates)  # Implement this method
        if market_prices is None:
            return None
        
        stock_returns = np.diff(prices) / prices[:-1]
        market_returns = np.diff(market_prices) / market_prices[:-1]
        
        covariance = np.cov(stock_returns, market_returns)[0][1]
        market_variance = np.var(market_returns)
        
        return covariance / market_variance if market_variance != 0 else None

    def _get_market_prices(self, dates):
        # Placeholder method - implement to fetch market index prices
        # You might want to store market index data in a separate table
        return None

    def _calculate_sharpe_ratio(self, prices, risk_free_rate=0.02):
        returns = np.diff(prices) / prices[:-1]
        excess_returns = returns - risk_free_rate/252
        return np.sqrt(252) * excess_returns.mean() / excess_returns.std()

    def _calculate_max_drawdown(self, prices):
        peak = prices[0]
        max_drawdown = 0
        for price in prices[1:]:
            if price > peak:
                peak = price
            drawdown = (peak - price) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        return max_drawdown

    def insert_metrics(self, symbol, metrics):
        with self.get_cursor() as cursor:
            cursor.execute('''
                INSERT INTO stock_metrics 
                (symbol, date, cagr_1y, cagr_3y, cagr_5y, volatility_1y, ma_50, ma_200, rsi_14, beta_1y, sharpe_ratio_1y, max_drawdown_1y)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE
                SET 
                    cagr_1y = EXCLUDED.cagr_1y,
                    cagr_3y = EXCLUDED.cagr_3y,
                    cagr_5y = EXCLUDED.cagr_5y,
                    volatility_1y = EXCLUDED.volatility_1y,
                    ma_50 = EXCLUDED.ma_50,
                    ma_200 = EXCLUDED.ma_200,
                    rsi_14 = EXCLUDED.rsi_14,
                    beta_1y = EXCLUDED.beta_1y,
                    sharpe_ratio_1y = EXCLUDED.sharpe_ratio_1y,
                    max_drawdown_1y = EXCLUDED.max_drawdown_1y
            ''', (
                symbol,
                metrics['date'],
                metrics['cagr_1y'],
                metrics['cagr_3y'],
                metrics['cagr_5y'],
                metrics['volatility_1y'],
                metrics['ma_50'],
                metrics['ma_200'],
                metrics['rsi_14'],
                metrics['beta_1y'],
                metrics['sharpe_ratio_1y'],
                metrics['max_drawdown_1y']
            ))
        logger.info(f"Metrics inserted/updated for {symbol} on {metrics['date']}")

    def update_all_metrics(self):
        symbols = self.get_all_symbols()
        end_date = date.today()
        for symbol in symbols:
            metrics = self.calculate_metrics(symbol, end_date)
            if metrics:
                self.insert_metrics(symbol, metrics)

    def get_all_symbols(self):
        with self.get_cursor(commit=False) as cursor:
            cursor.execute("SELECT DISTINCT symbol FROM stock_prices")
            return [row[0] for row in cursor.fetchall()]