import os
import duckdb
import pytz
from datetime import datetime
from logzero import logger
from typing import List, Dict, Any, Optional
from src.data.token_manager import TokenManager

# Constants
IST = pytz.timezone('Asia/Kolkata')
MAX_TOKENS_PER_REQUEST = 50
MARKET_OPEN_TIME = "09:15:00"
MARKET_CLOSE_TIME = "15:30:00"

class AngelMarketData:
    def __init__(self, token_manager: TokenManager):
        """Initialize the Angel Market Data manager.
        
        Args:
            token_manager (TokenManager): Instance of TokenManager for token operations
        """
        self.db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
        self.token_manager = token_manager
        self.setup_database()

    def setup_database(self) -> None:
        """Create the required tables for storing real-time market data"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Drop existing tables
            con.execute("DROP TABLE IF EXISTS realtime_spot_data")
            con.execute("DROP TABLE IF EXISTS realtime_futures_data")
            con.execute("DROP TABLE IF EXISTS realtime_options_data")
            
            # Create real-time spot data table
            con.execute("""
                CREATE TABLE realtime_spot_data (
                    token VARCHAR,
                    symbol VARCHAR,
                    name VARCHAR,
                    exchange VARCHAR,
                    ltp DOUBLE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    last_trade_qty INTEGER,
                    avg_trade_price DOUBLE,
                    volume BIGINT,
                    total_buy_qty BIGINT,
                    total_sell_qty BIGINT,
                    best_bid_price DOUBLE,
                    best_ask_price DOUBLE,
                    net_change DOUBLE,
                    percent_change DOUBLE,
                    lower_circuit DOUBLE,
                    upper_circuit DOUBLE,
                    week_low_52 DOUBLE,
                    week_high_52 DOUBLE,
                    best_bid_orders INTEGER,
                    best_ask_orders INTEGER,
                    exch_feed_time TIMESTAMP,
                    exch_trade_time TIMESTAMP,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (token, timestamp)
                )
            """)
            
            # Create real-time futures data table
            con.execute("""
                CREATE TABLE realtime_futures_data (
                    token VARCHAR,
                    symbol VARCHAR,
                    name VARCHAR,
                    exchange VARCHAR,
                    ltp DOUBLE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    last_trade_qty INTEGER,
                    avg_trade_price DOUBLE,
                    volume BIGINT,
                    oi BIGINT,
                    total_buy_qty BIGINT,
                    total_sell_qty BIGINT,
                    best_bid_price DOUBLE,
                    best_ask_price DOUBLE,
                    net_change DOUBLE,
                    percent_change DOUBLE,
                    lower_circuit DOUBLE,
                    upper_circuit DOUBLE,
                    week_low_52 DOUBLE,
                    week_high_52 DOUBLE,
                    best_bid_orders INTEGER,
                    best_ask_orders INTEGER,
                    exch_feed_time TIMESTAMP,
                    exch_trade_time TIMESTAMP,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (token, timestamp)
                )
            """)
            
            # Create real-time options data table
            con.execute("""
                CREATE TABLE realtime_options_data (
                    token VARCHAR,
                    symbol VARCHAR,
                    name VARCHAR,
                    exchange VARCHAR,
                    ltp DOUBLE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    last_trade_qty INTEGER,
                    avg_trade_price DOUBLE,
                    volume BIGINT,
                    oi BIGINT,
                    total_buy_qty BIGINT,
                    total_sell_qty BIGINT,
                    best_bid_price DOUBLE,
                    best_ask_price DOUBLE,
                    net_change DOUBLE,
                    percent_change DOUBLE,
                    lower_circuit DOUBLE,
                    upper_circuit DOUBLE,
                    week_low_52 DOUBLE,
                    week_high_52 DOUBLE,
                    best_bid_orders INTEGER,
                    best_ask_orders INTEGER,
                    strike DOUBLE,
                    option_type VARCHAR,
                    exch_feed_time TIMESTAMP,
                    exch_trade_time TIMESTAMP,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (token, timestamp)
                )
            """)
            
            logger.info("Real-time market data tables created successfully")
        except Exception as e:
            logger.error(f"Error setting up database tables: {e}")
            raise
        finally:
            if con:
                con.close()

    def _get_exchange_tokens(self, token_type: str) -> List[Dict[str, str]]:
        """Get exchange tokens for a specific type.
        
        Args:
            token_type (str): Type of tokens to fetch ('SPOT', 'FUTURES', or 'OPTIONS')
            
        Returns:
            List[Dict[str, str]]: List of exchange tokens with format {"exchangeType": "type", "tokens": "token"}
        """
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Get tokens based on type
            result = con.execute("""
                SELECT 
                    CASE 
                        WHEN exch_seg = 'NSE' THEN 'NSE'
                        ELSE 'NFO'
                    END as exchange_type,
                    token
                FROM tokens
                WHERE token_type = ?
                ORDER BY symbol
            """, [token_type]).fetchall()
            
            return [
                {"exchangeType": row[0], "tokens": row[1]}
                for row in result
            ]
            
        except Exception as e:
            logger.error(f"Error getting {token_type} tokens: {e}")
            return []
        finally:
            if con:
                con.close()

    def _chunk_tokens(self, tokens: List[Dict[str, str]], chunk_size: int = MAX_TOKENS_PER_REQUEST) -> List[List[Dict[str, str]]]:
        """Split tokens into chunks for API requests.
        
        Args:
            tokens (List[Dict[str, str]]): List of token dictionaries
            chunk_size (int): Maximum tokens per chunk
            
        Returns:
            List[List[Dict[str, str]]]: List of token chunks
        """
        return [tokens[i:i + chunk_size] for i in range(0, len(tokens), chunk_size)]

    def _store_spot_data(self, market_data: List[Dict[str, Any]]) -> bool:
        """Store real-time spot market data.
        
        Args:
            market_data (List[Dict[str, Any]]): List of market data points
            
        Returns:
            bool: True if successful, False otherwise
        """
        con = None
        try:
            con = duckdb.connect(self.db_file)
            timestamp = datetime.now(IST).replace(tzinfo=None)
            
            # Get token to name mapping
            token_names = dict(con.execute("""
                SELECT token, name 
                FROM tokens 
                WHERE token_type = 'SPOT'
            """).fetchall())
            
            # Prepare data for insertion
            values = []
            for data in market_data:
                # Get best bid/ask from depth
                depth = data.get('depth', {})
                best_bid = depth.get('buy', [{}])[0].get('price', 0.0)
                best_ask = depth.get('sell', [{}])[0].get('price', 0.0)
                best_bid_orders = depth.get('buy', [{}])[0].get('orders', 0)
                best_ask_orders = depth.get('sell', [{}])[0].get('orders', 0)
                
                # Parse exchange timestamps
                exch_feed_time = datetime.strptime(data['exchFeedTime'], "%d-%b-%Y %H:%M:%S").replace(tzinfo=None)
                exch_trade_time = datetime.strptime(data['exchTradeTime'], "%d-%b-%Y %H:%M:%S").replace(tzinfo=None)
                
                values.append((
                    data['symbolToken'],
                    data['tradingSymbol'],
                    token_names.get(data['symbolToken']),  # Get name from mapping
                    data['exchange'],
                    float(data['ltp']),
                    float(data['open']),
                    float(data['high']),
                    float(data['low']),
                    float(data['close']),
                    int(data['lastTradeQty']),
                    float(data['avgPrice']),
                    int(data['tradeVolume']),
                    int(data['totBuyQuan']),
                    int(data['totSellQuan']),
                    float(best_bid),
                    float(best_ask),
                    float(data['netChange']),
                    float(data['percentChange']),
                    float(data['lowerCircuit']),
                    float(data['upperCircuit']),
                    float(data['52WeekLow']),
                    float(data['52WeekHigh']),
                    int(best_bid_orders),
                    int(best_ask_orders),
                    exch_feed_time,
                    exch_trade_time,
                    timestamp
                ))
            
            # Insert data
            con.executemany("""
                INSERT INTO realtime_spot_data (
                    token, symbol, name, exchange, ltp, open, high, low, close, 
                    last_trade_qty, avg_trade_price, volume,
                    total_buy_qty, total_sell_qty, best_bid_price, best_ask_price,
                    net_change, percent_change, lower_circuit, upper_circuit,
                    week_low_52, week_high_52, best_bid_orders, best_ask_orders,
                    exch_feed_time, exch_trade_time, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, values)
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing spot market data: {e}")
            return False
        finally:
            if con:
                con.close()

    def _store_futures_data(self, market_data: List[Dict[str, Any]]) -> bool:
        """Store real-time futures market data.
        
        Args:
            market_data (List[Dict[str, Any]]): List of market data points
            
        Returns:
            bool: True if successful, False otherwise
        """
        con = None
        try:
            con = duckdb.connect(self.db_file)
            timestamp = datetime.now(IST).replace(tzinfo=None)
            
            # Get token to name mapping
            token_names = dict(con.execute("""
                SELECT token, name 
                FROM tokens 
                WHERE token_type = 'FUTURES'
            """).fetchall())
            
            # Prepare data for insertion
            values = []
            for data in market_data:
                # Get best bid/ask from depth
                depth = data.get('depth', {})
                best_bid = depth.get('buy', [{}])[0].get('price', 0.0)
                best_ask = depth.get('sell', [{}])[0].get('price', 0.0)
                best_bid_orders = depth.get('buy', [{}])[0].get('orders', 0)
                best_ask_orders = depth.get('sell', [{}])[0].get('orders', 0)
                
                # Parse exchange timestamps
                exch_feed_time = datetime.strptime(data['exchFeedTime'], "%d-%b-%Y %H:%M:%S").replace(tzinfo=None)
                exch_trade_time = datetime.strptime(data['exchTradeTime'], "%d-%b-%Y %H:%M:%S").replace(tzinfo=None)
                
                values.append((
                    data['symbolToken'],
                    data['tradingSymbol'],
                    token_names.get(data['symbolToken']),  # Get name from mapping
                    data['exchange'],
                    float(data['ltp']),
                    float(data['open']),
                    float(data['high']),
                    float(data['low']),
                    float(data['close']),
                    int(data['lastTradeQty']),
                    float(data['avgPrice']),
                    int(data['tradeVolume']),
                    int(data.get('opnInterest', 0)),
                    int(data['totBuyQuan']),
                    int(data['totSellQuan']),
                    float(best_bid),
                    float(best_ask),
                    float(data['netChange']),
                    float(data['percentChange']),
                    float(data['lowerCircuit']),
                    float(data['upperCircuit']),
                    float(data['52WeekLow']),
                    float(data['52WeekHigh']),
                    int(best_bid_orders),
                    int(best_ask_orders),
                    exch_feed_time,
                    exch_trade_time,
                    timestamp
                ))
            
            # Insert data
            con.executemany("""
                INSERT INTO realtime_futures_data (
                    token, symbol, name, exchange, ltp, open, high, low, close,
                    last_trade_qty, avg_trade_price, volume, oi,
                    total_buy_qty, total_sell_qty, best_bid_price, best_ask_price,
                    net_change, percent_change, lower_circuit, upper_circuit,
                    week_low_52, week_high_52, best_bid_orders, best_ask_orders,
                    exch_feed_time, exch_trade_time, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, values)
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing futures market data: {e}")
            return False
        finally:
            if con:
                con.close()

    def _store_options_data(self, market_data: List[Dict[str, Any]]) -> bool:
        """Store real-time options market data.
        
        Args:
            market_data (List[Dict[str, Any]]): List of market data points
            
        Returns:
            bool: True if successful, False otherwise
        """
        con = None
        try:
            con = duckdb.connect(self.db_file)
            timestamp = datetime.now(IST).replace(tzinfo=None)
            
            # Get token to name mapping
            token_names = dict(con.execute("""
                SELECT token, name 
                FROM tokens 
                WHERE token_type = 'OPTIONS'
            """).fetchall())
            
            # Prepare data for insertion
            values = []
            for data in market_data:
                # Get best bid/ask from depth
                depth = data.get('depth', {})
                best_bid = depth.get('buy', [{}])[0].get('price', 0.0)
                best_ask = depth.get('sell', [{}])[0].get('price', 0.0)
                best_bid_orders = depth.get('buy', [{}])[0].get('orders', 0)
                best_ask_orders = depth.get('sell', [{}])[0].get('orders', 0)
                
                # Parse exchange timestamps
                exch_feed_time = datetime.strptime(data['exchFeedTime'], "%d-%b-%Y %H:%M:%S").replace(tzinfo=None)
                exch_trade_time = datetime.strptime(data['exchTradeTime'], "%d-%b-%Y %H:%M:%S").replace(tzinfo=None)
                
                # Extract option type from symbol (last 2 characters)
                option_type = data['tradingSymbol'][-2:] if data['tradingSymbol'][-2:] in ['CE', 'PE'] else None
                
                values.append((
                    data['symbolToken'],
                    data['tradingSymbol'],
                    token_names.get(data['symbolToken']),  # Get name from mapping
                    data['exchange'],
                    float(data['ltp']),
                    float(data['open']),
                    float(data['high']),
                    float(data['low']),
                    float(data['close']),
                    int(data['lastTradeQty']),
                    float(data['avgPrice']),
                    int(data['tradeVolume']),
                    int(data.get('opnInterest', 0)),
                    int(data['totBuyQuan']),
                    int(data['totSellQuan']),
                    float(best_bid),
                    float(best_ask),
                    float(data['netChange']),
                    float(data['percentChange']),
                    float(data['lowerCircuit']),
                    float(data['upperCircuit']),
                    float(data['52WeekLow']),
                    float(data['52WeekHigh']),
                    int(best_bid_orders),
                    int(best_ask_orders),
                    float(data.get('strike', 0.0)),
                    option_type,
                    exch_feed_time,
                    exch_trade_time,
                    timestamp
                ))
            
            # Insert data
            con.executemany("""
                INSERT INTO realtime_options_data (
                    token, symbol, name, exchange, ltp, open, high, low, close,
                    last_trade_qty, avg_trade_price, volume, oi,
                    total_buy_qty, total_sell_qty, best_bid_price, best_ask_price,
                    net_change, percent_change, lower_circuit, upper_circuit,
                    week_low_52, week_high_52, best_bid_orders, best_ask_orders,
                    strike, option_type, exch_feed_time, exch_trade_time, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, values)
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing options market data: {e}")
            return False
        finally:
            if con:
                con.close()

    async def fetch_and_store_market_data(self, smart_api) -> bool:
        """Fetch and store real-time market data for all token types.
        
        Args:
            smart_api: Initialized SmartAPI instance
            
        Returns:
            bool: True if all operations were successful, False otherwise
        """
        try:
            # Get tokens for each type
            spot_tokens = self._get_exchange_tokens('SPOT')
            futures_tokens = self._get_exchange_tokens('FUTURES')
            options_tokens = self._get_exchange_tokens('OPTIONS')
            
            # Process spot data
            for chunk in self._chunk_tokens(spot_tokens):
                # Create exchangeTokens format
                exchange_tokens = {}
                for token in chunk:
                    exchange = token['exchangeType']
                    if exchange not in exchange_tokens:
                        exchange_tokens[exchange] = []
                    exchange_tokens[exchange].append(token['tokens'])
                
                market_data = smart_api.getMarketData(
                    mode="FULL",
                    exchangeTokens=exchange_tokens
                )
                if not market_data.get('data'):
                    logger.error(f"Failed to get spot market data: {market_data.get('message', 'Unknown error')}")
                    continue
                if not self._store_spot_data(market_data['data']):
                    return False
            
            # Process futures data
            for chunk in self._chunk_tokens(futures_tokens):
                # Create exchangeTokens format
                exchange_tokens = {}
                for token in chunk:
                    exchange = token['exchangeType']
                    if exchange not in exchange_tokens:
                        exchange_tokens[exchange] = []
                    exchange_tokens[exchange].append(token['tokens'])
                
                market_data = smart_api.getMarketData(
                    mode="FULL",
                    exchangeTokens=exchange_tokens
                )
                if not market_data.get('data'):
                    logger.error(f"Failed to get futures market data: {market_data.get('message', 'Unknown error')}")
                    continue
                if not self._store_futures_data(market_data['data']):
                    return False
            
            # Process options data
            for chunk in self._chunk_tokens(options_tokens):
                # Create exchangeTokens format
                exchange_tokens = {}
                for token in chunk:
                    exchange = token['exchangeType']
                    if exchange not in exchange_tokens:
                        exchange_tokens[exchange] = []
                    exchange_tokens[exchange].append(token['tokens'])
                
                market_data = smart_api.getMarketData(
                    mode="FULL",
                    exchangeTokens=exchange_tokens
                )
                if not market_data.get('data'):
                    logger.error(f"Failed to get options market data: {market_data.get('message', 'Unknown error')}")
                    continue
                if not self._store_options_data(market_data['data']):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error fetching and storing market data: {e}")
            return False

    def _get_strike_interval(self, name: str, expiry: str) -> float:
        """Calculate the strike interval for a given stock
        
        Args:
            name (str): Stock name
            expiry (str): Expiry date
            
        Returns:
            float: Most common strike interval
        """
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Get all strikes for this stock and expiry, ordered
            logger.info(f"Calculating strike interval for {name} with expiry {expiry}")
            
            # First get all strikes and convert from paisa to rupees
            strikes_result = con.execute("""
                SELECT DISTINCT strike/100 as strike_price
                FROM tokens
                WHERE name = ?
                AND expiry = ?
                AND token_type = 'OPTIONS'
                AND strike IS NOT NULL
                ORDER BY strike_price
            """, [name, expiry]).fetchall()
            
            if not strikes_result:
                logger.error(f"No strikes found for {name} with expiry {expiry}")
                return None
                
            strikes = [row[0] for row in strikes_result]
            logger.info(f"Found {len(strikes)} unique strikes: {strikes[:10]} ...")
            
            # Calculate intervals between consecutive strikes
            intervals = []
            for i in range(1, len(strikes)):
                interval = round(strikes[i] - strikes[i-1], 2)
                intervals.append(interval)
            
            if not intervals:
                logger.error("No intervals could be calculated")
                return None
                
            logger.info(f"Calculated intervals between strikes: {intervals[:10]} ...")
            
            # Get the most frequent interval
            result = con.execute("""
                WITH strike_diffs AS (
                    SELECT 
                        ROUND(strike/100 - LAG(strike/100) OVER (ORDER BY strike), 2) as interval
                    FROM (
                        SELECT DISTINCT strike
                        FROM tokens
                        WHERE name = ?
                        AND expiry = ?
                        AND token_type = 'OPTIONS'
                        AND strike IS NOT NULL
                        ORDER BY strike
                    )
                )
                SELECT interval, COUNT(*) as frequency
                FROM strike_diffs
                WHERE interval > 0
                GROUP BY interval
                ORDER BY frequency DESC, interval ASC
                LIMIT 1
            """, [name, expiry]).fetchone()
            
            if not result:
                logger.error("Could not determine most common interval")
                return None
                
            interval, frequency = result
            logger.info(f"Most common interval: {interval} (occurs {frequency} times)")
            
            return interval
            
        except Exception as e:
            logger.error(f"Error calculating strike interval for {name}: {e}")
            return None
        finally:
            if con:
                con.close()

    def _get_atm_strikes(self, name: str, future_price: float, strike_interval: float, num_strikes: int = 0) -> List[float]:
        """Get ATM strikes for a stock
        
        Args:
            name (str): Stock name
            future_price (float): Current futures price
            strike_interval (float): Strike interval for this stock
            num_strikes (int): Number of additional strikes to select above and below ATM (0 for ATM only)
            
        Returns:
            List[float]: List of strike prices to include
        """
        try:
            # Find ATM strike (closest strike to future price)
            base_strike = round(future_price / strike_interval) * strike_interval
            
            if num_strikes == 0:
                return [base_strike]
                
            # Generate strikes above and below if requested
            strikes = []
            for i in range(-num_strikes, num_strikes + 1):
                strike = base_strike + (i * strike_interval)
                strikes.append(strike)
            
            return strikes
            
        except Exception as e:
            logger.error(f"Error calculating ATM strikes for {name}: {e}")
            return [] 