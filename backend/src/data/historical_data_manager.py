import os
import duckdb
import pytz
import pandas as pd
import time
from datetime import datetime, timedelta
from logzero import logger
from typing import List, Dict, Any, Optional
from data.token_manager import TokenManager

# Constants
IST = pytz.timezone('Asia/Kolkata')
SPOT_START_DATE = "1992-01-01"  # Historical start date for spot data
API_RATE_LIMIT = 1  # 1 request per second as per documentation
MAX_RETRIES = 3  # Maximum number of API retries
RETRY_DELAY = 2  # Delay between retries in seconds

class HistoricalDataManager:
    def __init__(self, token_manager: TokenManager):
        """Initialize the HistoricalDataManager with database configuration."""
        self.db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
        self.token_manager = token_manager
        self.setup_database()
        self.last_api_call = 0  # Track last API call time for rate limiting

    def setup_database(self) -> None:
        """Create the historical_data table if it doesn't exist"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Drop existing table to handle schema changes
            con.execute("DROP TABLE IF EXISTS historical_data")
            
            con.execute("""
                CREATE TABLE historical_data (
                    token VARCHAR,
                    symbol VARCHAR,
                    name VARCHAR,
                    timestamp TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    oi BIGINT,
                    token_type VARCHAR,  -- 'SPOT', 'FUTURES', or 'OPTIONS'
                    download_timestamp TIMESTAMP,
                    PRIMARY KEY (token, timestamp)
                )
            """)
            logger.info("Historical data table created/verified successfully")
        except Exception as e:
            logger.error(f"Error setting up historical data table: {e}")
            raise
        finally:
            if con:
                con.close()

    def get_tokens_by_type(self, token_type: str) -> List[Dict[str, Any]]:
        """Get list of tokens by type from the tokens table"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            result = con.execute("""
                SELECT 
                    token,
                    symbol,
                    name,
                    expiry,
                    token_type
                FROM tokens
                WHERE token_type = ?
            """, [token_type]).fetchall()
            
            if result:
                logger.info(f"Found {len(result)} {token_type} tokens")
            else:
                logger.warning(f"No {token_type} tokens found")
            
            return [
                {
                    "token": row[0],
                    "symbol": row[1],
                    "name": row[2],
                    "expiry": row[3],
                    "token_type": row[4]
                }
                for row in result
            ]
            
        except Exception as e:
            logger.error(f"Error getting {token_type} tokens: {e}")
            return []
        finally:
            if con:
                con.close()

    def _rate_limit(self):
        """Implement rate limiting for API calls"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call
        
        if time_since_last_call < API_RATE_LIMIT:
            sleep_time = API_RATE_LIMIT - time_since_last_call
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_api_call = time.time()

    def _get_candle_data_with_retry(self, connector, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get candle data with retry logic"""
        retries = 0
        while retries < MAX_RETRIES:
            try:
                # Rate limit API calls
                self._rate_limit()
                
                # Make API call
                historical_data = connector.getCandleData(params)
                
                if historical_data.get('status') and historical_data.get('data'):
                    return historical_data
                else:
                    logger.warning(f"API error: {historical_data.get('message', 'Unknown error')}")
                    
            except Exception as e:
                logger.warning(f"API call failed (attempt {retries + 1}/{MAX_RETRIES}): {str(e)}")
            
            retries += 1
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        
        return None

    def _store_historical_data(self, historical_data: Dict[str, Any], token_info: Dict[str, Any]) -> bool:
        """Store historical data in the database"""
        if not historical_data or 'data' not in historical_data:
            logger.warning("No historical data received for storage")
            return False
            
        con = None
        try:
            current_time = datetime.now(IST).replace(tzinfo=None)
            records = []
            
            for idx, candle in enumerate(historical_data['data']):
                try:
                    # Parse timestamp without debug logs
                    ts = pd.to_datetime(candle[0])
                    if ts.tz is None:
                        ts = ts.tz_localize('UTC')
                    timestamp = ts.tz_convert(IST).replace(tzinfo=None)
                    
                    # Convert numeric values with proper error handling
                    try:
                        open_price = float(candle[1])
                        high_price = float(candle[2])
                        low_price = float(candle[3])
                        close_price = float(candle[4])
                        volume = int(candle[5])
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid numeric data in candle: {candle}")
                        continue
                    
                    # Validate price data
                    if any(p <= 0 for p in [open_price, high_price, low_price, close_price]):
                        logger.warning(f"Invalid price data (<=0) in candle: {candle}")
                        continue
                    
                    if not (low_price <= open_price <= high_price and 
                           low_price <= close_price <= high_price):
                        logger.warning(f"Invalid price range in candle: {candle}")
                        continue
                    
                    if volume < 0:
                        logger.warning(f"Invalid volume (<0) in candle: {candle}")
                        continue
                    
                    records.append({
                        'token': token_info['token'],
                        'symbol': token_info['symbol'],
                        'name': token_info['name'],
                        'timestamp': timestamp,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume': volume,
                        'oi': 0,  # oi (not available in candle data)
                        'token_type': token_info['token_type'],
                        'download_timestamp': current_time
                    })
                except (ValueError, IndexError) as e:
                    logger.error(f"Invalid candle data at index {idx}: {candle} | Error: {str(e)}")
                    continue
            
            if records:
                # Convert with explicit datetime resolution
                df = pd.DataFrame(records)
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=False).astype('datetime64[us]')
                df['download_timestamp'] = pd.to_datetime(df['download_timestamp'], utc=False).astype('datetime64[us]')
                
                # Store data in database
                con = duckdb.connect(self.db_file)
                con.execute("SET timezone = 'Asia/Kolkata'")
                
                # Delete existing records for this token and timestamp
                con.execute("""
                    DELETE FROM historical_data 
                    WHERE token = ? 
                    AND timestamp IN (SELECT timestamp FROM df)
                """, [token_info['token']])
                
                # Insert new records from DataFrame
                con.execute("INSERT INTO historical_data SELECT * FROM df")
                
                logger.info(f"Successfully stored {len(records)} records for {token_info['symbol']}")
                return True
            else:
                logger.warning(f"No valid records found for {token_info['symbol']}")
                return False
                
        except Exception as e:
            logger.error(f"Storage failed for {token_info['symbol']}: {str(e)}")
            return False
        finally:
            if con:
                con.close()

    def download_spot_data(self, connector) -> bool:
        """Download historical spot data from 1992"""
        try:
            spot_tokens = self.get_tokens_by_type("SPOT")
            if not spot_tokens:
                logger.error("No spot tokens found")
                return False

            success_count = 0
            error_count = 0
            
            for token_info in spot_tokens:
                try:
                    logger.info(f"Fetching spot data for {token_info['symbol']}")
                    
                    # Format dates properly
                    from_date = datetime.strptime(SPOT_START_DATE, "%Y-%m-%d")
                    to_date = datetime.now(IST)
                    
                    params = {
                        "exchange": "NSE",
                        "symboltoken": token_info['token'],
                        "interval": "ONE_DAY",
                        "fromdate": from_date.strftime("%Y-%m-%d 09:00"),
                        "todate": to_date.strftime("%Y-%m-%d %H:%M")
                    }
                    
                    historical_data = self._get_candle_data_with_retry(connector, params)
                    if historical_data and self._store_historical_data(historical_data, token_info):
                        success_count += 1
                    else:
                        error_count += 1
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing spot data for {token_info['symbol']}: {str(e)}")
                    continue

            logger.info(f"\nSpot data processing summary:")
            logger.info(f"- Successfully processed: {success_count}")
            logger.info(f"- Failed to process: {error_count}")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error in spot data processing: {e}")
            return False

    def download_futures_data(self, connector) -> bool:
        """Download current day's futures data"""
        try:
            futures_tokens = self.get_tokens_by_type("FUTURES")
            if not futures_tokens:
                logger.error("No futures tokens found")
                return False

            success_count = 0
            error_count = 0
            
            # Get current date in IST
            current_date = datetime.now(IST)
            
            for token_info in futures_tokens:
                try:
                    logger.info(f"Fetching futures data for {token_info['symbol']}")
                    
                    params = {
                        "exchange": "NFO",
                        "symboltoken": token_info['token'],
                        "interval": "ONE_DAY",
                        "fromdate": current_date.strftime("%Y-%m-%d 09:00"),
                        "todate": current_date.strftime("%Y-%m-%d 15:30")
                    }
                    
                    historical_data = self._get_candle_data_with_retry(connector, params)
                    if historical_data and self._store_historical_data(historical_data, token_info):
                        success_count += 1
                    else:
                        error_count += 1
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing futures data for {token_info['symbol']}: {str(e)}")
                    continue

            logger.info(f"\nFutures data processing summary:")
            logger.info(f"- Successfully processed: {success_count}")
            logger.info(f"- Failed to process: {error_count}")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error in futures data processing: {e}")
            return False

    def download_options_data(self, connector) -> bool:
        """Download current day's options data"""
        try:
            options_tokens = self.get_tokens_by_type("OPTIONS")
            if not options_tokens:
                logger.error("No options tokens found")
                return False

            success_count = 0
            error_count = 0
            
            # Get current date in IST
            current_date = datetime.now(IST)
            
            for token_info in options_tokens:
                try:
                    logger.info(f"Fetching options data for {token_info['symbol']}")
                    
                    params = {
                        "exchange": "NFO",
                        "symboltoken": token_info['token'],
                        "interval": "ONE_DAY",
                        "fromdate": current_date.strftime("%Y-%m-%d 09:00"),
                        "todate": current_date.strftime("%Y-%m-%d 15:30")
                    }
                    
                    historical_data = self._get_candle_data_with_retry(connector, params)
                    if historical_data and self._store_historical_data(historical_data, token_info):
                        success_count += 1
                    else:
                        error_count += 1
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing options data for {token_info['symbol']}: {str(e)}")
                    continue

            logger.info(f"\nOptions data processing summary:")
            logger.info(f"- Successfully processed: {success_count}")
            logger.info(f"- Failed to process: {error_count}")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error in options data processing: {e}")
            return False

    def fetch_and_store_historical_data(self, api_connector) -> bool:
        """Fetch and store historical data for all tokens"""
        # Check if token data is current
        if not self.token_manager.is_market_data_current():
            logger.info("Token data not current, refreshing tokens first...")
            if not self.token_manager.download_and_store_tokens():
                logger.error("Failed to refresh token data")
                return False

        # Get all spot tokens
        con = None
        try:
            con = duckdb.connect(self.db_file)
            spot_tokens = con.execute("""
                SELECT token, symbol, name, exch_seg 
                FROM tokens 
                WHERE token_type = 'SPOT'
            """).fetchall()
            
            logger.info(f"Found {len(spot_tokens)} SPOT tokens")
            
            # First check if we have historical data already
            for token, symbol, name, exchange in spot_tokens:
                # Check if we already have current data for this token
                has_current_data = self._is_historical_data_current(con, token)
                
                if has_current_data:
                    logger.info(f"Historical data for {symbol} is current, skipping...")
                    continue
                    
                logger.info(f"Fetching spot data for {symbol}")
                # Rest of the code for fetching and storing data...
                
            # Return True if all operations were successful
            return True
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return False
        finally:
            if con:
                con.close()

    def _is_historical_data_current(self, con, token: str) -> bool:
        """Check if we already have current historical data for this token"""
        try:
            # Get latest trading day (exclude weekends and holidays)
            now = datetime.now(IST)
            current_date = now.date()
            
            # If it's before market close time (15:30), we consider previous trading day as latest
            if now.time() < datetime.strptime("15:30:00", "%H:%M:%S").time():
                current_date = self._get_previous_trading_day(current_date)
            
            # Check if we have data for the latest trading day
            result = con.execute("""
                SELECT COUNT(*) 
                FROM historical_data 
                WHERE token = ? 
                AND timestamp::DATE = ?
            """, [token, current_date]).fetchone()[0]
            
            return result > 0
        except Exception as e:
            logger.error(f"Error checking if historical data is current: {e}")
            return False

    def _get_previous_trading_day(self, date):
        """Get the previous trading day, skipping weekends"""
        prev_day = date - timedelta(days=1)
        # Skip weekends (5=Saturday, 6=Sunday)
        while prev_day.weekday() >= 5:
            prev_day = prev_day - timedelta(days=1)
        return prev_day