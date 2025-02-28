import os
import duckdb
import pytz
import pandas as pd
import time
from datetime import datetime, timedelta
from logzero import logger
from typing import List, Dict, Any, Optional, Tuple
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
        """Create database tables if they don't exist"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Use standard TIMESTAMP without precision specification
            con.execute("""
                CREATE TABLE IF NOT EXISTS historical_data (
                    token VARCHAR,
                    symbol VARCHAR,
                    name VARCHAR,
                    timestamp TIMESTAMP,  -- Changed from TIMESTAMP_NS
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    oi BIGINT,
                    token_type VARCHAR,
                    download_timestamp TIMESTAMP,  -- Changed from TIMESTAMP_NS
                    PRIMARY KEY (token, timestamp)
                )
            """)
            logger.info("Historical data table created/verified successfully")
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
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
        con = None
        try:
            logger.debug(f"Storing historical data for {token_info['symbol']}")
            logger.debug(f"Token info: {token_info}")
            
            if not historical_data or 'data' not in historical_data:
                logger.warning("No historical data received for storage")
                return False
            
            # Define column names
            columns = ['token', 'symbol', 'name', 'timestamp', 'open', 'high', 'low', 
                       'close', 'volume', 'oi', 'token_type', 'download_timestamp']
            
            # Store records as strings or primitive types to avoid timestamp conversion issues
            string_records = []
            for candle in historical_data['data']:
                try:
                    # Parse timestamp 
                    ts = pd.to_datetime(candle[0])
                    if ts.tz is None:
                        ts = ts.tz_localize('UTC')
                    timestamp = ts.tz_convert(IST).tz_localize(None)
                    
                    # Format as string in ISO format
                    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Convert numeric values
                    open_price = float(candle[1])
                    high_price = float(candle[2])
                    low_price = float(candle[3])
                    close_price = float(candle[4])
                    volume = int(candle[5])
                    
                    # Add to string records
                    string_records.append((
                        token_info['token'],
                        token_info['symbol'],
                        token_info['name'],
                        timestamp_str,  # Use string format
                        open_price, 
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        0,  # oi
                        token_info['token_type'],
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current time as string
                    ))
                except Exception as e:
                    logger.error(f"Error processing candle data: {e}")
                    continue
            
            if string_records:
                # Create DataFrame with explicit column names
                df = pd.DataFrame(string_records, columns=columns)
                
                # Convert to pandas datetime64 type
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=False)
                df['download_timestamp'] = pd.to_datetime(df['download_timestamp'], utc=False)
                
                # Connect to database
                con = duckdb.connect(self.db_file)
                
                # Get date for deletion
                date_str = pd.to_datetime(df['timestamp'].iloc[0]).strftime("%Y-%m-%d")
                
                # Delete existing records
                con.execute(
                    "DELETE FROM historical_data WHERE token = ? AND CAST(timestamp AS DATE) = ?",
                    [token_info['token'], date_str]
                )
                
                # Insert records directly from DataFrame
                con.execute("INSERT INTO historical_data SELECT * FROM df")
                
                # After storing the data, verify what we've stored
                stored_count = con.execute("""
                    SELECT COUNT(*), MIN(timestamp)::DATE, MAX(timestamp)::DATE 
                    FROM historical_data 
                    WHERE token = ?
                """, [token_info['token']]).fetchone()
                
                logger.info(f"Stored {len(string_records)} records for {token_info['symbol']}. " 
                           f"DB now has {stored_count[0]} records from {stored_count[1]} to {stored_count[2]}")
                
                return True
            else:
                logger.warning(f"No valid records found for {token_info['symbol']}")
                return False
                
        except Exception as e:
            logger.error(f"Storage failed for {token_info['symbol']}: {str(e)}")
            logger.exception("Full traceback:")
            return False
        finally:
            if con:
                con.close()

    def download_spot_data(self, connector, token_info: Dict[str, Any]) -> bool:
        """Download historical spot data for a token"""
        try:
            token, symbol, name, exchange = token_info
            
            # Get current date in IST
            current_date = datetime.now(IST)
            
            # Get previous trading day
            prev_day = self._get_previous_trading_day(current_date.date())
            
            params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": "ONE_DAY",
                "fromdate": prev_day.strftime("%Y-%m-%d 09:00"),
                "todate": prev_day.strftime("%Y-%m-%d 15:30")
            }
            
            historical_data = self._get_candle_data_with_retry(connector, params)
            if historical_data and self._store_historical_data(historical_data, token_info):
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error downloading spot data for {token_info[1]}: {e}")
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

    def fetch_and_store_historical_data(self, connector) -> bool:
        """Main function to fetch and store historical data for all tokens"""
        try:
            # Check if token data is current
            if not self.token_manager.is_market_data_current():
                logger.info("Token data not current, refreshing tokens first...")
                if not self.token_manager.download_and_store_tokens():
                    logger.error("Failed to refresh token data")
                    return False

            # Get all spot tokens
            con = duckdb.connect(self.db_file)
            spot_tokens = con.execute("""
                SELECT token, symbol, name, exch_seg 
                FROM tokens 
                WHERE token_type = 'SPOT'
            """).fetchall()
            
            logger.info(f"Found {len(spot_tokens)} SPOT tokens")
            logger.debug(f"First few tokens: {spot_tokens[:3]}")
            
            success_count = 0
            error_count = 0
            
            # Process each token
            for token_info in spot_tokens:
                token, symbol = token_info[0], token_info[1]
                logger.debug(f"Processing token_info: {token_info}")
                
                # Skip if data is current
                if self._is_historical_data_current(con, token):
                    logger.info(f"Historical data for {symbol} is current, skipping...")
                    continue
                    
                logger.info(f"Fetching historical data for {symbol}")
                if self._download_token_data(connector, token_info):
                    success_count += 1
                else:
                    error_count += 1
                    
            logger.info(f"\nHistorical Data Download Summary:")
            logger.info(f"- Successfully processed: {success_count}")
            logger.info(f"- Failed to process: {error_count}")
            
            return success_count > 0 or error_count == 0
            
        except Exception as e:
            logger.error(f"Error in historical data collection: {e}")
            logger.exception("Detailed traceback:")
            return False
        finally:
            if con:
                con.close()

    def _download_token_data(self, connector, token_info: Tuple) -> bool:
        """Download historical data for a single token"""
        try:
            # Add detailed logging of token_info
            logger.debug(f"Token info received: {token_info}")
            logger.debug(f"Token info type: {type(token_info)}")
            
            # Unpack tuple correctly
            token, symbol, name, exchange = token_info
            
            logger.info(f"Processing token data: token={token}, symbol={symbol}, name={name}, exchange={exchange}")
            
            # Get previous trading day
            today = datetime.now(IST).date()
            if today.year > datetime.now().year:
                today = datetime.now().date()  # Correct the year if it's in the future
            
            prev_day = self._get_previous_trading_day(today)
            logger.debug(f"Previous trading day: {prev_day}")
            
            # For technical indicators, we need at least 30-60 days of data
            # Calculate start date (60 trading days ~ 84 calendar days to account for weekends/holidays)
            start_day = prev_day - timedelta(days=84)
            
            # Skip weekends for start day
            while start_day.weekday() >= 5:
                start_day = start_day - timedelta(days=1)
            
            params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": "ONE_DAY",
                "fromdate": start_day.strftime("%Y-%m-%d 09:00"),
                "todate": prev_day.strftime("%Y-%m-%d 15:30")
            }
            logger.debug(f"API parameters: {params}")
            
            historical_data = self._get_candle_data_with_retry(connector, params)
            if not historical_data:
                logger.error(f"Failed to get historical data for {symbol}")
                return False
            
            # Convert token_info tuple to dict for _store_historical_data
            token_info_dict = {
                'token': token,
                'symbol': symbol,
                'name': name,
                'exchange': exchange,
                'token_type': 'SPOT'  # Add token_type for SPOT data
            }
            
            # Validate data before storing
            if 'data' in historical_data and len(historical_data['data']) > 0:
                logger.debug(f"Retrieved {len(historical_data['data'])} candles for {symbol}")
                return self._store_historical_data(historical_data, token_info_dict)
            else:
                logger.warning(f"No historical data records found for {symbol}")
                return False
            
        except Exception as e:
            logger.error(f"Error downloading data for {token_info[1]}: {str(e)}")
            logger.exception("Detailed traceback:")
            return False

    def _is_historical_data_current(self, con, token: str) -> bool:
        """Check if we already have current historical data for this token"""
        try:
            # Get latest trading day (exclude weekends and holidays)
            now = datetime.now(IST)
            current_date = now.date()
            
            # If it's before market close time (15:30), we consider previous trading day as latest
            if now.time() < datetime.strptime("15:30:00", "%H:%M:%S").time():
                current_date = self._get_previous_trading_day(current_date)
            
            # Format date as string for correct comparison
            date_str = current_date.strftime("%Y-%m-%d")
            
            # Check if we have data for the latest trading day using correct DuckDB syntax
            result = con.execute("""
                SELECT COUNT(*) 
                FROM historical_data 
                WHERE token = ? 
                AND CAST(timestamp AS DATE) = ?
            """, [token, date_str]).fetchone()[0]
            
            return result > 0
        except Exception as e:
            logger.error(f"Error checking if historical data is current: {e}")
            return False

    def _get_previous_trading_day(self, date):
        """Get the previous trading day, skipping weekends"""
        # Ensure we're working with valid dates
        current_year = datetime.now().year
        if date.year > current_year:
            logger.warning(f"Future date detected ({date}), using current date instead")
            return self._get_previous_trading_day(datetime.now().date())
        if date.year < 1992:
            logger.warning(f"Invalid historical date ({date}), using 1992-01-01 instead")
            return datetime(1992, 1, 1).date()
        
        prev_day = date - timedelta(days=1)
        # Skip weekends (5=Saturday, 6=Sunday)
        while prev_day.weekday() >= 5:
            prev_day = prev_day - timedelta(days=1)
        return prev_day