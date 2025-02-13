import os
import duckdb
import pytz
from datetime import datetime, timedelta
from logzero import logger
from typing import List, Dict, Any, Optional
from src.data.token_manager import TokenManager

# Constants
IST = pytz.timezone('Asia/Kolkata')

class HistoricalDataManager:
    def __init__(self, token_manager: TokenManager):
        """Initialize the HistoricalDataManager with database configuration."""
        self.db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
        self.token_manager = token_manager
        self.setup_database()

    def setup_database(self) -> None:
        """Create the historical_data table if it doesn't exist"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            con.execute("""
                CREATE TABLE IF NOT EXISTS historical_data (
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

    def get_nfo_tokens(self) -> List[Dict[str, Any]]:
        """Get list of NFO tokens for futures from the tokens table"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Get current date in IST
            current_date = datetime.now(IST).date()
            
            # Query to get NFO tokens for nearest expiry
            result = con.execute("""
                WITH parsed_dates AS (
                    SELECT 
                        token,
                        symbol,
                        name,
                        expiry,
                        strptime(expiry, '%d%b%Y') as expiry_date
                    FROM tokens
                    WHERE instrumenttype = 'OPTIDX'
                    AND exch_seg = 'NFO'
                ),
                nearest_expiry AS (
                    SELECT MIN(expiry_date) as expiry_date
                    FROM parsed_dates
                    WHERE expiry_date >= CURRENT_DATE
                )
                SELECT 
                    pd.token,
                    pd.symbol,
                    pd.name,
                    pd.expiry
                FROM parsed_dates pd
                JOIN nearest_expiry ne 
                ON pd.expiry_date = ne.expiry_date
                LIMIT 5  -- Start with a small subset for testing
            """).fetchall()
            
            if result:
                logger.info(f"Found {len(result)} NFO tokens for nearest expiry")
                for row in result:
                    logger.debug(f"Token: {row[0]}, Symbol: {row[1]}, Name: {row[2]}, Expiry: {row[3]}")
            else:
                logger.warning("No NFO tokens found")
            
            return [
                {
                    "token": row[0],
                    "symbol": row[1],
                    "name": row[2],
                    "expiry": row[3]
                }
                for row in result
            ]
            
        except Exception as e:
            logger.error(f"Error getting NFO tokens: {e}")
            return []
        finally:
            if con:
                con.close()

    def is_data_current(self) -> bool:
        """Check if we already have current market data"""
        try:
            con = duckdb.connect(self.db_file)
            
            # Get current date in IST for comparison
            current_date = datetime.now(IST).date()
            
            result = con.execute("""
                SELECT 
                    MAX(download_timestamp) as last_download,
                    COUNT(*) as total_records 
                FROM historical_data
                WHERE timestamp::DATE = ?
            """, [current_date]).fetchone()
            
            if result[0] is None:
                logger.info("No historical data records found for today")
                return False
                
            last_download = result[0]
            total_records = result[1]
            logger.info(f"Found {total_records} historical records with last download at {last_download}")
            
            # Check if data is from today
            return last_download.date() == current_date
            
        except Exception as e:
            logger.error(f"Error checking historical data currency: {e}")
            return False
        finally:
            if con:
                con.close()

    def fetch_and_store_historical_data(self, connector) -> bool:
        """
        Fetch historical data for all NFO tokens and store in database
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.token_manager.is_market_data_current():
            logger.info("Token data not current, refreshing tokens first...")
            if not self.token_manager.download_and_store_tokens():
                logger.error("Failed to refresh token data")
                return False

        if self.is_data_current():
            logger.info("Historical data is already current. Skipping download.")
            return True

        con = None
        try:
            con = duckdb.connect(self.db_file)
            nfo_tokens = self.get_nfo_tokens()
            
            if not nfo_tokens:
                logger.error("No NFO tokens found to fetch historical data")
                return False

            logger.info(f"Fetching historical data for {len(nfo_tokens)} tokens")
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)  # Get 1 year of data
            
            success_count = 0
            error_count = 0
            
            for token_info in nfo_tokens:
                try:
                    logger.info(f"Fetching data for {token_info['symbol']}")
                    
                    params = {
                        "exchange": "NFO",
                        "symboltoken": token_info['token'],
                        "interval": "ONE_DAY",
                        "fromdate": start_date.strftime("%Y-%m-%d %H:%M"), 
                        "todate": end_date.strftime("%Y-%m-%d %H:%M")
                    }
                    
                    historical_data = connector.getCandleData(params)
                    
                    if historical_data is not None and 'data' in historical_data:
                        # Convert historical data to list of tuples for bulk insert
                        current_time = datetime.now(IST).replace(tzinfo=None)
                        
                        records = []
                        for candle in historical_data['data']:
                            try:
                                # Parse ISO timestamp and convert to naive datetime
                                timestamp = datetime.fromisoformat(candle[0].replace('Z', '+00:00'))
                                timestamp = timestamp.astimezone(IST).replace(tzinfo=None)
                                
                                # Candle data format: [timestamp, open, high, low, close, volume]
                                records.append((
                                    token_info['token'],
                                    token_info['symbol'],
                                    token_info['name'],
                                    timestamp,
                                    float(candle[1]),  # open
                                    float(candle[2]),  # high
                                    float(candle[3]),  # low
                                    float(candle[4]),  # close
                                    int(candle[5]),    # volume
                                    0,  # oi (not available in candle data)
                                    current_time
                                ))
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Invalid candle data format: {candle}")
                                continue
                        
                        if records:
                            # First delete any existing records for this token
                            con.execute("""
                                DELETE FROM historical_data 
                                WHERE token = ? 
                                AND timestamp::DATE >= ?::DATE
                            """, [token_info['token'], start_date])
                            
                            # Then insert new records
                            con.executemany("""
                                INSERT INTO historical_data 
                                (token, symbol, name, timestamp, open, high, low, close, 
                                 volume, oi, download_timestamp)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, records)
                            
                            success_count += 1
                            logger.debug(f"Successfully stored {len(records)} records for {token_info['symbol']}")
                        else:
                            logger.warning(f"No valid records found for {token_info['symbol']}")
                    else:
                        logger.warning(f"No data returned for {token_info['symbol']}")
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing {token_info['symbol']}: {str(e)}")
                    continue

            logger.info(f"\nHistorical data processing summary:")
            logger.info(f"- Total tokens processed: {len(nfo_tokens)}")
            logger.info(f"- Successfully processed: {success_count}")
            logger.info(f"- Failed to process: {error_count}")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error in historical data processing: {e}")
            return False
        finally:
            if con:
                con.close() 