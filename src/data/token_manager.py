import os
import requests
import duckdb
import pytz
from datetime import datetime
from logzero import logger
from typing import List, Dict, Any, Optional

# Constants
ANGEL_API_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
IST = pytz.timezone('Asia/Kolkata')
MARKET_OPEN_TIME = "09:15:00"

class TokenManager:
    def __init__(self):
        """Initialize the TokenManager with database configuration."""
        self.db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
        self.setup_database()

    def setup_database(self) -> None:
        """Create the tokens table if it doesn't exist"""
        con = None
        try:
            # Create new connection and table
            con = duckdb.connect(self.db_file)
            con.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    token VARCHAR,
                    symbol VARCHAR,
                    formatted_symbol VARCHAR,
                    name VARCHAR,
                    expiry VARCHAR,
                    strike DOUBLE,
                    lotsize VARCHAR,
                    instrumenttype VARCHAR,
                    exch_seg VARCHAR,
                    tick_size DOUBLE,
                    download_timestamp TIMESTAMP
                )
            """)
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            raise
        finally:
            if con:
                con.close()

    def is_market_data_current(self) -> bool:
        """Check if we already have current market data"""
        try:
            con = duckdb.connect(self.db_file)
            
            result = con.execute("""
                SELECT MAX(download_timestamp) as last_download,
                       COUNT(*) as total_records 
                FROM tokens
            """).fetchone()
            
            if result[0] is None:
                logger.info("No existing records found in database")
                return False
                
            last_download = result[0]
            total_records = result[1]
            logger.info(f"Found {total_records} records with last download at {last_download}")
            
            current_date = datetime.now(IST).replace(tzinfo=None).date()
            market_open_time = datetime.strptime(
                f"{current_date} {MARKET_OPEN_TIME}", 
                "%Y-%m-%d %H:%M:%S"
            )
            
            return (last_download.date() == current_date and 
                    last_download >= market_open_time)
            
        except Exception as e:
            logger.error(f"Error checking market data currency: {e}")
            return False
        finally:
            if con:
                con.close()

    def download_and_store_tokens(self) -> bool:
        """
        Download tokens from Angel Broking and store them in the database
        Returns:
            bool: True if successful, False otherwise
        """
        con = None
        try:
            if self.is_market_data_current():
                logger.info("Market data is already current. Skipping download.")
                return True
            
            logger.info("Downloading token data from Angel Broking...")
            response = requests.get(ANGEL_API_URL)
            response.raise_for_status()
            tokens_data = response.json()
            logger.info(f"Downloaded {len(tokens_data)} tokens from API")
            
            current_time = datetime.now(IST).replace(tzinfo=None)
            
            con = duckdb.connect(self.db_file)
            logger.info("Truncating existing data from tokens table...")
            con.execute("TRUNCATE TABLE tokens")
            
            inserted_count = 0
            error_count = 0
            
            for token in tokens_data:
                try:
                    formatted_expiry = token.get('expiry', '')
                    option_type = token['symbol'][-2:] if token.get('instrumenttype') in ['OPTSTK', 'OPTIDX'] else ''
                    strike = float(token.get('strike', 0))
                    strike_price = int(strike/100) if strike > 0 else 0
                    
                    formatted_symbol = token['symbol']
                    if formatted_expiry and option_type and strike_price > 0:
                        formatted_symbol = (f"{token['name']}{formatted_expiry}"
                                         f"{strike_price}{option_type}")
                    
                    con.execute("""
                        INSERT INTO tokens 
                        (token, symbol, formatted_symbol, name, expiry, strike, 
                         lotsize, instrumenttype, exch_seg, tick_size, 
                         download_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        token['token'], token['symbol'], formatted_symbol, 
                        token.get('name', ''), token.get('expiry', ''), float(token.get('strike', 0)), 
                        token.get('lotsize', ''), token.get('instrumenttype', ''), 
                        token.get('exch_seg', ''), float(token.get('tick_size', 0)), 
                        current_time
                    ])
                    inserted_count += 1
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error inserting token {token.get('token', 'unknown')}: {e}")
                    continue
                
            final_count = con.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
            
            logger.info(f"\nToken processing summary:")
            logger.info(f"- Total tokens downloaded: {len(tokens_data)}")
            logger.info(f"- Successfully inserted: {inserted_count}")
            logger.info(f"- Failed to insert: {error_count}")
            logger.info(f"- Total records in database: {final_count}")
            
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error downloading tokens: {e}")
            return False
        except Exception as e:
            logger.error(f"Error storing tokens: {e}")
            return False
        finally:
            if con:
                con.close()

if __name__ == "__main__":
    try:
        token_manager = TokenManager()
        success = token_manager.download_and_store_tokens()
        if success:
            logger.info("Token download and storage completed successfully")
        else:
            logger.error("Token download and storage failed")
    except Exception as e:
        logger.error(f"Error in main execution: {e}") 