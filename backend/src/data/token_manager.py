import os
import requests
import duckdb
import pytz
import pandas as pd
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
            
            # Check if table exists instead of dropping it
            table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'tokens'").fetchone()[0] > 0
            
            # Only create if it doesn't exist
            if not table_exists:
                con.execute("""
                    CREATE TABLE tokens (
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
                        token_type VARCHAR,  -- 'SPOT', 'FUTURES', or 'OPTIONS'
                        download_timestamp TIMESTAMP
                    )
                """)
                logger.info("Tokens table created successfully")
            else:
                logger.info("Tokens table already exists")
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
            
            logger.info(f"Checking if market data is current in database: {self.db_file}")
            
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
            
            logger.info(f"Current date: {current_date}")
            logger.info(f"Market open time: {market_open_time}")
            logger.info(f"Last download date: {last_download.date()}")
            logger.info(f"Is last download same as current date? {last_download.date() == current_date}")
            logger.info(f"Is last download after market open? {last_download >= market_open_time}")
            
            is_current = (last_download.date() == current_date and last_download >= market_open_time)
            logger.info(f"Is market data current? {is_current}")
            
            return is_current
            
        except Exception as e:
            logger.error(f"Error checking if market data is current: {e}")
            # Print stack trace for debugging
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            if con:
                con.close()

    def download_and_store_tokens(self) -> bool:
        """
        Download and store relevant tokens:
        1. Future tokens for nearest expiry
        2. Corresponding spot stock tokens
        3. Option tokens for the same stocks
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.is_market_data_current():
                logger.info("Market data is already current. Skipping download.")
                return True
            
            logger.info("Downloading token data from Angel Broking...")
            response = requests.get(ANGEL_API_URL)
            response.raise_for_status()
            tokens_data = response.json()
            logger.info(f"Downloaded {len(tokens_data)} tokens from API")
            
            # Convert to DataFrame
            df = pd.DataFrame(tokens_data)
            current_time = datetime.now(IST).replace(tzinfo=None)
            
            # Add download timestamp
            df['download_timestamp'] = current_time
            
            # Convert expiry to datetime for comparison
            df['expiry_date'] = pd.to_datetime(df['expiry'], format='%d%b%Y', errors='coerce')
            
            # Step 1: Get Future tokens for nearest expiry
            logger.info("Processing futures tokens...")
            futures_mask = (
                (df['exch_seg'] == 'NFO') & 
                (df['instrumenttype'] == 'FUTSTK') &
                (df['expiry_date'] >= pd.Timestamp.now())
            )
            futures_df = df[futures_mask].copy()  # Create a copy to avoid warnings
            
            if futures_df.empty:
                logger.error("No future tokens found")
                return False
                
            # Get nearest expiry
            min_expiry = futures_df['expiry_date'].min()
            futures_df = futures_df[futures_df['expiry_date'] == min_expiry]
            futures_df['token_type'] = 'FUTURES'
            logger.info(f"Found {len(futures_df)} futures for expiry {min_expiry.strftime('%d%b%Y')}")
            
            # Step 2: Get corresponding spot tokens
            logger.info("Processing spot tokens...")
            stock_names = futures_df['name'].unique()
            spot_mask = (
                (df['exch_seg'] == 'NSE') & 
                (df['name'].isin(stock_names)) & 
                (df['symbol'].str.endswith('-EQ', na=False))
            )
            spot_df = df[spot_mask].copy()  # Create a copy to avoid warnings
            spot_df['token_type'] = 'SPOT'
            logger.info(f"Found {len(spot_df)} spot tokens")
            
            # Step 3: Get options tokens for nearest expiry
            logger.info("Processing options tokens...")
            options_mask = (
                (df['exch_seg'] == 'NFO') & 
                (df['instrumenttype'] == 'OPTSTK') &
                (df['name'].isin(stock_names)) & 
                (df['expiry_date'] == min_expiry)
            )
            options_df = df[options_mask].copy()  # Create a copy to avoid warnings
            options_df['token_type'] = 'OPTIONS'
            logger.info(f"Found {len(options_df)} options tokens")
            
            # Combine all tokens
            final_df = pd.concat([futures_df, spot_df, options_df], ignore_index=True)
            
            # Format symbols for options
            mask = final_df['instrumenttype'].isin(['OPTSTK', 'OPTIDX'])
            final_df.loc[mask, 'formatted_symbol'] = (
                final_df.loc[mask].apply(
                    lambda x: f"{x['name']}{x['expiry']}{int(float(x['strike'])/100)}{x['symbol'][-2:]}" 
                    if pd.notna(x['strike']) and float(x['strike']) > 0 
                    else x['symbol'],
                    axis=1
                )
            )
            
            # Fill NaN values in formatted_symbol with original symbol
            final_df['formatted_symbol'] = final_df['formatted_symbol'].fillna(final_df['symbol'])
            
            # Drop the temporary expiry_date column and ensure proper data types
            final_df = final_df.drop(columns=['expiry_date'])
            final_df['strike'] = pd.to_numeric(final_df['strike'], errors='coerce')
            final_df['tick_size'] = pd.to_numeric(final_df['tick_size'], errors='coerce')
            
            # Convert columns to match database schema
            columns = [
                'token', 'symbol', 'formatted_symbol', 'name', 'expiry', 
                'strike', 'lotsize', 'instrumenttype', 'exch_seg', 'tick_size',
                'token_type', 'download_timestamp'
            ]
            final_df = final_df[columns]  # Ensure correct column order
            
            # Store in database
            con = duckdb.connect(self.db_file)
            con.execute("TRUNCATE TABLE tokens")
            
            # Convert DataFrame to DuckDB table
            con.execute("""
                CREATE TEMP TABLE temp_tokens (
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
                    token_type VARCHAR,
                    download_timestamp TIMESTAMP
                )
            """)
            con.execute("INSERT INTO temp_tokens SELECT * FROM final_df")
            con.execute("INSERT INTO tokens SELECT * FROM temp_tokens")
            con.execute("DROP TABLE temp_tokens")
            
            # Log summary by token type
            summary = final_df.groupby('token_type').size()
            logger.info("\nToken processing summary:")
            for type_, count in summary.items():
                logger.info(f"- {type_}: {count} tokens")
            
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error downloading tokens: {e}")
            return False
        except Exception as e:
            logger.error(f"Error storing tokens: {e}")
            return False

    def connect(self):
        """Create and return a database connection"""
        try:
            return duckdb.connect(self.db_file)
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

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