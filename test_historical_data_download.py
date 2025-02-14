import os
import sys
from logzero import logger
from SmartApi import SmartConnect
import pyotp
from dotenv import load_dotenv
import duckdb

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.data.token_manager import TokenManager
from src.data.historical_data_manager import HistoricalDataManager
from src.utils.truncate_tables import truncate_tables

def test_historical_data_download():
    try:
        # Load environment variables
        load_dotenv()
        
        # First truncate the tables
        logger.info("Truncating existing tables...")
        truncate_tables()
        
        # Create Angel One connector instance
        connector = SmartConnect(os.getenv('ANGEL_ONE_APP_KEY'))
        totp = pyotp.TOTP(os.getenv('ANGEL_ONE_TOTP_SECRET'))
        
        # Generate session
        logger.info("Generating session...")
        data = connector.generateSession(
            os.getenv('ANGEL_ONE_CLIENT_ID'),
            os.getenv('ANGEL_ONE_PIN'),
            totp.now()
        )
        
        if not data.get('status'):
            logger.error(f"Failed to generate session: {data.get('message', 'Unknown error')}")
            return
            
        logger.info("✅ Session generated successfully")
        
        # Initialize managers
        token_manager = TokenManager()
        historical_manager = HistoricalDataManager(token_manager)
        
        # First ensure we have current token data
        logger.info("Refreshing token data...")
        if not token_manager.download_and_store_tokens():
            logger.error("❌ Failed to refresh token data")
            return
        
        # Test spot data download
        logger.info("\nTesting spot data download...")
        spot_success = historical_manager.download_spot_data(connector)
        if spot_success:
            logger.info("✅ Spot data downloaded successfully")
        else:
            logger.error("❌ Failed to download spot data")
        
        # Test futures data download
        logger.info("\nTesting futures data download...")
        futures_success = historical_manager.download_futures_data(connector)
        if futures_success:
            logger.info("✅ Futures data downloaded successfully")
        else:
            logger.error("❌ Failed to download futures data")
        
        # Test options data download
        logger.info("\nTesting options data download...")
        options_success = historical_manager.download_options_data(connector)
        if options_success:
            logger.info("✅ Options data downloaded successfully")
        else:
            logger.error("❌ Failed to download options data")
        
        # Verify the data in database
        con = None
        try:
            con = duckdb.connect(historical_manager.db_file)
            
            # Check spot data
            spot_stats = con.execute("""
                SELECT 
                    COUNT(DISTINCT token) as unique_tokens,
                    COUNT(*) as total_records,
                    MIN(timestamp) as earliest_date,
                    MAX(timestamp) as latest_date
                FROM historical_data
                WHERE token_type = 'SPOT'
            """).fetchone()
            
            logger.info("\nSpot Data Statistics:")
            logger.info(f"- Unique tokens: {spot_stats[0]}")
            logger.info(f"- Total records: {spot_stats[1]}")
            logger.info(f"- Date range: {spot_stats[2]} to {spot_stats[3]}")
            
            # Check futures data
            futures_stats = con.execute("""
                SELECT 
                    COUNT(DISTINCT token) as unique_tokens,
                    COUNT(*) as total_records,
                    MIN(timestamp) as earliest_date,
                    MAX(timestamp) as latest_date
                FROM historical_data
                WHERE token_type = 'FUTURES'
            """).fetchone()
            
            logger.info("\nFutures Data Statistics:")
            logger.info(f"- Unique tokens: {futures_stats[0]}")
            logger.info(f"- Total records: {futures_stats[1]}")
            logger.info(f"- Date range: {futures_stats[2]} to {futures_stats[3]}")
            
            # Check options data
            options_stats = con.execute("""
                SELECT 
                    COUNT(DISTINCT token) as unique_tokens,
                    COUNT(*) as total_records,
                    MIN(timestamp) as earliest_date,
                    MAX(timestamp) as latest_date
                FROM historical_data
                WHERE token_type = 'OPTIONS'
            """).fetchone()
            
            logger.info("\nOptions Data Statistics:")
            logger.info(f"- Unique tokens: {options_stats[0]}")
            logger.info(f"- Total records: {options_stats[1]}")
            logger.info(f"- Date range: {options_stats[2]} to {options_stats[3]}")
            
        except Exception as e:
            logger.error(f"Error verifying data: {e}")
        finally:
            if con:
                con.close()
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_historical_data_download() 