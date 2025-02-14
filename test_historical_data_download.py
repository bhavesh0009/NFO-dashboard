import os
import sys
import traceback
from logzero import logger, setup_logger
from SmartApi import SmartConnect
import pyotp
from dotenv import load_dotenv
import duckdb
import logzero

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.data.token_manager import TokenManager
from src.data.historical_data_manager import HistoricalDataManager
from src.utils.truncate_tables import truncate_tables

# Set the logger level to INFO
setup_logger(name=__name__, level=logzero.INFO)

def limit_tokens_by_type(token_manager: TokenManager, token_type: str, limit: int = 5):
    """Get limited number of tokens by type."""
    con = None
    try:
        con = duckdb.connect(token_manager.db_file)
        result = con.execute(f"""
            SELECT 
                token,
                symbol,
                name,
                expiry,
                token_type
            FROM tokens
            WHERE token_type = ?
            LIMIT ?
        """, [token_type, limit]).fetchall()
        
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
    finally:
        if con:
            con.close()

class TestHistoricalDataManager(HistoricalDataManager):
    """A test version of HistoricalDataManager that limits tokens to 5"""
    def get_tokens_by_type(self, token_type: str) -> list:
        """Override to return only 5 tokens"""
        return limit_tokens_by_type(self.token_manager, token_type)

def test_spot_data_download():
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
        historical_manager = TestHistoricalDataManager(token_manager)  # Use the test version
        
        # First ensure we have current token data
        logger.info("Refreshing token data...")
        if not token_manager.download_and_store_tokens():
            logger.error("❌ Failed to refresh token data")
            return
            
        # Test spot data download
        logger.info("\nTesting spot data download (5 tokens)...")
        
        # First, let's see which 5 tokens we're going to download
        spot_tokens = historical_manager.get_tokens_by_type("SPOT")
        logger.info("Selected tokens for download:")
        for token in spot_tokens:
            logger.info(f"- {token['symbol']} ({token['token']})")
        
        spot_success = historical_manager.download_spot_data(connector)
        if spot_success:
            logger.info("✅ Spot data downloaded successfully")
        else:
            logger.error("❌ Failed to download spot data")
        
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
            logger.info(f"- Unique tokens: {spot_stats[0]} (should be exactly 5)")
            logger.info(f"- Total records: {spot_stats[1]}")
            logger.info(f"- Date range: {spot_stats[2]} to {spot_stats[3]}")
            
            # Print detailed data for each token
            logger.info("\nDetailed Data Verification:")
            tokens_data = con.execute("""
                SELECT 
                    token,
                    symbol,
                    COUNT(*) as records,
                    MIN(timestamp) as first_date,
                    MAX(timestamp) as last_date,
                    MIN(close) as min_close,
                    MAX(close) as max_close,
                    SUM(volume) as total_volume
                FROM historical_data
                WHERE token_type = 'SPOT'
                GROUP BY token, symbol
                ORDER BY symbol
            """).fetchall()
            
            for row in tokens_data:
                logger.info(f"\nToken: {row[1]} ({row[0]})")
                logger.info(f"- Records: {row[2]:,}")
                logger.info(f"- Date Range: {row[3]} to {row[4]}")
                logger.info(f"- Price Range: {row[5]:,.2f} to {row[6]:,.2f}")
                logger.info(f"- Total Volume: {row[7]:,}")
                
                # Sample some actual data points
                sample_data = con.execute("""
                    SELECT timestamp, open, high, low, close, volume
                    FROM historical_data
                    WHERE token = ?
                    ORDER BY timestamp DESC
                    LIMIT 3
                """, [row[0]]).fetchall()
                
                logger.info("- Recent Data Points:")
                for point in sample_data:
                    logger.info(f"  {point[0]}: O={point[1]:.2f} H={point[2]:.2f} L={point[3]:.2f} C={point[4]:.2f} V={point[5]:,}")
            
        except Exception as e:
            logger.error(f"Error verifying data: {e}")
            logger.error(traceback.format_exc())
        finally:
            if con:
                con.close()
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_spot_data_download() 