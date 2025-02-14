import os
import sys
from logzero import logger
from dotenv import load_dotenv

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.data.token_manager import TokenManager
from src.utils.truncate_tables import truncate_tables

def test_token_filtering():
    try:
        # Load environment variables
        load_dotenv()
        
        # First truncate the tables
        logger.info("Truncating existing tables...")
        truncate_tables()
        
        # Initialize token manager
        logger.info("Initializing token manager...")
        token_manager = TokenManager()
        
        # Download and store tokens
        logger.info("Downloading and storing tokens...")
        if token_manager.download_and_store_tokens():
            logger.info("✅ Token download and storage completed successfully")
            
            # Verify the data
            con = token_manager.connect()
            
            # Check futures tokens
            futures = con.execute("""
                SELECT COUNT(DISTINCT name) as stock_count,
                       COUNT(*) as total_count
                FROM tokens
                WHERE instrumenttype = 'FUTSTK'
            """).fetchone()
            logger.info(f"\nFutures Tokens:")
            logger.info(f"- Unique stocks: {futures[0]}")
            logger.info(f"- Total futures: {futures[1]}")
            
            # Check spot tokens
            spots = con.execute("""
                SELECT COUNT(*) as count
                FROM tokens
                WHERE exch_seg = 'NSE'
                AND symbol LIKE '%-EQ'
            """).fetchone()
            logger.info(f"\nSpot Tokens:")
            logger.info(f"- Total spots: {spots[0]}")
            
            # Check options tokens
            options = con.execute("""
                SELECT COUNT(DISTINCT name) as stock_count,
                       COUNT(*) as total_count
                FROM tokens
                WHERE instrumenttype = 'OPTSTK'
            """).fetchone()
            logger.info(f"\nOptions Tokens:")
            logger.info(f"- Unique stocks: {options[0]}")
            logger.info(f"- Total options: {options[1]}")
            
            # Verify all spots have corresponding futures
            missing_futures = con.execute("""
                SELECT t1.name, t1.symbol
                FROM tokens t1
                WHERE t1.exch_seg = 'NSE'
                AND t1.symbol LIKE '%-EQ'
                AND NOT EXISTS (
                    SELECT 1
                    FROM tokens t2
                    WHERE t2.instrumenttype = 'FUTSTK'
                    AND t2.name = t1.name
                )
            """).fetchall()
            
            if missing_futures:
                logger.warning("\nSpot stocks missing futures:")
                for row in missing_futures:
                    logger.warning(f"- {row[0]} ({row[1]})")
            else:
                logger.info("\n✅ All spot stocks have corresponding futures")
            
            con.close()
        else:
            logger.error("❌ Token download and storage failed")
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_token_filtering() 