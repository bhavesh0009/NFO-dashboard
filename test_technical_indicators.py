import os
import sys
import traceback
from logzero import logger, setup_logger
import duckdb
import logzero
from datetime import datetime, timedelta
import pytz

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.data.technical_indicators import TechnicalIndicatorManager
from src.data.token_manager import TokenManager

# Set the logger level to INFO
setup_logger(name=__name__, level=logzero.INFO)

def test_technical_indicators():
    try:
        # Initialize managers
        token_manager = TokenManager()
        indicator_manager = TechnicalIndicatorManager()
        
        # Get 5 spot tokens for testing
        con = None
        try:
            con = duckdb.connect(token_manager.db_file)
            spot_tokens = con.execute("""
                SELECT DISTINCT 
                    h.token,
                    h.symbol,
                    COUNT(*) as record_count
                FROM historical_data h
                WHERE h.token_type = 'SPOT'
                GROUP BY h.token, h.symbol
                HAVING COUNT(*) > 200  -- Ensure we have enough data for MA calculation
                LIMIT 5
            """).fetchall()
            
            if not spot_tokens:
                logger.error("No spot tokens found with sufficient historical data")
                return
                
            logger.info("\nSelected tokens for technical analysis:")
            for token, symbol, count in spot_tokens:
                logger.info(f"- {symbol} ({token}): {count} records")
            
            # Calculate indicators for each token
            for token, symbol, _ in spot_tokens:
                logger.info(f"\nCalculating indicators for {symbol}...")
                if indicator_manager.calculate_indicators(token):
                    # Verify the calculations
                    latest = indicator_manager.get_latest_indicators(token)
                    if latest:
                        logger.info(f"\nLatest indicators for {symbol}:")
                        logger.info(f"- Date: {latest['date']}")
                        logger.info(f"- 200-day MA Distance: {latest['ma_200_distance']:.2f}%")
                        logger.info(f"- 21-day High/Low: {latest['high_21d']:.2f}/{latest['low_21d']:.2f}")
                        logger.info(f"- 52-week High/Low: {latest['high_52w']:.2f}/{latest['low_52w']:.2f}")
                        logger.info(f"- ATH/ATL: {latest['ath']:.2f}/{latest['atl']:.2f}")
                        logger.info(f"- 15-day Avg Volume: {latest['volume_15d_avg']:,.0f}")
                        logger.info(f"- Volume Ratio: {latest['volume_ratio']:.2f}")
                        logger.info(f"- Breakout Detected: {latest['breakout_detected']}")
                    else:
                        logger.error(f"No indicators found for {symbol}")
                else:
                    logger.error(f"Failed to calculate indicators for {symbol}")
            
            # Verify overall statistics
            stats = con.execute("""
                SELECT 
                    COUNT(DISTINCT token) as token_count,
                    COUNT(*) as total_records,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    COUNT(CASE WHEN breakout_detected THEN 1 END) as breakout_count
                FROM technical_indicators
            """).fetchone()
            
            logger.info("\nOverall Technical Indicators Statistics:")
            logger.info(f"- Tokens processed: {stats[0]}")
            logger.info(f"- Total records: {stats[1]}")
            logger.info(f"- Date range: {stats[2]} to {stats[3]}")
            logger.info(f"- Breakout signals detected: {stats[4]}")
            
        finally:
            if con:
                con.close()
                
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_technical_indicators() 