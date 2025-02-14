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

def test_latest_market_data():
    try:
        # Initialize managers
        token_manager = TokenManager()
        indicator_manager = TechnicalIndicatorManager()
        
        # First download and store tokens
        logger.info("Downloading and storing tokens...")
        if not token_manager.download_and_store_tokens():
            logger.error("Failed to download and store tokens")
            return
            
        # Verify token data
        con = None
        try:
            con = duckdb.connect(token_manager.db_file)
            token_stats = con.execute("""
                SELECT 
                    token_type,
                    COUNT(*) as count
                FROM tokens
                GROUP BY token_type
                ORDER BY token_type
            """).fetchall()
            
            logger.info("\nToken Statistics:")
            for token_type, count in token_stats:
                logger.info(f"- {token_type}: {count} tokens")
        finally:
            if con:
                con.close()
        
        # Calculate technical indicators for all tokens
        logger.info("\nCalculating technical indicators for all tokens...")
        if not indicator_manager.calculate_all_indicators():
            logger.error("Failed to calculate technical indicators")
            return
        
        # Update latest market data
        logger.info("\nUpdating latest market data...")
        if not indicator_manager.update_latest_market_data():
            logger.error("Failed to update latest market data")
            return
        
        # Verify the data
        con = None
        try:
            con = duckdb.connect(indicator_manager.db_file)
            
            # Get summary statistics
            stats = con.execute("""
                SELECT 
                    COUNT(*) as record_count,
                    COUNT(DISTINCT token_type) as token_types,
                    MIN(date) as data_date,
                    MAX(date) as data_date,
                    COUNT(CASE WHEN breakout_detected THEN 1 END) as breakouts,
                    COUNT(CASE WHEN ma_200_distance > 0 THEN 1 END) as above_ma200,
                    COUNT(CASE WHEN rsi_14 > 70 THEN 1 END) as overbought,
                    COUNT(CASE WHEN rsi_14 < 30 THEN 1 END) as oversold
                FROM latest_market_data
            """).fetchone()
            
            logger.info("\nLatest Market Data Statistics:")
            logger.info(f"- Total Records: {stats[0]}")
            logger.info(f"- Token Types: {stats[1]}")
            logger.info(f"- Data Date: {stats[2]}")
            logger.info(f"- Breakout Signals: {stats[4]}")
            logger.info(f"- Above 200 MA: {stats[5]}")
            logger.info(f"- Overbought (RSI > 70): {stats[6]}")
            logger.info(f"- Oversold (RSI < 30): {stats[7]}")
            
            # Sample some records
            sample_data = con.execute("""
                SELECT 
                    symbol,
                    name,
                    lotsize,
                    close,
                    volume,
                    ma_200_distance,
                    rsi_14,
                    macd,
                    breakout_detected
                FROM latest_market_data
                ORDER BY volume DESC
                LIMIT 5
            """).fetchall()
            
            logger.info("\nTop 5 Stocks by Volume:")
            for row in sample_data:
                logger.info(f"\nStock: {row[0]} ({row[1]})")
                logger.info(f"- Lot Size: {row[2]}")
                logger.info(f"- Close: {row[3]:.2f}")
                logger.info(f"- Volume: {row[4]:,}")
                logger.info(f"- MA200 Distance: {row[5]:.2f}%")
                logger.info(f"- RSI: {row[6]:.2f}")
                logger.info(f"- MACD: {row[7]:.2f}")
                logger.info(f"- Breakout: {row[8]}")
            
            # Check for potential trading signals
            signals = con.execute("""
                SELECT 
                    symbol,
                    name,
                    close,
                    volume,
                    volume_ratio,
                    rsi_14,
                    ma_200_distance
                FROM latest_market_data
                WHERE 
                    breakout_detected 
                    OR (volume > volume_15d_avg * 2 AND close > high_21d)
                    OR (rsi_14 < 30 AND ma_200_distance > -5)
                ORDER BY volume DESC
                LIMIT 5
            """).fetchall()
            
            if signals:
                logger.info("\nPotential Trading Signals:")
                for row in signals:
                    logger.info(f"\nStock: {row[0]} ({row[1]})")
                    logger.info(f"- Close: {row[2]:.2f}")
                    logger.info(f"- Volume: {row[3]:,}")
                    logger.info(f"- Volume Ratio: {row[4]:.2f}")
                    logger.info(f"- RSI: {row[5]:.2f}")
                    logger.info(f"- MA200 Distance: {row[6]:.2f}%")
            else:
                logger.info("\nNo trading signals detected")
            
        finally:
            if con:
                con.close()
                
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_latest_market_data() 