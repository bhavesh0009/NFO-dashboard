import os
from datetime import datetime, timedelta
import duckdb
import pytz
from logzero import logger
from typing import List, Dict, Any, Optional
import pandas as pd
import pandas_ta as ta

IST = pytz.timezone('Asia/Kolkata')

class TechnicalIndicatorManager:
    def __init__(self, test_connection=None):
        """Initialize the Technical Indicator Manager.
        
        Args:
            test_connection: Optional DuckDB connection for testing
        """
        self.db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
        self.test_connection = test_connection
        self.setup_database()

    def setup_database(self) -> None:
        """Create the technical_indicators table if it doesn't exist"""
        con = None
        try:
            # Use test connection if provided, otherwise create new connection
            con = self.test_connection if self.test_connection else duckdb.connect(self.db_file)
            
            # Drop existing table to handle schema changes
            con.execute("DROP TABLE IF EXISTS technical_indicators")
            
            con.execute("""
                CREATE TABLE technical_indicators (
                    token VARCHAR,
                    symbol VARCHAR,
                    date DATE,
                    ma_200 DOUBLE,
                    ma_50 DOUBLE,
                    ma_20 DOUBLE,
                    ma_200_distance DOUBLE,
                    high_21d DOUBLE,
                    low_21d DOUBLE,
                    high_52w DOUBLE,
                    low_52w DOUBLE,
                    ath DOUBLE,
                    atl DOUBLE,
                    volume_15d_avg DOUBLE,
                    volume_ratio DOUBLE,
                    rsi_14 DOUBLE,
                    macd DOUBLE,
                    macd_signal DOUBLE,
                    macd_hist DOUBLE,
                    bb_upper DOUBLE,
                    bb_middle DOUBLE,
                    bb_lower DOUBLE,
                    breakout_detected BOOLEAN,
                    calculation_timestamp TIMESTAMP,
                    PRIMARY KEY (token, date)
                )
            """)
            
            # Create latest market data table
            con.execute("DROP TABLE IF EXISTS latest_market_data")
            con.execute("""
                CREATE TABLE latest_market_data (
                    token VARCHAR PRIMARY KEY,
                    symbol VARCHAR,
                    name VARCHAR,
                    lotsize VARCHAR,
                    token_type VARCHAR,
                    date DATE,
                    -- OHLCV Data
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    -- Technical Indicators
                    ma_200 DOUBLE,
                    ma_50 DOUBLE,
                    ma_20 DOUBLE,
                    ma_200_distance DOUBLE,
                    high_21d DOUBLE,
                    low_21d DOUBLE,
                    high_52w DOUBLE,
                    low_52w DOUBLE,
                    ath DOUBLE,
                    atl DOUBLE,
                    volume_15d_avg DOUBLE,
                    volume_ratio DOUBLE,
                    rsi_14 DOUBLE,
                    macd DOUBLE,
                    macd_signal DOUBLE,
                    macd_hist DOUBLE,
                    bb_upper DOUBLE,
                    bb_middle DOUBLE,
                    bb_lower DOUBLE,
                    breakout_detected VARCHAR,  -- 'BREAKOUT', 'BREAKDOWN', or NULL
                    last_updated TIMESTAMP
                )
            """)
            
            # Create daily summary table
            con.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary (
                    token VARCHAR PRIMARY KEY,
                    symbol VARCHAR,
                    name VARCHAR,
                    date DATE,
                    -- Price Data
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    -- Technical Indicators
                    ma_200 DOUBLE,
                    ma_50 DOUBLE,
                    ma_20 DOUBLE,
                    ma_200_distance DOUBLE,
                    high_21d DOUBLE,
                    low_21d DOUBLE,
                    high_52w DOUBLE,
                    low_52w DOUBLE,
                    ath DOUBLE,
                    atl DOUBLE,
                    volume_15d_avg DOUBLE,
                    volume_ratio DOUBLE,
                    rsi_14 DOUBLE,
                    macd DOUBLE,
                    macd_signal DOUBLE,
                    macd_hist DOUBLE,
                    bb_upper DOUBLE,
                    bb_middle DOUBLE,
                    bb_lower DOUBLE,
                    breakout_detected VARCHAR,
                    last_updated TIMESTAMP
                )
            """)
            
            logger.info("Technical indicators and latest market data tables created successfully")
        except Exception as e:
            logger.error(f"Error setting up database tables: {e}")
            raise
        finally:
            # Only close if we created a new connection
            if con and not self.test_connection:
                con.close()

    def update_latest_market_data(self) -> bool:
        """Update the latest market data table with most recent data"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Get the latest data for each token
            con.execute("""
                -- First clear existing data
                DELETE FROM latest_market_data;
                
                -- Insert latest data
                WITH latest_dates AS (
                    -- Get the latest date for each token
                    SELECT 
                        token,
                        MAX(date) as latest_date
                    FROM technical_indicators
                    GROUP BY token
                ),
                latest_technical AS (
                    -- Get latest technical indicators
                    SELECT t.*
                    FROM technical_indicators t
                    INNER JOIN latest_dates ld 
                        ON t.token = ld.token 
                        AND t.date = ld.latest_date
                ),
                latest_historical AS (
                    -- Get latest historical data
                    SELECT 
                        h.token,
                        h.open,
                        h.high,
                        h.low,
                        h.close,
                        h.volume
                    FROM historical_data h
                    INNER JOIN latest_dates ld 
                        ON h.token = ld.token 
                        AND h.timestamp::DATE = ld.latest_date
                )
                INSERT INTO latest_market_data
                SELECT 
                    t.token,
                    t.symbol,
                    tok.name,
                    tok.lotsize,
                    tok.token_type,
                    t.date,
                    -- OHLCV Data
                    h.open,
                    h.high,
                    h.low,
                    h.close,
                    h.volume,
                    -- Technical Indicators
                    t.ma_200,
                    t.ma_50,
                    t.ma_20,
                    t.ma_200_distance,
                    t.high_21d,
                    t.low_21d,
                    t.high_52w,
                    t.low_52w,
                    t.ath,
                    t.atl,
                    t.volume_15d_avg,
                    t.volume_ratio,
                    t.rsi_14,
                    t.macd,
                    t.macd_signal,
                    t.macd_hist,
                    t.bb_upper,
                    t.bb_middle,
                    t.bb_lower,
                    t.breakout_detected,
                    NOW() as last_updated
                FROM latest_technical t
                INNER JOIN latest_historical h ON t.token = h.token
                INNER JOIN tokens tok ON t.token = tok.token
                WHERE tok.token_type = 'SPOT'
            """)
            
            # Verify the update
            result = con.execute("""
                SELECT 
                    COUNT(*) as record_count,
                    MIN(date) as data_date,
                    COUNT(CASE WHEN breakout_detected THEN 1 END) as breakouts
                FROM latest_market_data
            """).fetchone()
            
            logger.info(f"Latest market data updated successfully:")
            logger.info(f"- Records: {result[0]}")
            logger.info(f"- Date: {result[1]}")
            logger.info(f"- Breakout signals: {result[2]}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating latest market data: {e}")
            return False
        finally:
            if con:
                con.close()

    def _get_historical_data(self, token: str) -> Optional[pd.DataFrame]:
        """Get historical data for a token and prepare it for technical analysis"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            current_date = datetime.now(IST).date()
            
            # Get data excluding current date
            df = con.execute("""
                SELECT 
                    token,
                    symbol,
                    timestamp::DATE as date,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM historical_data
                WHERE token = ?
                AND timestamp::DATE < ?
                ORDER BY timestamp ASC
            """, [token, current_date]).df()
            
            if df.empty:
                logger.warning(f"No historical data found for token {token}")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting historical data for token {token}: {e}")
            return None
        finally:
            if con:
                con.close()

    def calculate_indicators(self, token: str) -> bool:
        """Calculate technical indicators for a token."""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # First check if we have enough data for this token
            data_check = con.execute("""
                SELECT COUNT(*) as count
                FROM historical_data 
                WHERE token = ?
            """, [token]).fetchone()
            
            if not data_check or data_check[0] < 60:  # Need at least 60 days for proper indicators
                logger.warning(f"Insufficient historical data for token {token}: {data_check[0] if data_check else 0} records")
                return False
            
            # Create temp table with historical data
            con.execute("""
                CREATE TEMPORARY TABLE temp_data AS
                SELECT 
                    h.token,
                    t.symbol,
                    CAST(h.timestamp AS DATE) as date,  # Use explicit CAST
                    h.open,
                    h.high,
                    h.low,
                    h.close,
                    h.volume
                FROM historical_data h
                JOIN tokens t ON h.token = t.token
                WHERE h.token = ?
                ORDER BY h.timestamp
            """, [token])
            
            # Verify temp table has data
            temp_check = con.execute("SELECT COUNT(*) FROM temp_data").fetchone()
            if not temp_check or temp_check[0] == 0:
                logger.error(f"No data in temp table for token {token}")
                return False
            
            # Calculate indicators individually for better control
            # Moving Averages
            df = con.execute("""
                SELECT 
                    token,
                    symbol,
                    date,
                    close,
                    volume,
                    ma_200,
                    ma_50,
                    ma_20,
                    rsi_14,
                    macd,
                    macd_signal,
                    macd_hist,
                    bb_upper,
                    bb_middle,
                    bb_lower,
                    high_21d,
                    low_21d,
                    high_52w,
                    low_52w,
                    ath,
                    atl
                FROM temp_data
            """).df()
            
            if df.empty:
                logger.error(f"No data in temp_data for token {token}")
                return False
            
            # Calculate indicators
            df['ma_200'] = ta.sma(df['close'], length=200)
            df['ma_50'] = ta.sma(df['close'], length=50)
            df['ma_20'] = ta.sma(df['close'], length=20)
            
            # RSI
            df['rsi_14'] = ta.rsi(df['close'], length=14)
            
            # MACD
            macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
            df['macd'] = macd['MACD_12_26_9']
            df['macd_signal'] = macd['MACDs_12_26_9']
            df['macd_hist'] = macd['MACDh_12_26_9']
            
            # Bollinger Bands
            bbands = ta.bbands(df['close'], length=20, std=2)
            df['bb_upper'] = bbands['BBU_20_2.0']
            df['bb_middle'] = bbands['BBM_20_2.0']
            df['bb_lower'] = bbands['BBL_20_2.0']
            
            # Calculate additional metrics using DuckDB
            con.execute("""
                WITH price_levels AS (
                    SELECT 
                        token,
                        symbol,
                        date,
                        close,
                        volume,
                        ma_200,
                        ma_50,
                        ma_20,
                        rsi_14,
                        macd,
                        macd_signal,
                        macd_hist,
                        bb_upper,
                        bb_middle,
                        bb_lower,
                        MAX(close) OVER (
                            ORDER BY date ASC
                            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                        ) as high_21d,
                        MIN(close) OVER (
                            ORDER BY date ASC
                            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                        ) as low_21d,
                        MAX(close) OVER (
                            ORDER BY date ASC
                            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
                        ) as high_52w,
                        MIN(close) OVER (
                            ORDER BY date ASC
                            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
                        ) as low_52w,
                        MAX(close) OVER () as ath,
                        MIN(close) OVER () as atl
                    FROM temp_data
                ),
                volume_analysis AS (
                    SELECT 
                        *,
                        AVG(volume) OVER (
                            ORDER BY date ASC
                            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                        ) as volume_15d_avg,
                        volume / NULLIF(LAG(volume) OVER (ORDER BY date), 0) as volume_ratio
                    FROM price_levels
                )
                INSERT INTO technical_indicators
                SELECT 
                    token,
                    symbol,
                    date,
                    ma_200,
                    ma_50,
                    ma_20,
                    ((close / NULLIF(ma_200, 0)) - 1) * 100 as ma_200_distance,
                    high_21d,
                    low_21d,
                    high_52w,
                    low_52w,
                    ath,
                    atl,
                    volume_15d_avg,
                    volume_ratio,
                    rsi_14,
                    macd,
                    macd_signal,
                    macd_hist,
                    bb_upper,
                    bb_middle,
                    bb_lower,
                    -- Calculate breakout/breakdown detection
                    CASE 
                        WHEN volume > 2 * volume_15d_avg 
                            AND close > high_21d 
                            AND ((close - high_21d) / high_21d) <= 0.02 
                        THEN 'BREAKOUT'
                        WHEN volume > 2 * volume_15d_avg 
                            AND close < low_21d 
                            AND ((low_21d - close) / close) <= 0.005 
                        THEN 'BREAKDOWN'
                        ELSE NULL
                    END as breakout_detected,
                    ? as calculation_timestamp
                FROM volume_analysis
                WHERE ma_200 IS NOT NULL
            """, [datetime.now(IST).replace(tzinfo=None)])
            
            # Verify calculations
            result = con.execute("""
                SELECT COUNT(*) as record_count
                FROM technical_indicators
                WHERE token = ?
            """, [token]).fetchone()
            
            logger.info(f"Calculated indicators for token {token}: {result[0]} records")
            return True
            
        except Exception as e:
            logger.error(f"Error calculating indicators for token {token}: {e}")
            return False
        finally:
            if con:
                con.execute("DROP TABLE IF EXISTS temp_data")
                con.close()

    def get_latest_indicators(self, token: str) -> Optional[Dict[str, Any]]:
        """Get the latest technical indicators for a token."""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            result = con.execute("""
                SELECT *
                FROM technical_indicators
                WHERE token = ?
                ORDER BY date DESC
                LIMIT 1
            """, [token]).fetchone()
            
            if result:
                column_names = [desc[0] for desc in con.description]
                return dict(zip(column_names, result))
            return None
            
        except Exception as e:
            logger.error(f"Error getting indicators for token {token}: {e}")
            return None
        finally:
            if con:
                con.close()

    def calculate_all_indicators(self) -> bool:
        """Calculate technical indicators for all tokens."""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
            # Get unique spot tokens
            tokens = con.execute("""
                SELECT DISTINCT h.token
                FROM historical_data h
                JOIN tokens t ON h.token = t.token
                WHERE t.token_type = 'SPOT'
            """).fetchall()
            
            if not tokens:
                logger.error("No spot tokens found for technical indicator calculation")
                return False
            
            logger.info(f"Found {len(tokens)} spot tokens for technical analysis")
            
            success_count = 0
            error_count = 0
            
            for token_row in tokens:
                token = token_row[0]
                try:
                    if self.calculate_indicators(token):
                        success_count += 1
                        logger.info(f"Successfully calculated indicators for token: {token}")
                    else:
                        error_count += 1
                        logger.error(f"Failed to calculate indicators for token: {token}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error calculating indicators for token {token}: {e}")
            
            logger.info(f"\nTechnical Indicators Calculation Summary:")
            logger.info(f"- Successfully processed: {success_count}")
            logger.info(f"- Failed to process: {error_count}")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error calculating all indicators: {e}")
            return False
        finally:
            if con:
                con.close()

    def update_daily_summary(self) -> bool:
        """Update the daily summary table with latest technical indicators and price data"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            current_date = datetime.now(IST).date()
            
            # Update daily summary table
            con.execute("""
                -- First clear existing data
                DELETE FROM daily_summary;
                
                -- Insert latest data for each token
                INSERT INTO daily_summary
                WITH latest_data AS (
                    -- Get the latest date for each token
                    SELECT 
                        h.token,
                        h.symbol,
                        t.name,
                        h.timestamp::DATE as date,
                        h.open,
                        h.high,
                        h.low,
                        h.close,
                        h.volume,
                        -- Technical Indicators
                        ti.ma_200,
                        ti.ma_50,
                        ti.ma_20,
                        ti.ma_200_distance,
                        ti.high_21d,
                        ti.low_21d,
                        ti.high_52w,
                        ti.low_52w,
                        ti.ath,
                        ti.atl,
                        ti.volume_15d_avg,
                        ti.volume_ratio,
                        ti.rsi_14,
                        ti.macd,
                        ti.macd_signal,
                        ti.macd_hist,
                        ti.bb_upper,
                        ti.bb_middle,
                        ti.bb_lower,
                        ti.breakout_detected,
                        NOW() as last_updated,
                        ROW_NUMBER() OVER (
                            PARTITION BY h.token 
                            ORDER BY h.timestamp DESC
                        ) as rn
                    FROM historical_data h
                    JOIN tokens t ON h.token = t.token
                    JOIN technical_indicators ti ON h.token = ti.token 
                        AND h.timestamp::DATE = ti.date
                    WHERE t.token_type = 'SPOT'
                    AND h.timestamp::DATE < ?
                )
                SELECT 
                    token,
                    symbol,
                    name,
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ma_200,
                    ma_50,
                    ma_20,
                    ma_200_distance,
                    high_21d,
                    low_21d,
                    high_52w,
                    low_52w,
                    ath,
                    atl,
                    volume_15d_avg,
                    volume_ratio,
                    rsi_14,
                    macd,
                    macd_signal,
                    macd_hist,
                    bb_upper,
                    bb_middle,
                    bb_lower,
                    breakout_detected,
                    last_updated
                FROM latest_data
                WHERE rn = 1
            """, [current_date])
            
            # Verify the update
            result = con.execute("""
                SELECT 
                    COUNT(*) as record_count,
                    COUNT(CASE WHEN breakout_detected IS NOT NULL THEN 1 END) as breakouts
                FROM daily_summary
            """).fetchone()
            
            logger.info(f"Daily summary updated successfully:")
            logger.info(f"- Total records: {result[0]}")
            logger.info(f"- Breakout signals: {result[1]}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating daily summary: {e}")
            return False
        finally:
            if con:
                con.close() 