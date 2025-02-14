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
    def __init__(self):
        """Initialize the Technical Indicator Manager."""
        self.db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
        self.setup_database()

    def setup_database(self) -> None:
        """Create the technical_indicators table if it doesn't exist"""
        con = None
        try:
            con = duckdb.connect(self.db_file)
            
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
            
            logger.info("Technical indicators and latest market data tables created successfully")
        except Exception as e:
            logger.error(f"Error setting up database tables: {e}")
            raise
        finally:
            if con:
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
        """Calculate technical indicators using pandas-ta"""
        try:
            # Get historical data
            df = self._get_historical_data(token)
            if df is None:
                return False
            
            # Calculate indicators individually for better control
            # Moving Averages
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
            con = None
            try:
                con = duckdb.connect(self.db_file)
                
                # Convert DataFrame back to DuckDB table
                con.execute("CREATE TEMP TABLE temp_data AS SELECT * FROM df")
                
                # Calculate remaining indicators and store results
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
                
            finally:
                if con:
                    con.execute("DROP TABLE IF EXISTS temp_data")
                    con.close()
            
        except Exception as e:
            logger.error(f"Error calculating indicators for token {token}: {e}")
            return False

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
            
            # Get unique tokens
            tokens = con.execute("""
                SELECT DISTINCT token
                FROM historical_data
                WHERE token_type = 'SPOT'
            """).fetchall()
            
            success_count = 0
            error_count = 0
            
            for token_row in tokens:
                token = token_row[0]
                if self.calculate_indicators(token):
                    success_count += 1
                else:
                    error_count += 1
            
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