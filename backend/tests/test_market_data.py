import os
import sys
import json
import duckdb
import asyncio
from logzero import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pytz import timezone

# Add backend directory to Python path
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from src.data.token_manager import TokenManager
from src.api.angel_one_connector import AngelOneConnector
from src.data.angel_market_data import AngelMarketData

load_dotenv()

IST = timezone('Asia/Kolkata')

def log_request_response(request_data: dict, response_data: dict, data_type: str):
    """Log the request and response data.
    
    Args:
        request_data (dict): Request payload
        response_data (dict): Response data
        data_type (str): Type of data (SPOT/FUTURES/OPTIONS)
    """
    logger.info(f"\n{'='*20} {data_type} Request/Response {'='*20}")
    logger.info(f"\nRequest Payload:")
    logger.info(json.dumps(request_data, indent=2))
    
    logger.info(f"\nRaw Response:")
    logger.info(json.dumps(response_data, indent=2))
    
    if response_data and isinstance(response_data, dict):
        logger.info(f"\nResponse Structure:")
        for key, value in response_data.items():
            if isinstance(value, (list, dict)):
                logger.info(f"{key}: {type(value).__name__} with {len(value)} items")
            else:
                logger.info(f"{key}: {type(value).__name__} = {value}")

def test_strike_intervals(market_data_manager: AngelMarketData, con: duckdb.DuckDBPyConnection):
    """Test strike interval calculation"""
    logger.info("\nTesting strike interval calculation...")
    
    # Get RELIANCE options data
    result = con.execute("""
        SELECT DISTINCT name, expiry
        FROM tokens
        WHERE token_type = 'OPTIONS'
        AND name = 'RELIANCE'
        LIMIT 1
    """).fetchone()
    
    if not result:
        logger.error("No RELIANCE options found for testing strike intervals")
        return False
        
    name, expiry = result
    
    # Calculate strike interval
    interval = market_data_manager._get_strike_interval(name, expiry)
    if not interval:
        logger.error(f"Failed to calculate strike interval for {name}")
        return False
        
    logger.info(f"✅ Successfully calculated strike interval for {name}: {interval}")
    return True

def test_atm_strikes(market_data_manager: AngelMarketData, con: duckdb.DuckDBPyConnection):
    """Test ATM strike selection"""
    logger.info("\nTesting ATM strike selection...")
    
    # Get RELIANCE futures price from stored data
    result = con.execute("""
        SELECT t.name, t.expiry, f.ltp
        FROM tokens t
        JOIN (
            SELECT symbol, ltp
            FROM realtime_futures_data
            WHERE timestamp = (
                SELECT MAX(timestamp)
                FROM realtime_futures_data
            )
        ) f ON REPLACE(f.symbol, '27FEB25FUT', '') = t.name
        WHERE t.token_type = 'OPTIONS'
        AND t.name = 'RELIANCE'
        LIMIT 1
    """).fetchone()
    
    if not result:
        logger.error("No suitable RELIANCE data found for testing ATM strikes")
        return False
        
    name, expiry, future_price = result
    
    # Get strike interval
    interval = market_data_manager._get_strike_interval(name, expiry)
    if not interval:
        logger.error(f"Failed to get strike interval for {name}")
        return False
        
    # Get ATM strikes
    strikes = market_data_manager._get_atm_strikes(name, future_price, interval)
    if not strikes:
        logger.error(f"Failed to calculate ATM strikes for {name}")
        return False
        
    logger.info(f"✅ Successfully calculated ATM strikes for {name}:")
    logger.info(f"- Future Price: {future_price}")
    logger.info(f"- Strike Interval: {interval}")
    logger.info(f"- Selected Strikes: {strikes}")
    
    # Get options at these strikes
    options = con.execute("""
        SELECT token, symbol, strike
        FROM tokens
        WHERE token_type = 'OPTIONS'
        AND name = 'RELIANCE'
        AND strike IN (
            SELECT UNNEST(?)
        )
        ORDER BY strike
    """, [strikes]).fetchall()
    
    if options:
        logger.info("\nFound matching options:")
        for opt in options:
            logger.info(f"- {opt[1]} (Strike: {opt[2]})")
    else:
        logger.warning("No matching options found for calculated strikes")
    
    return True

async def test_market_data():
    """Test real-time market data fetching and ATM options selection"""
    try:
        # Initialize managers and connect to API
        token_manager = TokenManager()
        market_data_manager = AngelMarketData(token_manager=token_manager)
        
        connector = AngelOneConnector()
        if not connector.connect():
            logger.error("Failed to connect to Angel One API")
            return False
            
        # Refresh tokens if needed
        if not token_manager.is_market_data_current():
            logger.info("Token data not current, refreshing tokens...")
            if not token_manager.download_and_store_tokens():
                logger.error("Failed to refresh token data")
                return False
        
        # Get test data
        con = None
        try:
            con = duckdb.connect(os.getenv('DB_FILE', 'nfo_data.duckdb'))
            
            # Test strike interval calculation
            if not test_strike_intervals(market_data_manager, con):
                return False
                
            # Test spot data first to get LTP for futures
            spot_token = con.execute("""
                SELECT token, symbol, exch_seg
                FROM tokens 
                WHERE token_type = 'SPOT'
                AND name = 'RELIANCE'
                LIMIT 1
            """).fetchone()
            
            if not spot_token:
                logger.error("Failed to get RELIANCE spot token")
                return False
                
            # Test spot data
            logger.info("\nTesting SPOT market data...")
            spot_request = {
                "mode": "FULL",
                "exchangeTokens": {
                    spot_token[2]: [spot_token[0]]
                }
            }
            spot_data = connector.api.getMarketData(**spot_request)
            log_request_response(spot_request, spot_data, "SPOT")
            
            if spot_data and spot_data.get('status'):
                fetched_data = spot_data.get('data', {}).get('fetched', [])
                if fetched_data:
                    logger.info("Storing SPOT market data...")
                    if not market_data_manager._store_spot_data(fetched_data):
                        logger.error("❌ Failed to store SPOT market data")
                        return False
                    logger.info("✅ Successfully stored SPOT market data")
            
            # Test futures data
            futures_token = con.execute("""
                SELECT token, symbol, exch_seg
                FROM tokens 
                WHERE token_type = 'FUTURES'
                AND name = 'RELIANCE'
                LIMIT 1
            """).fetchone()
            
            if not futures_token:
                logger.error("Failed to get RELIANCE futures token")
                return False
                
            logger.info("\nTesting FUTURES market data...")
            futures_request = {
                "mode": "FULL",
                "exchangeTokens": {
                    futures_token[2]: [futures_token[0]]
                }
            }
            futures_data = connector.api.getMarketData(**futures_request)
            log_request_response(futures_request, futures_data, "FUTURES")
            
            if futures_data and futures_data.get('status'):
                fetched_data = futures_data.get('data', {}).get('fetched', [])
                if fetched_data:
                    logger.info("Storing FUTURES market data...")
                    if not market_data_manager._store_futures_data(fetched_data):
                        logger.error("❌ Failed to store FUTURES market data")
                        return False
                    logger.info("✅ Successfully stored FUTURES market data")
            
            # Test ATM strike selection
            if not test_atm_strikes(market_data_manager, con):
                return False
            
            # Test options data with ATM strikes
            logger.info("\nTesting OPTIONS market data with ATM strikes...")
            
            # Get ATM options for testing (both CE and PE)
            options_tokens = con.execute("""
                WITH atm_strike AS (
                    SELECT 
                        ROUND(f.ltp/10.0) * 10.0 as strike_price
                    FROM realtime_futures_data f
                    WHERE f.name = 'RELIANCE'
                    AND f.timestamp = (SELECT MAX(timestamp) FROM realtime_futures_data)
                )
                SELECT t.token, t.symbol, t.exch_seg, t.strike, t.formatted_symbol
                FROM tokens t
                CROSS JOIN atm_strike a
                WHERE t.token_type = 'OPTIONS'
                AND t.name = 'RELIANCE'
                AND t.strike/100 = a.strike_price
                AND (t.symbol LIKE '%CE' OR t.symbol LIKE '%PE')
                ORDER BY t.symbol
                LIMIT 2
            """).fetchall()
            
            if not options_tokens:
                logger.error("Failed to get RELIANCE ATM options tokens")
                return False
                
            for option_token in options_tokens:
                logger.info(f"Testing with ATM option: {option_token[1]} (Strike: {option_token[3]})")
                
                options_request = {
                    "mode": "FULL",
                    "exchangeTokens": {
                        option_token[2]: [option_token[0]]
                    }
                }
                options_data = connector.api.getMarketData(**options_request)
                log_request_response(options_request, options_data, "OPTIONS")
                
                if options_data and options_data.get('status'):
                    fetched_data = options_data.get('data', {}).get('fetched', [])
                    if fetched_data:
                        logger.info("Storing OPTIONS market data...")
                        if not market_data_manager._store_options_data(fetched_data):
                            logger.error("❌ Failed to store OPTIONS market data")
                            return False
                        logger.info("✅ Successfully stored OPTIONS market data")
            
            logger.info("\nMarket data test completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during testing: {e}")
            return False
        finally:
            if con:
                con.close()
            
    except Exception as e:
        logger.error(f"Market data test failed: {str(e)}")
        return False
    finally:
        try:
            connector.api.terminateSession(os.getenv('ANGEL_ONE_CLIENT_ID'))
            logger.info("API session terminated")
        except:
            pass

async def test_market_data_continuous():
    """Test continuous market data fetching for multiple equities"""
    try:
        # Initialize managers and connect to API
        token_manager = TokenManager()
        market_data_manager = AngelMarketData(token_manager=token_manager)
        
        connector = AngelOneConnector()
        if not connector.connect():
            logger.error("Failed to connect to Angel One API")
            return False
            
        # Refresh tokens if needed
        if not token_manager.is_market_data_current():
            logger.info("Token data not current, refreshing tokens...")
            if not token_manager.download_and_store_tokens():
                logger.error("Failed to refresh token data")
                return False

        # Load initial tokens for all symbols
        con = duckdb.connect(os.getenv('DB_FILE', 'nfo_data.duckdb'))
        symbols = ['RELIANCE', 'ITC', 'ZOMATO', 'MRF', 'IDEA']
        
        # Get spot and futures tokens for all symbols
        spot_tokens = con.execute("""
            SELECT token, symbol, name, exch_seg
            FROM tokens 
            WHERE token_type = 'SPOT'
            AND name IN (SELECT UNNEST(?))
        """, [symbols]).fetchall()
        
        futures_tokens = con.execute("""
            SELECT token, symbol, name, exch_seg
            FROM tokens 
            WHERE token_type = 'FUTURES'
            AND name IN (SELECT UNNEST(?))
        """, [symbols]).fetchall()

        while True:  # 1-minute loop
            try:
                current_time = datetime.now(IST)
                logger.info(f"\nFetching data at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Parallel fetch spot & futures prices
                spot_request = {
                    "mode": "FULL",
                    "exchangeTokens": {
                        "NSE": [token[0] for token in spot_tokens]
                    }
                }
                futures_request = {
                    "mode": "FULL",
                    "exchangeTokens": {
                        "NFO": [token[0] for token in futures_tokens]
                    }
                }
                
                # Make API calls
                spot_data = connector.api.getMarketData(**spot_request)
                futures_data = connector.api.getMarketData(**futures_request)
                
                # Store spot and futures data
                if spot_data and spot_data.get('status'):
                    market_data_manager._store_spot_data(spot_data.get('data', {}).get('fetched', []))
                
                if futures_data and futures_data.get('status'):
                    market_data_manager._store_futures_data(futures_data.get('data', {}).get('fetched', []))
                
                # Process futures prices and get ATM strikes for each symbol
                for symbol in symbols:
                    try:
                        # Get latest futures price
                        future_result = con.execute("""
                            SELECT f.ltp, t.name, t.expiry
                            FROM realtime_futures_data f
                            JOIN tokens t ON t.name = f.name
                            WHERE f.name = ?
                            AND f.timestamp = (
                                SELECT MAX(timestamp)
                                FROM realtime_futures_data
                                WHERE name = ?
                            )
                        """, [symbol, symbol]).fetchone()
                        
                        if not future_result:
                            logger.warning(f"No futures data found for {symbol}")
                            continue
                            
                        future_price, name, expiry = future_result
                        
                        # Calculate strike interval and ATM strikes
                        interval = market_data_manager._get_strike_interval(name, expiry)
                        if not interval:
                            logger.error(f"Failed to get strike interval for {name}")
                            continue
                            
                        strikes = market_data_manager._get_atm_strikes(name, future_price, interval)
                        if not strikes:
                            logger.error(f"Failed to calculate ATM strikes for {name}")
                            continue
                            
                        # Get ATM options tokens
                        options_tokens = con.execute("""
                            SELECT t.token, t.symbol, t.exch_seg
                            FROM tokens t
                            WHERE t.token_type = 'OPTIONS'
                            AND t.name = ?
                            AND t.strike/100 IN (SELECT UNNEST(?))
                            AND (t.symbol LIKE '%CE' OR t.symbol LIKE '%PE')
                            ORDER BY t.strike, t.symbol
                        """, [name, strikes]).fetchall()
                        
                        if not options_tokens:
                            logger.warning(f"No ATM options found for {name}")
                            continue
                            
                        # Fetch and store options data
                        options_request = {
                            "mode": "FULL",
                            "exchangeTokens": {
                                "NFO": [token[0] for token in options_tokens]
                            }
                        }
                        
                        options_data = connector.api.getMarketData(**options_request)
                        if options_data and options_data.get('status'):
                            market_data_manager._store_options_data(options_data.get('data', {}).get('fetched', []))
                            
                    except Exception as e:
                        logger.error(f"Error processing {symbol}: {str(e)}")
                        continue
                
                # Wait for next minute
                next_minute = (current_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
                wait_seconds = (next_minute - datetime.now(IST)).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                    
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                await asyncio.sleep(60)  # Wait a minute before retrying
                continue
                
    except Exception as e:
        logger.error(f"Market data test failed: {str(e)}")
        return False
    finally:
        try:
            if 'con' in locals():
                con.close()
            connector.api.terminateSession(os.getenv('ANGEL_ONE_CLIENT_ID'))
            logger.info("API session terminated")
        except:
            pass

if __name__ == "__main__":
    asyncio.run(test_market_data_continuous()) 