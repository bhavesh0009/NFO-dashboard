import os
import sys
import json
import duckdb
import asyncio
from logzero import logger
from dotenv import load_dotenv

# Add backend directory to Python path
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from src.data.token_manager import TokenManager
from src.api.angel_one_connector import AngelOneConnector
from src.data.angel_market_data import AngelMarketData

load_dotenv()

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

async def test_market_data():
    """Test real-time market data fetching and log response structure"""
    try:
        # Initialize token manager and market data manager
        token_manager = TokenManager()
        market_data_manager = AngelMarketData(token_manager=token_manager)
        
        # Connect to API
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
        
        # Get one token of each type
        con = None
        try:
            con = duckdb.connect(os.getenv('DB_FILE', 'nfo_data.duckdb'))
            
            # Get sample tokens (first 1 of each type)
            spot_token = con.execute("""
                SELECT token, symbol, exch_seg
                FROM tokens 
                WHERE token_type = 'SPOT'
                LIMIT 1
            """).fetchone()
            
            futures_token = con.execute("""
                SELECT token, symbol, exch_seg
                FROM tokens 
                WHERE token_type = 'FUTURES'
                LIMIT 1
            """).fetchone()
            
            options_token = con.execute("""
                SELECT token, symbol, exch_seg
                FROM tokens 
                WHERE token_type = 'OPTIONS'
                LIMIT 1
            """).fetchone()
            
        finally:
            if con:
                con.close()
        
        if not all([spot_token, futures_token, options_token]):
            logger.error("Failed to get sample tokens")
            return False
        
        # Log token details
        logger.info("\nTesting with tokens:")
        logger.info(f"SPOT: {spot_token}")
        logger.info(f"FUTURES: {futures_token}")
        logger.info(f"OPTIONS: {options_token}")
        
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
        
        # Store spot data
        if spot_data and spot_data.get('status'):
            fetched_data = spot_data.get('data', {}).get('fetched', [])
            if fetched_data:
                logger.info("Storing SPOT market data...")
                if market_data_manager._store_spot_data(fetched_data):
                    logger.info("✅ Successfully stored SPOT market data")
                else:
                    logger.error("❌ Failed to store SPOT market data")
        
        # Test futures data
        logger.info("\nTesting FUTURES market data...")
        futures_request = {
            "mode": "FULL",
            "exchangeTokens": {
                futures_token[2]: [futures_token[0]]
            }
        }
        futures_data = connector.api.getMarketData(**futures_request)
        log_request_response(futures_request, futures_data, "FUTURES")
        
        # Store futures data
        if futures_data and futures_data.get('status'):
            fetched_data = futures_data.get('data', {}).get('fetched', [])
            if fetched_data:
                logger.info("Storing FUTURES market data...")
                if market_data_manager._store_futures_data(fetched_data):
                    logger.info("✅ Successfully stored FUTURES market data")
                else:
                    logger.error("❌ Failed to store FUTURES market data")
        
        # Test options data
        logger.info("\nTesting OPTIONS market data...")
        options_request = {
            "mode": "FULL",
            "exchangeTokens": {
                options_token[2]: [options_token[0]]
            }
        }
        options_data = connector.api.getMarketData(**options_request)
        log_request_response(options_request, options_data, "OPTIONS")
        
        # Store options data
        if options_data and options_data.get('status'):
            fetched_data = options_data.get('data', {}).get('fetched', [])
            if fetched_data:
                logger.info("Storing OPTIONS market data...")
                if market_data_manager._store_options_data(fetched_data):
                    logger.info("✅ Successfully stored OPTIONS market data")
                else:
                    logger.error("❌ Failed to store OPTIONS market data")
        
        logger.info("\nMarket data structure test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Market data test failed: {str(e)}")
        return False
    finally:
        try:
            connector.api.terminateSession(os.getenv('ANGEL_ONE_CLIENT_ID'))
            logger.info("API session terminated")
        except:
            pass

if __name__ == "__main__":
    asyncio.run(test_market_data()) 