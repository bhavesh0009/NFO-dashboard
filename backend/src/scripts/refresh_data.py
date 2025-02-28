"""Script to refresh market data for the day."""
import os
import sys
from datetime import datetime
import pytz
from logzero import logger, logfile
from SmartApi import SmartConnect
import pyotp

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from data.token_manager import TokenManager
from data.historical_data_manager import HistoricalDataManager
from data.technical_indicators import TechnicalIndicatorManager

# Constants
IST = pytz.timezone('Asia/Kolkata')

def setup_logging():
    """Setup logging configuration"""
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    current_time = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'refresh_data_{current_time}.log')
    logfile(log_file, maxBytes=1e6, backupCount=3)

def connect_to_api():
    """Connect to Angel One API"""
    try:
        # Get API credentials from environment
        api_key = os.getenv('ANGEL_ONE_APP_KEY')
        client_id = os.getenv('ANGEL_ONE_CLIENT_ID')
        pin = os.getenv('ANGEL_ONE_PIN')
        totp_secret = os.getenv('ANGEL_ONE_TOTP_SECRET')
        
        if not all([api_key, client_id, pin, totp_secret]):
            raise ValueError("Missing API credentials in environment variables")
        
        # Generate TOTP
        totp = pyotp.TOTP(totp_secret)
        
        # Initialize API connection
        smart_api = SmartConnect(api_key=api_key)
        
        # Generate session
        data = smart_api.generateSession(client_id, pin, totp.now())
        
        if data['status']:
            logger.info("Successfully connected to Angel One API")
            return smart_api
        else:
            raise ConnectionError(f"Session generation failed: {data.get('message', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"API connection failed: {str(e)}")
        raise

def refresh_market_data():
    """Refresh market data for the day"""
    try:
        # Setup logging
        setup_logging()
        logger.info("Starting market data refresh")
        
        # Initialize managers
        token_manager = TokenManager()
        historical_manager = HistoricalDataManager(token_manager)
        indicator_manager = TechnicalIndicatorManager()
        
        # Connect to API
        smart_api = connect_to_api()
        
        # Fetch and store all data
        logger.info("Fetching and storing market data...")
        if not historical_manager.fetch_and_store_historical_data(smart_api):
            raise Exception("Failed to fetch and store market data")
            
        # Calculate technical indicators
        logger.info("Calculating technical indicators...")
        if not indicator_manager.calculate_all_indicators():
            raise Exception("Failed to calculate technical indicators")
            
        # Update daily summary
        logger.info("Updating daily summary...")
        if not indicator_manager.update_daily_summary():
            raise Exception("Failed to update daily summary")
            
        # Update latest market data
        logger.info("Updating latest market data...")
        if not indicator_manager.update_latest_market_data():
            raise Exception("Failed to update latest market data")
            
        logger.info("Market data refresh completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Market data refresh failed: {str(e)}")
        return False
    finally:
        try:
            smart_api.terminateSession(os.getenv('ANGEL_ONE_CLIENT_ID'))
            logger.info("API session terminated")
        except:
            pass

if __name__ == "__main__":
    from dotenv import load_dotenv
    
    # Load environment variables
    env_file = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
    load_dotenv(env_file)
    
    # Run refresh
    success = refresh_market_data()
    sys.exit(0 if success else 1) 