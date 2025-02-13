import os
import sys
from logzero import logger
from SmartApi import SmartConnect
import pyotp
from dotenv import load_dotenv

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.data.token_manager import TokenManager
from src.data.historical_data_manager import HistoricalDataManager

def test_historical_data():
    try:
        # Load environment variables
        load_dotenv()
        
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
        
        # Fetch and store historical data
        logger.info("Fetching historical data...")
        if historical_manager.fetch_and_store_historical_data(connector):
            logger.info("✅ Historical data fetched and stored successfully")
        else:
            logger.error("❌ Failed to fetch and store historical data")
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        # Print full error traceback for debugging
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_historical_data() 