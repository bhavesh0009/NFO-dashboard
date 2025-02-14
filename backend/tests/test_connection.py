import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.api.angel_one_connector import AngelOneConnector
from logzero import logger

def test_connection():
    try:
        # Create connector instance
        connector = AngelOneConnector()
        
        # Test connection
        logger.info("Testing connection to Angel One API...")
        if connector.connect():
            logger.info("✅ Connection successful!")
            
            # Test profile retrieval
            logger.info("Fetching profile information...")
            profile = connector.get_profile()
            if profile:
                logger.info(f"✅ Profile retrieved successfully!")
                logger.info(f"Name: {profile.get('name', 'N/A')}")
                logger.info(f"Email: {profile.get('email', 'N/A')}")
            else:
                logger.error("❌ Failed to retrieve profile")
        else:
            logger.error("❌ Connection failed")
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        # Print full error traceback for debugging
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_connection() 