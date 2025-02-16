import os
import duckdb
from logzero import logger
from dotenv import load_dotenv

load_dotenv()

def truncate_tables():
    """Utility function to truncate all tables in the database"""
    db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
    
    try:
        con = duckdb.connect(db_file)
        logger.info("Connected to database")
        
        # List of tables to truncate
        tables = [
            'tokens', 
            'historical_data',
            'technical_indicators',
            'latest_market_data',
            'realtime_spot_data',
            'realtime_futures_data',
            'realtime_options_data'
        ]
        
        for table in tables:
            try:
                con.execute(f"TRUNCATE TABLE {table}")
                logger.info(f"✅ Successfully truncated table: {table}")
            except Exception as e:
                logger.error(f"Failed to truncate table {table}: {e}")
                
    except Exception as e:
        logger.error(f"Database connection error: {e}")
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    truncate_tables() 