import os
import sys
import pytest
from fastapi.testclient import TestClient
from datetime import datetime
import pytz
from typing import Dict, Any
import duckdb

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.api.market_data_api import app, set_test_db
from src.data.technical_indicators import TechnicalIndicatorManager

# Constants
IST = pytz.timezone('Asia/Kolkata')
client = TestClient(app)

@pytest.fixture(scope="session")
def test_db():
    """Create test database and required tables"""
    # Use in-memory database for testing
    os.environ['DB_FILE'] = ':memory:'
    
    # Create a single connection for the entire test session
    con = duckdb.connect(':memory:')
    
    # Set the test connection for the API
    set_test_db(con)
    
    # Initialize tables using the same connection
    indicator_manager = TechnicalIndicatorManager(test_connection=con)
    indicator_manager.setup_database()
    
    # Insert test market data using the same connection
    con.execute("""
        INSERT INTO latest_market_data VALUES (
            '1234', 'TEST', 'Test Stock', '100', 'SPOT', '2024-03-10',
            100.0, 105.0, 95.0, 102.0, 1000000,
            98.0, 99.0, 101.0, 4.0,
            110.0, 90.0, 120.0, 80.0,
            150.0, 50.0, 900000.0, 1.1,
            65.0, 0.5, 0.3, 0.2,
            105.0, 100.0, 95.0,
            'BREAKOUT',
            CURRENT_TIMESTAMP
        )
    """)
    
    yield con
    
    # Cleanup
    set_test_db(None)  # Reset test connection
    con.close()

def test_get_all_market_data(test_db):
    """Test getting all market data"""
    response = client.get("/api/v1/market-data")
    assert response.status_code == 200
    
    data = response.json()
    assert isinstance(data, dict)
    assert "status" in data
    assert "message" in data
    assert "data" in data
    assert "count" in data
    assert "last_updated" in data
    
    # Verify response structure
    assert data["status"] == "success"
    assert isinstance(data["data"], list)
    assert isinstance(data["count"], int)
    assert data["count"] == len(data["data"])
    
    # Verify data fields
    if data["data"]:
        first_record = data["data"][0]
        required_fields = {
            "token", "symbol", "name", "lotsize", "token_type",
            "date", "open", "high", "low", "close", "volume",
            "ma_200", "ma_50", "ma_20", "ma_200_distance",
            "high_21d", "low_21d", "high_52w", "low_52w",
            "ath", "atl", "volume_15d_avg", "volume_ratio",
            "rsi_14", "macd", "macd_signal", "macd_hist",
            "bb_upper", "bb_middle", "bb_lower",
            "breakout_detected", "last_updated"
        }
        assert all(field in first_record for field in required_fields)
        
        # Verify numeric fields
        assert isinstance(first_record["open"], (int, float))
        assert isinstance(first_record["high"], (int, float))
        assert isinstance(first_record["low"], (int, float))
        assert isinstance(first_record["close"], (int, float))
        assert isinstance(first_record["volume"], int)
        
        # Verify optional fields can be None
        optional_fields = {
            "ma_200", "ma_50", "ma_20", "ma_200_distance",
            "high_21d", "low_21d", "high_52w", "low_52w",
            "ath", "atl", "volume_15d_avg", "volume_ratio",
            "rsi_14", "macd", "macd_signal", "macd_hist",
            "bb_upper", "bb_middle", "bb_lower",
            "breakout_detected"
        }
        for field in optional_fields:
            assert first_record[field] is None or isinstance(first_record[field], (int, float, str))

def test_get_token_data(test_db):
    """Test getting data for a specific token"""
    # Test with valid token
    response = client.get("/api/v1/market-data/1234")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "success"
    assert len(data["data"]) == 1
    assert data["data"][0]["token"] == "1234"
    
    # Test with invalid token
    response = client.get("/api/v1/market-data/invalid_token")
    assert response.status_code == 404

def test_error_handling():
    """Test error handling"""
    # Test database connection error (requires mocking in a real test)
    # This is just a placeholder for now
    pass

if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 