import os
import sys
import pytest
from fastapi.testclient import TestClient
from datetime import datetime
import pytz
from typing import Dict, Any

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.api.market_data_api import app

# Constants
IST = pytz.timezone('Asia/Kolkata')
client = TestClient(app)

def test_get_all_market_data():
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

def test_get_token_data():
    """Test getting data for a specific token"""
    # First get all data to get a valid token
    all_data = client.get("/api/v1/market-data").json()
    
    if all_data["data"]:
        # Test with valid token
        token = all_data["data"][0]["token"]
        response = client.get(f"/api/v1/market-data/{token}")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "success"
        assert len(data["data"]) == 1
        assert data["data"][0]["token"] == token
    
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