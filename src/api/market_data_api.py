from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import duckdb
import os
from datetime import datetime
import pytz
from typing import List, Dict, Any, Optional
from logzero import logger
from pydantic import BaseModel, Field

# Constants
IST = pytz.timezone('Asia/Kolkata')

# Initialize FastAPI app
app = FastAPI(
    title="NFO Dashboard API",
    description="API for serving NFO market data and technical indicators",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for response validation
class MarketData(BaseModel):
    token: str = Field(..., description="Token ID")
    symbol: str = Field(..., description="Trading symbol")
    name: Optional[str] = Field(None, description="Company name")
    lotsize: Optional[str] = Field(None, description="Lot size")
    token_type: str = Field(..., description="Token type (SPOT/FUTURES/OPTIONS)")
    date: datetime = Field(..., description="Data date")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Closing price")
    volume: int = Field(..., description="Trading volume")
    ma_200: Optional[float] = Field(None, description="200-day moving average")
    ma_50: Optional[float] = Field(None, description="50-day moving average")
    ma_20: Optional[float] = Field(None, description="20-day moving average")
    ma_200_distance: Optional[float] = Field(None, description="Distance from 200-day MA (%)")
    high_21d: Optional[float] = Field(None, description="21-day high")
    low_21d: Optional[float] = Field(None, description="21-day low")
    high_52w: Optional[float] = Field(None, description="52-week high")
    low_52w: Optional[float] = Field(None, description="52-week low")
    ath: Optional[float] = Field(None, description="All-time high")
    atl: Optional[float] = Field(None, description="All-time low")
    volume_15d_avg: Optional[float] = Field(None, description="15-day average volume")
    volume_ratio: Optional[float] = Field(None, description="Volume ratio")
    rsi_14: Optional[float] = Field(None, description="14-day RSI")
    macd: Optional[float] = Field(None, description="MACD")
    macd_signal: Optional[float] = Field(None, description="MACD signal")
    macd_hist: Optional[float] = Field(None, description="MACD histogram")
    bb_upper: Optional[float] = Field(None, description="Bollinger Band upper")
    bb_middle: Optional[float] = Field(None, description="Bollinger Band middle")
    bb_lower: Optional[float] = Field(None, description="Bollinger Band lower")
    breakout_detected: Optional[str] = Field(None, description="Breakout/Breakdown signal")
    last_updated: datetime = Field(..., description="Last update timestamp")

class MarketDataResponse(BaseModel):
    status: str = Field(..., description="Response status")
    message: str = Field(..., description="Response message")
    data: List[MarketData] = Field(..., description="Market data")
    count: int = Field(..., description="Number of records")
    last_updated: datetime = Field(..., description="Data timestamp")

def get_db_connection():
    """Create and return a database connection"""
    try:
        db_file = os.getenv('DB_FILE', 'nfo_data.duckdb')
        return duckdb.connect(db_file)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/api/v1/market-data", response_model=MarketDataResponse)
async def get_market_data() -> MarketDataResponse:
    """
    Get latest market data for all tokens
    Returns:
        MarketDataResponse: Market data response object
    """
    try:
        con = get_db_connection()
        
        # Get all market data
        result = con.execute("""
            SELECT *,
                   MAX(last_updated) OVER () as data_timestamp
            FROM latest_market_data
            ORDER BY symbol ASC
        """).fetchall()
        
        if not result:
            return MarketDataResponse(
                status="success",
                message="No data found",
                data=[],
                count=0,
                last_updated=datetime.now(IST).replace(tzinfo=None)
            )
        
        # Convert to list of dictionaries
        column_names = [desc[0] for desc in con.description]
        market_data = []
        data_timestamp = None
        
        for row in result:
            data_dict = dict(zip(column_names, row))
            data_timestamp = data_dict.pop('data_timestamp')
            market_data.append(MarketData(**data_dict))
        
        return MarketDataResponse(
            status="success",
            message="Data retrieved successfully",
            data=market_data,
            count=len(market_data),
            last_updated=data_timestamp or datetime.now(IST).replace(tzinfo=None)
        )
        
    except Exception as e:
        logger.error(f"Error fetching market data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching market data: {str(e)}"
        )
    finally:
        if 'con' in locals():
            con.close()

@app.get("/api/v1/market-data/{token}", response_model=MarketDataResponse)
async def get_token_data(token: str) -> MarketDataResponse:
    """
    Get latest market data for a specific token
    Args:
        token (str): Token ID
    Returns:
        MarketDataResponse: Market data response object
    """
    try:
        con = get_db_connection()
        
        # Get data for specific token
        result = con.execute("""
            SELECT *,
                   last_updated as data_timestamp
            FROM latest_market_data
            WHERE token = ?
        """, [token]).fetchall()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for token {token}"
            )
        
        # Convert to list of dictionaries
        column_names = [desc[0] for desc in con.description]
        market_data = []
        data_timestamp = None
        
        for row in result:
            data_dict = dict(zip(column_names, row))
            data_timestamp = data_dict.pop('data_timestamp')
            market_data.append(MarketData(**data_dict))
        
        return MarketDataResponse(
            status="success",
            message=f"Data retrieved successfully for token {token}",
            data=market_data,
            count=len(market_data),
            last_updated=data_timestamp or datetime.now(IST).replace(tzinfo=None)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching token data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching token data: {str(e)}"
        )
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))
    debug = os.getenv('API_DEBUG', 'True').lower() == 'true'
    
    uvicorn.run(
        "market_data_api:app",
        host=host,
        port=port,
        reload=debug
    ) 