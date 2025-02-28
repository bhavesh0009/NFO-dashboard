# FnO Trading Data Dashboard - Project Specification

## 1. Project Title

FnO Trading Data Dashboard

## 2. Project Goal

- To create a personal dashboard that fetches, stores, and displays daily FnO (Futures and Options) stock data from Angel One API
- To provide tabular data and basic charts to aid in personal trading decisions
- It will have few technical indicators, distance from 52w high and 52w low, ATH, ATL, breakout etc. information.
- It will also have data for price of ATM options, IV, OI, volume, change in OI, change in volume, change in price etc.
- To keep the project simple and focused on core functionality for initial personal use and potential sharing with a small group

## 3. Data Requirements

### 3.1 Data Scope

- Focus on specific stocks for initial implementation: RELIANCE, ITC, ZOMATO, MRF, IDEA
- Real-time data updates at 1-minute intervals during market hours
- Historical data for derived calculations
- Spot data from 1992 onwards
- Current day's data for futures and options
- ATM options tracking (1 CE + 1 PE) for each stock

### 3.2 Data Points and Sources

#### Stock Market Data (Source: Stock)

1. Basic Price Data
   - Open, High, Low, Close
   - Volume
   - Stock Name and Symbol
   - Historical data from 1992 for analysis
   - Last Traded Price (LTP)
   - Last Trade Quantity
   - Average Trade Price
   - Total Buy/Sell Quantities
   - Best Bid/Ask Prices
   - Net Change and Percent Change

#### Futures Data (Source: Futures)

1. Future Price (Current Day)
   - OHLC Data
   - LTP and Volume
   - Open Interest
   - Best Bid/Ask Prices
   - Total Buy/Sell Quantities
   - Net Change and Percent Change
2. Futures Premium % (derived: futures vs stock close)
3. Only nearest expiry contracts

#### Options Data (Source: Options)

1. ATM Option Price (Current Day)
   - One ATM Call (CE) and one ATM Put (PE) per stock
   - ATM strike selection based on:
     - Current futures price
     - Stock-specific strike intervals
     - Closest available strike to futures price
   - OHLC Data
   - LTP and Volume
   - Open Interest
   - Best Bid/Ask Prices
   - Total Buy/Sell Quantities
   - Strike Price
   - Option Type (CE/PE)
   - Net Change and Percent Change
2. Implied Volatility (IV)
3. ATM Price % relative to Future Price (derived)
4. ATM Premium % (derived: future and stock price)
5. Only nearest expiry contracts

#### Technical Indicators (Source: Stock-Derived)

1. Moving Averages
   - 200 Days MA Distance (%)
2. Price Levels
   - 21 Days High
   - 21 Days Low
   - 52 Weeks High/Low
   - All-Time High (ATH)
   - All-Time Low (ATL)
3. Volume Analysis
   - Today's Volume % (compared to 15-day average)
4. Pattern Recognition
   - Breakout Detection

#### Market Data (Source: To Be Determined)

1. Corporate Actions
   - Result Dates
2. Trading Restrictions
   - Securities in Ban Period
3. Market Sentiment
   - Put-Call Ratio (PCR)
   - Open Interest (OI) Change

### 3.3 Database Schema

#### tokens Table

```sql
CREATE TABLE tokens (
    token VARCHAR,
    symbol VARCHAR,
    formatted_symbol VARCHAR,
    name VARCHAR,
    expiry VARCHAR,
    strike DOUBLE,
    lotsize VARCHAR,
    instrumenttype VARCHAR,
    exch_seg VARCHAR,
    tick_size DOUBLE,
    token_type VARCHAR,  -- 'SPOT', 'FUTURES', or 'OPTIONS'
    download_timestamp TIMESTAMP
)
```

#### historical_data Table

```sql
CREATE TABLE historical_data (
    token VARCHAR,
    symbol VARCHAR,
    name VARCHAR,
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    oi BIGINT,
    token_type VARCHAR,  -- 'SPOT', 'FUTURES', or 'OPTIONS'
    download_timestamp TIMESTAMP,
    PRIMARY KEY (token, timestamp)
)
```

#### realtime_spot_data Table

```sql
CREATE TABLE realtime_spot_data (
    token VARCHAR,
    symbol VARCHAR,
    exchange VARCHAR,
    ltp DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    last_trade_qty INTEGER,
    avg_trade_price DOUBLE,
    volume BIGINT,
    total_buy_qty BIGINT,
    total_sell_qty BIGINT,
    best_bid_price DOUBLE,
    best_ask_price DOUBLE,
    net_change DOUBLE,
    percent_change DOUBLE,
    lower_circuit DOUBLE,
    upper_circuit DOUBLE,
    week_low_52 DOUBLE,
    week_high_52 DOUBLE,
    best_bid_orders INTEGER,  -- Number of orders at best bid
    best_ask_orders INTEGER,  -- Number of orders at best ask
    exch_feed_time TIMESTAMP,
    exch_trade_time TIMESTAMP,
    timestamp TIMESTAMP,
    PRIMARY KEY (token, timestamp)
)
```

#### realtime_futures_data Table

```sql
CREATE TABLE realtime_futures_data (
    token VARCHAR,
    symbol VARCHAR,
    exchange VARCHAR,
    ltp DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    last_trade_qty INTEGER,
    avg_trade_price DOUBLE,
    volume BIGINT,
    oi BIGINT,
    total_buy_qty BIGINT,
    total_sell_qty BIGINT,
    best_bid_price DOUBLE,
    best_ask_price DOUBLE,
    net_change DOUBLE,
    percent_change DOUBLE,
    lower_circuit DOUBLE,
    upper_circuit DOUBLE,
    week_low_52 DOUBLE,
    week_high_52 DOUBLE,
    best_bid_orders INTEGER,  -- Number of orders at best bid
    best_ask_orders INTEGER,  -- Number of orders at best ask
    exch_feed_time TIMESTAMP,
    exch_trade_time TIMESTAMP,
    timestamp TIMESTAMP,
    PRIMARY KEY (token, timestamp)
)
```

#### realtime_options_data Table

```sql
CREATE TABLE realtime_options_data (
    token VARCHAR,
    symbol VARCHAR,
    exchange VARCHAR,
    ltp DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    last_trade_qty INTEGER,
    avg_trade_price DOUBLE,
    volume BIGINT,
    oi BIGINT,
    total_buy_qty BIGINT,
    total_sell_qty BIGINT,
    best_bid_price DOUBLE,
    best_ask_price DOUBLE,
    net_change DOUBLE,
    percent_change DOUBLE,
    lower_circuit DOUBLE,
    upper_circuit DOUBLE,
    week_low_52 DOUBLE,
    week_high_52 DOUBLE,
    best_bid_orders INTEGER,  -- Number of orders at best bid
    best_ask_orders INTEGER,  -- Number of orders at best ask
    strike DOUBLE,
    option_type VARCHAR,  -- 'CE' or 'PE'
    exch_feed_time TIMESTAMP,
    exch_trade_time TIMESTAMP,
    timestamp TIMESTAMP,
    PRIMARY KEY (token, timestamp)
)
```

#### technical_indicators Table

```sql
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
    breakout_detected VARCHAR,  -- 'BREAKOUT', 'BREAKDOWN', or NULL
    calculation_timestamp TIMESTAMP,
    PRIMARY KEY (token, date)
)
```

#### latest_market_data Table

```sql
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
```

## 4. Target Audience

- Primarily for personal use
- Potentially shared with 2-3 friends with similar trading interests

## 5. Phased Development Approach

### Phase 1: Backend Development (Data Acquisition and Storage)

Build a Python backend to:

- ✅ Authenticate with Angel One API
- ✅ Set up DuckDB database with required tables
- ✅ Implement token data management (download and store NFO tokens)
- ✅ Implement historical data management (fetch and store daily candle data)
  - ✅ Token type identification (SPOT/FUTURES/OPTIONS)
  - ✅ Rate limiting implementation (1 request/second)
  - ✅ Retry logic for API calls
  - ✅ Fix timestamp conversion issues
  - ✅ Handle API timeouts
  - ✅ Implement data validation
  - ✅ Add progress tracking for long downloads
  - ✅ Implement chunked downloads for historical data
  - ✅ Add proper error handling and recovery
  - ✅ Add detailed logging and verification
- ✅ Process and enrich data with technical indicators
  - ✅ Moving Averages (20, 50, 200 days)
  - ✅ RSI (14 periods)
  - ✅ MACD (12, 26, 9)
  - ✅ Bollinger Bands
  - ✅ Price Levels (21d, 52w, ATH/ATL)
  - ✅ Volume Analysis
  - ✅ Breakout Detection
  - ✅ Create normalized latest market data view
- ✅ Create FastAPI endpoints to serve data
  - ✅ Implement GET /api/v1/market-data endpoint
  - ✅ Implement GET /api/v1/market-data/{token} endpoint
  - ✅ Add Pydantic models for validation
  - ✅ Add error handling and logging
  - ✅ Add CORS middleware
  - ✅ Add API documentation (Swagger/ReDoc)
  - ✅ Add test coverage
- ⬜ Implement real-time market data storage
  - ✅ Create tables for real-time data
  - ✅ Implement data fetching
  - ✅ Add proper error logging for storage operations
  - ✅ Add data validation before storage
  - ✅ Add storage success/failure logging
- ⬜ Implement daily data fetching automation

**Current Status**: Successfully implemented real-time market data fetching and storage with the following features:

1. ✅ Storage Operations:
   - Successfully storing SPOT, FUTURES, and OPTIONS market data
   - Proper error logging implemented
   - Data validation before storage
   - Storage success/failure confirmation added

2. ✅ Data Validation:
   - Validation for all fields before storage
   - Handling of missing or invalid data
   - Logging of validation failures

3. ✅ Error Handling:
   - Improved error messages
   - Proper cleanup for failed operations
   - Detailed error logging

4. ✅ Logging:
   - Detailed logging for storage operations
   - Success/failure metrics
   - Data quality metrics

**Next Steps**:

1. Set up daily data fetching automation
2. Implement the frontend dashboard
3. Add derived calculations (e.g., IV for options)
4. Implement market sentiment indicators
5. Add corporate actions tracking

### Phase 2: Frontend Development (Dashboard Visualization)

Develop a Next.js frontend with Shadcn/ui to:

- Consume the FastAPI backend API
- Display tabular data fetched from the API
- Implement basic charts (using a charting library) to visualize the data
- Create a simple, user-friendly dashboard layout

**Output of Phase 2**: Interactive web dashboard displaying FnO data in tables and charts, accessible via a web browser.

## 6. Tech Stack

### Backend (Phase 1)

- ✅ Programming Language: Python
- ✅ Data Source API: Angel One API (using smartapi-python v1.3.5)
- ✅ Database: DuckDB v0.9.2
- ✅ Authentication: pyotp v2.9.0
- ✅ Environment Variables: python-dotenv v1.0.1
- ✅ Timezone Handling: pytz v2024.1
- ✅ Logging: logzero v1.7.0
- ✅ API Framework: FastAPI v0.110.0
- ⬜ Scheduling: schedule library (pending)

### Frontend (Phase 2)

- Framework: Next.js (React framework)
- UI Library: Shadcn/ui (Tailwind CSS based components)
- State Management: React Context API (or Zustand/Recoil for simplicity if state becomes complex)
- HTTP Client: fetch API (or axios for convenience)
- Charting Library: Chart.js (or alternatives like Recharts, Nivo)

## 7. Phase 1: Backend Development - Detailed Steps & Checklist

### 7.1. Setup Development Environment ✅

- ✅ Install Python
- ✅ Create virtual environment
- ✅ Install required packages:

  ```bash
  pip install -r requirements.txt
  ```

### 7.2. Angel One API Integration ✅

- ✅ Get Angel One API credentials
- ✅ Create .env file with credentials
- ✅ Implement authentication code
- ✅ Test API connectivity
- ✅ Implement error handling

### 7.3. DuckDB Database Setup ✅

- ✅ Design and create tokens table
- ✅ Design and create historical_data table
- ✅ Implement database connection management
- ✅ Add data validation and error handling

### 7.4. Data Processing and Storage ✅

#### 7.4.1. Token Data Management ✅

- ✅ Basic Token Data
  - [x] Token ID
  - [x] Symbol
  - [x] Formatted Symbol
  - [x] Name
  - [x] Expiry
  - [x] Strike Price
  - [x] Lot Size
  - [x] Instrument Type
  - [x] Exchange Segment
  - [x] Tick Size
  - [x] Download Timestamp

#### 7.4.2. Historical Stock Data ✅

- ✅ OHLCV Data
  - [x] Open Price
  - [x] High Price
  - [x] Low Price
  - [x] Close Price
  - [x] Volume
  - [x] Timestamp
  - [x] Download Timestamp

#### 7.4.3. Technical Indicators ✅

- Pattern Recognition
  - [x] Breakout Detection Logic:
    - Volume > 2x 15-day average volume
    - Close > 21-day high
    - Close should not be more than 2% above 21-day high
  - [x] Breakdown Detection Logic:
    - Volume > 2x 15-day average volume
    - Close < 21-day low
    - Close should be within 0.5% of 21-day low
  - [x] Support/Resistance Levels (using MA and Bollinger Bands)
  - [x] Trend Direction (using MACD and MA)

**Implementation Strategy**: ✅

1. Data Preparation:
   - [x] Filter out current date from calculations
   - [x] Ensure proper date ordering
   - [x] Handle missing data points

2. Calculation Approach:
   - [x] Use pandas-ta for complex indicators
   - [x] Use DuckDB window functions for price levels
   - [x] Calculate ratios and percentages
   - [x] Store results in technical_indicators table

3. Performance Optimization:
   - [x] Use hybrid approach (pandas-ta + DuckDB)
   - [x] Implement batch processing
   - [x] Add proper indexing

4. Validation:
   - [x] Verify calculation accuracy
   - [x] Check for edge cases
   - [x] Validate against known values

#### 7.4.4. Options Data

- ATM Options
  - [x] ATM Strike Price Calculation
    - [x] Dynamic strike interval calculation per stock
    - [x] Precise ATM strike selection based on futures price
    - [x] Support for different strike intervals (1.0, 5.0, 10.0, 500.0 etc.)
  - [x] ATM Call and Put Selection
    - [x] One CE and one PE at ATM strike
    - [x] Real-time updates based on futures price movement
  - [ ] ATM Call/Put IV Calculation
  - [ ] Premium Analysis
- Market Sentiment
  - [ ] Put-Call Ratio (PCR)
  - [ ] OI Change
  - [ ] OI Interpretation

#### 7.4.5. Market Data (To Be Implemented) ⬜

- Corporate Actions
  - [ ] Result Dates
  - [ ] Result Updates
- Trading Status
  - [ ] Ban Period Status
  - [ ] Circuit Limits
  - [ ] Trading Restrictions

### 7.4.6. Data Collection Process

1. Token Management
   - [x] Initial token download and categorization
   - [x] Proper token type identification (SPOT/FUTURES/OPTIONS)
   - [x] Token to name mapping for efficient joins

2. Real-time Data Collection
   - [x] 1-minute interval data collection
   - [x] Parallel data fetching for efficiency
   - [x] Proper error handling and retry logic
   - [x] Market hours validation

3. Data Processing Flow
   - [x] Load initial tokens for configured symbols
   - [x] Parallel fetch of spot & futures prices
   - [x] Process futures prices for ATM calculation
   - [x] Calculate and update ATM strikes
   - [x] Fetch and store ATM options data
   - [x] Timestamp all stored data

4. Error Handling
   - [x] API call retry logic
   - [x] Data validation before storage
   - [x] Connection error recovery
   - [x] Proper error logging

5. Performance Optimization
   - [x] Batch API calls for multiple symbols
   - [x] Efficient database operations
   - [x] Proper connection management
   - [x] Resource cleanup

### 7.4.7. Data Storage

1. Real-time Tables
   - [x] Spot data storage with timestamp
   - [x] Futures data storage with timestamp
   - [x] Options data storage with timestamp
   - [x] Proper indexing for efficient queries

2. Data Validation
   - [x] Price range validation
   - [x] Strike price validation
   - [x] Timestamp validation
   - [x] Symbol mapping validation

### 7.5. FastAPI API Development ✅

- ✅ Create FastAPI application
  - [x] Initialize FastAPI app
  - [x] Add CORS middleware
  - [x] Add API documentation
- ✅ Define data retrieval endpoints
  - [x] GET /api/v1/market-data
  - [x] GET /api/v1/market-data/{token}
- ✅ Add Pydantic models
  - [x] MarketData model
  - [x] MarketDataResponse model
- ✅ Add error handling
  - [x] Database connection errors
  - [x] Not found errors
  - [x] Validation errors
- ✅ Add test coverage
  - [x] Endpoint testing
  - [x] Data validation testing
  - [x] Error handling testing

### 7.6. Daily Data Fetching Automation

#### 1. Market Data Collection Setup ⬜

- ⬜ Create MarketDataCollector class in `backend/src/data/market_data_collector.py`
- ⬜ Implement token refresh and validation logic
- ⬜ Add configuration for data collection intervals (default 5 seconds)
- ⬜ Implement rate limiting and API call optimization

#### 2. Data Collection Implementation ⬜

- ⬜ Implement spot data collection (LTP every 5 seconds)
- ⬜ Implement futures data collection (LTP every 5 seconds)
- ⬜ Implement ATM options data collection (LTP every 5 seconds)
- ⬜ Add token batching (50 tokens per request)
- ⬜ Implement ATM strike calculation and tracking

#### 3. Data Storage Implementation ⬜

- ⬜ Create tables for real-time data storage
  - ⬜ realtime_spot_data
  - ⬜ realtime_futures_data
  - ⬜ realtime_options_data
- ⬜ Implement efficient data insertion methods
- ⬜ Add data validation and error handling

#### 4. Market Hours Management ⬜

- ⬜ Implement market hours detection (9:15 AM - 3:45 PM)
- ⬜ Add pre-market preparation (9:00 AM - 9:15 AM)
- ⬜ Implement graceful shutdown at market close

#### 5. Error Handling and Recovery ⬜

- ⬜ Implement connection error recovery
- ⬜ Add retry logic for failed API calls
- ⬜ Implement data validation and cleanup
- ⬜ Add error logging and notifications

#### 6. Performance Optimization ⬜

- ⬜ Implement token batching optimization
- ⬜ Add caching for frequently accessed data
- ⬜ Optimize database operations
- ⬜ Add performance monitoring

#### 7. Monitoring and Logging ⬜

- ⬜ Add detailed logging for all operations
- ⬜ Implement health checks
- ⬜ Add performance metrics collection
- ⬜ Create monitoring dashboard

#### 8. Testing ⬜

- ⬜ Create unit tests for data collection
- ⬜ Add integration tests
- ⬜ Implement load testing
- ⬜ Add error scenario testing

#### 9. Documentation ⬜

- ⬜ Update API documentation
- ⬜ Add system architecture documentation
- ⬜ Create troubleshooting guide
- ⬜ Document configuration options

#### Completion Criteria

- [ ] All data collection components implemented and tested
- [ ] Error handling and recovery mechanisms in place
- [ ] Performance optimizations completed
- [ ] Monitoring and logging system operational
- [ ] Documentation updated

### 7.7. Logging ✅

- ✅ Implement logging for API requests
- ✅ Add database operation logging
- ✅ Add error and exception logging
- ✅ Include processing summaries

### Phase 1 Completion Criteria

- [x] Backend code organization
- [x] Angel One API integration
- [x] DuckDB setup
- [x] FastAPI endpoints
- [ ] Automated data fetching
- [x] Logging implementation
- [x] API testing

## 8. Phase 2: Frontend Development - High-Level Steps & Checklist

### 8.1. Frontend Project Setup

- Create Next.js project
- Configure Shadcn/ui and Tailwind CSS
- Install charting library

### 8.2. API Integration in Frontend

- Implement API calls
- Add error handling

### 8.3. Table Display

- Implement data tables
- Add sorting/pagination (optional)

### 8.4. Chart Integration

- Select chart types
- Implement data visualization
- Connect to API data

### 8.5. Dashboard Layout and UI

- Design dashboard layout
- Arrange components
- Add UI elements

### 8.6. State Management

- Implement if needed
- Use React Context API or alternatives

### Phase 2 Completion Criteria

- [ ] Project setup
- [ ] API integration
- [ ] Table implementation
- [ ] Chart implementation
- [ ] Dashboard layout
- [ ] Frontend testing

## 9. Stretch Goals (Optional Enhancements)

### Backend

- Advanced technical indicators
- Query optimization
- Data caching
- Enhanced error handling
- User authentication

### Frontend

- Advanced visualizations
- Enhanced filtering
- User preferences
- Responsive design
- Advanced analysis tools

## 10. Timeline

- Phase 1 (Backend): 1-2 weeks
- Phase 2 (Frontend): 1-2 weeks

## 11. Assumptions and Dependencies

- Angel One API access
- Data format compatibility
- DuckDB suitability
- Basic programming knowledge

## 12. Risks and Mitigation

### API Risks

- **Risk**: API changes/unavailability
- **Mitigation**: Monitoring, error handling, alternative sources

### Documentation Risks

- **Risk**: API documentation challenges
- **Mitigation**: Thorough review, support engagement

### Data Risks

- **Risk**: Data accuracy issues
- **Mitigation**: Validation, logging, testing

## Current Status

### Recent Fixes

- Fixed timestamp precision issues by using standard `TIMESTAMP` types
- Implemented proper date handling with explicit casting
- Added data verification and improved logging
- Increased historical data lookback period to 84 days for technical indicators

### Current Issues

1. **Primary Key Constraint Violations**
   - Getting duplicate key errors when storing historical data
   - Example error:

  ```log
  Constraint Error: Duplicate key "token: 14309, timestamp: 2025-02-27 00:00:00" 
  violates primary key constraint
  ```

- Root cause: DELETE operation not properly clearing existing records before INSERT
- Affects daily records with the same date

2. **System Clock Issues**
   - System clock appears to be set to 2025
   - Causing inconsistencies in date handling
   - Need more robust date validation

## Implementation Plan

### 1. Fix Historical Data Storage

Current schema:

  ```sql
  CREATE TABLE historical_data (
      token VARCHAR,
      symbol VARCHAR,
      name VARCHAR,
      timestamp TIMESTAMP,
      open DOUBLE,
      high DOUBLE,
      low DOUBLE,
      close DOUBLE,
      volume BIGINT,
      oi BIGINT,
      token_type VARCHAR,
      download_timestamp TIMESTAMP,
      PRIMARY KEY (token, timestamp)
  )
  ```

Required changes:

- Implement UPSERT instead of DELETE + INSERT
- Add proper date validation
- Improve error handling for duplicate records

### 2. Technical Indicators Calculation

Current issues:

- Insufficient historical data for some tokens
- NULL values in temporary tables
- Date conversion inconsistencies

Required changes:

- Add data sufficiency validation
- Improve temporary table handling
- Fix date casting in SQL queries

### 3. Date Handling Improvements

Required changes:

- Add consistent date validation across all modules
- Implement reference date system
- Add timezone handling for all date operations

## Dependencies

- DuckDB for data storage
- Pandas and pandas-ta for technical analysis
- FastAPI for API endpoints
- Python 3.9+

## Testing Requirements

- Add unit tests for date handling
- Add integration tests for data storage
- Add validation tests for technical indicators

## Next Steps

1. Update historical data storage:

  ```python
  def _store_historical_data(self, historical_data: Dict[str, Any], token_info: Dict[str, Any]) -> bool:
      """Store historical data using UPSERT pattern"""
      # Implementation here
  ```

2. Improve technical indicators calculation:

  ```python
  def calculate_indicators(self, token: str) -> bool:
      """Calculate technical indicators with improved validation"""
      # Implementation here
  ```

3. Add robust date handling:

  ```python
  def validate_and_normalize_date(self, date: datetime) -> datetime:
      """Ensure dates are valid and normalized"""
      # Implementation here
  ```
