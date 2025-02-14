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

- Focus on stocks eligible for futures and options trading only
- Daily data updates during market hours
- Historical data for derived calculations

### 3.2 Data Points and Sources

#### Stock Market Data (Source: Stock)

1. Basic Price Data
   - Open, High, Low, Close
   - Volume
   - Stock Name and Symbol

#### Futures Data (Source: Futures)

1. Future Price
2. Futures Premium % (derived: futures vs stock close)

#### Options Data (Source: Options)

1. ATM Option Price
2. Implied Volatility (IV)
3. ATM Price % relative to Future Price (derived)
4. ATM Premium % (derived: future and stock price)

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
    download_timestamp TIMESTAMP,
    PRIMARY KEY (token, timestamp)
)
```

#### technical_indicators Table (To Be Created)

```sql
CREATE TABLE technical_indicators (
    token VARCHAR,
    symbol VARCHAR,
    date DATE,
    ma_200_distance DOUBLE,
    high_21d DOUBLE,
    low_21d DOUBLE,
    high_52w DOUBLE,
    low_52w DOUBLE,
    ath DOUBLE,
    atl DOUBLE,
    volume_15d_avg DOUBLE,
    volume_ratio DOUBLE,
    breakout_detected BOOLEAN,
    PRIMARY KEY (token, date)
)
```

#### options_data Table (To Be Created)

```sql
CREATE TABLE options_data (
    token VARCHAR,
    symbol VARCHAR,
    date DATE,
    atm_price DOUBLE,
    iv DOUBLE,
    atm_future_ratio DOUBLE,
    future_premium_pct DOUBLE,
    atm_premium_pct DOUBLE,
    pcr DOUBLE,
    oi_change BIGINT,
    PRIMARY KEY (token, date)
)
```

#### market_data Table (To Be Created)

```sql
CREATE TABLE market_data (
    token VARCHAR,
    symbol VARCHAR,
    date DATE,
    result_date DATE,
    in_ban BOOLEAN,
    PRIMARY KEY (token, date)
)
```

## 4. Target Audience

- Primarily for personal use
- Potentially shared with 2-3 friends with similar trading interests

## 5. Phased Development Approach

### Phase 1: Backend Development (Data Acquisition and Storage - In Progress)

Build a Python backend to:

- ✅ Authenticate with Angel One API
- ✅ Set up DuckDB database with required tables
- ✅ Implement token data management (download and store NFO tokens)
- ✅ Implement historical data management (fetch and store daily candle data)
- 🔄 Optimize data fetching and storage for OPTSTK (Option Stocks)
- ⬜ Process and enrich data with technical indicators
- ⬜ Create FastAPI endpoints to serve data
- ⬜ Implement daily data fetching automation

**Current Status**: Core data fetching and storage functionality is implemented. Working on optimizing the token filtering and historical data retrieval for option stocks.

**Output of Phase 1**: Functional backend with API endpoints that provide tabular FnO data from DuckDB. Frontend not required in this phase, data can be tested via API clients (like Postman, Insomnia, or curl).

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
- ⬜ API Framework: FastAPI (pending)
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

### 7.4. Data Processing and Storage 🔄

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

#### 7.4.3. Technical Indicators (To Be Implemented) ⬜

- Moving Averages
  - [ ] 200-day MA
  - [ ] MA Distance (%)
  - [ ] 50-day MA (optional)
  - [ ] 20-day MA (optional)
- Price Levels
  - [ ] 21-day High
  - [ ] 21-day Low
  - [ ] 52-week High
  - [ ] 52-week Low
  - [ ] All-Time High (ATH)
  - [ ] All-Time Low (ATL)
  - [ ] Distance from ATH/ATL (%)
- Volume Analysis
  - [ ] 15-day Average Volume
  - [ ] Volume Ratio (Today/Average)
  - [ ] Volume Breakout Detection
- Pattern Recognition
  - [ ] Breakout Detection Logic
  - [ ] Support/Resistance Levels
  - [ ] Trend Direction

#### 7.4.4. Options Data (To Be Implemented) ⬜

- ATM Options
  - [ ] ATM Strike Price
  - [ ] ATM Call Price
  - [ ] ATM Put Price
  - [ ] ATM Call IV
  - [ ] ATM Put IV
- Premium Analysis
  - [ ] Future Premium (%)
  - [ ] ATM Premium (%)
  - [ ] ATM Price to Future Ratio
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

### 7.4.6. Data Validation and Quality Checks 🔄

#### Price Data Validation

- [ ] Check for missing values
- [ ] Validate price ranges (no negative prices)
- [ ] Verify High ≥ Low
- [ ] Verify Open/Close within High-Low range
- [ ] Check for unusual price movements

#### Volume Data Validation

- [ ] Check for negative volumes
- [ ] Validate against typical ranges
- [ ] Flag unusual volume spikes

#### Derived Data Validation

- [ ] Verify technical indicator calculations
- [ ] Validate percentage calculations
- [ ] Check for calculation artifacts

#### Data Freshness

- [ ] Verify data timestamp currency
- [ ] Check update frequencies
- [ ] Monitor data gaps

### 7.4.7. Data Enrichment Pipeline ⬜

#### Technical Analysis

- [ ] Implement MA calculations
- [ ] Calculate price levels
- [ ] Develop breakout detection
- [ ] Add volume analysis

#### Options Analysis

- [ ] Calculate IV
- [ ] Determine ATM strikes
- [ ] Compute premium percentages

#### Market Analysis

- [ ] Aggregate sentiment indicators
- [ ] Process corporate actions
- [ ] Track trading restrictions

### 7.5. FastAPI API Development ⬜

- ⬜ Create FastAPI application
- ⬜ Define data retrieval endpoints
- ⬜ Add query parameters support
- ⬜ Implement error handling
- ⬜ Add API documentation

### 7.6. Daily Data Fetching Automation ⬜

- ⬜ Create orchestration script
- ⬜ Set up scheduling
- ⬜ Add monitoring
- ⬜ Implement retry logic

### 7.7. Logging ✅

- ✅ Implement logging for API requests
- ✅ Add database operation logging
- ✅ Add error and exception logging
- ✅ Include processing summaries

### Phase 1 Completion Criteria

- [x] Backend code organization
- [x] Angel One API integration
- [x] DuckDB setup
- [ ] FastAPI endpoints
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
