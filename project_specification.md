# FnO Trading Data Dashboard - Project Specification

## 1. Project Title

FnO Trading Data Dashboard

## 2. Project Goal

- To create a personal dashboard that fetches, stores, and displays daily FnO (Futures and Options) stock data from Angel One API
- To provide tabular data and basic charts to aid in personal trading decisions
- It will have few technical indicators, distance from 52w high and 52w low, ATH, ATL, breakout etc. information.
- It will also have data for price of ATM options, IV, OI, volume, change in OI, change in volume, change in price etc.
- To keep the project simple and focused on core functionality for initial personal use and potential sharing with a small group

## 3. Target Audience

- Primarily for personal use
- Potentially shared with 2-3 friends with similar trading interests

## 4. Phased Development Approach

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

## 5. Tech Stack

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

## 6. Phase 1: Backend Development - Detailed Steps & Checklist

### 6.1. Setup Development Environment ✅

- ✅ Install Python
- ✅ Create virtual environment
- ✅ Install required packages:

  ```bash
  pip install -r requirements.txt
  ```

### 6.2. Angel One API Integration ✅

- ✅ Get Angel One API credentials
- ✅ Create .env file with credentials
- ✅ Implement authentication code
- ✅ Test API connectivity
- ✅ Implement error handling

### 6.3. DuckDB Database Setup ✅

- ✅ Design and create tokens table
- ✅ Design and create historical_data table
- ✅ Implement database connection management
- ✅ Add data validation and error handling

### 6.4. Data Processing and Storage 🔄

- ✅ Implement token data fetching
- ✅ Store token data in DuckDB
- ✅ Implement historical data fetching
- ✅ Store historical data in DuckDB
- 🔄 Optimize token filtering for OPTSTK
- ⬜ Add technical indicators
- ⬜ Implement data enrichment

### 6.5. FastAPI API Development ⬜

- ⬜ Create FastAPI application
- ⬜ Define data retrieval endpoints
- ⬜ Add query parameters support
- ⬜ Implement error handling
- ⬜ Add API documentation

### 6.6. Daily Data Fetching Automation ⬜

- ⬜ Create orchestration script
- ⬜ Set up scheduling
- ⬜ Add monitoring
- ⬜ Implement retry logic

### 6.7. Logging ✅

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

## 7. Phase 2: Frontend Development - High-Level Steps & Checklist

### 7.1. Frontend Project Setup

- Create Next.js project
- Configure Shadcn/ui and Tailwind CSS
- Install charting library

### 7.2. API Integration in Frontend

- Implement API calls
- Add error handling

### 7.3. Table Display

- Implement data tables
- Add sorting/pagination (optional)

### 7.4. Chart Integration

- Select chart types
- Implement data visualization
- Connect to API data

### 7.5. Dashboard Layout and UI

- Design dashboard layout
- Arrange components
- Add UI elements

### 7.6. State Management

- Implement if needed
- Use React Context API or alternatives

### Phase 2 Completion Criteria

- [ ] Project setup
- [ ] API integration
- [ ] Table implementation
- [ ] Chart implementation
- [ ] Dashboard layout
- [ ] Frontend testing

## 8. Stretch Goals (Optional Enhancements)

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

## 9. Timeline

- Phase 1 (Backend): 1-2 weeks
- Phase 2 (Frontend): 1-2 weeks

## 10. Assumptions and Dependencies

- Angel One API access
- Data format compatibility
- DuckDB suitability
- Basic programming knowledge

## 11. Risks and Mitigation

### API Risks

- **Risk**: API changes/unavailability
- **Mitigation**: Monitoring, error handling, alternative sources

### Documentation Risks

- **Risk**: API documentation challenges
- **Mitigation**: Thorough review, support engagement

### Data Risks

- **Risk**: Data accuracy issues
- **Mitigation**: Validation, logging, testing
