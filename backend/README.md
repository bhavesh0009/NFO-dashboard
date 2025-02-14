# NFO Dashboard Backend

Backend service for NFO Dashboard providing data management and API endpoints.

## Setup

1. Create and activate virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up environment variables:

```bash
cp .env.example .env
# Edit .env with your credentials
```

## Running the Service

1. Start the FastAPI server:

```bash
python src/api/market_data_api.py
```

The API will be available at:

- Main endpoint: <http://localhost:8000/api/v1/market-data>
- Swagger docs: <http://localhost:8000/docs>
- ReDoc: <http://localhost:8000/redoc>

## Running Tests

```bash
pytest tests/ -v
```

## Project Structure

```
backend/
├── src/                    # Source code
│   ├── api/               # FastAPI routes and endpoints
│   ├── data/              # Data management modules
│   └── utils/             # Utility functions
├── tests/                 # Test files
├── data/                  # Database files
├── .env                   # Environment variables (git-ignored)
├── .env.example          # Environment template
└── requirements.txt       # Python dependencies
```

## API Endpoints

1. GET `/api/v1/market-data`
   - Get latest market data for all tokens
   - Returns OHLCV data and technical indicators

2. GET `/api/v1/market-data/{token}`
   - Get latest market data for a specific token
   - Returns detailed data for the specified token

## Data Update Schedule

- Spot data: Daily at 15:45 IST (market close)
- F&O data: Every 5-15 minutes during market hours
