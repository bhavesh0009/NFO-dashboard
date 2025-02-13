# FnO Trading Data Dashboard

A personal dashboard for fetching, storing, and displaying daily FnO (Futures and Options) stock data from Angel One API.

## Project Setup

### Prerequisites

1. Python 3.8 or higher
2. Angel One Trading Account with API access
3. API Credentials from Angel One:
   - API Key
   - Client ID
   - PIN
   - TOTP Secret

### Installation Steps

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd nfo-dashboard
   ```

2. Create a virtual environment:

   ```bash
   # Windows
   python -m venv venv
   .\venv\Scripts\activate

   # Linux/Mac
   python -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Create environment file:

   ```bash
   # Copy the example env file
   cp .env.example .env
   
   # Edit .env with your credentials
   # ANGEL_ONE_APP_KEY=your_app_key_here
   # ANGEL_ONE_CLIENT_ID=your_client_id_here
   # ANGEL_ONE_PIN=your_pin_here
   # ANGEL_ONE_TOTP_SECRET=your_totp_secret_here
   ```

### Testing the Setup

1. Test Angel One API connection:

   ```bash
   python src/api/angel_one_connector.py
   ```

2. Test token data download:

   ```bash
   python src/data/token_manager.py
   ```

## Project Structure

```
nfo-dashboard/
├── src/
│   ├── api/
│   │   └── angel_one_connector.py   # Angel One API connection handling
│   ├── data/
│   │   └── token_manager.py         # Token data management
│   └── utils/                       # Utility functions
├── tests/                           # Test files
├── .env.example                     # Environment variables template
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

## Development Status

Currently implementing Phase 1 (Backend Development) as per the project specification. This includes:

- [x] Environment setup
- [x] Angel One API integration
- [x] DuckDB database setup
- [ ] FastAPI endpoints
- [ ] Automated data fetching
- [ ] Logging implementation
- [ ] API testing

## License

This project is for personal use and learning purposes.
