# NFO Dashboard Frontend Design Specification

## 1. Design Goals

- Create a clean, data-dense dashboard optimized for trading decisions
- Focus on quick scanning of key metrics across multiple stocks
- Highlight important signals and breakouts
- Support both table and chart views of data

## 2. Core Views

### 2.1 Market Overview Table

- Compact, dense table showing all stocks
- Sticky header with column sorting
- Color coding for price changes and signals
- Quick filters for token types (Spot/F&O)
- Columns:
  - Symbol & Name
  - OHLC prices
  - Volume with % change
  - Key indicators (MA, RSI, MACD)
  - Breakout signals
  - Distance from key levels (52w H/L, ATH/ATL)


## 3. Visual Hierarchy

### 3.1 Color Scheme

- Light theme default with dark mode support
- Color coding:
  - Green: Positive changes, bullish signals
  - Red: Negative changes, bearish signals  
  - Blue: Neutral information
  - Yellow: Warnings/alerts
  - Gray: Secondary information

### 3.2 Typography

- Clear, readable font (Inter or similar)
- Three size hierarchies:
  - Large: Current price, major headers
  - Medium: Table headers, key metrics
  - Small: Detailed data, secondary info

### 3.3 Layout

- Fixed header with filters and controls
- Main content area with responsive grid
- Sidebar for detailed view
- Bottom bar for system status

## 4. Interactive Elements

### 4.1 Core Controls

- Search bar with symbol autocomplete
- Quick filters for:
  - Token types (Spot/Futures/Options)
  - Price change %
  - Volume change %
  - Technical signals
- Column visibility toggles
- View mode switches (table/cards)
- Dynamic column visibility. User can show/hide columns as per their choice. 
- Support of pagination.

### 4.2 Data Interactions

- Click row for detailed view


## 5. Performance Considerations

### 5.1 Data Display

- Virtual scrolling for large tables
- Progressive loading of charts
- Cached calculations
- Debounced search/filters

### 5.2 Updates

- Real-time price updates
- Batched indicator updates
- Visual indicators for data freshness
- Clear loading states

## 6. Mobile Responsiveness

### 6.1 Mobile Views

- Simplified table view
- Stacked metrics
- Swipe gestures for navigation
- Bottom navigation bar
- Optimized charts for touch

### 6.2 Tablet Views

- Split screen capabilities
- Touch-optimized controls
- Landscape orientation support

## 7. Accessibility

- High contrast ratios
- Keyboard navigation
- Screen reader support
- Clear focus indicators
- Scalable text
- Alternative text for charts

## 8. Error States

- Clear error messages
- Fallback UI components
- Offline indicators
- Data staleness warnings
- Retry mechanisms
- Graceful degradation
