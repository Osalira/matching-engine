# Matching Engine Service

This service implements a high-performance FIFO matching algorithm for the Day Trading System, handling order matching and order book management.

## Features

- FIFO (First In, First Out) order matching algorithm
- Price-time priority ordering
- Real-time order book management
- Concurrent processing with thread safety
- Trade notifications via channels
- RESTful API endpoints
- Health monitoring

## Prerequisites

- Go 1.21+
- Make (optional, for build automation)

## Setup

1. Install Go dependencies:
```bash
go mod download
```

2. Build the service:
```bash
go build -o matching-engine
```

## Running the Service

Start the service:
```bash
./matching-engine
```

Or directly with Go:
```bash
go run main.go
```

The service will run on `http://localhost:8080`

## API Endpoints

### Order Management

- `POST /api/orders` - Place a new order
  ```json
  {
    "symbol": "AAPL",
    "order_type": "BUY",
    "price": 150.00,
    "quantity": 100,
    "user_id": "user123"
  }
  ```
  Response:
  ```json
  {
    "order_id": "order_1234567890",
    "status": "accepted"
  }
  ```

### Health Check

- `GET /health` - Service health check

## Order Matching Algorithm

The service implements a FIFO matching algorithm with the following characteristics:

1. **Price-Time Priority**:
   - Buy orders are sorted by price (descending) and time (ascending)
   - Sell orders are sorted by price (ascending) and time (ascending)
   - Orders at the same price level are matched in time priority (FIFO)

2. **Matching Process**:
   - New orders are immediately tried for matching
   - Buy orders match with lowest-priced sell orders
   - Sell orders match with highest-priced buy orders
   - Partial fills are supported

3. **Order Book Management**:
   - Thread-safe operations using mutexes
   - Efficient order insertion maintaining sort order
   - Automatic cleanup of filled orders

## Trade Notifications

When orders are matched, the service generates trade notifications with:
- Buy and sell order IDs
- Executed price and quantity
- Buyer and seller user IDs
- Timestamp
- Symbol

These notifications can be consumed by other services to:
1. Update user wallets
2. Update stock positions
3. Send notifications to users
4. Update trade history

## Performance Considerations

- Uses Go channels for efficient trade notifications
- Thread-safe operations for concurrent processing
- Efficient data structures for order book management
- Optimized sorting and matching algorithms

## Development

- Run tests:
```bash
go test ./...
```

- Run with debug logging:
```bash
LOG_LEVEL=debug ./matching-engine
```

## Environment Variables

```env
PORT=8080                    # Server port
LOG_LEVEL=INFO              # Logging level (DEBUG, INFO, WARN, ERROR)
TRADE_CHANNEL_BUFFER=1000   # Buffer size for trade notifications
```

## Security Notes

- Input validation on all requests
- Concurrent access protection
- Error handling and logging
- Health monitoring
- Rate limiting should be implemented at the API Gateway level

## Integration with Other Services

The matching engine integrates with:
1. Trading Service - Receives orders and sends trade notifications
2. Logging Service - Audit trail of trades
3. API Gateway - External request routing

## Architecture Notes

1. **Order Book Per Symbol**:
   - Each trading symbol has its own order book
   - Order books are created on demand
   - Thread-safe operations per order book

2. **Trade Processing**:
   - Matches are processed immediately
   - Trade notifications are buffered
   - Asynchronous notification processing

3. **Error Handling**:
   - Validation errors return 400
   - System errors return 500
   - Detailed error messages for debugging 