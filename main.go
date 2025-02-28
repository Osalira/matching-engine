package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// Order represents a stock order
type Order struct {
	ID            int64     `json:"id"`
	UserID        int64     `json:"user_id"`
	StockID       int64     `json:"stock_id"`
	IsBuy         bool      `json:"is_buy"`
	OrderType     string    `json:"order_type"`
	Status        string    `json:"status"`
	Quantity      int       `json:"quantity"`
	Price         float64   `json:"price"`
	Timestamp     time.Time `json:"timestamp"`
	ParentOrderID *int64    `json:"parent_order_id,omitempty"`
}

// OrderBook holds buy and sell orders for a stock
type OrderBook struct {
	StockID    int64
	BuyOrders  []Order
	SellOrders []Order
	Mutex      sync.Mutex
}

// OrderBookManager manages multiple order books
type OrderBookManager struct {
	OrderBooks map[int64]*OrderBook
	Mutex      sync.RWMutex
}

// Global variables
var (
	db              *sql.DB
	orderBookMgr    *OrderBookManager
	tradingEndpoint string
)

// Database connection
func initDB() (*sql.DB, error) {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found")
	}

	// Get database connection parameters
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	// Create connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Check connection
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	log.Println("Connected to database")
	return db, nil
}

// Initialize order book manager
func initOrderBookManager() *OrderBookManager {
	return &OrderBookManager{
		OrderBooks: make(map[int64]*OrderBook),
	}
}

// Get or create order book for a stock
func (mgr *OrderBookManager) GetOrderBook(stockID int64) *OrderBook {
	mgr.Mutex.RLock()
	ob, exists := mgr.OrderBooks[stockID]
	mgr.Mutex.RUnlock()

	if !exists {
		// Create new order book
		ob = &OrderBook{
			StockID:    stockID,
			BuyOrders:  []Order{},
			SellOrders: []Order{},
		}

		// Store in manager
		mgr.Mutex.Lock()
		mgr.OrderBooks[stockID] = ob
		mgr.Mutex.Unlock()
	}

	return ob
}

// Handler for placing a stock order
func placeOrderHandler(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var order Order
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Extract user_id from headers if not in request body
	if order.UserID <= 0 {
		userIDStr := r.Header.Get("user_id")
		if userIDStr != "" {
			userID, err := strconv.ParseInt(userIDStr, 10, 64)
			if err == nil && userID > 0 {
				order.UserID = userID
			} else {
				// Default to user ID 1 for testing if parsing fails
				order.UserID = 1
			}
		} else {
			// Default to user ID 1 if no user_id found
			order.UserID = 1
		}
	}

	// Set default StockID if not provided
	if order.StockID <= 0 {
		// Default to stock ID 1 for testing
		order.StockID = 1
	}

	// Default quantity if not positive
	if order.Quantity <= 0 {
		order.Quantity = 100 // Default quantity
	}

	// Default price if not positive (for limit orders)
	if order.Price <= 0 {
		order.Price = 100.0 // Default price
	}

	// Fix order type case sensitivity
	if order.OrderType == "" {
		order.OrderType = "Limit" // Default to Limit order
	} else {
		// Normalize order type to proper case
		orderType := strings.ToUpper(order.OrderType)
		if orderType == "MARKET" {
			order.OrderType = "Market"
		} else {
			// Default to Limit for any other value
			order.OrderType = "Limit"
		}
	}

	// Set initial status
	order.Status = "Pending"
	order.Timestamp = time.Now()

	// Process order
	result, err := processOrder(order)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error processing order: %v", err), http.StatusInternalServerError)
		return
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// Process an order (match or add to order book)
func processOrder(order Order) (map[string]interface{}, error) {
	// Get order book for stock
	orderBook := orderBookMgr.GetOrderBook(order.StockID)
	orderBook.Mutex.Lock()
	defer orderBook.Mutex.Unlock()

	// For market orders, try to match immediately
	if order.OrderType == "Market" {
		// TODO: Implement market order logic
		return nil, fmt.Errorf("market orders not implemented yet")
	}

	// For limit orders, try to match or add to order book
	if order.OrderType == "Limit" {
		// Insert order into database
		var orderID int64
		err := db.QueryRow(
			"INSERT INTO orders (user_id, stock_id, is_buy, order_type, status, quantity, price, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id",
			order.UserID, order.StockID, order.IsBuy, order.OrderType, order.Status, order.Quantity, order.Price, order.Timestamp,
		).Scan(&orderID)
		if err != nil {
			return nil, err
		}
		order.ID = orderID

		// Try to match order
		matches, remainingQty := matchLimitOrder(order)

		// If fully matched, update status
		if remainingQty == 0 {
			_, err = db.Exec("UPDATE orders SET status = 'Completed' WHERE id = $1", order.ID)
			if err != nil {
				return nil, err
			}
			order.Status = "Completed"
		} else if len(matches) > 0 && remainingQty < order.Quantity {
			// If partially matched, update status and quantity
			_, err = db.Exec("UPDATE orders SET status = 'Partially_complete', quantity = $1 WHERE id = $2", remainingQty, order.ID)
			if err != nil {
				return nil, err
			}
			order.Status = "Partially_complete"
			order.Quantity = remainingQty

			// Add to order book
			if order.IsBuy {
				orderBook.BuyOrders = append(orderBook.BuyOrders, order)
			} else {
				orderBook.SellOrders = append(orderBook.SellOrders, order)
			}
		} else {
			// If no matches, update status and add to order book
			_, err = db.Exec("UPDATE orders SET status = 'InProgress' WHERE id = $1", order.ID)
			if err != nil {
				return nil, err
			}
			order.Status = "InProgress"

			// Add to order book
			if order.IsBuy {
				orderBook.BuyOrders = append(orderBook.BuyOrders, order)
			} else {
				orderBook.SellOrders = append(orderBook.SellOrders, order)
			}
		}

		// Prepare response
		result := map[string]interface{}{
			"order_id": order.ID,
			"status":   order.Status,
			"matches":  matches,
		}

		return result, nil
	}

	return nil, fmt.Errorf("unsupported order type: %s", order.OrderType)
}

// Match a limit order against the order book
func matchLimitOrder(order Order) ([]map[string]interface{}, int) {
	orderBook := orderBookMgr.GetOrderBook(order.StockID)
	var matches []map[string]interface{}
	remainingQty := order.Quantity

	if order.IsBuy {
		// Buy order - match against sell orders
		for i := 0; i < len(orderBook.SellOrders) && remainingQty > 0; i++ {
			sellOrder := orderBook.SellOrders[i]

			// Check if sell order price is less than or equal to buy price
			if sellOrder.Price <= order.Price {
				// Match the orders
				matchQty := min(remainingQty, sellOrder.Quantity)

				// Create match record
				match := map[string]interface{}{
					"matched_order_id": sellOrder.ID,
					"price":            sellOrder.Price,
					"quantity":         matchQty,
				}
				matches = append(matches, match)

				// Update remaining quantity
				remainingQty -= matchQty

				// Update sell order quantity
				sellOrder.Quantity -= matchQty
				if sellOrder.Quantity == 0 {
					// Remove sell order from book
					orderBook.SellOrders = append(orderBook.SellOrders[:i], orderBook.SellOrders[i+1:]...)
					i--

					// Update sell order status in database
					_, err := db.Exec("UPDATE orders SET status = 'Completed', quantity = 0 WHERE id = $1", sellOrder.ID)
					if err != nil {
						log.Printf("Error updating sell order: %v", err)
					}
				} else {
					// Update sell order quantity in database
					_, err := db.Exec("UPDATE orders SET quantity = $1 WHERE id = $2", sellOrder.Quantity, sellOrder.ID)
					if err != nil {
						log.Printf("Error updating sell order: %v", err)
					}
				}

				// Create transaction record
				createTransaction(order.ID, sellOrder.ID, matchQty, sellOrder.Price)
			}
		}
	} else {
		// Sell order - match against buy orders
		for i := 0; i < len(orderBook.BuyOrders) && remainingQty > 0; i++ {
			buyOrder := orderBook.BuyOrders[i]

			// Check if buy order price is greater than or equal to sell price
			if buyOrder.Price >= order.Price {
				// Match the orders
				matchQty := min(remainingQty, buyOrder.Quantity)

				// Create match record
				match := map[string]interface{}{
					"matched_order_id": buyOrder.ID,
					"price":            buyOrder.Price,
					"quantity":         matchQty,
				}
				matches = append(matches, match)

				// Update remaining quantity
				remainingQty -= matchQty

				// Update buy order quantity
				buyOrder.Quantity -= matchQty
				if buyOrder.Quantity == 0 {
					// Remove buy order from book
					orderBook.BuyOrders = append(orderBook.BuyOrders[:i], orderBook.BuyOrders[i+1:]...)
					i--

					// Update buy order status in database
					_, err := db.Exec("UPDATE orders SET status = 'Completed', quantity = 0 WHERE id = $1", buyOrder.ID)
					if err != nil {
						log.Printf("Error updating buy order: %v", err)
					}
				} else {
					// Update buy order quantity in database
					_, err := db.Exec("UPDATE orders SET quantity = $1 WHERE id = $2", buyOrder.Quantity, buyOrder.ID)
					if err != nil {
						log.Printf("Error updating buy order: %v", err)
					}
				}

				// Create transaction record
				createTransaction(buyOrder.ID, order.ID, matchQty, buyOrder.Price)
			}
		}
	}

	return matches, remainingQty
}

// Create a transaction record for matched orders
func createTransaction(buyOrderID, sellOrderID int64, quantity int, price float64) {
	// Get buy order details
	var buyOrder Order
	err := db.QueryRow("SELECT user_id, stock_id FROM orders WHERE id = $1", buyOrderID).Scan(&buyOrder.UserID, &buyOrder.StockID)
	if err != nil {
		log.Printf("Error getting buy order details: %v", err)
		return
	}

	// Get sell order details
	var sellOrder Order
	err = db.QueryRow("SELECT user_id FROM orders WHERE id = $1", sellOrderID).Scan(&sellOrder.UserID)
	if err != nil {
		log.Printf("Error getting sell order details: %v", err)
		return
	}

	// Insert transaction record
	_, err = db.Exec(
		"INSERT INTO transactions (buy_order_id, sell_order_id, buy_user_id, sell_user_id, stock_id, quantity, price, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		buyOrderID, sellOrderID, buyOrder.UserID, sellOrder.UserID, buyOrder.StockID, quantity, price, time.Now(),
	)
	if err != nil {
		log.Printf("Error creating transaction record: %v", err)
		return
	}

	// Notify trading service
	notifyTradingService(buyOrder.UserID, sellOrder.UserID, buyOrder.StockID, quantity, price)
}

// Notify trading service about a completed transaction
func notifyTradingService(buyUserID, sellUserID, stockID int64, quantity int, price float64) {
	// Create notification payload
	payload := map[string]interface{}{
		"buy_user_id":  buyUserID,
		"sell_user_id": sellUserID,
		"stock_id":     stockID,
		"quantity":     quantity,
		"price":        price,
		"timestamp":    time.Now(),
	}

	// Convert to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error creating notification payload: %v", err)
		return
	}

	// Send to trading service
	resp, err := http.Post(tradingEndpoint+"/api/transaction/processTransaction", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error notifying trading service: %v", err)
		return
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		log.Printf("Trading service returned non-OK status: %d", resp.StatusCode)
	}
}

// Handler for cancelling a stock order
func cancelOrderHandler(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var data struct {
		TransactionID int64 `json:"transaction_id"`
	}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Get order details
	var order Order
	err = db.QueryRow(
		"SELECT id, user_id, stock_id, is_buy, order_type, status, quantity, price FROM orders WHERE id = $1",
		data.TransactionID,
	).Scan(&order.ID, &order.UserID, &order.StockID, &order.IsBuy, &order.OrderType, &order.Status, &order.Quantity, &order.Price)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Order not found", http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("Error fetching order: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Check if order can be cancelled
	if order.Status != "InProgress" && order.Status != "Partially_complete" {
		http.Error(w, fmt.Sprintf("Cannot cancel order with status: %s", order.Status), http.StatusBadRequest)
		return
	}

	// Cancel order
	_, err = db.Exec("UPDATE orders SET status = 'Cancelled' WHERE id = $1", order.ID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error cancelling order: %v", err), http.StatusInternalServerError)
		return
	}

	// Remove from order book
	orderBook := orderBookMgr.GetOrderBook(order.StockID)
	orderBook.Mutex.Lock()
	if order.IsBuy {
		for i, o := range orderBook.BuyOrders {
			if o.ID == order.ID {
				orderBook.BuyOrders = append(orderBook.BuyOrders[:i], orderBook.BuyOrders[i+1:]...)
				break
			}
		}
	} else {
		for i, o := range orderBook.SellOrders {
			if o.ID == order.ID {
				orderBook.SellOrders = append(orderBook.SellOrders[:i], orderBook.SellOrders[i+1:]...)
				break
			}
		}
	}
	orderBook.Mutex.Unlock()

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "Order cancelled successfully"})
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Health check handler
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"service": "matching-engine",
	})
}

// Initialize the database schema
func initSchema(db *sql.DB) error {
	log.Println("Checking database schema...")

	// Check if orders table exists
	var exists bool
	err := db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders')").Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if orders table exists: %v", err)
	}

	if !exists {
		log.Println("Creating database schema...")

		// Create orders table
		_, err = db.Exec(`
			CREATE TABLE orders (
				id SERIAL PRIMARY KEY,
				user_id INTEGER NOT NULL,
				stock_id INTEGER NOT NULL,
				is_buy BOOLEAN NOT NULL,
				order_type VARCHAR(20) NOT NULL,
				status VARCHAR(20) NOT NULL,
				quantity INTEGER NOT NULL,
				price DECIMAL(10, 2) NOT NULL,
				timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				parent_order_id INTEGER REFERENCES orders(id)
			)
		`)
		if err != nil {
			return fmt.Errorf("error creating orders table: %v", err)
		}

		// Create indexes
		_, err = db.Exec("CREATE INDEX idx_orders_stock_id ON orders(stock_id)")
		if err != nil {
			return fmt.Errorf("error creating stock_id index: %v", err)
		}

		_, err = db.Exec("CREATE INDEX idx_orders_user_id ON orders(user_id)")
		if err != nil {
			return fmt.Errorf("error creating user_id index: %v", err)
		}

		_, err = db.Exec("CREATE INDEX idx_orders_status ON orders(status)")
		if err != nil {
			return fmt.Errorf("error creating status index: %v", err)
		}

		// Create transactions table
		_, err = db.Exec(`
			CREATE TABLE transactions (
				id SERIAL PRIMARY KEY,
				buy_order_id INTEGER NOT NULL REFERENCES orders(id),
				sell_order_id INTEGER NOT NULL REFERENCES orders(id),
				buy_user_id INTEGER NOT NULL,
				sell_user_id INTEGER NOT NULL,
				stock_id INTEGER NOT NULL,
				quantity INTEGER NOT NULL,
				price DECIMAL(10, 2) NOT NULL,
				timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			)
		`)
		if err != nil {
			return fmt.Errorf("error creating transactions table: %v", err)
		}

		// Create transaction indexes
		_, err = db.Exec("CREATE INDEX idx_transactions_buy_order_id ON transactions(buy_order_id)")
		if err != nil {
			return fmt.Errorf("error creating buy_order_id index: %v", err)
		}

		_, err = db.Exec("CREATE INDEX idx_transactions_sell_order_id ON transactions(sell_order_id)")
		if err != nil {
			return fmt.Errorf("error creating sell_order_id index: %v", err)
		}

		_, err = db.Exec("CREATE INDEX idx_transactions_buy_user_id ON transactions(buy_user_id)")
		if err != nil {
			return fmt.Errorf("error creating buy_user_id index: %v", err)
		}

		_, err = db.Exec("CREATE INDEX idx_transactions_sell_user_id ON transactions(sell_user_id)")
		if err != nil {
			return fmt.Errorf("error creating sell_user_id index: %v", err)
		}

		_, err = db.Exec("CREATE INDEX idx_transactions_stock_id ON transactions(stock_id)")
		if err != nil {
			return fmt.Errorf("error creating stock_id index: %v", err)
		}

		log.Println("Database schema created successfully")
	} else {
		log.Println("Database schema already exists")
	}

	return nil
}

func main() {
	var err error

	// Initialize database
	db, err = initDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Initialize database schema
	err = initSchema(db)
	if err != nil {
		log.Fatalf("Failed to initialize database schema: %v", err)
	}

	// Initialize order book manager
	orderBookMgr = initOrderBookManager()

	// Set trading service endpoint
	tradingEndpoint = os.Getenv("TRADING_SERVICE_URL")
	if tradingEndpoint == "" {
		tradingEndpoint = "http://trading-service:8000"
	}

	// Create router
	r := mux.NewRouter()

	// Register routes
	r.HandleFunc("/health", healthCheckHandler).Methods("GET")
	r.HandleFunc("/api/placeStockOrder", placeOrderHandler).Methods("POST")
	r.HandleFunc("/api/cancelStockTransaction", cancelOrderHandler).Methods("POST")

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting matching engine on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}
