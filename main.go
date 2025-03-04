package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	db               *sql.DB
	orderBookMgr     *OrderBookManager
	tradingEndpoint  string = "http://api-gateway:5000/transaction/processTransaction" // Updated to use API Gateway with port 5000
	serviceAuthToken string                                                            // Token for service-to-service authentication
)

// Database connection
func initDB() (*sql.DB, error) {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found")
	}

	// Get service auth token from environment variables
	serviceAuthToken = os.Getenv("SERVICE_AUTH_TOKEN")
	if serviceAuthToken == "" {
		log.Println("Warning: SERVICE_AUTH_TOKEN not found in environment, service-to-service authentication may fail")
	} else {
		log.Println("Service authentication token loaded successfully")
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

// Get the best sell price for a stock (lowest price)
func getBestSellPrice(stockID int64) (float64, bool) {
	orderBook := orderBookMgr.GetOrderBook(stockID)
	orderBook.Mutex.Lock()
	defer orderBook.Mutex.Unlock()

	// If there are no sell orders, return 0 and false
	if len(orderBook.SellOrders) == 0 {
		return 0, false
	}

	// Find the lowest priced sell order
	bestPrice := orderBook.SellOrders[0].Price
	for _, order := range orderBook.SellOrders {
		if order.Price < bestPrice {
			bestPrice = order.Price
		}
	}

	return bestPrice, true
}

// Notify trading service about a price update
func notifyPriceUpdate(stockID int64, price float64) {
	// Prepare request data
	priceData := map[string]interface{}{
		"stock_id":      stockID,
		"current_price": price,
	}

	// Wrap in an array as expected by the API
	priceDataArray := []map[string]interface{}{priceData}

	jsonData, err := json.Marshal(priceDataArray)
	if err != nil {
		log.Printf("Error marshaling price data: %v", err)
		return
	}

	// Use the API Gateway with the correct port (5000, not 4000)
	// and path (/setup/updateStockPrices)
	priceEndpoint := "http://api-gateway:5000/setup/updateStockPrices"
	log.Printf("Using price update endpoint: %s", priceEndpoint)

	// Create HTTP request
	req, err := http.NewRequest("POST", priceEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error creating HTTP request: %v", err)
		return
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-REQUEST-FROM", "matching-engine") // Identify as matching engine

	// Add authentication token if available
	if serviceAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+serviceAuthToken)
		log.Printf("Added service authentication token to price update request")
	} else {
		log.Println("Warning: No service authentication token found")
	}

	// Send request
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending price update to trading service: %v", err)
		return
	}
	defer resp.Body.Close()

	// Read response
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading price update response: %v", err)
		return
	}

	log.Printf("Price update response (status %d): %s", resp.StatusCode, string(respBody))

	if resp.StatusCode != http.StatusOK {
		log.Printf("Non-OK response from trading service for price update: %d", resp.StatusCode)
	}
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
		}
	}

	// Set default stock ID if not provided
	if order.StockID <= 0 {
		order.StockID = 1 // Default to first stock for testing
	}

	// Normalize order type to title case
	if strings.ToLower(order.OrderType) == "market" {
		order.OrderType = "Market"
	} else {
		order.OrderType = "Limit" // Default to Limit order
	}

	// Set the initial status and timestamp
	order.Status = "InProgress"
	order.Timestamp = time.Now()

	// Process the order
	result, err := processOrder(order)
	if err != nil {
		log.Printf("Error processing order: %v", err)
		http.Error(w, fmt.Sprintf("Error processing order: %v", err), http.StatusInternalServerError)
		return
	}

	// Update the order ID from the result
	if orderID, ok := result["order_id"].(int64); ok {
		order.ID = orderID
	}
	if status, ok := result["status"].(string); ok {
		order.Status = status
	}

	// Notify trading service about the order status
	notifyOrderStatus(order)

	// If this is a sell order, check if we should update the stock price
	if !order.IsBuy {
		// Only update price if this is a sell order
		bestPrice, exists := getBestSellPrice(order.StockID)
		if exists {
			log.Printf("Updating price for stock %d to best sell price: %f", order.StockID, bestPrice)
			notifyPriceUpdate(order.StockID, bestPrice)
		} else {
			log.Printf("No sell orders available for stock %d after placing order", order.StockID)
		}
	}

	// Return order details
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// Process an order (match or add to order book)
func processOrder(order Order) (map[string]interface{}, error) {
	// Get order book for stock
	orderBook := orderBookMgr.GetOrderBook(order.StockID)
	orderBook.Mutex.Lock()
	defer orderBook.Mutex.Unlock()

	// Insert order into database first, regardless of type
	var orderID int64
	err := db.QueryRow(
		"INSERT INTO orders (user_id, stock_id, is_buy, order_type, status, quantity, price, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id",
		order.UserID, order.StockID, order.IsBuy, order.OrderType, order.Status, order.Quantity, order.Price, order.Timestamp,
	).Scan(&orderID)
	if err != nil {
		return nil, err
	}
	order.ID = orderID

	// For market orders, try to match immediately at best available price
	if order.OrderType == "Market" {
		// Market orders match at the best available price
		var matches []map[string]interface{}
		remainingQty := order.Quantity

		if order.IsBuy {
			// Buy market order - match against sell orders (sorted by lowest price first)
			// For market buy orders, we take the lowest sell prices available
			sellOrders := orderBook.SellOrders
			if len(sellOrders) == 0 {
				// No sell orders available, update status to pending
				_, err = db.Exec("UPDATE orders SET status = 'Pending' WHERE id = $1", order.ID)
				if err != nil {
					return nil, err
				}
				order.Status = "Pending"

				// Add to order book for future matching
				orderBook.BuyOrders = append(orderBook.BuyOrders, order)

				// Return status
				return map[string]interface{}{
					"order_id": order.ID,
					"status":   order.Status,
					"matches":  matches,
				}, nil
			}

			// Sort sell orders by price (low to high) - already sorted in the order book
			// This ensures market buy orders get the best (lowest) price
			for i := 0; i < len(sellOrders) && remainingQty > 0; i++ {
				sellOrder := sellOrders[i]

				// Market buy orders match at the seller's asking price
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
		} else {
			// Sell market order - match against buy orders (sorted by highest price first)
			// For market sell orders, we take the highest buy prices available
			buyOrders := orderBook.BuyOrders
			if len(buyOrders) == 0 {
				// No buy orders available, update status to pending
				_, err = db.Exec("UPDATE orders SET status = 'Pending' WHERE id = $1", order.ID)
				if err != nil {
					return nil, err
				}
				order.Status = "Pending"

				// Add to order book for future matching
				orderBook.SellOrders = append(orderBook.SellOrders, order)

				// Return status
				return map[string]interface{}{
					"order_id": order.ID,
					"status":   order.Status,
					"matches":  matches,
				}, nil
			}

			// TODO: Sort buy orders by price (high to low) for optimal matching
			// For now, we'll just use the existing order
			for i := 0; i < len(buyOrders) && remainingQty > 0; i++ {
				buyOrder := buyOrders[i]

				// Market sell orders match at the buyer's bid price
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

		// Update order status based on matches
		if remainingQty == 0 {
			// Fully matched
			_, err = db.Exec("UPDATE orders SET status = 'Completed' WHERE id = $1", order.ID)
			if err != nil {
				return nil, err
			}
			order.Status = "Completed"
		} else if len(matches) > 0 {
			// Partially matched
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
			// No matches, update status to in progress
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
		return map[string]interface{}{
			"order_id": order.ID,
			"status":   order.Status,
			"matches":  matches,
		}, nil
	}

	// For limit orders, try to match or add to order book
	if order.OrderType == "Limit" {
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
	// Prepare request
	transactionData := map[string]interface{}{
		"buy_user_id":  buyUserID,
		"sell_user_id": sellUserID,
		"stock_id":     stockID,
		"quantity":     quantity,
		"price":        price,
		"timestamp":    time.Now().Format(time.RFC3339),
	}

	jsonData, err := json.Marshal(transactionData)
	if err != nil {
		log.Printf("Error marshaling transaction data: %v", err)
		return
	}

	// Get trading service URL from environment or use default
	endpoint := os.Getenv("TRADING_SERVICE_URL")
	if endpoint == "" {
		endpoint = tradingEndpoint
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error creating HTTP request: %v", err)
		return
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("user_id", "0")                      // System user ID for service-to-service authentication
	req.Header.Set("X-REQUEST-FROM", "matching-engine") // Identify as matching engine

	// Add authentication token if available
	if serviceAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+serviceAuthToken)
		log.Printf("Added service authentication token to request (first 10 chars): %s...", serviceAuthToken[:10])
	} else {
		log.Println("Warning: No service authentication token found")
	}

	// Send request
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending request to trading service: %v", err)
		return
	}
	defer resp.Body.Close()

	// Read response
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response from trading service: %v", err)
		return
	}

	log.Printf("Order notification response (status %d): %s", resp.StatusCode, string(respBody))

	if resp.StatusCode != http.StatusOK {
		log.Printf("Non-OK response from trading service: %d", resp.StatusCode)
	}
}

// Notify trading service about an order status
func notifyOrderStatus(order Order) {
	// Prepare request data
	orderData := map[string]interface{}{
		"user_id":           order.UserID,
		"stock_id":          order.StockID,
		"is_buy":            order.IsBuy,
		"order_type":        order.OrderType,
		"status":            order.Status,
		"quantity":          order.Quantity,
		"price":             order.Price,
		"timestamp":         order.Timestamp.Format(time.RFC3339),
		"order_id":          order.ID,
		"notification_type": "new_order",
	}

	// Add matches data if this is a completed market order with price=0
	if order.OrderType == "Market" && (order.Status == "Completed" || order.Status == "Partially_complete") {
		// Fetch the transaction to get the actual execution price
		var actualPrice float64
		var matchRows *sql.Rows
		var err error

		if order.IsBuy {
			matchRows, err = db.Query(
				"SELECT price FROM transactions WHERE buy_order_id = $1 ORDER BY timestamp DESC",
				order.ID,
			)
		} else {
			matchRows, err = db.Query(
				"SELECT price FROM transactions WHERE sell_order_id = $1 ORDER BY timestamp DESC",
				order.ID,
			)
		}

		if err != nil {
			log.Printf("Error querying transactions for order %d: %v", order.ID, err)
		} else {
			defer matchRows.Close()

			matches := []map[string]interface{}{}
			totalQuantity := 0

			for matchRows.Next() {
				err := matchRows.Scan(&actualPrice)
				if err != nil {
					log.Printf("Error scanning transaction price: %v", err)
					continue
				}

				if actualPrice > 0 {
					// Determine the quantity for this match (for simplicity using the full quantity here)
					matchQuantity := order.Quantity
					if totalQuantity+matchQuantity > order.Quantity {
						matchQuantity = order.Quantity - totalQuantity
					}

					// Create a match entry with the actual execution price
					match := map[string]interface{}{
						"matched_order_id": 0, // We don't track the matched order ID in this context
						"price":            actualPrice,
						"quantity":         matchQuantity,
					}
					matches = append(matches, match)
					totalQuantity += matchQuantity

					log.Printf("Adding match with price %f for order %d", actualPrice, order.ID)
				}
			}

			// Add matches array if we found valid matches
			if len(matches) > 0 {
				orderData["matches"] = matches
				// Update the price in the notification to the actual price
				// This helps the trading service create the wallet transaction with the correct amount
				orderData["price"] = actualPrice
				log.Printf("Updated order notification for market order %d with match price %f", order.ID, actualPrice)
			}
		}
	}

	jsonData, err := json.Marshal(orderData)
	if err != nil {
		log.Printf("Error marshaling order data: %v", err)
		return
	}

	// Get trading service URL from environment or use default
	endpoint := os.Getenv("TRADING_SERVICE_URL")
	if endpoint == "" {
		endpoint = tradingEndpoint
	}

	// Create HTTP request - use processOrderStatus endpoint
	statusEndpoint := strings.Replace(endpoint, "processTransaction", "processOrderStatus", 1)
	req, err := http.NewRequest("POST", statusEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error creating HTTP request: %v", err)
		return
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("user_id", strconv.FormatInt(order.UserID, 10)) // Pass user ID for authentication
	req.Header.Set("X-REQUEST-FROM", "matching-engine")            // Identify as matching engine

	// Add authentication token if available
	if serviceAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+serviceAuthToken)
	} else {
		log.Println("Warning: No service authentication token found")
	}

	// Send request
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending order status to trading service: %v", err)
		return
	}
	defer resp.Body.Close()

	// Read response
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response from trading service: %v", err)
		return
	}

	log.Printf("Order status notification response (status %d): %s", resp.StatusCode, string(respBody))

	if resp.StatusCode != http.StatusOK {
		log.Printf("Non-OK response from trading service: %d", resp.StatusCode)
	}
}

// Handler for cancelling a stock order
func cancelOrderHandler(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var data struct {
		TransactionID string `json:"transaction_id"`
	}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Convert string transaction_id to int64
	transactionID, err := strconv.ParseInt(data.TransactionID, 10, 64)
	if err != nil {
		http.Error(w, "Invalid transaction ID format: must be a valid number", http.StatusBadRequest)
		return
	}

	// Get order details
	var order Order
	err = db.QueryRow(
		"SELECT id, user_id, stock_id, is_buy, order_type, status, quantity, price, timestamp FROM orders WHERE id = $1",
		transactionID,
	).Scan(&order.ID, &order.UserID, &order.StockID, &order.IsBuy, &order.OrderType, &order.Status, &order.Quantity, &order.Price, &order.Timestamp)
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

	// Update the order status to Cancelled
	order.Status = "Cancelled"

	// Notify trading service about the cancellation
	go notifyOrderStatus(order)

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
	log.Println("Health check requested")

	// Check database connection
	err := db.Ping()
	if err != nil {
		log.Printf("Health check failed: %v", err)
		http.Error(w, "Database connection failed", http.StatusServiceUnavailable)
		return
	}

	// Check if orders table exists
	var exists bool
	err = db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders')").Scan(&exists)
	if err != nil || !exists {
		log.Printf("Health check failed: orders table not found or error: %v", err)
		http.Error(w, "Database schema is incomplete", http.StatusServiceUnavailable)
		return
	}

	// Return healthy response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","message":"Matching engine is running properly"}`))
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
		log.Println("Warning: TRADING_SERVICE_URL not found in environment, using default URL")
		tradingEndpoint = "http://api-gateway:5000/transaction/processTransaction"
	}
	log.Printf("Using trading service endpoint: %s", tradingEndpoint)

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
