package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"matching-engine/engine"
)

// Global variables
var (
	orderBooks = make(map[string]*engine.OrderBook)
	booksMutex sync.RWMutex
	tradeChan  = make(chan engine.Trade, 1000) // Buffer for 1000 trades
)

// OrderRequest represents an incoming order request
type OrderRequest struct {
	Symbol    string  `json:"symbol"`
	OrderType string  `json:"order_type"` // "BUY" or "SELL"
	Price     float64 `json:"price"`
	Quantity  int64   `json:"quantity"`
	UserID    string  `json:"user_id"`
}

func init() {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
}

func main() {
	// Start trade notification processor
	go processTradeNotifications()

	// Create router
	r := mux.NewRouter()

	// Register routes
	r.HandleFunc("/health", healthCheck).Methods("GET")
	r.HandleFunc("/api/orders", placeOrder).Methods("POST")

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Info().Msgf("Starting matching engine server on port %s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatal().Err(err).Msg("Server failed to start")
	}
}

// healthCheck handles health check requests
func healthCheck(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

// placeOrder handles new order placement requests
func placeOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate request
	if err := validateOrderRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get or create order book for the symbol
	book := getOrCreateOrderBook(req.Symbol)

	// Create order
	order := engine.Order{
		ID:        fmt.Sprintf("order_%d", time.Now().UnixNano()),
		Type:      engine.OrderType(req.OrderType),
		Symbol:    req.Symbol,
		Price:     decimal.NewFromFloat(req.Price),
		Quantity:  req.Quantity,
		UserID:    req.UserID,
		Timestamp: time.Now(),
	}

	// Add order to book
	if err := book.AddOrder(order); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Return success response
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": order.ID,
		"status":   "accepted",
	})
}

// validateOrderRequest validates the incoming order request
func validateOrderRequest(req OrderRequest) error {
	if req.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if req.OrderType != "BUY" && req.OrderType != "SELL" {
		return fmt.Errorf("invalid order type: must be BUY or SELL")
	}
	if req.Price <= 0 {
		return fmt.Errorf("price must be positive")
	}
	if req.Quantity <= 0 {
		return fmt.Errorf("quantity must be positive")
	}
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	return nil
}

// getOrCreateOrderBook gets or creates an order book for a symbol
func getOrCreateOrderBook(symbol string) *engine.OrderBook {
	booksMutex.Lock()
	defer booksMutex.Unlock()

	book, exists := orderBooks[symbol]
	if !exists {
		book = engine.NewOrderBook(symbol, tradeChan)
		orderBooks[symbol] = book
	}
	return book
}

// processTradeNotifications processes matched trades
func processTradeNotifications() {
	for trade := range tradeChan {
		// Log the trade
		log.Info().
			Str("buy_order_id", trade.BuyOrderID).
			Str("sell_order_id", trade.SellOrderID).
			Str("symbol", trade.Symbol).
			Str("price", trade.Price.String()).
			Int64("quantity", trade.Quantity).
			Msg("Trade executed")

		// TODO: Notify other services about the trade
		// This could involve:
		// 1. Updating user wallets
		// 2. Updating stock positions
		// 3. Sending notifications
		// 4. Updating trade history
	}
} 