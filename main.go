package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"matching-engine/engine"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

// Global variables
var (
	orderBooks = make(map[string]*engine.OrderBook)
	booksMutex sync.RWMutex
	tradeChan  = make(chan engine.Trade, 1000) // Buffer for 1000 trades
	clients    = make(map[*websocket.Conn]bool)
	clientsMux sync.RWMutex
	upgrader   = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true // In production, implement proper origin checking
		},
	}
)

// OrderRequest represents an incoming order request
type OrderRequest struct {
	Symbol    string  `json:"symbol"`
	OrderType string  `json:"order_type"` // "BUY" or "SELL"
	Price     float64 `json:"price"`
	Quantity  int64   `json:"quantity"`
	UserID    string  `json:"user_id"`
}

// TradeNotification represents a trade notification sent to clients
type TradeNotification struct {
	Type      string       `json:"type"`
	Trade     engine.Trade `json:"trade"`
	Timestamp time.Time    `json:"timestamp"`
}

// TradeProcessor handles trade processing with retries and rollback
type TradeProcessor struct {
	maxRetries        int
	tradingServiceURL string
	walletServiceURL  string
}

func NewTradeProcessor() *TradeProcessor {
	return &TradeProcessor{
		maxRetries:        3,
		tradingServiceURL: os.Getenv("TRADING_SERVICE_URL"),
		walletServiceURL:  os.Getenv("WALLET_SERVICE_URL"),
	}
}

// TradeUpdate represents an order status update after a trade
type TradeUpdate struct {
	OrderID      string `json:"order_id"`
	Status       string `json:"status"`
	ExecutedQty  int64  `json:"executed_quantity"`
	RemainingQty int64  `json:"remaining_quantity"`
}

func init() {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
}

func main() {
	// Initialize trade processor
	tradeProcessor := NewTradeProcessor()

	// Start trade notification processor
	go tradeProcessor.processTradeNotifications()

	// Create router
	r := mux.NewRouter()

	// Register routes
	r.HandleFunc("/health", healthCheck).Methods("GET")
	r.HandleFunc("/api/orders", placeOrder).Methods("POST")
	r.HandleFunc("/ws", handleWebSocket)

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

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade connection to WebSocket")
		return
	}
	defer conn.Close()

	// Register client
	clientsMux.Lock()
	clients[conn] = true
	clientsMux.Unlock()

	// Remove client on disconnect
	defer func() {
		clientsMux.Lock()
		delete(clients, conn)
		clientsMux.Unlock()
	}()

	// Keep connection alive
	for {
		// Read messages (if needed)
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func broadcastTrade(trade engine.Trade) {
	notification := TradeNotification{
		Type:      "trade_executed",
		Trade:     trade,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(notification)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal trade notification")
		return
	}

	clientsMux.RLock()
	defer clientsMux.RUnlock()

	for client := range clients {
		err := client.WriteMessage(websocket.TextMessage, payload)
		if err != nil {
			log.Error().Err(err).Msg("Failed to send trade notification to client")
			client.Close()
			delete(clients, client)
		}
	}
}

func (tp *TradeProcessor) processTradeNotifications() {
	for trade := range tradeChan {
		err := tp.processSingleTrade(trade)
		if err != nil {
			log.Error().Err(err).
				Str("buyOrderID", trade.BuyOrderID).
				Str("sellOrderID", trade.SellOrderID).
				Msg("Failed to process trade after all retries")

			// Attempt rollback
			if rollbackErr := tp.rollbackTrade(trade); rollbackErr != nil {
				log.Error().Err(rollbackErr).Msg("Failed to rollback trade")
			}
		}
	}
}

func (tp *TradeProcessor) processSingleTrade(trade engine.Trade) error {
	var lastErr error

	for attempt := 0; attempt < tp.maxRetries; attempt++ {
		// Update order statuses
		if err := tp.updateOrderStatuses(trade); err != nil {
			lastErr = fmt.Errorf("failed to update order statuses: %w", err)
			continue
		}

		// Create trade record
		if err := tp.createTradeRecord(trade); err != nil {
			lastErr = fmt.Errorf("failed to create trade record: %w", err)
			continue
		}

		// Update wallets
		if err := tp.updateWallets(trade); err != nil {
			lastErr = fmt.Errorf("failed to update wallets: %w", err)
			continue
		}

		// Broadcast trade notification
		broadcastTrade(trade)
		return nil
	}

	return lastErr
}

func (tp *TradeProcessor) updateOrderStatuses(trade engine.Trade) error {
	// Create updates for both orders
	buyUpdate := TradeUpdate{
		OrderID:      trade.BuyOrderID,
		ExecutedQty:  trade.Quantity,
		RemainingQty: 0, // Will be set by the trading service
		Status:       "PARTIALLY_COMPLETE",
	}

	sellUpdate := TradeUpdate{
		OrderID:      trade.SellOrderID,
		ExecutedQty:  trade.Quantity,
		RemainingQty: 0, // Will be set by the trading service
		Status:       "PARTIALLY_COMPLETE",
	}

	// Send updates to trading service
	buyUpdateReq, err := json.Marshal(buyUpdate)
	if err != nil {
		return fmt.Errorf("failed to marshal buy order update: %w", err)
	}

	sellUpdateReq, err := json.Marshal(sellUpdate)
	if err != nil {
		return fmt.Errorf("failed to marshal sell order update: %w", err)
	}

	// Send updates to trading service
	resp, err := http.Post(
		fmt.Sprintf("%s/api/trading/orders/update", tp.tradingServiceURL),
		"application/json",
		bytes.NewBuffer(buyUpdateReq),
	)
	if err != nil {
		return fmt.Errorf("failed to send buy order update: %w", err)
	}
	defer resp.Body.Close()

	resp, err = http.Post(
		fmt.Sprintf("%s/api/trading/orders/update", tp.tradingServiceURL),
		"application/json",
		bytes.NewBuffer(sellUpdateReq),
	)
	if err != nil {
		return fmt.Errorf("failed to send sell order update: %w", err)
	}
	defer resp.Body.Close()

	return nil
}

func (tp *TradeProcessor) createTradeRecord(trade engine.Trade) error {
	client := &http.Client{Timeout: 10 * time.Second}

	tradeRecord, err := json.Marshal(trade)
	if err != nil {
		return fmt.Errorf("failed to marshal trade record: %w", err)
	}

	resp, err := client.Post(
		fmt.Sprintf("%s/api/trades", tp.tradingServiceURL),
		"application/json",
		bytes.NewBuffer(tradeRecord),
	)
	if err != nil {
		return fmt.Errorf("failed to create trade record: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create trade record, status: %d", resp.StatusCode)
	}

	return nil
}

func (tp *TradeProcessor) updateWallets(trade engine.Trade) error {
	// Calculate trade amount
	tradeAmount := trade.Price.Mul(decimal.NewFromInt(trade.Quantity))

	// Update buyer's wallet
	buyerUpdate := map[string]interface{}{
		"user_id":  trade.BuyerUserID,
		"symbol":   trade.Symbol,
		"quantity": trade.Quantity,
		"amount":   tradeAmount.Neg(), // Negative amount for purchase
	}

	buyerUpdateReq, err := json.Marshal(buyerUpdate)
	if err != nil {
		return fmt.Errorf("failed to marshal buyer wallet update: %w", err)
	}

	// Update seller's wallet
	sellerUpdate := map[string]interface{}{
		"user_id":  trade.SellerUserID,
		"symbol":   trade.Symbol,
		"quantity": -trade.Quantity, // Negative quantity for sale
		"amount":   tradeAmount,     // Positive amount for sale
	}

	sellerUpdateReq, err := json.Marshal(sellerUpdate)
	if err != nil {
		return fmt.Errorf("failed to marshal seller wallet update: %w", err)
	}

	// Send updates to wallet service
	resp, err := http.Post(
		fmt.Sprintf("%s/api/wallet/update", tp.walletServiceURL),
		"application/json",
		bytes.NewBuffer(buyerUpdateReq),
	)
	if err != nil {
		return fmt.Errorf("failed to update buyer wallet: %w", err)
	}
	defer resp.Body.Close()

	resp, err = http.Post(
		fmt.Sprintf("%s/api/wallet/update", tp.walletServiceURL),
		"application/json",
		bytes.NewBuffer(sellerUpdateReq),
	)
	if err != nil {
		return fmt.Errorf("failed to update seller wallet: %w", err)
	}
	defer resp.Body.Close()

	return nil
}

func (tp *TradeProcessor) rollbackTrade(trade engine.Trade) error {
	// Rollback order status updates
	buyUpdate := TradeUpdate{
		OrderID: trade.BuyOrderID,
		Status:  "PENDING",
	}

	sellUpdate := TradeUpdate{
		OrderID: trade.SellOrderID,
		Status:  "PENDING",
	}

	client := &http.Client{Timeout: 10 * time.Second}

	// Rollback buy order
	buyUpdateReq, err := json.Marshal(buyUpdate)
	if err != nil {
		return fmt.Errorf("failed to marshal buy order rollback: %w", err)
	}

	resp, err := client.Post(
		fmt.Sprintf("%s/api/orders/%s/rollback", tp.tradingServiceURL, trade.BuyOrderID),
		"application/json",
		bytes.NewBuffer(buyUpdateReq),
	)
	if err != nil {
		return fmt.Errorf("failed to rollback buy order: %w", err)
	}
	defer resp.Body.Close()

	// Rollback sell order
	sellUpdateReq, err := json.Marshal(sellUpdate)
	if err != nil {
		return fmt.Errorf("failed to marshal sell order rollback: %w", err)
	}

	resp, err = client.Post(
		fmt.Sprintf("%s/api/orders/%s/rollback", tp.tradingServiceURL, trade.SellOrderID),
		"application/json",
		bytes.NewBuffer(sellUpdateReq),
	)
	if err != nil {
		return fmt.Errorf("failed to rollback sell order: %w", err)
	}
	defer resp.Body.Close()

	return nil
}
