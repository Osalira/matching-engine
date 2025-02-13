package engine

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// OrderType represents the type of order (Buy or Sell)
type OrderType string

const (
	Buy  OrderType = "BUY"
	Sell OrderType = "SELL"
)

// Order represents a trading order in the system
type Order struct {
	ID        string          // Unique order identifier
	Type      OrderType       // Buy or Sell
	Symbol    string          // Stock symbol
	Price     decimal.Decimal // Order price
	Quantity  int64          // Order quantity
	UserID    string          // User who placed the order
	Timestamp time.Time       // When the order was placed
}

// Trade represents a matched trade between a buy and sell order
type Trade struct {
	BuyOrderID     string
	SellOrderID    string
	Symbol         string
	Price          decimal.Decimal
	Quantity       int64
	ExecutedAt     time.Time
	BuyerUserID    string
	SellerUserID   string
}

// OrderBook maintains the state of all orders for a particular stock
type OrderBook struct {
	Symbol      string
	BuyOrders   []Order // Sorted by price (desc) and time (asc)
	SellOrders  []Order // Sorted by price (asc) and time (asc)
	mutex       sync.RWMutex
	TradeNotify chan<- Trade // Channel to notify about executed trades
}

// NewOrderBook creates a new order book for a stock symbol
func NewOrderBook(symbol string, tradeNotify chan<- Trade) *OrderBook {
	return &OrderBook{
		Symbol:      symbol,
		BuyOrders:   make([]Order, 0),
		SellOrders:  make([]Order, 0),
		TradeNotify: tradeNotify,
	}
}

// AddOrder adds a new order to the order book and attempts to match it
func (ob *OrderBook) AddOrder(order Order) error {
	ob.mutex.Lock()
	defer ob.mutex.Unlock()

	// Validate order
	if order.Symbol != ob.Symbol {
		return fmt.Errorf("invalid symbol: expected %s, got %s", ob.Symbol, order.Symbol)
	}

	// Try to match the order immediately
	if order.Type == Buy {
		ob.matchBuyOrder(order)
	} else {
		ob.matchSellOrder(order)
	}

	return nil
}

// matchBuyOrder attempts to match a buy order with existing sell orders
func (ob *OrderBook) matchBuyOrder(buyOrder Order) {
	// While we have sell orders and the buy order isn't fully filled
	for len(ob.SellOrders) > 0 && buyOrder.Quantity > 0 {
		sellOrder := ob.SellOrders[0]

		// If the lowest sell price is higher than buy price, no match is possible
		if sellOrder.Price.GreaterThan(buyOrder.Price) {
			break
		}

		// Determine the matched quantity
		matchedQty := min(buyOrder.Quantity, sellOrder.Quantity)

		// Create and notify about the trade
		trade := Trade{
			BuyOrderID:     buyOrder.ID,
			SellOrderID:    sellOrder.ID,
			Symbol:         ob.Symbol,
			Price:          sellOrder.Price, // Use sell order price for execution
			Quantity:       matchedQty,
			ExecutedAt:     time.Now(),
			BuyerUserID:    buyOrder.UserID,
			SellerUserID:   sellOrder.UserID,
		}

		// Notify about the trade
		if ob.TradeNotify != nil {
			ob.TradeNotify <- trade
		}

		// Update quantities
		buyOrder.Quantity -= matchedQty
		ob.SellOrders[0].Quantity -= matchedQty

		// Remove the sell order if fully filled
		if ob.SellOrders[0].Quantity == 0 {
			ob.SellOrders = ob.SellOrders[1:]
		}
	}

	// If buy order still has quantity, add it to the book
	if buyOrder.Quantity > 0 {
		ob.insertBuyOrder(buyOrder)
	}
}

// matchSellOrder attempts to match a sell order with existing buy orders
func (ob *OrderBook) matchSellOrder(sellOrder Order) {
	// While we have buy orders and the sell order isn't fully filled
	for len(ob.BuyOrders) > 0 && sellOrder.Quantity > 0 {
		buyOrder := ob.BuyOrders[0]

		// If the highest buy price is lower than sell price, no match is possible
		if buyOrder.Price.LessThan(sellOrder.Price) {
			break
		}

		// Determine the matched quantity
		matchedQty := min(sellOrder.Quantity, buyOrder.Quantity)

		// Create and notify about the trade
		trade := Trade{
			BuyOrderID:     buyOrder.ID,
			SellOrderID:    sellOrder.ID,
			Symbol:         ob.Symbol,
			Price:          buyOrder.Price, // Use buy order price for execution
			Quantity:       matchedQty,
			ExecutedAt:     time.Now(),
			BuyerUserID:    buyOrder.UserID,
			SellerUserID:   sellOrder.UserID,
		}

		// Notify about the trade
		if ob.TradeNotify != nil {
			ob.TradeNotify <- trade
		}

		// Update quantities
		sellOrder.Quantity -= matchedQty
		ob.BuyOrders[0].Quantity -= matchedQty

		// Remove the buy order if fully filled
		if ob.BuyOrders[0].Quantity == 0 {
			ob.BuyOrders = ob.BuyOrders[1:]
		}
	}

	// If sell order still has quantity, add it to the book
	if sellOrder.Quantity > 0 {
		ob.insertSellOrder(sellOrder)
	}
}

// insertBuyOrder inserts a buy order maintaining price-time priority
func (ob *OrderBook) insertBuyOrder(order Order) {
	// Find insertion point maintaining price-time priority
	i := sort.Search(len(ob.BuyOrders), func(i int) bool {
		// If prices are equal, newer orders go after existing ones (FIFO)
		if ob.BuyOrders[i].Price.Equal(order.Price) {
			return false
		}
		// Otherwise, sort by price in descending order
		return ob.BuyOrders[i].Price.LessThan(order.Price)
	})

	// Insert the order
	ob.BuyOrders = append(ob.BuyOrders, Order{})
	copy(ob.BuyOrders[i+1:], ob.BuyOrders[i:])
	ob.BuyOrders[i] = order
}

// insertSellOrder inserts a sell order maintaining price-time priority
func (ob *OrderBook) insertSellOrder(order Order) {
	// Find insertion point maintaining price-time priority
	i := sort.Search(len(ob.SellOrders), func(i int) bool {
		// If prices are equal, newer orders go after existing ones (FIFO)
		if ob.SellOrders[i].Price.Equal(order.Price) {
			return false
		}
		// Otherwise, sort by price in ascending order
		return ob.SellOrders[i].Price.GreaterThan(order.Price)
	})

	// Insert the order
	ob.SellOrders = append(ob.SellOrders, Order{})
	copy(ob.SellOrders[i+1:], ob.SellOrders[i:])
	ob.SellOrders[i] = order
}

// Helper function to find minimum of two int64s
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
} 