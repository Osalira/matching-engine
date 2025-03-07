package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQClient handles the connection to RabbitMQ
type RabbitMQClient struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName map[string]string
	isConnected  bool
}

// NewRabbitMQClient creates a new RabbitMQ client
func NewRabbitMQClient() *RabbitMQClient {
	return &RabbitMQClient{
		exchangeName: map[string]string{
			"order_events":  "order_events",
			"user_events":   "user_events",
			"system_events": "system_events",
		},
		isConnected: false,
	}
}

// Connect establishes a connection to RabbitMQ
func (r *RabbitMQClient) Connect() error {
	host := os.Getenv("RABBITMQ_HOST")
	if host == "" {
		host = "rabbitmq"
	}

	port := os.Getenv("RABBITMQ_PORT")
	if port == "" {
		port = "5672"
	}

	user := os.Getenv("RABBITMQ_USER")
	if user == "" {
		user = "guest"
	}

	password := os.Getenv("RABBITMQ_PASSWORD")
	if password == "" {
		password = "guest"
	}

	vhost := os.Getenv("RABBITMQ_VHOST")
	if vhost == "" {
		vhost = "/"
	}

	// Create connection string
	connStr := fmt.Sprintf("amqp://%s:%s@%s:%s%s", user, password, host, port, vhost)

	// Connect with retry logic
	var err error
	for i := 0; i < 5; i++ {
		log.Printf("Attempting to connect to RabbitMQ (attempt %d)...", i+1)
		r.conn, err = amqp.Dial(connStr)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		time.Sleep(time.Duration(2*i) * time.Second)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ after multiple attempts: %w", err)
	}

	// Create channel
	r.channel, err = r.conn.Channel()
	if err != nil {
		r.conn.Close()
		return fmt.Errorf("failed to open a channel: %w", err)
	}

	// Declare exchanges
	for _, exchange := range r.exchangeName {
		err = r.channel.ExchangeDeclare(
			exchange, // name
			"topic",  // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			r.channel.Close()
			r.conn.Close()
			return fmt.Errorf("failed to declare exchange %s: %w", exchange, err)
		}
	}

	r.isConnected = true
	log.Println("Successfully connected to RabbitMQ")
	return nil
}

// PublishEvent publishes an event to the specified exchange with the given routing key
func (r *RabbitMQClient) PublishEvent(exchange, routingKey string, event map[string]interface{}) error {
	if !r.isConnected {
		if err := r.Connect(); err != nil {
			return fmt.Errorf("not connected to RabbitMQ: %w", err)
		}
	}

	// Add timestamp if not present
	if _, ok := event["timestamp"]; !ok {
		event["timestamp"] = time.Now().Format(time.RFC3339)
	}

	// Convert event to JSON
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get exchange name from map or use provided name
	exchangeName, ok := r.exchangeName[exchange]
	if !ok {
		exchangeName = exchange
	}

	// Publish message
	err = r.channel.PublishWithContext(
		ctx,
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		// Try to reconnect and publish again
		log.Printf("Failed to publish event, attempting to reconnect: %v", err)
		if reconnectErr := r.Connect(); reconnectErr != nil {
			return fmt.Errorf("failed to reconnect to RabbitMQ: %w", reconnectErr)
		}

		// Try publishing again
		err = r.channel.PublishWithContext(
			ctx,
			exchangeName, // exchange
			routingKey,   // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Body:         body,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to publish event after reconnect: %w", err)
		}
	}

	log.Printf("Published event to %s.%s: %s", exchangeName, routingKey, string(body))
	return nil
}

// Close closes the RabbitMQ connection
func (r *RabbitMQClient) Close() {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
	r.isConnected = false
	log.Println("RabbitMQ connection closed")
}

// Global RabbitMQ client instance
var rabbitMQClient *RabbitMQClient

// InitRabbitMQ initializes the RabbitMQ client
func InitRabbitMQ() error {
	rabbitMQClient = NewRabbitMQClient()
	return rabbitMQClient.Connect()
}

// PublishEvent is a helper function to publish an event using the global client
func PublishEvent(exchange, routingKey string, event map[string]interface{}) error {
	if rabbitMQClient == nil {
		if err := InitRabbitMQ(); err != nil {
			return err
		}
	}
	return rabbitMQClient.PublishEvent(exchange, routingKey, event)
}
