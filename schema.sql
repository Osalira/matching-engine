-- Drop existing tables if they exist
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS orders CASCADE;

-- Create orders table
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    stock_id INTEGER NOT NULL,
    is_buy BOOLEAN NOT NULL,
    order_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    quantity BIGINT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    parent_order_id INTEGER REFERENCES orders(id)
);

-- Create index on stock_id for faster lookup
CREATE INDEX idx_orders_stock_id ON orders(stock_id);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);

-- Create transactions table
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    buy_order_id INTEGER NOT NULL REFERENCES orders(id),
    sell_order_id INTEGER NOT NULL REFERENCES orders(id),
    buy_user_id INTEGER NOT NULL,
    sell_user_id INTEGER NOT NULL,
    stock_id INTEGER NOT NULL,
    quantity BIGINT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes on transaction table
CREATE INDEX idx_transactions_buy_order_id ON transactions(buy_order_id);
CREATE INDEX idx_transactions_sell_order_id ON transactions(sell_order_id);
CREATE INDEX idx_transactions_buy_user_id ON transactions(buy_user_id);
CREATE INDEX idx_transactions_sell_user_id ON transactions(sell_user_id);
CREATE INDEX idx_transactions_stock_id ON transactions(stock_id); 