CREATE TABLE IF NOT EXISTS consumed_events (
    event_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(150),
    product_id VARCHAR(150),
    quantity INTEGER NOT NULL,
    correlation_id VARCHAR(150),
    consumed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
