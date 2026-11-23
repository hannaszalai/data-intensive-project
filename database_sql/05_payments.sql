
CREATE TABLE IF NOT EXISTS payments (
    payment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL,
    restaurant_region_id VARCHAR(20) NOT NULL,

    amount NUMERIC(12,2) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    payment_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    transaction_reference VARCHAR(100) UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_payments_orders
        FOREIGN KEY (order_id, restaurant_region_id)
        REFERENCES orders(order_id, restaurant_region_id),

    CONSTRAINT uq_payments_order
        UNIQUE (order_id, restaurant_region_id),

    CONSTRAINT chk_payment_amount
        CHECK (amount >= 0),

    CONSTRAINT chk_payment_status
        CHECK (payment_status IN (
            'PENDING',
            'COMPLETED',
            'FAILED',
            'REFUNDED'
        )),

    CONSTRAINT chk_payment_method
        CHECK (payment_method IN (
            'CARD',
            'CASH',
            'WALLET'
        ))
);
