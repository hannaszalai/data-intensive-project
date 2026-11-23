
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone_number VARCHAR(30),
    password_hash TEXT NOT NULL,
    role VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_user_role
        CHECK (role IN (
            'CUSTOMER',
            'DRIVER',
            'RESTAURANT',
            'ADMIN'
        ))
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT PRIMARY KEY,
    default_address TEXT,

    CONSTRAINT fk_customer_user
        FOREIGN KEY (customer_id)
        REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id BIGINT PRIMARY KEY,
    transport_type VARCHAR(30),
    is_available BOOLEAN DEFAULT FALSE,

    CONSTRAINT fk_driver_user
        FOREIGN KEY (driver_id)
        REFERENCES users(user_id)
);
