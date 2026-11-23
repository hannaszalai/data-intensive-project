
-- =====================================================
-- FoodGo - Project 2
-- File: 06_seed_data.sql
-- Description: Generate mock data for PostgreSQL
--
-- Creates:
--   120 demo customers
--   300 regional orders
--   600 order items
--   300 mock payments
--
-- Regions:
--   FI-LPR: Lappeenranta
--   FI-HEL: Helsinki
--
-- Prerequisites:
--   users, customers, orders, order_items,
--   payments and regional partitions must exist.
--
-- Note:
--   Restaurant and menu item IDs are placeholders.
--   They must be synchronized with MongoDB.
-- =====================================================

BEGIN;

-- =====================================================
-- 1. GENERATE CUSTOMERS
-- =====================================================

INSERT INTO users (
    username,
    email,
    phone_number,
    password_hash,
    role
)
SELECT
    'demo_customer_' || n,
    'customer' || n || '@foodgo.test',
    '+358400' || LPAD(n::TEXT, 6, '0'),
    'DEMO_ONLY_NOT_A_PASSWORD_HASH',
    'CUSTOMER'
FROM generate_series(1, 120) AS n
ON CONFLICT (username) DO NOTHING;


INSERT INTO customers (
    customer_id,
    default_address
)
SELECT
    u.user_id,
    CASE
        WHEN split_part(u.username, '_', 3)::INT % 2 = 0
        THEN 'Helsinki, Finland'
        ELSE 'Lappeenranta, Finland'
    END
FROM users u
WHERE u.username ~ '^demo_customer_[0-9]+$'
  AND split_part(u.username, '_', 3)::INT
      BETWEEN 1 AND 120
ON CONFLICT (customer_id) DO NOTHING;


-- =====================================================
-- 2. GENERATE REGIONAL ORDERS AND ORDER ITEMS
-- =====================================================

DO $$
DECLARE
    i INTEGER;
    v_customer_id BIGINT;
    v_order_id BIGINT;
    v_region VARCHAR(20);
    v_restaurant_id VARCHAR(64);
    v_address TEXT;
BEGIN

    FOR i IN 1..300 LOOP

        -- Choose the restaurant region
        IF i % 2 = 0 THEN
            v_region := 'FI-HEL';
            v_restaurant_id := 'REST-HEL-001';
        ELSE
            v_region := 'FI-LPR';
            v_restaurant_id := 'REST-LPR-001';
        END IF;

        -- Unique marker for each seeded order
        v_address :=
            'FOODGO-SEED-' ||
            LPAD(i::TEXT, 4, '0');

        -- Skip previously generated orders
        IF EXISTS (
            SELECT 1
            FROM orders
            WHERE delivery_address = v_address
              AND restaurant_region_id = v_region
        ) THEN
            CONTINUE;
        END IF;

        -- Find an existing demo customer
        SELECT c.customer_id
        INTO STRICT v_customer_id
        FROM customers c
        JOIN users u
          ON u.user_id = c.customer_id
        WHERE u.username =
            'demo_customer_' ||
            (((i - 1) % 120) + 1);

        -- Create the order
        INSERT INTO orders (
            customer_id,
            restaurant_id,
            restaurant_region_id,
            status,
            total_amount,
            delivery_address
        )
        VALUES (
            v_customer_id,
            v_restaurant_id,
            v_region,
            CASE
                WHEN i % 5 = 0 THEN 'DELIVERED'
                WHEN i % 5 = 1 THEN 'PENDING'
                WHEN i % 5 = 2 THEN 'ACCEPTED'
                WHEN i % 5 = 3 THEN 'PREPARING'
                ELSE 'ON_THE_WAY'
            END,
            25.50,
            v_address
        )
        RETURNING order_id INTO v_order_id;

        -- Create two items for the order
        INSERT INTO order_items (
            order_id,
            restaurant_region_id,
            line_no,
            menu_item_id,
            amount,
            unit_price
        )
        VALUES
        (
            v_order_id,
            v_region,
            1,
            'ITEM-001',
            2,
            9.50
        ),
        (
            v_order_id,
            v_region,
            2,
            'ITEM-002',
            1,
            6.50
        );

    END LOOP;

END $$;


-- =====================================================
-- 3. GENERATE PAYMENTS
-- =====================================================

INSERT INTO payments (
    order_id,
    restaurant_region_id,
    amount,
    payment_method,
    payment_status,
    transaction_reference
)
SELECT
    o.order_id,
    o.restaurant_region_id,
    o.total_amount,

    CASE
        WHEN o.order_id % 2 = 0
        THEN 'CARD'
        ELSE 'CASH'
    END,

    CASE
        WHEN o.status = 'DELIVERED'
        THEN 'COMPLETED'
        ELSE 'PENDING'
    END,

    'MOCK-' ||
    o.restaurant_region_id ||
    '-' || o.order_id

FROM orders o
WHERE o.delivery_address LIKE 'FOODGO-SEED-%'

ON CONFLICT (
    order_id,
    restaurant_region_id
)
DO NOTHING;

COMMIT;


-- =====================================================
-- 4. VERIFY GENERATED DATA
-- =====================================================

SELECT 'Customers' AS entity, COUNT(*) AS total
FROM customers

UNION ALL

SELECT 'Orders', COUNT(*)
FROM orders

UNION ALL

SELECT 'OrderItems', COUNT(*)
FROM order_items

UNION ALL

SELECT 'Payments', COUNT(*)
FROM payments;


-- =====================================================
-- 5. VERIFY REGIONAL PARTITIONING
-- =====================================================

SELECT
    restaurant_region_id,
    tableoid::regclass AS physical_partition,
    COUNT(*) AS total_orders
FROM orders
WHERE delivery_address LIKE 'FOODGO-SEED-%'
GROUP BY restaurant_region_id, tableoid
ORDER BY restaurant_region_id;


-- =====================================================
-- 6. VERIFY ORDER TOTALS
-- =====================================================

SELECT
    o.order_id,
    o.restaurant_region_id,
    o.total_amount,
    SUM(oi.amount * oi.unit_price) AS calculated_total
FROM orders o

JOIN order_items oi
    ON oi.order_id = o.order_id
   AND oi.restaurant_region_id =
       o.restaurant_region_id

WHERE o.delivery_address LIKE 'FOODGO-SEED-%'

GROUP BY
    o.order_id,
    o.restaurant_region_id,
    o.total_amount

HAVING o.total_amount <>
       SUM(oi.amount * oi.unit_price);
