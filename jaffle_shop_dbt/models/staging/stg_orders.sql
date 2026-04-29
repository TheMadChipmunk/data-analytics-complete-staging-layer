-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('jaffle_shop', 'orders') }}
),

orders AS (
    SELECT
        id          AS order_id,
        user_id     AS customer_id,  -- user_id → customer_id pour cohérence FK
        order_date,
        status
    FROM source
)

SELECT * FROM orders
