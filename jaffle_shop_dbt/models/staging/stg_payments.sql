WITH source AS (
    SELECT * FROM {{ source('jaffle_shop', 'payments') }}
),

payments AS (
    SELECT
        id              AS payment_id,
        order_id,
        payment_method,
        amount / 100.0  AS amount  -- Conversion cents → dollars
    FROM source
)

SELECT * FROM payments
