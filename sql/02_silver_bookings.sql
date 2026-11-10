
-- 1. silver_bookings_prepared
-- This materialized view removes duplicates, standardizes text, converts data types, and fills missing nights and total amounts.

CREATE OR REFRESH MATERIALIZED VIEW silver_bookings_prepared
AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id
            ORDER BY loaded_at, source_file
        ) AS row_num
    FROM bronze_hotel_bookings
),

standardized AS (
    SELECT
        booking_id,
        NULLIF(TRIM(guest_id), '') AS guest_id,

        TRY_CAST(booking_date AS DATE) AS booking_date,
        TRY_CAST(check_in_date AS DATE) AS check_in_date,
        TRY_CAST(check_out_date AS DATE) AS check_out_date,

        TRY_CAST(nights AS INT) AS original_nights,

        INITCAP(TRIM(hotel_name)) AS hotel_name,

        COALESCE(
            INITCAP(NULLIF(TRIM(city), '')),
            'Unknown'
        ) AS city,

        INITCAP(TRIM(region)) AS region,
        INITCAP(TRIM(room_type)) AS room_type,

        COALESCE(
            INITCAP(NULLIF(TRIM(booking_channel), '')),
            'Unknown'
        ) AS booking_channel,

        TRY_CAST(guests AS INT) AS guests,
        TRY_CAST(room_rate AS DECIMAL(18,2)) AS room_rate,

        TRY_CAST(total_amount AS DECIMAL(18,2))
            AS original_total_amount,

        COALESCE(
            TRY_CAST(discount_amount AS DECIMAL(18,2)),
            0
        ) AS discount_amount,

        INITCAP(TRIM(meal_plan)) AS meal_plan,
        INITCAP(TRIM(payment_method)) AS payment_method,
        INITCAP(TRIM(payment_status)) AS payment_status,
        INITCAP(TRIM(booking_status)) AS booking_status,

        TRY_CAST(guest_rating AS INT) AS guest_rating,

        source_file,
        loaded_at

    FROM ranked
    WHERE row_num = 1
),

calculated AS (
    SELECT
        *,
        COALESCE(
            original_nights,
            DATEDIFF(check_out_date, check_in_date)
        ) AS calculated_nights
    FROM standardized
)

SELECT
    booking_id,
    guest_id,
    booking_date,
    check_in_date,
    check_out_date,
    calculated_nights AS nights,
    hotel_name,
    city,
    region,
    room_type,
    booking_channel,
    guests,
    room_rate,

    COALESCE(
        original_total_amount,
        calculated_nights * room_rate
    ) AS total_amount,

    discount_amount,
    meal_plan,
    payment_method,
    payment_status,
    booking_status,
    guest_rating,
    source_file,
    loaded_at

FROM calculated;

-- 2. silver_bookings_clean
-- This materialized view keeps valid bookings and applies Databricks data-quality expectations.


CREATE OR REFRESH MATERIALIZED VIEW silver_bookings_clean
(
    CONSTRAINT valid_stay_dates
        EXPECT (
            check_in_date IS NOT NULL
            AND check_out_date IS NOT NULL
            AND check_out_date > check_in_date
        )
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_room_rate
        EXPECT (
            room_rate IS NOT NULL
            AND room_rate >= 0
        )
        ON VIOLATION DROP ROW
)
AS
SELECT *
FROM silver_bookings_prepared;


-- 3. silver_bookings_rejected
-- This materialized view stores invalid bookings with their rejection reasons.


CREATE OR REFRESH MATERIALIZED VIEW silver_bookings_rejected
AS
SELECT
    *,
    CASE
        WHEN check_in_date IS NULL
          OR check_out_date IS NULL
          OR check_out_date <= check_in_date
        THEN 'Invalid stay dates'

        WHEN room_rate IS NULL
        THEN 'Missing or invalid room rate'

        WHEN room_rate < 0
        THEN 'Negative room rate'

        ELSE 'Other validation failure'
    END AS reject_reason

FROM silver_bookings_prepared

WHERE
    check_in_date IS NULL
    OR check_out_date IS NULL
    OR check_out_date <= check_in_date
    OR room_rate IS NULL
    OR room_rate < 0;



