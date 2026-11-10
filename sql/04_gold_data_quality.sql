
CREATE OR REFRESH MATERIALIZED VIEW gold_data_quality
AS
WITH bronze_quality AS (
    SELECT
        COUNT(*) AS bronze_rows,

        COUNT(*) - COUNT(DISTINCT booking_id)
            AS duplicate_rows_by_id,

        COUNT_IF(
            NULLIF(TRIM(city), '') IS NULL
        ) AS missing_city_rows,

        COUNT_IF(
            NULLIF(TRIM(booking_channel), '') IS NULL
        ) AS missing_channel_rows,

        COUNT_IF(
            NULLIF(TRIM(nights), '') IS NULL
        ) AS missing_nights_rows,

        COUNT_IF(
            NULLIF(TRIM(total_amount), '') IS NULL
        ) AS missing_amount_rows,

        COUNT_IF(
            NULLIF(TRIM(guest_id), '') IS NULL
        ) AS missing_guest_id_rows,

        COUNT_IF(
            TRY_CAST(check_in_date AS DATE) IS NULL
            OR TRY_CAST(check_out_date AS DATE) IS NULL
            OR TRY_CAST(check_out_date AS DATE)
               <= TRY_CAST(check_in_date AS DATE)
        ) AS invalid_date_rows,

        COUNT_IF(
            TRY_CAST(room_rate AS DECIMAL(18,2)) < 0
        ) AS negative_rate_rows

    FROM bronze_hotel_bookings
),

prepared_quality AS (
    SELECT
        COUNT(*) AS prepared_rows
    FROM silver_bookings_prepared
),

clean_quality AS (
    SELECT
        COUNT(*) AS clean_rows,

        COUNT_IF(booking_status = 'Completed')
            AS completed_bookings,

        COUNT_IF(booking_status = 'Cancelled')
            AS cancelled_bookings,

        COUNT(DISTINCT booking_id)
            AS unique_clean_bookings

    FROM silver_bookings_clean
),

rejected_quality AS (
    SELECT
        COUNT(*) AS rejected_rows
    FROM silver_bookings_rejected
),

gold_monthly_quality AS (
    SELECT
        COALESCE(SUM(bookings), 0)
            AS gold_monthly_bookings_count,

        COALESCE(SUM(cancelled_bookings), 0)
            AS gold_cancelled_bookings_count

    FROM gold_monthly_bookings
),

gold_hotel_quality AS (
    SELECT
        COALESCE(SUM(bookings), 0)
            AS gold_hotel_bookings_count
    FROM gold_hotel_summary
),

gold_room_quality AS (
    SELECT
        COALESCE(SUM(bookings), 0)
            AS gold_room_bookings_count
    FROM gold_room_type_summary
),

gold_channel_quality AS (
    SELECT
        COALESCE(SUM(bookings), 0)
            AS gold_channel_bookings_count
    FROM gold_channel_summary
)

SELECT
    b.bronze_rows,
    p.prepared_rows,
    c.clean_rows,
    r.rejected_rows,

    b.bronze_rows - p.prepared_rows
        AS duplicate_rows,

    b.duplicate_rows_by_id,

    b.missing_city_rows,
    b.missing_channel_rows,
    b.missing_nights_rows,
    b.missing_amount_rows,
    b.missing_guest_id_rows,
    b.invalid_date_rows,
    b.negative_rate_rows,

    c.completed_bookings,
    c.cancelled_bookings,
    c.unique_clean_bookings,

    gm.gold_monthly_bookings_count,
    gh.gold_hotel_bookings_count,
    gr.gold_room_bookings_count,
    gc.gold_channel_bookings_count,

    CASE
        WHEN b.bronze_rows =
             c.clean_rows
             + r.rejected_rows
             + (b.bronze_rows - p.prepared_rows)
         AND p.prepared_rows =
             c.clean_rows + r.rejected_rows
         AND c.clean_rows = c.unique_clean_bookings
        THEN 'PASSED'
        ELSE 'FAILED'
    END AS reconciliation_status,

    CASE
        WHEN gm.gold_monthly_bookings_count = c.clean_rows
         AND gh.gold_hotel_bookings_count = c.clean_rows
         AND gr.gold_room_bookings_count = c.clean_rows
         AND gc.gold_channel_bookings_count = c.clean_rows
         AND gm.gold_cancelled_bookings_count =
             c.cancelled_bookings
        THEN 'PASSED'
        ELSE 'FAILED'
    END AS gold_validation_status

FROM bronze_quality b
CROSS JOIN prepared_quality p
CROSS JOIN clean_quality c
CROSS JOIN rejected_quality r
CROSS JOIN gold_monthly_quality gm
CROSS JOIN gold_hotel_quality gh
CROSS JOIN gold_room_quality gr
CROSS JOIN gold_channel_quality gc;
