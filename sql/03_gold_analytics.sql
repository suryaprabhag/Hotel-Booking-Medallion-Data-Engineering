-- Gold table 1: Monthly bookings
-- Business requirement BR-01: Track total bookings, cancelled bookings, completed room nights, and net revenue by stay month.


CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_bookings
AS
SELECT
    DATE_TRUNC('MONTH', check_in_date) AS stay_month,

    COUNT(*) AS bookings,

    SUM(
        CASE
            WHEN booking_status = 'Cancelled'
            THEN 1 ELSE 0
        END
    ) AS cancelled_bookings,

    SUM(
        CASE
            WHEN booking_status = 'Completed'
            THEN nights ELSE 0
        END
    ) AS room_nights,

    SUM(
        CASE
            WHEN booking_status = 'Completed'
            THEN COALESCE(total_amount, 0)
                 - COALESCE(discount_amount, 0)
            ELSE 0
        END
    ) AS net_revenue

FROM silver_bookings_clean

GROUP BY DATE_TRUNC('MONTH', check_in_date);


-- Gold table 2: Hotel performance
-- Business requirement BR-02: Compare hotel performance by region, city and hotel name, including Average Daily Rate (ADR).


CREATE OR REFRESH MATERIALIZED VIEW gold_hotel_summary
AS
WITH hotel_metrics AS (
    SELECT
        region,
        city,
        hotel_name,

        COUNT(*) AS bookings,

        SUM(
            CASE
                WHEN booking_status = 'Completed'
                THEN nights ELSE 0
            END
        ) AS room_nights,

        SUM(
            CASE
                WHEN booking_status = 'Completed'
                THEN COALESCE(total_amount, 0)
                     - COALESCE(discount_amount, 0)
                ELSE 0
            END
        ) AS net_revenue

    FROM silver_bookings_clean

    GROUP BY
        region,
        city,
        hotel_name
)

SELECT
    region,
    city,
    hotel_name,
    bookings,
    room_nights,
    net_revenue,

    ROUND(
        net_revenue / NULLIF(room_nights, 0),
        2
    ) AS avg_daily_rate

FROM hotel_metrics;


-- Gold table 3: Room type performance
-- Business requirement BR-03: Analyze the performance of Standard, Deluxe, Suite and Family Room bookings.


CREATE OR REFRESH MATERIALIZED VIEW gold_room_type_summary
AS
SELECT
    room_type,

    COUNT(*) AS bookings,

    SUM(
        CASE
            WHEN booking_status = 'Completed'
            THEN nights ELSE 0
        END
    ) AS room_nights,

    SUM(
        CASE
            WHEN booking_status = 'Completed'
            THEN COALESCE(total_amount, 0)
                 - COALESCE(discount_amount, 0)
            ELSE 0
        END
    ) AS net_revenue,

    ROUND(
        AVG(
            CASE
                WHEN booking_status = 'Completed'
                THEN guest_rating
            END
        ),
        2
    ) AS avg_rating

FROM silver_bookings_clean

GROUP BY room_type;


-- Gold table 4: Booking channel performance
-- Business requirement BR-04: Compare Website, Mobile App, Travel Agent and Walk In bookings.


CREATE OR REFRESH MATERIALIZED VIEW gold_channel_summary
AS
WITH channel_metrics AS (
    SELECT
        booking_channel,

        COUNT(*) AS bookings,

        SUM(
            CASE
                WHEN booking_status = 'Cancelled'
                THEN 1 ELSE 0
            END
        ) AS cancelled_bookings,

        SUM(
            CASE
                WHEN booking_status = 'Completed'
                THEN COALESCE(total_amount, 0)
                     - COALESCE(discount_amount, 0)
                ELSE 0
            END
        ) AS net_revenue

    FROM silver_bookings_clean

    GROUP BY booking_channel
)

SELECT
    booking_channel,
    bookings,
    cancelled_bookings,

    ROUND(
        cancelled_bookings * 100.0
        / NULLIF(bookings, 0),
        2
    ) AS cancellation_rate_pct,

    net_revenue

FROM channel_metrics;
