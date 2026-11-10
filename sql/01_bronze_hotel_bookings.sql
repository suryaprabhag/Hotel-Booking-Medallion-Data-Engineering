
CREATE OR REFRESH STREAMING TABLE bronze_hotel_bookings
AS
SELECT
    *,
    _metadata.file_name AS source_file,
    current_timestamp() AS loaded_at
FROM STREAM read_files(
    '/Volumes/hotel_bookings/default/bookings_volume/',
    format => 'csv',
    header => true
);


