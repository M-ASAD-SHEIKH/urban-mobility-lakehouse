-- 1. monthly_summary
SELECT * FROM mobility.gold.monthly_summary;

-- 2. trips_by_zone
SELECT Zone, Borough, total_trips, avg_fare
FROM mobility.gold.trips_by_zone
ORDER BY total_trips DESC
LIMIT 10;

-- 3. trips_by_hour
SELECT * FROM mobility.gold.trips_by_hour;

-- 4. rejected_records
SELECT reject_reason, COUNT(*) AS rejected_rows
FROM mobility.silver.trips_quarantine
GROUP BY reject_reason;
