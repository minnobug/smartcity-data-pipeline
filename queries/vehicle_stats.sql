-- ============================================================
-- vehicle_stats.sql
-- Thống kê phương tiện, tốc độ, thời tiết trên HCM → Vũng Tàu
-- Database: smartcity_db  |  Engine: AWS Athena
-- ============================================================

-- ── 1. Thống kê tốc độ theo hãng xe ─────────────────────────
SELECT
    make,
    fuelType,
    COUNT(*)                            AS trip_count,
    ROUND(AVG(speed_kmh), 1)            AS avg_speed_kmh,
    ROUND(MIN(speed_kmh), 1)            AS min_speed_kmh,
    ROUND(MAX(speed_kmh), 1)            AS max_speed_kmh,
    ROUND(STDDEV(speed_kmh), 2)         AS stddev_speed
FROM smartcity_db.vehicle_data
GROUP BY make, fuelType
ORDER BY avg_speed_kmh DESC;


-- ── 2. Phân phối tốc độ theo giờ ─────────────────────────────
SELECT
    hour,
    COUNT(*)                            AS observations,
    ROUND(AVG(speed_kmh), 1)            AS avg_speed,
    ROUND(APPROX_PERCENTILE(speed_kmh, 0.50), 1) AS p50_speed,
    ROUND(APPROX_PERCENTILE(speed_kmh, 0.95), 1) AS p95_speed
FROM smartcity_db.vehicle_data
GROUP BY hour
ORDER BY hour;


-- ── 3. Tỷ lệ xe điện (EV) theo ngày ─────────────────────────
SELECT
    date,
    COUNT(*)                                                    AS total_vehicles,
    SUM(CAST(is_ev AS INTEGER))                                 AS ev_count,
    ROUND(100.0 * SUM(CAST(is_ev AS INTEGER)) / COUNT(*), 1)   AS ev_pct
FROM smartcity_db.vehicle_data
GROUP BY date
ORDER BY date DESC;


-- ── 4. Top model xe phổ biến nhất ────────────────────────────
SELECT
    make,
    model,
    year,
    COUNT(*)                            AS trip_count,
    ROUND(AVG(speed_kmh), 1)            AS avg_speed_kmh
FROM smartcity_db.vehicle_data
GROUP BY make, model, year
ORDER BY trip_count DESC
LIMIT 15;


-- ── 5. Join vehicle + weather: tốc độ theo điều kiện thời tiết
SELECT
    w.weatherCondition,
    w.aqi_category,
    COUNT(v.id)                         AS vehicle_obs,
    ROUND(AVG(v.speed_kmh), 1)          AS avg_speed_kmh,
    ROUND(AVG(w.temperature), 1)        AS avg_temp_c,
    ROUND(AVG(w.heat_index), 1)         AS avg_heat_index,
    ROUND(AVG(w.humidity), 0)           AS avg_humidity_pct
FROM smartcity_db.vehicle_data  v
JOIN smartcity_db.weather_data  w
  ON  v.vehicle_id = w.vehicle_id
  AND v.date       = w.date
GROUP BY w.weatherCondition, w.aqi_category
ORDER BY avg_speed_kmh DESC;


-- ── 6. Tổng quan hàng ngày (dashboard KPI) ───────────────────
SELECT
    v.date,
    COUNT(DISTINCT v.vehicle_id)        AS unique_vehicles,
    COUNT(v.id)                         AS total_records,
    ROUND(AVG(v.speed_kmh), 1)          AS avg_speed_kmh,
    SUM(CAST(v.is_ev AS INTEGER))       AS ev_trips,
    ROUND(AVG(w.temperature), 1)        AS avg_temp_c,
    COUNT(e.id)                         AS total_incidents,
    SUM(CAST(e.is_real_incident AS INTEGER)) AS real_incidents
FROM      smartcity_db.vehicle_data   v
LEFT JOIN smartcity_db.weather_data   w ON v.vehicle_id = w.vehicle_id AND v.date = w.date
LEFT JOIN smartcity_db.emergency_data e ON v.vehicle_id = e.vehicle_id AND v.date = e.date
GROUP BY v.date
ORDER BY v.date DESC;