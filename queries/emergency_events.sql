-- ============================================================
-- emergency_events.sql
-- Phân tích sự cố khẩn cấp trên tuyến HCM → Vũng Tàu
-- Database: smartcity_db  |  Engine: AWS Athena
-- ============================================================

-- ── 1. Tổng hợp theo ngày & loại sự cố ──────────────────────
SELECT
    date,
    type                                        AS incident_type,
    status,
    COUNT(*)                                    AS total_events,
    SUM(CAST(is_active        AS INTEGER))      AS active_count,
    SUM(CAST(is_real_incident AS INTEGER))      AS real_incident_count
FROM smartcity_db.emergency_data
GROUP BY date, type, status
ORDER BY date DESC, total_events DESC;


-- ── 2. Active incidents đang diễn ra (hôm nay) ───────────────
SELECT
    id,
    vehicle_id,
    incident_id    AS "incidentId",
    type,
    timestamp,
    location,
    description
FROM smartcity_db.emergency_data
WHERE is_active = TRUE
  AND is_real_incident = TRUE
  AND date = CAST(current_date AS VARCHAR)
ORDER BY timestamp DESC;


-- ── 3. Tỷ lệ giải quyết theo loại sự cố ─────────────────────
SELECT
    type                                            AS incident_type,
    COUNT(*)                                        AS total,
    SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END)   AS resolved,
    SUM(CASE WHEN status = 'Active'   THEN 1 ELSE 0 END)   AS still_active,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END) / COUNT(*),
        1
    )                                               AS resolution_rate_pct
FROM smartcity_db.emergency_data
WHERE is_real_incident = TRUE
GROUP BY type
ORDER BY total DESC;


-- ── 4. Phân bố sự cố theo giờ trong ngày ────────────────────
SELECT
    HOUR(timestamp)         AS hour_of_day,
    type                    AS incident_type,
    COUNT(*)                AS event_count
FROM smartcity_db.emergency_data
WHERE is_real_incident = TRUE
GROUP BY HOUR(timestamp), type
ORDER BY hour_of_day, event_count DESC;


-- ── 5. Xe có nhiều sự cố nhất ────────────────────────────────
SELECT
    vehicle_id,
    COUNT(*)                                                AS total_incidents,
    SUM(CAST(is_real_incident AS INTEGER))                  AS real_incidents,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT type), ', ')              AS incident_types
FROM smartcity_db.emergency_data
GROUP BY vehicle_id
HAVING SUM(CAST(is_real_incident AS INTEGER)) > 0
ORDER BY real_incidents DESC
LIMIT 20;