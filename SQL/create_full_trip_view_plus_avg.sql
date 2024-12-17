DROP MATERIALIZED VIEW IF EXISTS daily_report;

CREATE MATERIALIZED VIEW daily_report AS
SELECT
    vp.entity_id,
    t.trip_id,
    t.route_id,
    r.route_short_name,
    t.stop_sequence,
    t.arrival_delay,
    (to_timestamp(t.timestamp) AT TIME ZONE 'Europe/Warsaw') AS local_timestamp,
    vp.latitude,
    vp.longitude,
    stt.stop_id,
    s.stop_name,
    stt.arrival_time,
    stt.departure_time,
    tr.service_id,
    r.route_type,
    t.version_id,
    c.start_date AS service_start_date,
    c.end_date AS service_end_date,
    tr.direction_id,
    tr.trip_headsign,

    MIN(stt.arrival_time) OVER (PARTITION BY t.trip_id) AS trip_start_time,
    MAX(stt.departure_time) OVER (PARTITION BY t.trip_id) AS trip_end_time,

    -- Średnie opóźnienie linii, ignorując stop_sequence=0
    AVG(t.arrival_delay) FILTER (WHERE t.stop_sequence > 0) OVER (PARTITION BY r.route_id) AS route_avg_delay,

    -- Średnie opóźnienie linii dla danego przystanku, ignorując stop_sequence=0
    AVG(t.arrival_delay) FILTER (WHERE t.stop_sequence > 0) OVER (PARTITION BY r.route_id, stt.stop_id) AS route_stop_avg_delay

FROM trip_updates t
JOIN trips tr ON t.trip_id = tr.trip_id AND t.version_id = tr.version_id
JOIN routes r ON tr.route_id = r.route_id AND tr.version_id = r.version_id
JOIN stop_times stt ON t.trip_id = stt.trip_id AND t.version_id = stt.version_id AND t.stop_sequence = stt.stop_sequence
LEFT JOIN vehicle_positions vp ON t.trip_id = vp.trip_id AND t.version_id = vp.version_id AND t.timestamp = vp.timestamp
LEFT JOIN calendar c ON tr.service_id = c.service_id AND tr.version_id = c.version_id
LEFT JOIN stops s ON stt.stop_id = s.stop_id
WHERE t.timestamp >= EXTRACT(EPOCH FROM TIMESTAMP '2024-12-09 04:00:00 Europe/Warsaw')
  AND t.timestamp < EXTRACT(EPOCH FROM TIMESTAMP '2024-12-10 04:00:00 Europe/Warsaw')
ORDER BY t.trip_id, t.timestamp, t.stop_sequence;

CREATE INDEX idx_daily_report_tripid_local_timestamp ON daily_report(trip_id, local_timestamp);
CREATE INDEX idx_daily_report_tripid_stop_sequence ON daily_report(trip_id, stop_sequence);
CREATE INDEX idx_daily_report_arrival_delay ON daily_report(arrival_delay);
CREATE INDEX idx_daily_report_route_id ON daily_report(route_id);
CREATE INDEX idx_daily_report_service_id ON daily_report(service_id);
CREATE INDEX idx_daily_report_route_stop_id ON daily_report(route_id, stop_id);
CLUSTER daily_report USING idx_daily_report_tripid_local_timestamp;
ANALYZE daily_report;
