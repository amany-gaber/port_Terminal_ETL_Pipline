-- =============================================================================
-- PORT KPI DATA WAREHOUSE - USEFUL SQL QUERIES
-- =============================================================================
-- These queries help analyze the port operations data in the star schema

-- =============================================================================
-- 1. DATA QUALITY & COMPLETENESS
-- =============================================================================

-- Check row counts across all tables
SELECT 
    'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
UNION ALL SELECT 'dim_vessel', COUNT(*) FROM dim_vessel
UNION ALL SELECT 'dim_ship_owner', COUNT(*) FROM dim_ship_owner
UNION ALL SELECT 'dim_port', COUNT(*) FROM dim_port
UNION ALL SELECT 'dim_berth', COUNT(*) FROM dim_berth
UNION ALL SELECT 'dim_service', COUNT(*) FROM dim_service
UNION ALL SELECT 'dim_equipment', COUNT(*) FROM dim_equipment
UNION ALL SELECT 'dim_call', COUNT(*) FROM dim_call
UNION ALL SELECT 'fact_port_operations', COUNT(*) FROM fact_port_operations
ORDER BY table_name;

-- Check for null values in key fact table columns
SELECT 
    'call_key' as column_name, COUNT(*) as null_count FROM fact_port_operations WHERE call_key IS NULL
UNION ALL SELECT 'vessel_key', COUNT(*) FROM fact_port_operations WHERE vessel_key IS NULL
UNION ALL SELECT 'berth_key', COUNT(*) FROM fact_port_operations WHERE berth_key IS NULL
UNION ALL SELECT 'containers_handled', COUNT(*) FROM fact_port_operations WHERE containers_handled IS NULL
ORDER BY null_count DESC;

-- =============================================================================
-- 2. PORT OPERATIONS ANALYSIS
-- =============================================================================

-- Port-level summary
SELECT 
    p.port_name,
    COUNT(DISTINCT f.call_key) as total_calls,
    COUNT(DISTINCT f.vessel_key) as unique_vessels,
    SUM(f.containers_handled) as total_containers,
    ROUND(AVG(f.berth_operation_time), 2) as avg_berth_operation_hours,
    ROUND(AVG(f.operation_time), 2) as avg_total_operation_hours,
    SUM(CASE WHEN f.status = 'delayed' THEN 1 ELSE 0 END) as delayed_calls,
    ROUND(100.0 * SUM(CASE WHEN f.status = 'delayed' THEN 1 ELSE 0 END) / COUNT(DISTINCT f.call_key), 2) as delay_percentage
FROM fact_port_operations f
JOIN dim_port p ON f.port_key = p.port_key
GROUP BY p.port_name
ORDER BY total_containers DESC;

-- Detailed port operations with all dimensions
SELECT 
    f.fact_key,
    f.call_key,
    v.vessel_name,
    v.imo,
    p.port_name,
    b.berth_id,
    s.service_name,
    f.containers_handled,
    ROUND(f.operation_time::numeric, 2) as operation_hours,
    ROUND(f.berth_operation_time::numeric, 2) as berth_operation_hours,
    f.status,
    ROUND(f.delay_weight::numeric, 4) as delay_weight,
    d.full_date as arrival_date
FROM fact_port_operations f
LEFT JOIN dim_vessel v ON f.vessel_key = v.vessel_key
LEFT JOIN dim_port p ON f.port_key = p.port_key
LEFT JOIN dim_berth b ON f.berth_key = b.berth_key
LEFT JOIN dim_service s ON f.service_key = s.service_key
LEFT JOIN dim_date d ON f.arrival_date_key = d.date_key
LIMIT 20;

-- =============================================================================
-- 3. VESSEL PERFORMANCE ANALYSIS
-- =============================================================================

-- Vessel productivity statistics
SELECT 
    v.vessel_name,
    v.imo,
    so.ship_owner_name,
    COUNT(DISTINCT f.call_key) as calls_count,
    SUM(f.containers_handled) as total_containers,
    ROUND(AVG(f.vessel_gross_prdvty)::numeric, 2) as avg_gross_productivity,
    ROUND(AVG(f.vessel_net_prdvty)::numeric, 2) as avg_net_productivity,
    ROUND(AVG(f.operation_time)::numeric, 2) as avg_operation_hours,
    ROUND(AVG(f.berth_operation_time)::numeric, 2) as avg_berth_hours
FROM fact_port_operations f
JOIN dim_vessel v ON f.vessel_key = v.vessel_key
LEFT JOIN dim_ship_owner so ON v.ship_owner_key = so.ship_owner_key
GROUP BY v.vessel_key, v.vessel_name, v.imo, so.ship_owner_name
ORDER BY total_containers DESC;

-- Top 10 most active vessels
SELECT 
    RANK() OVER (ORDER BY COUNT(DISTINCT f.call_key) DESC) as rank,
    v.vessel_name,
    v.vessel_class,
    COUNT(DISTINCT f.call_key) as port_calls,
    SUM(f.containers_handled) as containers_handled,
    ROUND(AVG(f.operation_time)::numeric, 2) as avg_operation_hours
FROM fact_port_operations f
JOIN dim_vessel v ON f.vessel_key = v.vessel_key
GROUP BY v.vessel_key, v.vessel_name, v.vessel_class
ORDER BY port_calls DESC
LIMIT 10;

-- Vessel delays analysis
SELECT 
    v.vessel_name,
    COUNT(DISTINCT f.call_key) as total_calls,
    SUM(CASE WHEN f.status = 'delayed' THEN 1 ELSE 0 END) as delayed_calls,
    ROUND(100.0 * SUM(CASE WHEN f.status = 'delayed' THEN 1 ELSE 0 END) / COUNT(DISTINCT f.call_key), 2) as delay_percentage,
    ROUND(AVG(f.delay_weight)::numeric, 4) as avg_delay_weight
FROM fact_port_operations f
JOIN dim_vessel v ON f.vessel_key = v.vessel_key
GROUP BY v.vessel_key, v.vessel_name
HAVING COUNT(DISTINCT f.call_key) >= 5
ORDER BY delay_percentage DESC
LIMIT 15;

-- =============================================================================
-- 4. BERTH & TERMINAL UTILIZATION
-- =============================================================================

-- Berth utilization metrics
SELECT 
    p.port_name,
    b.berth_id,
    b.berth_custom_grouping,
    COUNT(DISTINCT f.call_key) as calls,
    SUM(f.containers_handled) as containers,
    ROUND(AVG(f.berth_operation_time)::numeric, 2) as avg_berth_hours,
    ROUND(AVG(f.berth_prdvty)::numeric, 2) as avg_berth_productivity
FROM fact_port_operations f
JOIN dim_berth b ON f.berth_key = b.berth_key
JOIN dim_port p ON f.port_key = p.port_key
GROUP BY p.port_name, b.berth_id, b.berth_custom_grouping
ORDER BY containers DESC;

-- Berth performance comparison
SELECT 
    b.berth_id,
    COUNT(DISTINCT f.call_key) as calls,
    ROUND(AVG(f.berth_operation_time)::numeric, 2) as avg_operation_hours,
    ROUND(MIN(f.berth_operation_time)::numeric, 2) as min_operation_hours,
    ROUND(MAX(f.berth_operation_time)::numeric, 2) as max_operation_hours,
    ROUND(STDDEV(f.berth_operation_time)::numeric, 2) as stddev_operation_hours
FROM fact_port_operations f
JOIN dim_berth b ON f.berth_key = b.berth_key
GROUP BY b.berth_id, b.berth_key
ORDER BY calls DESC;

-- =============================================================================
-- 5. SERVICE ROUTE ANALYSIS
-- =============================================================================

-- Service performance metrics
SELECT 
    s.service_name,
    COUNT(DISTINCT f.call_key) as total_calls,
    COUNT(DISTINCT f.vessel_key) as unique_vessels,
    SUM(f.containers_handled) as total_containers,
    ROUND(AVG(f.operation_time)::numeric, 2) as avg_operation_hours,
    ROUND(AVG(f.berth_prdvty)::numeric, 2) as avg_berth_productivity,
    SUM(CASE WHEN f.status = 'delayed' THEN 1 ELSE 0 END) as delayed_calls
FROM fact_port_operations f
LEFT JOIN dim_service s ON f.service_key = s.service_key
GROUP BY s.service_key, s.service_name
ORDER BY total_containers DESC;

-- =============================================================================
-- 6. EQUIPMENT ANALYSIS
-- =============================================================================

-- Equipment productivity
SELECT 
    e.eqp_type,
    e.eqp_type_custom,
    COUNT(DISTINCT f.equipment_key) as equipment_units,
    COUNT(DISTINCT f.call_key) as operations,
    ROUND(AVG(f.operating_time_avg)::numeric, 2) as avg_operating_hours,
    SUM(f.containers_handled) as total_containers_handled
FROM fact_port_operations f
LEFT JOIN dim_equipment e ON f.equipment_key = e.equipment_key
GROUP BY e.eqp_type, e.eqp_type_custom, e.equipment_key
ORDER BY operations DESC;

-- Container handling by equipment type
SELECT 
    e.equipment_type,
    SUM(f.te_tc20) as tc20_containers,
    SUM(f.te_tc40) as tc40_containers,
    SUM(f.te_deba) as deba_containers,
    SUM(f.te_emba) as emba_containers,
    SUM(f.te_deba_oog) as deba_oog,
    SUM(f.te_emba_oog) as emba_oog,
    COUNT(DISTINCT f.call_key) as operations
FROM fact_port_operations f
LEFT JOIN dim_equipment e ON f.equipment_key = e.equipment_key
WHERE e.equipment_type IS NOT NULL
GROUP BY e.equipment_type
ORDER BY operations DESC;

-- =============================================================================
-- 7. TIME-BASED ANALYSIS
-- =============================================================================

-- Monthly trend analysis
SELECT 
    d.year,
    d.month,
    COUNT(DISTINCT f.call_key) as calls,
    SUM(f.containers_handled) as containers,
    ROUND(AVG(f.operation_time)::numeric, 2) as avg_operation_hours,
    SUM(CASE WHEN f.status = 'delayed' THEN 1 ELSE 0 END) as delayed_calls
FROM fact_port_operations f
JOIN dim_date d ON f.arrival_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;

-- Busiest days (by number of operations)
SELECT 
    d.full_date,
    COUNT(DISTINCT f.call_key) as calls,
    SUM(f.containers_handled) as containers,
    ROUND(AVG(f.operation_time)::numeric, 2) as avg_operation_hours
FROM fact_port_operations f
JOIN dim_date d ON f.arrival_date_key = d.date_key
GROUP BY d.full_date
ORDER BY calls DESC
LIMIT 20;

-- =============================================================================
-- 8. DELAY & PERFORMANCE INDICATORS
-- =============================================================================

-- Overall delay statistics
SELECT 
    'Total Operations' as metric, COUNT(DISTINCT call_key) as value
FROM fact_port_operations
UNION ALL SELECT 'Delayed Operations', SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END)
FROM fact_port_operations
UNION ALL SELECT 'Non-Delayed Operations', SUM(CASE WHEN status = 'non delayed' THEN 1 ELSE 0 END)
FROM fact_port_operations
UNION ALL SELECT 'Avg Delay Weight', ROUND(AVG(delay_weight)::numeric, 4)
FROM fact_port_operations
UNION ALL SELECT 'Total Containers', SUM(containers_handled)
FROM fact_port_operations;

-- Delay distribution
SELECT 
    CASE 
        WHEN delay_weight < 0.3 THEN 'Very Low (0.0-0.3)'
        WHEN delay_weight < 0.6 THEN 'Low (0.3-0.6)'
        WHEN delay_weight < 0.8 THEN 'Medium (0.6-0.8)'
        ELSE 'High (0.8+)'
    END as delay_category,
    COUNT(*) as operation_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM fact_port_operations
WHERE delay_weight IS NOT NULL
GROUP BY delay_category
ORDER BY delay_weight DESC;

-- =============================================================================
-- 9. IDLE & WAITING TIME ANALYSIS
-- =============================================================================

-- Idle time statistics by port
SELECT 
    p.port_name,
    ROUND(AVG(f.idle_time_before_ops)::numeric, 2) as avg_idle_before,
    ROUND(AVG(f.idle_time_after_ops)::numeric, 2) as avg_idle_after,
    ROUND(AVG(f.anchorage_time_in_hours)::numeric, 2) as avg_anchorage_hours,
    COUNT(DISTINCT f.call_key) as calls
FROM fact_port_operations f
JOIN dim_port p ON f.port_key = p.port_key
GROUP BY p.port_key, p.port_name
ORDER BY avg_idle_before DESC;

-- =============================================================================
-- 10. PRODUCTIVITY METRICS
-- =============================================================================

-- Overall productivity analysis
SELECT 
    'Avg Vessel Gross Productivity' as metric, 
    ROUND(AVG(vessel_gross_prdvty)::numeric, 2) as value
FROM fact_port_operations
WHERE vessel_gross_prdvty > 0
UNION ALL SELECT 'Avg Vessel Net Productivity', 
    ROUND(AVG(vessel_net_prdvty)::numeric, 2)
FROM fact_port_operations
WHERE vessel_net_prdvty > 0
UNION ALL SELECT 'Avg Berth Productivity', 
    ROUND(AVG(berth_prdvty)::numeric, 2)
FROM fact_port_operations
WHERE berth_prdvty > 0;

-- Port productivity ranking
SELECT 
    RANK() OVER (ORDER BY ROUND(AVG(f.berth_prdvty)::numeric, 2) DESC) as rank,
    p.port_name,
    COUNT(DISTINCT f.call_key) as calls,
    ROUND(AVG(f.berth_prdvty)::numeric, 2) as avg_berth_productivity,
    ROUND(AVG(f.vessel_gross_prdvty)::numeric, 2) as avg_vessel_productivity
FROM fact_port_operations f
JOIN dim_port p ON f.port_key = p.port_key
GROUP BY p.port_key, p.port_name
ORDER BY rank;

-- =============================================================================
-- 11. SHIP OWNER PERFORMANCE
-- =============================================================================

-- Ship owner statistics
SELECT 
    so.ship_owner_name,
    so.ship_owner_group,
    COUNT(DISTINCT v.vessel_key) as fleet_size,
    COUNT(DISTINCT f.call_key) as total_calls,
    SUM(f.containers_handled) as total_containers,
    ROUND(AVG(f.operation_time)::numeric, 2) as avg_operation_hours,
    ROUND(AVG(f.delay_weight)::numeric, 4) as avg_delay_weight
FROM fact_port_operations f
JOIN dim_vessel v ON f.vessel_key = v.vessel_key
LEFT JOIN dim_ship_owner so ON v.ship_owner_key = so.ship_owner_key
WHERE so.ship_owner_name IS NOT NULL
GROUP BY so.ship_owner_key, so.ship_owner_name, so.ship_owner_group
ORDER BY total_containers DESC;

-- =============================================================================
-- 12. CONTAINER TYPE ANALYSIS
-- =============================================================================

-- Container type distribution
SELECT 
    SUM(te_tc20) as tc20_total,
    SUM(te_tc40) as tc40_total,
    SUM(te_deba) as deba_total,
    SUM(te_emba) as emba_total,
    SUM(te_deba_oog) as deba_oog_total,
    SUM(te_emba_oog) as emba_oog_total,
    ROUND(100.0 * SUM(te_tc20) / SUM(containers_handled), 2) as tc20_percentage,
    ROUND(100.0 * SUM(te_tc40) / SUM(containers_handled), 2) as tc40_percentage,
    SUM(containers_handled) as total_containers
FROM fact_port_operations
WHERE containers_handled > 0;

-- =============================================================================
-- 13. ADVANCED ANALYTICS
-- =============================================================================

-- Vessel performance vs port average
WITH port_avg AS (
    SELECT 
        f.port_key,
        ROUND(AVG(f.operation_time)::numeric, 2) as port_avg_operation_time
    FROM fact_port_operations f
    GROUP BY f.port_key
)
SELECT 
    v.vessel_name,
    p.port_name,
    COUNT(DISTINCT f.call_key) as calls,
    ROUND(AVG(f.operation_time)::numeric, 2) as vessel_avg_operation_time,
    ROUND(AVG(pa.port_avg_operation_time)::numeric, 2) as port_avg_operation_time,
    ROUND(AVG(f.operation_time)::numeric - AVG(pa.port_avg_operation_time)::numeric, 2) as difference
FROM fact_port_operations f
JOIN dim_vessel v ON f.vessel_key = v.vessel_key
JOIN dim_port p ON f.port_key = p.port_key
JOIN port_avg pa ON f.port_key = pa.port_key
GROUP BY v.vessel_key, v.vessel_name, p.port_key, p.port_name
HAVING COUNT(DISTINCT f.call_key) >= 3
ORDER BY difference DESC
LIMIT 20;

-- Operational efficiency index (lower is better)
SELECT 
    RANK() OVER (ORDER BY efficiency_index) as rank,
    vessel_name,
    port_name,
    efficiency_index
FROM (
    SELECT 
        v.vessel_name,
        p.port_name,
        ROUND(
            (AVG(f.operation_time) + AVG(f.delay_weight) * 10 + AVG(f.idle_time_before_ops)) / 3,
            2
        ) as efficiency_index,
        COUNT(DISTINCT f.call_key) as calls
    FROM fact_port_operations f
    JOIN dim_vessel v ON f.vessel_key = v.vessel_key
    JOIN dim_port p ON f.port_key = p.port_key
    WHERE f.operation_time > 0
    GROUP BY v.vessel_key, v.vessel_name, p.port_key, p.port_name
    HAVING COUNT(DISTINCT f.call_key) >= 3
) subq
ORDER BY efficiency_index
LIMIT 20;

-- =============================================================================
-- EXPORT QUERIES (useful for reporting)
-- =============================================================================

-- Export summary for BI tool
SELECT 
    'Summary' as report_type,
    COUNT(DISTINCT call_key) as total_calls,
    SUM(containers_handled) as total_containers,
    COUNT(DISTINCT vessel_key) as unique_vessels,
    COUNT(DISTINCT port_key) as ports,
    ROUND(AVG(operation_time)::numeric, 2) as avg_operation_hours
FROM fact_port_operations;
