-- gaggimate-archiver: DuckDB views + starter queries over the archive.
--
-- Run from the archive root (paths are relative to it):
--   host:    cd /path/to/your/archive && duckdb -cmd ".read /path/to/queries.sql"
--   dev box: cd <archive-dev> && duckdb, then .read ../queries.sql
-- Or from python: duckdb.connect().sql(open('queries.sql').read()).
--
-- Everything is derived and disposable; the views recompute from Parquet,
-- and Parquet rebuilds from raw/ (README).
--
-- Timestamps are stored as device epoch seconds (UTC). Sample `t` is ms
-- since shot start; `duration` is ms. Phases: 0..N in shot order; phase
-- NAMES are not projected into Parquet (they live in the .slog header;
-- scripts/plot_shot.py reads them from raw when labeling).
--
-- One measured pattern worth knowing before trusting averages (2026-08-18,
-- 50 shots): scale-connected shots (final_weight NOT NULL) average ~92 C
-- current temp - textbook brew - while unweighed pulls average ~124 C
-- (flushes / post-steam pulls). Aggregate temperature queries should filter
-- or group on `weighed` or they mix the two populations.

CREATE OR REPLACE VIEW shots AS
SELECT * FROM 'parquet/shots.parquet';

CREATE OR REPLACE VIEW samples AS
SELECT * FROM 'parquet/samples/*.parquet';

-- One row per shot with sample-derived stats joined in.
CREATE OR REPLACE VIEW shot_summary AS
SELECT
    s.shot_id,
    to_timestamp(s.timestamp)                    AS ts_utc,
    s.profile_name,
    round(s.duration / 1000.0, 1)                AS duration_s,
    s.sample_count,
    s.final_weight                               AS weight_g,
    s.final_weight IS NOT NULL                   AS weighed,
    s.rating,
    s.incomplete,
    round(a.peak_pressure, 1)                    AS peak_pressure_bar,
    round(a.avg_extract_temp, 1)                 AS avg_extract_temp_c,
    round(a.avg_temp_err, 2)                     AS avg_temp_err_c,
    round(a.peak_flow, 1)                        AS peak_flow_mls,
    a.phases
FROM shots s
JOIN (
    SELECT
        shot_id,
        max(cp)                                        AS peak_pressure,
        -- "extraction" = the back half of the phase sequence, where the
        -- pump is actually driving water through the puck
        avg(ct)  FILTER (phase >= 2)                   AS avg_extract_temp,
        avg(abs(ct - tt)) FILTER (phase >= 2)          AS avg_temp_err,
        max(fl)                                        AS peak_flow,
        max(phase) + 1                                 AS phases
    FROM samples GROUP BY shot_id
) a USING (shot_id);

-- Q1: the last 15 shots at a glance.
SELECT * FROM shot_summary ORDER BY ts_utc DESC LIMIT 15;

-- Q2: per-profile trends - the original ask: duration by profile, plus
-- weight and pressure, real espresso only (weighed).
SELECT
    profile_name,
    count(*)                            AS shots,
    round(avg(duration_s), 1)           AS avg_duration_s,
    round(stddev(duration_s), 1)        AS sd_duration_s,
    round(avg(weight_g), 1)             AS avg_weight_g,
    round(avg(peak_pressure_bar), 1)    AS avg_peak_bar,
    min(ts_utc)::DATE                   AS first_shot,
    max(ts_utc)::DATE                   AS last_shot
FROM shot_summary
WHERE weighed
GROUP BY profile_name
ORDER BY shots DESC;

-- Q3: repeatability week over week - is the routine converging? Duration
-- and weight spread per ISO week (weighed shots).
SELECT
    date_trunc('week', ts_utc)::DATE    AS week,
    count(*)                            AS shots,
    round(avg(duration_s), 1)           AS avg_duration_s,
    round(stddev(duration_s), 1)        AS sd_duration_s,
    round(avg(weight_g), 1)             AS avg_weight_g,
    round(stddev(weight_g), 1)          AS sd_weight_g
FROM shot_summary
WHERE weighed
GROUP BY 1 ORDER BY 1;

-- Q4: temperature discipline - shots whose extraction ran furthest from
-- target (avg |ct - tt| during phases >= 2). High values on weighed shots
-- are worth a look; high values on unweighed pulls are just the flush.
SELECT shot_id, ts_utc, profile_name, weighed,
       avg_extract_temp_c, avg_temp_err_c
FROM shot_summary
ORDER BY avg_temp_err_c DESC
LIMIT 10;

-- Q5: the weighed/unweighed split itself - two populations in one archive
-- (see header note). If this drifts from ~50/50, the routine changed.
SELECT weighed, count(*) AS shots,
       round(avg(duration_s), 1)          AS avg_duration_s,
       round(avg(avg_extract_temp_c), 1)  AS avg_extract_temp_c,
       round(avg(peak_pressure_bar), 1)   AS avg_peak_bar
FROM shot_summary
GROUP BY weighed;
