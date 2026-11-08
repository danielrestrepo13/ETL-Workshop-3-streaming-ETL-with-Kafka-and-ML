-- =============================================================================
-- Workshop-3 : kpis.sql
-- KPI queries for the dashboard (PostgreSQL, schema public).
--
-- They are the reference definition of every KPI: the dashboard must query the
-- database, never CSV files, and these queries let you check the numbers shown
-- in Power BI. In Power BI the three tables fact_predictions, dim_country and
-- dim_date can be imported and related by country_id and date_id; each query
-- can also be pasted as a SQL statement.
--
-- Conventions
--   prediction_error = actual_score - predicted_score (signed), so the average
--   error uses ABS(prediction_error): a signed average would cancel out.
--   Only events that were predicted successfully are in fact_predictions.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- KPI 1. Average prediction error
-- mean_absolute_error is the KPI. rmse is included to compare it with the test
-- metrics of model_training.ipynb (same model and same records: they must match
-- once the whole test split has been streamed).
-- -----------------------------------------------------------------------------
SELECT COUNT(*)                                                    AS predictions,
       ROUND(AVG(ABS(prediction_error))::numeric, 4)               AS mean_absolute_error,
       ROUND(SQRT(AVG(POWER(prediction_error, 2)))::numeric, 4)    AS rmse
FROM fact_predictions;


-- -----------------------------------------------------------------------------
-- KPI 2. Predictions by country
-- -----------------------------------------------------------------------------
SELECT c.country_name,
       COUNT(*)                                          AS predictions,
       ROUND(AVG(ABS(f.prediction_error))::numeric, 4)   AS mean_absolute_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name
ORDER BY predictions DESC, c.country_name;


-- -----------------------------------------------------------------------------
-- KPI 3. Predicted vs actual score (one row per prediction, for a scatter plot)
-- -----------------------------------------------------------------------------
SELECT f.prediction_id,
       c.country_name,
       d.year,
       f.actual_score,
       f.predicted_score,
       f.prediction_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
JOIN dim_date d    ON d.date_id    = f.date_id
ORDER BY d.year, c.country_name;


-- -----------------------------------------------------------------------------
-- KPI 4. Prediction trends over time (by historical year of the report)
-- -----------------------------------------------------------------------------
SELECT d.year,
       COUNT(*)                                          AS predictions,
       ROUND(AVG(f.actual_score)::numeric, 3)            AS avg_actual_score,
       ROUND(AVG(f.predicted_score)::numeric, 3)         AS avg_predicted_score,
       ROUND(AVG(ABS(f.prediction_error))::numeric, 4)   AS mean_absolute_error
FROM fact_predictions f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year
ORDER BY d.year;


-- =============================================================================
-- Operational queries (not required KPIs): they show that the pipeline handles
-- bad events without crashing and how fast events are processed.
-- =============================================================================

-- A. Raw events by processing status (VALID, INVALID_SCHEMA, INVALID_VALUES,
--    PREDICTION_ERROR). A row in RECEIVED means the consumer stopped in the
--    middle of processing that event.
SELECT processing_status,
       COUNT(*) AS events
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY events DESC, processing_status;


-- B. Events that were not accepted, with the reason
SELECT raw_event_id,
       event_id,
       processing_status,
       status_detail,
       received_at
FROM raw_happiness_events
WHERE processing_status <> 'VALID'
ORDER BY raw_event_id;


-- C. Processing latency: seconds between the moment the producer emitted the
--    event (event_time) and the moment the prediction was stored
SELECT COUNT(*)                                                                          AS predictions,
       ROUND(AVG(EXTRACT(EPOCH FROM (prediction_timestamp - event_time)))::numeric, 2)   AS avg_latency_seconds,
       ROUND(MAX(EXTRACT(EPOCH FROM (prediction_timestamp - event_time)))::numeric, 2)   AS max_latency_seconds
FROM fact_predictions;
