-- =====================================================================
-- Fabric semantic model refresh history store
-- Target : Lakehouse (Spark SQL / Delta), schema `monitoring`
-- Loader : load_semantic_model_refresh_history.ipynb (same folder) - MERGEs from the REST APIs
--   models  GET /v1/workspaces/{ws}/items?type=SemanticModel                      (Fabric API)
--   history GET /v1.0/myorg/groups/{ws}/datasets/{modelId}/refreshes              (Power BI API)
--   detail  GET /v1.0/myorg/groups/{ws}/datasets/{modelId}/refreshes/{requestId}  (enhanced refreshes only)
--
-- Refreshes are not Fabric job instances, so pipeline_run_history only sees
-- the ones a pipeline triggers; this table holds every refresh type
-- (scheduled, on-demand, pipeline/API, XMLA).
-- The API keeps only 20-60 refreshes per model (entries older than 3 days
-- are dropped once there are more than 20), so load at least daily.
-- Load with MERGE: a refresh is first seen in progress (status Unknown).
-- All timestamps are UTC; local time is derived in the view.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS monitoring;

-- ---------------------------------------------------------------------
-- 1. Refreshes - grain: one row per (semantic_model_id, request_id)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.semantic_model_refresh_history (
    semantic_model_id      STRING    NOT NULL COMMENT 'Dataset id (= Fabric SemanticModel item id)',
    request_id             STRING    NOT NULL COMMENT 'requestId; the refreshId of the enhanced-refresh detail call',
    refresh_id             BIGINT             COMMENT 'id (numeric refresh id)',
    workspace_id           STRING    NOT NULL,
    workspace_name         STRING,
    semantic_model_name    STRING    NOT NULL COMMENT 'Display name at load time',
    refresh_type           STRING             COMMENT 'Scheduled | OnDemand | ViaApi | ViaEnhancedApi (pipeline activity) | ViaXmlaEndpoint | OnDemandTraining',
    status                 STRING    NOT NULL COMMENT 'Unknown (in progress) | Completed | Failed | Cancelled | Disabled',
    extended_status        STRING             COMMENT 'extendedStatus, set on enhanced refreshes',
    start_time_utc         TIMESTAMP,
    end_time_utc           TIMESTAMP,
    duration_ms            BIGINT             COMMENT 'end - start; NULL while running',
    attempt_count          INT                COMMENT 'Number of refreshAttempts',
    error_code             STRING             COMMENT 'serviceExceptionJson.errorCode',
    error_description      STRING             COMMENT 'serviceExceptionJson.errorDescription',
    error_messages         STRING             COMMENT 'Error messages from the enhanced-refresh detail call (engine error, e.g. duplicate key)',
    service_exception_json STRING,
    attempts_json          STRING             COMMENT 'refreshAttempts as JSON',
    detail_json            STRING             COMMENT 'Enhanced-refresh detail payload (objects, messages); failed refreshes only',
    raw_json               STRING             COMMENT 'Full API payload, for fields not modelled yet',
    ingested_at_utc        TIMESTAMP NOT NULL COMMENT 'First time this refresh was loaded',
    updated_at_utc         TIMESTAMP NOT NULL COMMENT 'Last MERGE that changed this refresh'
)
USING DELTA
COMMENT 'Fabric / Power BI semantic model refresh history - one row per refresh'
TBLPROPERTIES (
    'delta.parquet.vorder.enabled'   = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ---------------------------------------------------------------------
-- 2. Refresh health - SGT times and one error column to read
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW monitoring.vw_semantic_model_refresh_health AS
SELECT semantic_model_id,
       request_id,
       workspace_name,
       semantic_model_name,
       refresh_type,
       status                                                          AS api_status,
       CASE
           WHEN status = 'Unknown' AND end_time_utc IS NULL THEN 'Running'
           ELSE status
       END                                                             AS effective_status,
       from_utc_timestamp(start_time_utc, 'Asia/Singapore')            AS start_time_sgt,
       from_utc_timestamp(end_time_utc,   'Asia/Singapore')            AS end_time_sgt,
       CAST(from_utc_timestamp(start_time_utc, 'Asia/Singapore') AS DATE) AS refresh_date_sgt,
       ROUND(duration_ms / 60000.0, 2)                                 AS duration_minutes,
       attempt_count,
       error_code,
       COALESCE(error_messages, error_description, error_code)         AS error_detail
FROM monitoring.semantic_model_refresh_history;
