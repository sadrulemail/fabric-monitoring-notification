-- =====================================================================
-- Fabric pipeline run history store
-- Target : Lakehouse (Spark SQL / Delta), schema `monitoring`
-- Loader : load_pipeline_run_history.ipynb (same folder) - MERGEs from the Fabric REST API
--   runs       GET  /v1/workspaces/{ws}/items/{pipelineId}/jobs/instances
--   activities POST /v1/workspaces/{ws}/datapipelines/pipelineruns/{runId}/queryactivityruns
--
-- Load with MERGE, not append: a run is first seen InProgress and changes
-- status later, so appending leaves several rows per run.
-- Volume is small (a few runs/day x ~20 activities), so no partitioning;
-- auto-compaction cleans up the small files the MERGEs produce.
-- All timestamps are UTC; local time is derived in the view.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS monitoring;

-- ---------------------------------------------------------------------
-- 1. Pipeline runs - grain: one row per run_id (Fabric job instance)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.pipeline_run_history (
    run_id              STRING    NOT NULL COMMENT 'jobs/instances.id; equals pipelineRunId on activity runs',
    workspace_id        STRING    NOT NULL,
    workspace_name      STRING,
    pipeline_id         STRING    NOT NULL COMMENT 'jobs/instances.itemId',
    pipeline_name       STRING    NOT NULL COMMENT 'Display name at load time',
    job_type            STRING             COMMENT 'jobs/instances.jobType (Pipeline)',
    invoke_type         STRING             COMMENT 'Scheduled | Manual',
    status              STRING    NOT NULL COMMENT 'NotStarted | InProgress | Completed | Failed | Cancelled | Deduped',
    start_time_utc      TIMESTAMP,
    end_time_utc        TIMESTAMP,
    duration_ms         BIGINT             COMMENT 'end - start; NULL while running',
    root_activity_id    STRING,
    failure_error_code  STRING             COMMENT 'failureReason.errorCode',
    failure_message     STRING             COMMENT 'failureReason.message',
    failure_request_id  STRING             COMMENT 'failureReason.requestId',
    raw_json            STRING             COMMENT 'Full API payload, for fields not modelled yet',
    ingested_at_utc     TIMESTAMP NOT NULL COMMENT 'First time this run was loaded',
    updated_at_utc      TIMESTAMP NOT NULL COMMENT 'Last MERGE that changed this run'
)
USING DELTA
COMMENT 'Fabric data pipeline run history - one row per run'
TBLPROPERTIES (
    'delta.parquet.vorder.enabled'   = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ---------------------------------------------------------------------
-- 2. Activity runs - grain: one row per activity_run_id
--    (each ForEach iteration is its own activity run)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.pipeline_activity_run_history (
    activity_run_id     STRING    NOT NULL COMMENT 'activityRunId',
    run_id              STRING    NOT NULL COMMENT 'pipelineRunId -> pipeline_run_history.run_id',
    workspace_id        STRING    NOT NULL,
    pipeline_id         STRING    NOT NULL,
    pipeline_name       STRING    NOT NULL,
    activity_name       STRING    NOT NULL,
    activity_type       STRING             COMMENT 'TridentNotebook | PBISemanticModelRefresh | Wait | MicrosoftTeams | Copy | ...',
    status              STRING    NOT NULL COMMENT 'Queued | InProgress | Succeeded | Failed | Skipped | Cancelled',
    start_time_utc      TIMESTAMP          COMMENT 'activityRunStart',
    end_time_utc        TIMESTAMP          COMMENT 'activityRunEnd',
    duration_ms         BIGINT             COMMENT 'durationInMs',
    retry_attempt       INT                COMMENT 'retryAttempt',
    iteration_hash      STRING             COMMENT 'iterationHash; set inside ForEach',
    error_code          STRING             COMMENT 'error.errorCode',
    error_message       STRING             COMMENT 'error.message (e.g. semantic model refresh detail)',
    error_failure_type  STRING             COMMENT 'error.failureType (UserError | SystemError | ...)',
    error_target        STRING             COMMENT 'error.target',
    input_json          STRING             COMMENT 'Activity input as JSON',
    output_json         STRING             COMMENT 'Activity output as JSON (notebook exit value, rows copied, ...)',
    ingested_at_utc     TIMESTAMP NOT NULL,
    updated_at_utc      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Fabric data pipeline activity run history - one row per activity run'
TBLPROPERTIES (
    'delta.parquet.vorder.enabled'   = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ---------------------------------------------------------------------
-- 3. Alert log - grain: one row per alerted run, so each run alerts once
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.pipeline_alert_log (
    run_id              STRING    NOT NULL COMMENT '-> pipeline_run_history.run_id',
    pipeline_name       STRING    NOT NULL,
    effective_status    STRING    NOT NULL COMMENT 'Status that triggered the alert (Failed | CompletedWithErrors | ...)',
    alerted_at_utc      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Runs already alerted on by load_pipeline_run_history'
TBLPROPERTIES (
    'delta.parquet.vorder.enabled'   = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ---------------------------------------------------------------------
-- 4. Run health. The run-level status can read Completed while a step
--    failed (steps chained on "Completed" don't fail the run), so the
--    effective status is derived from the activity rows.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW monitoring.vw_pipeline_run_health AS
WITH act AS (
    SELECT run_id,
           COUNT(*)                     AS activity_count,
           COUNT_IF(status = 'Failed')  AS failed_activity_count,
           NULLIF(array_join(array_sort(collect_set(
               CASE WHEN status = 'Failed' THEN activity_name END)), ', '), '') AS failed_activities
    FROM monitoring.pipeline_activity_run_history
    GROUP BY run_id
)
SELECT r.run_id,
       r.workspace_name,
       r.pipeline_name,
       r.invoke_type,
       r.status                                                         AS api_status,
       CASE
           WHEN r.status IN ('NotStarted', 'InProgress')  THEN 'Running'
           WHEN r.status IN ('Failed', 'Cancelled')       THEN r.status
           WHEN COALESCE(a.failed_activity_count, 0) > 0  THEN 'CompletedWithErrors'
           ELSE r.status
       END                                                              AS effective_status,
       from_utc_timestamp(r.start_time_utc, 'Asia/Singapore')           AS start_time_sgt,
       from_utc_timestamp(r.end_time_utc,   'Asia/Singapore')           AS end_time_sgt,
       CAST(from_utc_timestamp(r.start_time_utc, 'Asia/Singapore') AS DATE) AS run_date_sgt,
       ROUND(r.duration_ms / 60000.0, 2)                                AS duration_minutes,
       a.activity_count,
       a.failed_activity_count,
       a.failed_activities,
       r.failure_error_code,
       r.failure_message
FROM monitoring.pipeline_run_history r
LEFT JOIN act a ON a.run_id = r.run_id;
