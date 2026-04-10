-- 0007_alter_jobs_add_assigned_worker.sql

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS assigned_worker TEXT;

CREATE INDEX IF NOT EXISTS idx_jobs_job_type_status
    ON jobs (job_type, status);
