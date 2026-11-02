CREATE TABLE IF NOT EXISTS jobs (
    id            VARCHAR(36)    PRIMARY KEY,
    type          VARCHAR(100)   NOT NULL,
    payload       VARCHAR(100000) NOT NULL,
    status        VARCHAR(20)    NOT NULL,
    run_at        BIGINT         NOT NULL,
    attempts      INT            NOT NULL DEFAULT 0,
    max_attempts  INT            NOT NULL,
    cron          VARCHAR(100),
    run_count     BIGINT         NOT NULL DEFAULT 0,
    last_error    VARCHAR(2000),
    locked_by     VARCHAR(100),
    locked_until  BIGINT,
    created_at    BIGINT         NOT NULL,
    updated_at    BIGINT         NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs (status, run_at);
