-- Table: reporting.capability_samples

-- DROP TABLE IF EXISTS reporting.capability_samples;

CREATE TABLE IF NOT EXISTS reporting.capability_samples
(
    sample_id bigint NOT NULL GENERATED ALWAYS IDENTITY PRIMARY KEY,
    sample_values numeric[],
    created_at timestamp(3) without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS reporting.capability_samples
    OWNER to postgres;