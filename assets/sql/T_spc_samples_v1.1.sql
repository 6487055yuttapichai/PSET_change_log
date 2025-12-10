-- Table: reporting.spc_samples

-- DROP TABLE IF EXISTS reporting.spc_samples;

CREATE TABLE IF NOT EXISTS reporting.spc_samples
(
    sample_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_values numeric[],
    created_at timestamp(3) without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS reporting.spc_samples
    OWNER to postgres;