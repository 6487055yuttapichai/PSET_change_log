-- Table: reporting.psets

-- DROP TABLE IF EXISTS reporting.psets;

CREATE TABLE IF NOT EXISTS reporting.psets
(
    pset_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pset CITEXT COLLATE pg_catalog."default" NOT NULL,
    device_id BIGINT,
    target NUMERIC(8,2) NOT NULL DEFAULT 0,
    usl NUMERIC(8,2) NOT NULL DEFAULT 0,
    lsl NUMERIC(8,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS reporting.psets
    OWNER to postgres;