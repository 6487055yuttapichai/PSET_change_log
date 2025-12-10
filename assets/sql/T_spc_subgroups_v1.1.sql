-- Table: reporting.spc_subgroups

-- DROP TABLE IF EXISTS reporting.spc_subgroups;

CREATE TABLE IF NOT EXISTS reporting.spc_subgroups
(
    subgroup_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_id BIGINT NOT NULL,
    device_id BIGINT NOT NULL,
    pset_id SMALLINT NOT NULL,
    sample_date DATE NOT NULL,
    start_time TIME WITHOUT TIME ZONE NOT NULL,
    sample_results JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT spc_subgroups_sample_id_fkey FOREIGN KEY (sample_id)
        REFERENCES reporting.spc_samples (sample_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS reporting.spc_subgroups
    OWNER to postgres;