-- Table: reporting.capability_subgroups

-- DROP TABLE IF EXISTS reporting.capability_subgroups;

CREATE TABLE IF NOT EXISTS reporting.capability_subgroups
(
    subgroup_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_id BIGINT NOT NULL,
    device_id BIGINT NOT NULL,
    pset_id SMALLINT,
    sample_date DATE NOT NULL,
    start_time TIME WITHOUT TIME ZONE NOT NULL,
    sample_results JSONB NOT NULL DEFAULT '{}'::jsonb,
    type_id BIGINT,
    created_at TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT capability_subgroups_pset_id_fkey FOREIGN KEY (pset_id)
        REFERENCES reporting.psets (pset_id) 
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT capability_subgroups_sample_id_fkey FOREIGN KEY (sample_id)
        REFERENCES reporting.capability_samples (sample_id) 
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS reporting.capability_subgroups
    OWNER to postgres;