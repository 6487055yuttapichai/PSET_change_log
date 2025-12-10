-- Table: reporting.agw_devices

-- DROP TABLE IF EXISTS reporting.agw_devices;

CREATE TABLE IF NOT EXISTS reporting.agw_devices
(
    area citext COLLATE pg_catalog."default" NOT NULL,
    workcenter citext COLLATE pg_catalog."default" NOT NULL,
    station citext COLLATE pg_catalog."default" NOT NULL,
    device citext COLLATE pg_catalog."default" NOT NULL,
    address text COLLATE pg_catalog."default",
    port smallint,
    area_id bigint NOT NULL,
    workcenter_id bigint NOT NULL,
    station_id bigint NOT NULL,
    device_id bigint NOT NULL,
    equipment_device_id bigint DEFAULT 0
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS reporting.agw_devices
    OWNER to postgres;