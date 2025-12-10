-- FUNCTION: dbo.Report_Torque_Data_CC(timestamp without time zone, timestamp without time zone, character varying, character varying)

-- DROP FUNCTION IF EXISTS dbo."Report_Torque_Data_CC"(timestamp without time zone, timestamp without time zone, character varying, character varying);

CREATE OR REPLACE FUNCTION dbo."Report_Torque_Data_CC"(
	in_from_date timestamp without time zone,
	in_to_date timestamp without time zone,
	in_serial_number character varying,
	in_device character varying)
    RETURNS TABLE(device text, manufacturer text, address text, port text, serial_number text, server_time timestamp without time zone, dc_tool_time timestamp without time zone, pset text, cycle_count numeric, status text, torque_status text, angle_status text, rundown_angle_status text, torque numeric, angle numeric, rundown_angle numeric, torque_min numeric, torque_target numeric, torque_max numeric, angle_min numeric, angle_target numeric, angle_max numeric, rundown_angle_min numeric, rundown_angle_max numeric, psetlastchangetime text, torque_uom text, trace_curve text, device_id integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
	
    IF in_serial_number = '' THEN
        in_serial_number := '%';
    END IF;

    IF in_device = '' THEN
        in_device := '%';
    END IF;

    RETURN QUERY SELECT DISTINCT
        t."device"::TEXT,
		t."manufacturer"::TEXT,
		t."address"::TEXT,
		t."port"::TEXT,
        t."serialNumber"::TEXT AS "serial_number",
		(t."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::TIMESTAMP AS "server_time",
        t."timeStamp"::TIMESTAMP AS "dc_tool_time",
        LPAD(t."toolPSet", 3, '0')::TEXT AS "pset",
        t."tighteningID"::NUMERIC AS "cycle_count",
        t."tighteningStatus"::TEXT AS "status",
        t."torqueStatus"::TEXT AS "torque_status",
        t."angleStatus"::TEXT AS "angle_status",
		t."rundownAngleStatus"::TEXT AS "rundown_angle_status",
        t."torque"::NUMERIC AS "torque",
        t."angle"::NUMERIC AS "angle",
		t."rundownAngle"::NUMERIC AS "rundown_angle",
        t."torqueMinLimit"::NUMERIC AS "torque_min",
        t."torqueFinalTarget"::NUMERIC AS "torque_target",
        t."torqueMaxLimit"::NUMERIC AS "torque_max",
        t."angleMin"::NUMERIC AS "angle_min",
        t."finalAngleTarget"::NUMERIC AS "angle_target",
        t."angleMax"::NUMERIC AS "angle_max",
		t."rundownAngleMin"::NUMERIC AS "rundown_angle_min",
		t."rundownAngleMax"::NUMERIC AS "rundown_angle_max",
		t."psetlastchangetime"::TEXT,
		t."torqueUnit"::TEXT AS "torque_uom",
		t."trace_curve"::TEXT,
		t."device_id"::INTEGER
	FROM (
		SELECT 
			de.device,
			de.manufacturer,
			de.address,
			de.port,
			e."stringifiedDataJSON" -> 'payload' ->> 'numberVIN' AS "serialNumber",
			e."createdAt",
			e."stringifiedDataJSON" -> 'payload' ->> 'timeStamp' AS "timeStamp",
			e."stringifiedDataJSON" -> 'payload' ->> 'parameterSetID' AS "toolPSet",
			e."stringifiedDataJSON" -> 'payload' ->> 'tighteningID' AS "tighteningID",
			e."stringifiedDataJSON" -> 'payload' ->> 'tighteningStatus' AS "tighteningStatus",
			e."stringifiedDataJSON" -> 'payload' ->> 'torqueStatus' AS "torqueStatus",
			e."stringifiedDataJSON" -> 'payload' ->> 'angleStatus' AS "angleStatus",
			e."stringifiedDataJSON" -> 'payload' ->> 'torque' AS "torque",
			e."stringifiedDataJSON" -> 'payload' ->> 'angle' AS "angle",
			e."stringifiedDataJSON" -> 'payload' ->> 'torqueMinLimit' AS "torqueMinLimit",
			e."stringifiedDataJSON" -> 'payload' ->> 'torqueFinalTarget' AS "torqueFinalTarget",
			e."stringifiedDataJSON" -> 'payload' ->> 'torqueMaxLimit' AS "torqueMaxLimit",
			e."stringifiedDataJSON" -> 'payload' ->> 'angleMin' AS "angleMin",
			e."stringifiedDataJSON" -> 'payload' ->> 'finalAngleTarget' AS "finalAngleTarget",
			e."stringifiedDataJSON" -> 'payload' ->> 'angleMax' AS "angleMax",
			e."stringifiedDataJSON" -> 'payload' ->> 'rundownAngleStatus' AS "rundownAngleStatus",
			e."stringifiedDataJSON" -> 'payload' ->> 'rundownAngle' AS "rundownAngle",
			e."stringifiedDataJSON" -> 'payload' ->> 'rundownAngleMin' AS "rundownAngleMin",
			e."stringifiedDataJSON" -> 'payload' ->> 'rundownAngleMax' AS "rundownAngleMax",
			e."stringifiedDataJSON" -> 'payload' ->> 'timeLastChange' AS "psetlastchangetime",
			e."stringifiedDataJSON" -> 'payload' ->> 'torqueValuesUnit' AS "torqueUnit",
			CASE 
				WHEN tc.id IS NOT NULL
					THEN 'Y'
				ELSE 'N'
			END AS "trace_curve",
			e."deviceId" AS "device_id"
		FROM dbo."Order_EOR" AS e
		LEFT OUTER JOIN dbo."Order_EOR_TraceCurve" AS tc ON (e."deviceId" = tc."deviceId" AND (e."stringifiedDataJSON" -> 'payload' ->> 'tighteningID')::INTEGER = tc."tighteningId")
		INNER JOIN (
			SELECT d.id AS "deviceId", d.name AS "device", a.manufacturer, a.model, d.address, d.port
			FROM dbo."Equipment_Device" AS d
			LEFT OUTER JOIN dev.device_attributes AS a ON (d.id = a."deviceId")
		) AS de ON e."deviceId" = de."deviceId" 
		WHERE (e."createdAt"::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' >= in_from_date AND e."createdAt"::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' < in_to_date)
-- 		AND e."stringifiedDataJSON" -> 'payload' ->> 'numberVIN' LIKE in_serial_number
-- 		AND de.name LIKE in_device
	) AS t
	ORDER BY "server_time";
END;
$BODY$;

ALTER FUNCTION dbo."Report_Torque_Data_CC"(timestamp without time zone, timestamp without time zone, character varying, character varying)
    OWNER TO postgres;
