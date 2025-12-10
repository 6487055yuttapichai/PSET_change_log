-- FUNCTION: dbo.Report_Device_Activity_v001(timestamp without time zone, timestamp without time zone)

-- DROP FUNCTION IF EXISTS dbo."Report_Device_Activity_v001"(timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION dbo."Report_Device_Activity_v001"(
	in_from_date timestamp without time zone,
	in_to_date timestamp without time zone)
    RETURNS TABLE(device text, manufacturer text, address text, port text, server_time timestamp without time zone, tool_time timestamp without time zone, status text, tighten_id numeric) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    RETURN QUERY SELECT DISTINCT
		t.device::TEXT,
		t.manufacturer::TEXT,
		t.address::TEXT,
		t.port::TEXT,
		(t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::TIMESTAMP AS "server_time",
		t.tool_time::TIMESTAMP,
		t.status::TEXT,
		t.tighten_id::NUMERIC
	FROM (
		SELECT 
			ed.name AS "device",
			ed.manufacturer,
			ed.address,
			ed.port,
			e."createdAt" AS "created_at",
			e."stringifiedData" -> 'payload' ->> 'tighteningStatus' AS "status",
			(e."stringifiedData" -> 'payload' ->> 'timeStamp') AS "tool_time",
			e."stringifiedData" -> 'payload' ->> 'tighteningID' AS "tighten_id"
		FROM dbo."Order_Task_EOR" AS e
		INNER JOIN dbo."Equipment_Device" AS ed ON e."deviceId" = ed."id" 
		WHERE (e."createdAt"::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' >= in_from_date AND e."createdAt"::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' < (in_to_date + INTERVAL '1 day'))
	) AS t;
END;
$BODY$;

ALTER FUNCTION dbo."Report_Device_Activity_v001"(timestamp without time zone, timestamp without time zone)
    OWNER TO postgres;
