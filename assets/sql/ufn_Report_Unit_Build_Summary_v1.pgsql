-- FUNCTION: dbo.Report_Unit_Build_Summary(timestamp without time zone, timestamp without time zone, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS dbo."Report_Unit_Build_Summary"(timestamp without time zone, timestamp without time zone, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION dbo."Report_Unit_Build_Summary"(
	in_from_date timestamp without time zone,
	in_to_date timestamp without time zone,
	in_station character varying,
	in_model character varying,
	in_serial character varying)
    RETURNS TABLE(area text, station text, model text, serial_number text, operator text, order_start_time timestamp without time zone, order_end_time timestamp without time zone, order_timestamp timestamp without time zone, order_status text, task_timestamp timestamp without time zone, task_start_time timestamp without time zone, task_end_time timestamp without time zone, task_status text, trace_timestamp timestamp without time zone, station_id integer, task_id integer, is_zone boolean, task_id_array jsonb) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN

	IF in_station = '' THEN
		in_station := '%';
	END IF;
	IF in_model = '' THEN
		in_model := '%';
	END IF;
	IF in_serial = '' THEN
		in_serial := '%';
	END IF;
    
    RETURN QUERY SELECT DISTINCT
        st.area::TEXT,
		st.station::TEXT,
        mod.name::TEXT AS "model",
		o."serialNumber"::TEXT AS "serial_number",
        (usr.name || ' (' || usr.badge::TEXT || ')')::TEXT AS "operator",
        ot."startTime"::TIMESTAMP AS "order_start_time",
        ot."endTime"::TIMESTAMP AS "order_end_time",
        ot."updatedAt"::TIMESTAMP AS "order_timestamp",
        CASE WHEN (ot."startTime" IS NOT NULL AND ot."endTime" IS NOT NULL) THEN 'COMPLETED' ELSE 'OPEN' END::TEXT AS "order_status",
        tt."createdAt"::TIMESTAMP AS "task_timestamp",
        tt."startTime"::TIMESTAMP AS "task_start_time",
        tt."endTime"::TIMESTAMP AS "task_end_time",
        tts.name::TEXT AS "task_status",
        ot."createdAt"::TIMESTAMP AS "trace_timestamp",
        st."station_id"::INTEGER,
        tt."taskId"::INTEGER AS "task_id",
		tc."is_zone",
		-- tc."total_tasks"::NUMERIC,
		ot."taskIdArrayJSON"::JSONB AS "task_id_array"
    FROM dbo."Order_Order" AS o
    INNER JOIN dbo."Order_Trace" AS ot ON (o.id = ot."orderId")
    LEFT OUTER JOIN dbo."Order_Task_Trace" AS tt ON (ot.id = tt."orderTraceId")
    LEFT OUTER JOIN dbo."Order_Task_TraceStatus" AS tts ON (tt."taskTraceStatusId" = tts.id)
    INNER JOIN dbo."Manufacturing_Model" AS mod ON (o."modelId" = mod.id)
	INNER JOIN dbo."Stations" AS st ON (ot."stationId" = st.station_id)
	LEFT OUTER JOIN dbo."User_UserInfo" AS usr ON (ot."userInfoId" = usr.id)
    LEFT OUTER JOIN (
        /* TOTAL TASK COUNT */
        SELECT DISTINCT t.area, t.station, t.model_id, t.station_id, t.area_id, t.is_zone, t.task_id
            , MAX(CASE WHEN t.task_num < 999 THEN t.task_num END) OVER (PARTITION BY t.station, t.model_id) AS "total_tasks"
        FROM (
            SELECT t.id AS "task_id", st."area", st."station", ts."modelId" AS "model_id"
				, ts."sortOrder" AS "sort_order", st."area_id", st."station_id", tt."isZone" AS "is_zone"
                , CASE 
                    WHEN ts."sortOrder" >= 0 THEN 
                        ROW_NUMBER() OVER (
                            PARTITION BY st."station", ts."modelId", CASE WHEN ts."sortOrder" >= 0 THEN 1 ELSE 0 END 
                            ORDER BY ts."sortOrder"
                        ) 
                    ELSE 999 
                END AS "task_num"
            FROM dbo."Task_Task" AS t 
            LEFT OUTER JOIN dbo."Task_Position2Station" AS ts ON t.id = ts."taskId"
            LEFT OUTER JOIN dbo."Task_Type" AS tt ON t."typeId" = tt.id
            LEFT OUTER JOIN dbo."Stations" AS st ON ts."stationId" = st."station_id"
            WHERE ts."sortOrder" IS NOT NULL
            -- AND st.area IN (SELECT area FROM @Areas)
        ) t
    ) AS tc ON (ot."stationId" = tc.station_id AND o."modelId" = tc.model_id AND tt."taskId" = tc.task_id)
    WHERE (ot."updatedAt" >= in_from_date AND ot."updatedAt" < in_to_date)
    AND st.station LIKE in_station
    AND mod.name LIKE in_model
    AND o."serialNumber" LIKE in_serial;

END;
$BODY$;

ALTER FUNCTION dbo."Report_Unit_Build_Summary"(timestamp without time zone, timestamp without time zone, character varying, character varying, character varying)
    OWNER TO postgres;
