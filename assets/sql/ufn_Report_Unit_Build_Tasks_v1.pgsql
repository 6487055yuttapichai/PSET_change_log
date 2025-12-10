-- FUNCTION: dbo.Report_Unit_Build_Tasks(character varying, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS dbo."Report_Unit_Build_Tasks"(character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION dbo."Report_Unit_Build_Tasks"(
	in_model character varying,
	in_station character varying,
	in_operator character varying,
	in_serial character varying)
    RETURNS TABLE(area text, station text, model text, serial_number text, operator text, order_start_time timestamp without time zone, order_end_time timestamp without time zone, order_timestamp timestamp without time zone, order_status text, task_timestamp timestamp without time zone, task_start_time timestamp without time zone, task_end_time timestamp without time zone, task_status text, trace_timestamp timestamp without time zone, torque_status text, device text, pset text, bolt_count numeric, cycle_count numeric, torque numeric, angle numeric, torque_final_target numeric, angle_final_target numeric, station_id integer, extra_data_json text, task_id integer, task_name text, sort_order numeric, task_desc text, is_optional boolean, task_type text, is_zone boolean, zone text, task_num numeric) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN

	/* ORDER TASKS */
    RETURN QUERY SELECT DISTINCT 
		st.area::TEXT
        , st.station::TEXT
        , mod.name::TEXT AS "model"
        , o."serialNumber"::TEXT AS "serial_number"
        , (usr.name || ' (' || CAST(usr.badge AS varchar) || ')')::TEXT as "operator"
        , t."startTime"::TIMESTAMP WITHOUT TIME ZONE  AS "order_start_time"
        , t."endTime"::TIMESTAMP WITHOUT TIME ZONE AS "order_end_time"
        , t."updatedAt"::TIMESTAMP WITHOUT TIME ZONE AS "order_timestamp"
        , (CASE WHEN (t."startTime" IS NOT NULL AND t."endTime" IS NOT NULL) THEN 'COMPLETED' ELSE 'OPEN' END)::TEXT "order_status"
        , tt."createdAt"::TIMESTAMP WITHOUT TIME ZONE AS "task_timestamp"
        , tt."startTime"::TIMESTAMP WITHOUT TIME ZONE AS "task_start_time"
        , tt."endTime"::TIMESTAMP WITHOUT TIME ZONE AS "task_end_time"
        , tts.name::TEXT as "task_status"
        , t."createdAt"::TIMESTAMP WITHOUT TIME ZONE AS "trace_timestamp"
        , te."isOK"::TEXT as "torque_status"
        , (te."stringifiedData" -> 'inputParameters' -> 'toolParameters' ->> 'name')::TEXT AS "device"
        , (te."stringifiedData" -> 'inputParameters' -> 'toolParameters' ->> 'pSet')::TEXT AS "pset"
        , (te."stringifiedData" -> 'inputParameters' -> 'toolParameters' ->> 'boltCount')::NUMERIC AS "bolt_count"
        , (te."stringifiedData" -> 'payload' ->> 'tighteningID')::NUMERIC AS "cycle_count"
        , (te."stringifiedData" -> 'payload' ->> 'torque')::NUMERIC AS "torque"
        , (te."stringifiedData" -> 'payload' ->> 'angle')::NUMERIC AS "angle"
        , (te."stringifiedData" -> 'payload' ->> 'torqueFinalTarget')::NUMERIC AS "torque_final_target"
        , (te."stringifiedData" -> 'payload' ->> 'finalAngleTarget')::NUMERIC AS "angle_final_target"
        , st.station_id::INTEGER
        , tt."extraDataJSON"::TEXT AS "extra_data_json"
        , tt."taskId"::INTEGER AS "task_id"
        , tbs.task_name::TEXT
        , tbs.sort_order::NUMERIC
        , tbs.task_desc::TEXT
        , tbs.is_optional::BOOLEAN
        , tbs.task_type::TEXT
        , tbs.is_zone::BOOLEAN
        , tbs.zone::TEXT
        , tbs.task_num::NUMERIC
    FROM dbo."Order_Order" AS o
    JOIN dbo."Order_Trace" AS t ON o.id = t."orderId"
    LEFT OUTER JOIN dbo."Order_Task_Trace" AS tt ON t.id = tt."orderTraceId"
    LEFT OUTER JOIN dbo."Order_Task_EOR" AS te ON tt.id = te."taskTraceId"
    LEFT OUTER JOIN dbo."Order_Task_TraceStatus" AS tts ON tt."taskTraceStatusId" = tts.id
    INNER JOIN dbo."Manufacturing_Model" AS mod ON o."modelId" = mod.id
    INNER JOIN dbo."Stations" st ON t."stationId" = st.station_id
    LEFT OUTER JOIN dbo."User_UserInfo" usr ON t."userInfoId" = usr.id
    LEFT OUTER JOIN dbo."TasksByStation" AS tbs ON (tt."taskId" = tbs.task_id AND st.station_id = tbs.station_id AND mod.name = tbs.model)
    WHERE (st.station LIKE in_station)
    AND (mod.name LIKE in_model)
	AND ((usr.name || ' (' || usr.badge::TEXT || ')') LIKE in_operator)
    AND (o."serialNumber" LIKE in_serial);

END;
$BODY$;

ALTER FUNCTION dbo."Report_Unit_Build_Tasks"(character varying, character varying, character varying, character varying)
    OWNER TO postgres;
