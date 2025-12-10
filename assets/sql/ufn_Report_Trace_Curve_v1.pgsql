-- FUNCTION: dbo.Report_Trace_Curve(text[])

-- DROP FUNCTION IF EXISTS dbo."Report_Trace_Curve"(text[]);

CREATE OR REPLACE FUNCTION dbo."Report_Trace_Curve"(
	in_ids text[])
    RETURNS TABLE(device_id integer, tightening_id integer, mid text, trace_data text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    RETURN QUERY SELECT DISTINCT
		tc."deviceId"::INTEGER AS device_id,
		tc."tighteningId"::INTEGER AS tightening_id,
		tc.mid::TEXT,
		tc."stringifiedDataJSON"::TEXT AS trace_data
	FROM dbo."Order_EOR_TraceCurve" AS tc
	WHERE tc."deviceId" || '-' || tc."tighteningId" = ANY(in_ids);
END;
$BODY$;

ALTER FUNCTION dbo."Report_Trace_Curve"(text[])
    OWNER TO postgres;
