INSERT INTO dbo.change_log
    (Controller_Id, Station, PSET, JsonData)
VALUES
    (
        'CTRL-00000001',
        'C1-06R Brake Line Union',
        '001',
        jsonb_build_array(
        	jsonb_build_object(
        		'rev', 0,
        		'user', '',
        		'note', '',
        		'timestamp', CURRENT_TIMESTAMP
        		)
        	)
    );