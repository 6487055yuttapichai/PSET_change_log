INSERT INTO dbo.change_log
    (Controller_Id, Station, Model, PSET, JsonData)
VALUES
    (
        '001',
        'C1-06R Brake Line Union',
        'HUA',
        '001',
        jsonb_build_array(
        	jsonb_build_object(
        		'rev', 0,
        		'user', 'Z.lui',
        		'note', '',
        		'timestamp', CURRENT_TIMESTAMP
        		)
        	)
    );