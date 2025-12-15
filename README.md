<!-- Use {Ctrl + Shift + V} for Preview Mode-->
dev in python 3.13.9

## list of contents
- [install requirements](#requirements)
- [change_log table](#change_log)
- [psets_models table](#psets_models)

## requirements
This command is used to **install** all required libraries from the requirements file:
```cmd
pip install -r requirements.txt
```
This command is used to **generate (or get)** the list of currently installed libraries and save them into a requirements file:
```
pip freeze >> requirements.txt
```


## change_log
### Command to create a change_log table Create

```sql
CREATE TABLE dbo.change_log (
    Id serial4 NOT NULL,
    Controller_Id varchar(50) NOT NULL,
    Station varchar(255),
    Model varchar(100),
    PSET varchar(20),
    JsonData JSONB,
    CreatedAt timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT change_log_Item_ID_key UNIQUE (Controller_Id),
    CONSTRAINT change_log_pkey PRIMARY KEY (Id)
);
```

### Example of a command to insert data in to change_log

```sql
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
```


## psets_models
### Command to create a change_log table Create

```sql
CREATE TABLE dbo.tool_psets_models (
    Id serial4 NOT NULL,
    Station varchar(255) NOT NULL,
    Tool varchar(255),
    PSET text,
    ModelOfVehicle varchar(50)
);
```

### Restore command 
This command uses psql to connect to a PostgreSQL server and restore backup file
```
psql -h localhost -p 5454 -U postgres -d portal -f _Order_EOR__202511130254.sql
```

### Option Descriptions

| Option | Description | Example |
|--------|-------------|---------|
| **-h** | Host of the PostgreSQL server | `-h localhost` |
| **-p** | Port where PostgreSQL is running | `-p 5454` |
| **-U** | Username used to connect | `-U postgres` |
| **-d** | Target database to restore into | `-d portal` |
| **-f** | SQL file to be executed (input file) | `-f _Order_EOR__202511130254.sql` |
