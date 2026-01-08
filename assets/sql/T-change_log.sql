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