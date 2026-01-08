CREATE TABLE dbo.tool_psets_models (
    Id serial4 NOT NULL,
    Station varchar(255) NOT NULL,
    Tool varchar(255),
    PSET text,
    ModelOfVehicle varchar(50)
);