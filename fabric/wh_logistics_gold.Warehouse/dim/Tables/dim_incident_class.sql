CREATE TABLE [dim].[dim_incident_class] (
    [incident_class_key] BIGINT       IDENTITY NOT NULL,
    [incident_type]      VARCHAR (60) NOT NULL,
    [incident_category]  VARCHAR (60) NOT NULL,
    [incident_severity]  VARCHAR (60) NOT NULL,
    [incident_cause]     VARCHAR (60) NOT NULL,
    [preventable_status] VARCHAR (20) NOT NULL,
    [at_fault_status]    VARCHAR (20) NOT NULL,
    [injury_status]      VARCHAR (20) NOT NULL
);


GO