CREATE TABLE [dim].[dim_maintenance_class] (
    [maintenance_class_key] BIGINT       IDENTITY NOT NULL,
    [maintenance_type]      VARCHAR (60) NOT NULL,
    [service_urgency]       VARCHAR (60) NOT NULL
);


GO