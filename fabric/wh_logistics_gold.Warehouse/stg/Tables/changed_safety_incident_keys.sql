CREATE TABLE [stg].[changed_safety_incident_keys] (
    [run_id]      VARCHAR (64)  NOT NULL,
    [incident_id] VARCHAR (20)  NOT NULL,
    [detected_at] DATETIME2 (6) NOT NULL
);


GO