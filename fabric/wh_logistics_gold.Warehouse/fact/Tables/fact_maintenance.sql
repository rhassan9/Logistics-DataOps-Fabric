CREATE TABLE [fact].[fact_maintenance] (
    [maintenance_date_key]  INT             NOT NULL,
    [truck_key]             BIGINT          NOT NULL,
    [location_key]          BIGINT          NOT NULL,
    [maintenance_class_key] BIGINT          NOT NULL,
    [maintenance_id]        VARCHAR (20)    NOT NULL,
    [labor_hours]           DECIMAL (8, 2)  NULL,
    [labor_cost]            DECIMAL (14, 2) NULL,
    [parts_cost]            DECIMAL (14, 2) NULL,
    [downtime_hours]        DECIMAL (10, 2) NULL,
    [odometer_reading]      INT             NULL,
    [is_deleted_in_source]  BIT             NOT NULL
);


GO