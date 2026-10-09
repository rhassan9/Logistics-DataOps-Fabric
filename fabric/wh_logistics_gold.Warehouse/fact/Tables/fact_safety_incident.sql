CREATE TABLE [fact].[fact_safety_incident] (
    [incident_date_key]    INT             NOT NULL,
    [driver_key]           BIGINT          NOT NULL,
    [truck_key]            BIGINT          NOT NULL,
    [location_key]         BIGINT          NOT NULL,
    [incident_class_key]   BIGINT          NOT NULL,
    [incident_id]          VARCHAR (20)    NOT NULL,
    [trip_id]              VARCHAR (20)    NOT NULL,
    [vehicle_damage_cost]  DECIMAL (14, 2) NULL,
    [cargo_damage_cost]    DECIMAL (14, 2) NULL,
    [is_deleted_in_source] BIT             NOT NULL
);


GO