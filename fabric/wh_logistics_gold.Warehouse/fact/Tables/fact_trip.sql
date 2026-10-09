CREATE TABLE [fact].[fact_trip] (
    [dispatch_date_key]     INT             NOT NULL,
    [driver_key]            BIGINT          NOT NULL,
    [truck_key]             BIGINT          NOT NULL,
    [trailer_key]           BIGINT          NOT NULL,
    [route_key]             BIGINT          NOT NULL,
    [trip_id]               VARCHAR (20)    NOT NULL,
    [load_id]               VARCHAR (20)    NOT NULL,
    [actual_distance_miles] INT             NULL,
    [actual_duration_hours] DECIMAL (10, 2) NULL,
    [fuel_gallons_used]     DECIMAL (12, 2) NULL,
    [idle_time_hours]       DECIMAL (10, 2) NULL,
    [is_idle_implausible]   BIT             NOT NULL,
    [is_deleted_in_source]  BIT             NOT NULL
);


GO