CREATE TABLE [fact].[fact_fuel_purchase] (
    [purchase_date_key]    INT             NOT NULL,
    [truck_key]            BIGINT          NOT NULL,
    [driver_key]           BIGINT          NOT NULL,
    [route_key]            BIGINT          NOT NULL,
    [location_key]         BIGINT          NOT NULL,
    [fuel_purchase_id]     VARCHAR (20)    NOT NULL,
    [trip_id]              VARCHAR (20)    NOT NULL,
    [gallons]              DECIMAL (10, 2) NULL,
    [total_cost]           DECIMAL (14, 2) NULL,
    [is_capacity_exceeded] BIT             NOT NULL,
    [is_deleted_in_source] BIT             NOT NULL
);


GO