CREATE TABLE [fact].[fact_load_lifecycle] (
    [booked_date_key]           INT             NOT NULL,
    [dispatched_date_key]       INT             NOT NULL,
    [pickup_date_key]           INT             NOT NULL,
    [delivery_date_key]         INT             NOT NULL,
    [customer_key]              BIGINT          NOT NULL,
    [route_key]                 BIGINT          NOT NULL,
    [load_type_key]             BIGINT          NOT NULL,
    [fulfilling_driver_key]     BIGINT          NOT NULL,
    [fulfilling_truck_key]      BIGINT          NOT NULL,
    [fulfilling_trailer_key]    BIGINT          NOT NULL,
    [load_id]                   VARCHAR (20)    NOT NULL,
    [revenue]                   DECIMAL (14, 2) NULL,
    [fuel_surcharge]            DECIMAL (14, 2) NULL,
    [accessorial_charges]       DECIMAL (14, 2) NULL,
    [weight_lbs]                INT             NULL,
    [pieces]                    INT             NULL,
    [booking_to_dispatch_days]  INT             NULL,
    [dispatch_to_pickup_hours]  DECIMAL (10, 2) NULL,
    [pickup_to_delivery_hours]  DECIMAL (10, 2) NULL,
    [booking_to_delivery_hours] DECIMAL (10, 2) NULL,
    [is_timestamp_reversed]     BIT             NOT NULL
);


GO