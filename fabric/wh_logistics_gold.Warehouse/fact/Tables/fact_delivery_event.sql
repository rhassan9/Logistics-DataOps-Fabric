CREATE TABLE [fact].[fact_delivery_event] (
    [scheduled_date_key]         INT             NOT NULL,
    [actual_date_key]            INT             NOT NULL,
    [facility_key]               BIGINT          NOT NULL,
    [route_key]                  BIGINT          NOT NULL,
    [delivery_status_key]        BIGINT          NOT NULL,
    [event_id]                   VARCHAR (20)    NOT NULL,
    [load_id]                    VARCHAR (20)    NOT NULL,
    [trip_id]                    VARCHAR (20)    NOT NULL,
    [scheduled_datetime]         DATETIME2 (6)   NULL,
    [actual_datetime]            DATETIME2 (6)   NULL,
    [detention_minutes]          INT             NULL,
    [billable_detention_minutes] INT             NULL,
    [arrival_variance_minutes]   DECIMAL (10, 1) NULL,
    [is_deleted_in_source]       BIT             NOT NULL
);


GO