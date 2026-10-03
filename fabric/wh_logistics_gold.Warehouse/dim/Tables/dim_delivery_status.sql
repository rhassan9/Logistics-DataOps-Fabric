CREATE TABLE [dim].[dim_delivery_status] (
    [delivery_status_key] BIGINT       IDENTITY NOT NULL,
    [event_type]          VARCHAR (60) NOT NULL,
    [arrival_status]      VARCHAR (20) NOT NULL
);


GO