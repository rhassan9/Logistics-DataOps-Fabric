CREATE TABLE [dim].[dim_route] (
    [route_key]              BIGINT         IDENTITY NOT NULL,
    [route_id]               VARCHAR (20)   NOT NULL,
    [route_name]             VARCHAR (120)  NOT NULL,
    [origin_city]            VARCHAR (60)   NOT NULL,
    [origin_state_code]      VARCHAR (2)    NOT NULL,
    [origin_state_name]      VARCHAR (40)   NOT NULL,
    [destination_city]       VARCHAR (60)   NOT NULL,
    [destination_state_code] VARCHAR (2)    NOT NULL,
    [destination_state_name] VARCHAR (40)   NOT NULL,
    [typical_distance_miles] INT            NULL,
    [base_rate_per_mile]     DECIMAL (8, 4) NULL,
    [fuel_surcharge_rate]    DECIMAL (8, 4) NULL,
    [typical_transit_days]   INT            NULL,
    [valid_from_date_key]    INT            NOT NULL,
    [valid_to_date_key]      INT            NOT NULL,
    [is_current]             BIT            NOT NULL,
    [is_deleted]             BIT            NOT NULL,
    [version_reason]         VARCHAR (100)  NOT NULL
);


GO