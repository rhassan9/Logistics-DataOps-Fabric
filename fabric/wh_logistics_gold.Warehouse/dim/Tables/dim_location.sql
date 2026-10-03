CREATE TABLE [dim].[dim_location] (
    [location_key]        BIGINT       IDENTITY NOT NULL,
    [location_city]       VARCHAR (60) NOT NULL,
    [location_state_code] VARCHAR (2)  NOT NULL,
    [location_state_name] VARCHAR (40) NOT NULL,
    [location_country]    VARCHAR (40) NOT NULL,
    [state_source]        VARCHAR (40) NOT NULL
);


GO