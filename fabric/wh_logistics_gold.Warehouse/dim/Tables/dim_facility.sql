CREATE TABLE [dim].[dim_facility] (
    [facility_key]           BIGINT          IDENTITY NOT NULL,
    [facility_id]            VARCHAR (20)    NOT NULL,
    [facility_name]          VARCHAR (120)   NOT NULL,
    [facility_type]          VARCHAR (60)    NOT NULL,
    [facility_city]          VARCHAR (60)    NOT NULL,
    [facility_state_code]    VARCHAR (2)     NOT NULL,
    [facility_state_name]    VARCHAR (40)    NOT NULL,
    [operating_hours]        VARCHAR (60)    NOT NULL,
    [dock_doors]             INT             NULL,
    [facility_latitude]      DECIMAL (10, 6) NULL,
    [facility_longitude]     DECIMAL (10, 6) NULL,
    [is_centroid_coordinate] BIT             NULL,
    [valid_from_date_key]    INT             NOT NULL,
    [valid_to_date_key]      INT             NOT NULL,
    [is_current]             BIT             NOT NULL,
    [is_deleted]             BIT             NOT NULL,
    [version_reason]         VARCHAR (100)   NOT NULL
);


GO