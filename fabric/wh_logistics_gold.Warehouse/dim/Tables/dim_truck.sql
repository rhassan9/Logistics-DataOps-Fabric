CREATE TABLE [dim].[dim_truck] (
    [truck_key]              BIGINT        IDENTITY NOT NULL,
    [truck_id]               VARCHAR (20)  NOT NULL,
    [unit_number]            VARCHAR (20)  NOT NULL,
    [truck_make]             VARCHAR (60)  NOT NULL,
    [truck_status]           VARCHAR (60)  NOT NULL,
    [truck_home_terminal]    VARCHAR (60)  NOT NULL,
    [truck_model_year]       SMALLINT      NULL,
    [truck_acquisition_date] DATE          NULL,
    [acquisition_mileage]    INT           NULL,
    [tank_capacity_gallons]  INT           NULL,
    [truck_version_label]    VARCHAR (200) NOT NULL,
    [valid_from_date_key]    INT           NOT NULL,
    [valid_to_date_key]      INT           NOT NULL,
    [is_current]             BIT           NOT NULL,
    [is_deleted]             BIT           NOT NULL,
    [version_reason]         VARCHAR (100) NOT NULL
);


GO