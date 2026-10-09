CREATE TABLE [dim].[dim_driver] (
    [driver_key]              BIGINT        IDENTITY NOT NULL,
    [driver_id]               VARCHAR (20)  NOT NULL,
    [driver_name]             VARCHAR (120) NOT NULL,
    [driver_home_terminal]    VARCHAR (60)  NOT NULL,
    [employment_status]       VARCHAR (60)  NOT NULL,
    [driver_hire_date]        DATE          NULL,
    [driver_termination_date] DATE          NULL,
    [years_experience]        INT           NULL,
    [driver_version_label]    VARCHAR (200) NOT NULL,
    [valid_from_date_key]     INT           NOT NULL,
    [valid_to_date_key]       INT           NOT NULL,
    [is_current]              BIT           NOT NULL,
    [is_deleted]              BIT           NOT NULL,
    [version_reason]          VARCHAR (100) NOT NULL
);


GO