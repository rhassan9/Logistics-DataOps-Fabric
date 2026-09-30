CREATE TABLE [dim].[dim_date] (
    [date_key]          INT          NOT NULL,
    [full_date]         DATE         NULL,
    [calendar_year]     SMALLINT     NULL,
    [quarter_number]    SMALLINT     NULL,
    [quarter_label]     VARCHAR (14) NOT NULL,
    [month_number]      SMALLINT     NULL,
    [month_name]        VARCHAR (14) NOT NULL,
    [year_month_number] INT          NULL,
    [year_month_label]  VARCHAR (14) NOT NULL,
    [day_of_month]      SMALLINT     NULL,
    [is_complete_month] BIT          NOT NULL
);


GO