CREATE TABLE [dim].[dim_calendar] (
    [date_key]          INT          NOT NULL,
    [full_date]         DATE         NOT NULL,
    [calendar_year]     SMALLINT     NOT NULL,
    [quarter_number]    SMALLINT     NOT NULL,
    [quarter_label]     VARCHAR (14) NOT NULL,
    [month_number]      SMALLINT     NOT NULL,
    [month_name]        VARCHAR (14) NOT NULL,
    [year_month_number] INT          NOT NULL,
    [year_month_label]  VARCHAR (14) NOT NULL,
    [day_of_month]      SMALLINT     NOT NULL,
    [is_complete_month] BIT          NOT NULL
);


GO