CREATE TABLE [dim].[dim_trailer] (
    [trailer_key]              BIGINT        IDENTITY NOT NULL,
    [trailer_id]               VARCHAR (20)  NOT NULL,
    [trailer_number]           VARCHAR (20)  NOT NULL,
    [trailer_type]             VARCHAR (60)  NOT NULL,
    [trailer_model_year]       SMALLINT      NULL,
    [trailer_acquisition_date] DATE          NULL,
    [valid_from_date_key]      INT           NOT NULL,
    [valid_to_date_key]        INT           NOT NULL,
    [is_current]               BIT           NOT NULL,
    [is_deleted]               BIT           NOT NULL,
    [version_reason]           VARCHAR (100) NOT NULL
);


GO