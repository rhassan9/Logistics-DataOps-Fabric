CREATE TABLE [dim].[dim_load_type] (
    [load_type_key] BIGINT       IDENTITY NOT NULL,
    [booking_type]  VARCHAR (60) NOT NULL,
    [load_type]     VARCHAR (60) NOT NULL
);


GO