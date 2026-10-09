CREATE TABLE [log].[etl_run] (
    [run_id]              VARCHAR (64)   NOT NULL,
    [step_name]           VARCHAR (128)  NOT NULL,
    [source_table]        VARCHAR (128)  NULL,
    [event_type]          VARCHAR (16)   NOT NULL,
    [event_at]            DATETIME2 (6)  NOT NULL,
    [rows_read]           BIGINT         NULL,
    [rows_inserted]       BIGINT         NULL,
    [rows_updated]        BIGINT         NULL,
    [rows_deleted]        BIGINT         NULL,
    [rows_unknown_member] BIGINT         NULL,
    [message]             VARCHAR (4000) NULL
);


GO