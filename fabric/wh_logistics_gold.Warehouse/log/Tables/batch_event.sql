CREATE TABLE [log].[batch_event] (
    [batch_seq]      INT            NOT NULL,
    [batch_id]       VARCHAR (128)  NOT NULL,
    [event_type]     VARCHAR (20)   NOT NULL,
    [event_at]       DATETIME2 (6)  NOT NULL,
    [run_id]         VARCHAR (64)   NULL,
    [actor]          VARCHAR (128)  NULL,
    [message]        VARCHAR (4000) NULL,
    [integrity_note] VARCHAR (4000) NULL,
    [rows_written]   BIGINT         NULL
);


GO
