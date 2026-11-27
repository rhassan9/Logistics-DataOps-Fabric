CREATE TABLE [log].[batch_manifest] (
    [batch_seq]           INT            NOT NULL,
    [batch_id]            VARCHAR (128)  NOT NULL,
    [batch_mode]          VARCHAR (20)   NULL,
    [business_date]       DATE           NULL,
    [as_of_date]          DATE           NULL,
    [previous_batch_id]   VARCHAR (128)  NULL,
    [supersedes_batch_id] VARCHAR (128)  NULL,
    [content_hash]        VARCHAR (64)   NULL,
    [is_valid]            BIT            NOT NULL,
    [problems]            VARCHAR (4000) NULL,
    [table_count]         INT            NULL,
    [row_count]           BIGINT         NULL,
    [producer]            VARCHAR (256)  NULL,
    [manifest_path]       VARCHAR (1024) NOT NULL,
    [landed_at]           DATETIME2 (6)  NOT NULL,
    [discovered_at]       DATETIME2 (6)  NOT NULL,
    [discovery_run_id]    VARCHAR (64)   NOT NULL
);


GO

