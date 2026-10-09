CREATE TABLE [log].[cdf_batch] (
    [run_id]       VARCHAR (64)  NOT NULL,
    [step_name]    VARCHAR (128) NOT NULL,
    [source_table] VARCHAR (128) NOT NULL,
    [version_from] BIGINT        NULL,
    [version_to]   BIGINT        NOT NULL,
    [is_full_scan] BIT           NOT NULL,
    [detected_at]  DATETIME2 (6) NOT NULL
);


GO