CREATE TABLE [log].[reconciliation_result] (
    [run_id]             VARCHAR (64)    NOT NULL,
    [checked_at]         DATETIME2 (6)   NOT NULL,
    [baseline_name]      VARCHAR (64)    NOT NULL,
    [metric_name]        VARCHAR (64)    NOT NULL,
    [is_verified]        BIT             NOT NULL,
    [rows_compared]      INT             NULL,
    [rows_matched]       INT             NULL,
    [baseline_only]      INT             NULL,
    [gold_only]          INT             NULL,
    [max_abs_difference] DECIMAL (18, 4) NULL,
    [status]             VARCHAR (20)    NOT NULL
);


GO