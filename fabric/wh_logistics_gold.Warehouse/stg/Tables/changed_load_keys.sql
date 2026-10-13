CREATE TABLE [stg].[changed_load_keys] (
    [run_id]      VARCHAR (64)  NOT NULL,
    [load_id]     VARCHAR (20)  NOT NULL,
    [detected_at] DATETIME2 (6) NOT NULL
);


GO