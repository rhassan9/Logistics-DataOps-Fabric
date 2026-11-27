CREATE TABLE [log].[batch_claim_lock] (
    [lock_name]  VARCHAR (32)  NOT NULL,
    [held_by]    VARCHAR (64)  NULL,
    [touched_at] DATETIME2 (6) NULL
);


GO
