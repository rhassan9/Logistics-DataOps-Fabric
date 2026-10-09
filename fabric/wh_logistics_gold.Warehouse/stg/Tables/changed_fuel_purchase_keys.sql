CREATE TABLE [stg].[changed_fuel_purchase_keys] (
    [run_id]           VARCHAR (64)  NOT NULL,
    [fuel_purchase_id] VARCHAR (20)  NOT NULL,
    [detected_at]      DATETIME2 (6) NOT NULL
);


GO