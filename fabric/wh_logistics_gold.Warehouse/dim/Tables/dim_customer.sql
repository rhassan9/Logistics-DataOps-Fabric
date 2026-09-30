CREATE TABLE [dim].[dim_customer] (
    [customer_key]             BIGINT          IDENTITY NOT NULL,
    [customer_id]              VARCHAR (20)    NOT NULL,
    [customer_name]            VARCHAR (120)   NOT NULL,
    [customer_type]            VARCHAR (60)    NOT NULL,
    [primary_freight_type]     VARCHAR (60)    NOT NULL,
    [account_status]           VARCHAR (60)    NOT NULL,
    [credit_terms_days]        INT             NULL,
    [contract_start_date]      DATE            NULL,
    [annual_revenue_potential] DECIMAL (14, 2) NULL,
    [valid_from_date_key]      INT             NOT NULL,
    [valid_to_date_key]        INT             NOT NULL,
    [is_current]               BIT             NOT NULL,
    [version_reason]           VARCHAR (100)   NOT NULL
);


GO