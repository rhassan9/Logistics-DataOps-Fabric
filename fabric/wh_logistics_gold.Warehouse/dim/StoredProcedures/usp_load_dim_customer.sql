CREATE   PROCEDURE dim.usp_load_dim_customer
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT;

    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50301, 'dim.usp_load_dim_customer: load dim.dim_date first.', 1;

    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.customer_id) FROM dim.vw_src_customer AS v;
    IF @dupes <> 0
        THROW 50302, 'dim.usp_load_dim_customer: duplicate customer_id in silver.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE t
        SET customer_name            = v.customer_name,
            customer_type            = v.customer_type,
            primary_freight_type     = v.primary_freight_type,
            account_status           = v.account_status,
            credit_terms_days        = v.credit_terms_days,
            contract_start_date      = v.contract_start_date,
            annual_revenue_potential = v.annual_revenue_potential
        FROM dim.dim_customer AS t
        JOIN dim.vw_src_customer AS v ON v.customer_id = t.customer_id
        WHERE t.customer_key > 0
          AND EXISTS (SELECT t.customer_name, t.customer_type, t.primary_freight_type, t.account_status,
                             t.credit_terms_days, t.contract_start_date, t.annual_revenue_potential
                      EXCEPT
                      SELECT v.customer_name, v.customer_type, v.primary_freight_type, v.account_status,
                             v.credit_terms_days, v.contract_start_date, v.annual_revenue_potential);

        INSERT INTO dim.dim_customer
            (customer_id, customer_name, customer_type, primary_freight_type, account_status,
             credit_terms_days, contract_start_date, annual_revenue_potential,
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.customer_id, v.customer_name, v.customer_type, v.primary_freight_type, v.account_status,
               v.credit_terms_days, v.contract_start_date, v.annual_revenue_potential,
               @initial_from, 99991231, 1, 'Initial version'
        FROM dim.vw_src_customer AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_customer AS t WHERE t.customer_id = v.customer_id);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO