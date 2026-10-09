CREATE   PROCEDURE dim.usp_load_dim_customer
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT, @nulls INT, @flagged INT, @active INT;
 
    IF @allow_mass_delete IS NULL
        THROW 50303, 'dim.usp_load_dim_customer: @allow_mass_delete is required.', 1;
 
    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50301, 'dim.usp_load_dim_customer: load dim.dim_date first.', 1;
 
    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.customer_id) FROM dim.vw_src_customer AS v;
    IF @dupes <> 0
        THROW 50302, 'dim.usp_load_dim_customer: duplicate customer_id in silver.', 1;
 
    SELECT @nulls = COUNT(*) FROM dim.vw_src_customer AS v WHERE v.is_deleted_in_source IS NULL;
    IF @nulls <> 0
        THROW 50306, 'dim.usp_load_dim_customer: silver rows without _is_deleted_in_source.', 1;
 
    -- Deletion state of every existing member: flagged in silver, or absent from it.
    DROP TABLE IF EXISTS #customer_status;
    CREATE TABLE #customer_status (customer_id VARCHAR(20) NOT NULL, target_deleted BIT NOT NULL)
        WITH (DISTRIBUTION = ROUND_ROBIN);
 
    INSERT INTO #customer_status (customer_id, target_deleted)
    SELECT m.customer_id,
           CASE WHEN v.customer_id IS NULL OR v.is_deleted_in_source = 1 THEN 1 ELSE 0 END
    FROM (SELECT DISTINCT d.customer_id FROM dim.dim_customer AS d WHERE d.customer_key > 0) AS m
    LEFT JOIN dim.vw_src_customer AS v ON v.customer_id = m.customer_id;
 
    SELECT @flagged = COUNT(*)
    FROM #customer_status AS s
    JOIN dim.dim_customer AS t ON t.customer_id = s.customer_id AND t.is_current = 1
    WHERE s.target_deleted = 1 AND t.is_deleted = 0;
 
    SELECT @active = COUNT(*) FROM dim.dim_customer AS t
    WHERE t.customer_key > 0 AND t.is_current = 1 AND t.is_deleted = 0;
 
    IF @allow_mass_delete = 0 AND @flagged > 0 AND @flagged * 10 > @active
        THROW 50305, 'dim.usp_load_dim_customer: more than 10 percent of active customers would be marked deleted; investigate silver, then rerun with @allow_mass_delete = 1 if genuine.', 1;
 
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
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.customer_id, v.customer_name, v.customer_type, v.primary_freight_type, v.account_status,
               v.credit_terms_days, v.contract_start_date, v.annual_revenue_potential,
               @initial_from, 99991231, 1, v.is_deleted_in_source, 'Initial version'
        FROM dim.vw_src_customer AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_customer AS t WHERE t.customer_id = v.customer_id);
 
        UPDATE t
        SET is_deleted = s.target_deleted
        FROM dim.dim_customer AS t
        JOIN #customer_status AS s ON s.customer_id = t.customer_id
        WHERE t.customer_key > 0 AND t.is_deleted <> s.target_deleted;
 
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
 
    DROP TABLE IF EXISTS #customer_status;
END;

GO