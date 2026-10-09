CREATE   VIEW dim.vw_src_customer
AS
SELECT s.customer_id,
       COALESCE(s.customer_name,        'Not Recorded') AS customer_name,
       COALESCE(s.customer_type,        'Not Recorded') AS customer_type,
       COALESCE(s.primary_freight_type, 'Not Recorded') AS primary_freight_type,
       COALESCE(s.account_status,       'Not Recorded') AS account_status,
       s.credit_terms_days,
       s.contract_start_date,
       s.annual_revenue_potential,
       s._is_deleted_in_source                          AS is_deleted_in_source
FROM lh_logistics_silver.dbo.silver_customers AS s;

GO