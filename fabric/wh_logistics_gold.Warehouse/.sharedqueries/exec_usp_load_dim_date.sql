EXEC dim.usp_load_dim_date @start_date = '2022-01-01', @end_date = '2025-12-31', @complete_through_date = '2024-12-31';
EXEC dim.usp_load_dim_date @start_date = '2022-01-01', @end_date = '2025-12-31', @complete_through_date = '2024-12-31';

SELECT COUNT(*)                                          AS total_rows,        -- 1464
       SUM(CASE WHEN d.date_key > 0 THEN 1 ELSE 0 END)   AS date_rows,         -- 1461
       MIN(d.full_date)                                  AS first_date,        -- 2022-01-01
       MAX(d.full_date)                                  AS last_date,         -- 2025-12-31
       COUNT(DISTINCT CASE WHEN d.is_complete_month = 1
                           THEN d.year_month_number END) AS complete_months    -- 36
FROM dim.dim_date AS d;

SELECT d.date_key, d.year_month_label, d.quarter_label, d.is_complete_month
FROM dim.dim_date AS d
WHERE d.date_key IN (-2, -1, 0, 20240229, 20241231, 20250101, 20250103)
ORDER BY d.date_key;