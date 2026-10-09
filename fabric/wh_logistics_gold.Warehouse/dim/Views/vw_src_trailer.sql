CREATE   VIEW dim.vw_src_trailer
AS
SELECT s.trailer_id,
       COALESCE(s.trailer_number, 'Not Recorded') AS trailer_number,
       COALESCE(s.trailer_type,   'Not Recorded') AS trailer_type,
       s.model_year                               AS trailer_model_year,
       s.acquisition_date                         AS trailer_acquisition_date,
       s._is_deleted_in_source                    AS is_deleted_in_source
FROM lh_logistics_silver.dbo.silver_trailers AS s;

GO