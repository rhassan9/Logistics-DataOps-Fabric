CREATE   PROCEDURE dim.usp_seed_special_members
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE @valid_from INT, @n INT;
    DECLARE @valid_to   INT = 99991231;
 
    SELECT @valid_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @valid_from IS NULL
        THROW 50201, 'dim.usp_seed_special_members: load dim.dim_date first.', 1;
 
    -- dim_customer
    SELECT @n = COUNT(*) FROM dim.dim_customer AS t WHERE t.customer_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_customer ON;
        INSERT INTO dim.dim_customer
            (customer_key, customer_id, customer_name, customer_type, primary_freight_type, account_status,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        VALUES
            ( 0, '0',  'Missing',        'Missing',        'Missing',        'Missing',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-1, '-1', 'Unknown',        'Unknown',        'Unknown',        'Unknown',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-2, '-2', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', @valid_from, @valid_to, 1, 0, 'Special member');
        SET IDENTITY_INSERT dim.dim_customer OFF;
        DBCC CHECKIDENT ('dim.dim_customer', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50202, 'dim.usp_seed_special_members: dim_customer has a partial set of special members.', 1;
 
    -- dim_driver
    SELECT @n = COUNT(*) FROM dim.dim_driver AS t WHERE t.driver_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_driver ON;
        INSERT INTO dim.dim_driver
            (driver_key, driver_id, driver_name, driver_home_terminal, employment_status, driver_version_label,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        VALUES
            ( 0, '0',  'Missing',        'Missing',        'Missing',        'Missing',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-1, '-1', 'Unknown',        'Unknown',        'Unknown',        'Unknown',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-2, '-2', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', @valid_from, @valid_to, 1, 0, 'Special member');
        SET IDENTITY_INSERT dim.dim_driver OFF;
        DBCC CHECKIDENT ('dim.dim_driver', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50203, 'dim.usp_seed_special_members: dim_driver has a partial set of special members.', 1;
 
    -- dim_truck
    SELECT @n = COUNT(*) FROM dim.dim_truck AS t WHERE t.truck_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_truck ON;
        INSERT INTO dim.dim_truck
            (truck_key, truck_id, unit_number, truck_make, truck_status, truck_home_terminal, truck_version_label,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        VALUES
            ( 0, '0',  'Missing',        'Missing',        'Missing',        'Missing',        'Missing',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-1, '-1', 'Unknown',        'Unknown',        'Unknown',        'Unknown',        'Unknown',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-2, '-2', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', @valid_from, @valid_to, 1, 0, 'Special member');
        SET IDENTITY_INSERT dim.dim_truck OFF;
        DBCC CHECKIDENT ('dim.dim_truck', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50204, 'dim.usp_seed_special_members: dim_truck has a partial set of special members.', 1;
 
    -- dim_trailer
    SELECT @n = COUNT(*) FROM dim.dim_trailer AS t WHERE t.trailer_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_trailer ON;
        INSERT INTO dim.dim_trailer
            (trailer_key, trailer_id, trailer_number, trailer_type,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        VALUES
            ( 0, '0',  'Missing',        'Missing',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-1, '-1', 'Unknown',        'Unknown',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-2, '-2', 'Not Applicable', 'Not Applicable', @valid_from, @valid_to, 1, 0, 'Special member');
        SET IDENTITY_INSERT dim.dim_trailer OFF;
        DBCC CHECKIDENT ('dim.dim_trailer', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50205, 'dim.usp_seed_special_members: dim_trailer has a partial set of special members.', 1;
 
    -- dim_route
    SELECT @n = COUNT(*) FROM dim.dim_route AS t WHERE t.route_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_route ON;
        INSERT INTO dim.dim_route
            (route_key, route_id, route_name, origin_city, origin_state_code, origin_state_name,
             destination_city, destination_state_code, destination_state_name,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        VALUES
            ( 0, '0',  'Missing',        'Missing',        '0',  'Missing',        'Missing',        '0',  'Missing',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-1, '-1', 'Unknown',        'Unknown',        '-1', 'Unknown',        'Unknown',        '-1', 'Unknown',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-2, '-2', 'Not Applicable', 'Not Applicable', '-2', 'Not Applicable', 'Not Applicable', '-2', 'Not Applicable', @valid_from, @valid_to, 1, 0, 'Special member');
        SET IDENTITY_INSERT dim.dim_route OFF;
        DBCC CHECKIDENT ('dim.dim_route', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50206, 'dim.usp_seed_special_members: dim_route has a partial set of special members.', 1;
 
    -- dim_facility
    SELECT @n = COUNT(*) FROM dim.dim_facility AS t WHERE t.facility_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_facility ON;
        INSERT INTO dim.dim_facility
            (facility_key, facility_id, facility_name, facility_type, facility_city, facility_state_code,
             facility_state_name, operating_hours,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        VALUES
            ( 0, '0',  'Missing',        'Missing',        'Missing',        '0',  'Missing',        'Missing',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-1, '-1', 'Unknown',        'Unknown',        'Unknown',        '-1', 'Unknown',        'Unknown',        @valid_from, @valid_to, 1, 0, 'Special member'),
            (-2, '-2', 'Not Applicable', 'Not Applicable', 'Not Applicable', '-2', 'Not Applicable', 'Not Applicable', @valid_from, @valid_to, 1, 0, 'Special member');
        SET IDENTITY_INSERT dim.dim_facility OFF;
        DBCC CHECKIDENT ('dim.dim_facility', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50207, 'dim.usp_seed_special_members: dim_facility has a partial set of special members.', 1;
END;

GO