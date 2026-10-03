CREATE   PROCEDURE dim.usp_seed_special_members_derived
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @n INT;

    SELECT @n = COUNT(*) FROM dim.dim_load_type AS t WHERE t.load_type_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_load_type ON;
        INSERT INTO dim.dim_load_type (load_type_key, booking_type, load_type)
        VALUES (0, 'Missing', 'Missing'), (-1, 'Unknown', 'Unknown'), (-2, 'Not Applicable', 'Not Applicable');
        SET IDENTITY_INSERT dim.dim_load_type OFF;
        DBCC CHECKIDENT ('dim.dim_load_type', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50401, 'dim.usp_seed_special_members_derived: dim_load_type has a partial set of special members.', 1;

    SELECT @n = COUNT(*) FROM dim.dim_delivery_status AS t WHERE t.delivery_status_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_delivery_status ON;
        INSERT INTO dim.dim_delivery_status (delivery_status_key, event_type, arrival_status)
        VALUES (0, 'Missing', 'Missing'), (-1, 'Unknown', 'Unknown'), (-2, 'Not Applicable', 'Not Applicable');
        SET IDENTITY_INSERT dim.dim_delivery_status OFF;
        DBCC CHECKIDENT ('dim.dim_delivery_status', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50402, 'dim.usp_seed_special_members_derived: dim_delivery_status has a partial set of special members.', 1;

    SELECT @n = COUNT(*) FROM dim.dim_maintenance_class AS t WHERE t.maintenance_class_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_maintenance_class ON;
        INSERT INTO dim.dim_maintenance_class (maintenance_class_key, maintenance_type, service_urgency)
        VALUES (0, 'Missing', 'Missing'), (-1, 'Unknown', 'Unknown'), (-2, 'Not Applicable', 'Not Applicable');
        SET IDENTITY_INSERT dim.dim_maintenance_class OFF;
        DBCC CHECKIDENT ('dim.dim_maintenance_class', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50403, 'dim.usp_seed_special_members_derived: dim_maintenance_class has a partial set of special members.', 1;

    SELECT @n = COUNT(*) FROM dim.dim_incident_class AS t WHERE t.incident_class_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_incident_class ON;
        INSERT INTO dim.dim_incident_class
            (incident_class_key, incident_type, incident_category, incident_severity, incident_cause,
             preventable_status, at_fault_status, injury_status)
        VALUES
            ( 0, 'Missing',        'Missing',        'Missing',        'Missing',        'Missing',        'Missing',        'Missing'),
            (-1, 'Unknown',        'Unknown',        'Unknown',        'Unknown',        'Unknown',        'Unknown',        'Unknown'),
            (-2, 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable');
        SET IDENTITY_INSERT dim.dim_incident_class OFF;
        DBCC CHECKIDENT ('dim.dim_incident_class', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50404, 'dim.usp_seed_special_members_derived: dim_incident_class has a partial set of special members.', 1;

    SELECT @n = COUNT(*) FROM dim.dim_location AS t WHERE t.location_key IN (0, -1, -2);
    IF @n = 0
    BEGIN
        SET IDENTITY_INSERT dim.dim_location ON;
        INSERT INTO dim.dim_location
            (location_key, location_city, location_state_code, location_state_name, location_country, state_source)
        VALUES
            ( 0, 'Missing',        '0',  'Missing',        'Missing',        'Special member'),
            (-1, 'Unknown',        '-1', 'Unknown',        'Unknown',        'Special member'),
            (-2, 'Not Applicable', '-2', 'Not Applicable', 'Not Applicable', 'Special member');
        SET IDENTITY_INSERT dim.dim_location OFF;
        DBCC CHECKIDENT ('dim.dim_location', RESEED);
    END
    ELSE IF @n <> 3
        THROW 50405, 'dim.usp_seed_special_members_derived: dim_location has a partial set of special members.', 1;
END;

GO