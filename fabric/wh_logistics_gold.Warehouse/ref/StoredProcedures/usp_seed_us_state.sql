CREATE   PROCEDURE ref.usp_seed_us_state
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE FROM ref.us_state;

        INSERT INTO ref.us_state (state_code, state_name)
        VALUES
            ('AL','Alabama'),('AK','Alaska'),('AZ','Arizona'),('AR','Arkansas'),
            ('CA','California'),('CO','Colorado'),('CT','Connecticut'),('DE','Delaware'),
            ('DC','District of Columbia'),('FL','Florida'),('GA','Georgia'),('HI','Hawaii'),
            ('ID','Idaho'),('IL','Illinois'),('IN','Indiana'),('IA','Iowa'),
            ('KS','Kansas'),('KY','Kentucky'),('LA','Louisiana'),('ME','Maine'),
            ('MD','Maryland'),('MA','Massachusetts'),('MI','Michigan'),('MN','Minnesota'),
            ('MS','Mississippi'),('MO','Missouri'),('MT','Montana'),('NE','Nebraska'),
            ('NV','Nevada'),('NH','New Hampshire'),('NJ','New Jersey'),('NM','New Mexico'),
            ('NY','New York'),('NC','North Carolina'),('ND','North Dakota'),('OH','Ohio'),
            ('OK','Oklahoma'),('OR','Oregon'),('PA','Pennsylvania'),('RI','Rhode Island'),
            ('SC','South Carolina'),('SD','South Dakota'),('TN','Tennessee'),('TX','Texas'),
            ('UT','Utah'),('VT','Vermont'),('VA','Virginia'),('WA','Washington'),
            ('WV','West Virginia'),('WI','Wisconsin'),('WY','Wyoming');

        DECLARE @row_count INT, @distinct_codes INT;

        SELECT @row_count      = COUNT(*),
               @distinct_codes = COUNT(DISTINCT s.state_code)
        FROM ref.us_state AS s;

        IF @row_count <> 51 OR @distinct_codes <> 51
            THROW 50001, 'ref.us_state seed check failed: expected 51 distinct state codes.', 1;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO