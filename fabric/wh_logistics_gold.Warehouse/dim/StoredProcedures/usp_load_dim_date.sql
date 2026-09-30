CREATE   PROCEDURE dim.usp_load_dim_date
    @start_date             DATE,
    @end_date               DATE,
    @complete_through_date  DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @day_count INT, @row_count INT, @distinct_keys INT;

    IF @start_date IS NULL OR @end_date IS NULL OR @complete_through_date IS NULL
        THROW 50101, 'dim.usp_load_dim_date: all three parameters are required.', 1;

    -- A marked Power BI date table must span full years.
    IF MONTH(@start_date) <> 1 OR DAY(@start_date) <> 1
       OR MONTH(@end_date) <> 12 OR DAY(@end_date) <> 31
       OR @end_date < @start_date
        THROW 50102, 'dim.usp_load_dim_date: range must be whole calendar years, 1 Jan to 31 Dec.', 1;

    SET @day_count = DATEDIFF(DAY, @start_date, @end_date) + 1;

    IF @day_count > 10000
        THROW 50103, 'dim.usp_load_dim_date: range exceeds the 10,000-day generator.', 1;

    IF @complete_through_date < DATEADD(DAY, -1, @start_date) OR @complete_through_date > @end_date
        THROW 50104, 'dim.usp_load_dim_date: complete_through_date must lie within the range.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Keys are deterministic, so a full regeneration never changes a key facts depend on.
        DELETE FROM dim.dim_date;

        INSERT INTO dim.dim_date
            (date_key, full_date, calendar_year, quarter_number, quarter_label, month_number,
             month_name, year_month_number, year_month_label, day_of_month, is_complete_month)
        VALUES
            ( 0, NULL, NULL, NULL, 'Missing',        NULL, 'Missing',        NULL, 'Missing',        NULL, 0),
            (-1, NULL, NULL, NULL, 'Unknown',        NULL, 'Unknown',        NULL, 'Unknown',        NULL, 0),
            (-2, NULL, NULL, NULL, 'Not Applicable', NULL, 'Not Applicable', NULL, 'Not Applicable', NULL, 0);

        INSERT INTO dim.dim_date
            (date_key, full_date, calendar_year, quarter_number, quarter_label, month_number,
             month_name, year_month_number, year_month_label, day_of_month, is_complete_month)
        SELECT
            c.yr * 10000 + c.mo * 100 + c.dy,
            c.full_date,
            c.yr,
            c.qtr,
            'Q' + CAST(c.qtr AS VARCHAR(1)),
            c.mo,
            c.month_name,
            c.yr * 100 + c.mo,
            LEFT(c.month_name, 3) + ' ' + CAST(c.yr AS VARCHAR(4)),
            c.dy,
            CASE WHEN EOMONTH(c.full_date) <= @complete_through_date THEN 1 ELSE 0 END
        FROM
        (
            SELECT
                g.full_date,
                YEAR(g.full_date)               AS yr,
                DATEPART(QUARTER, g.full_date)  AS qtr,
                MONTH(g.full_date)              AS mo,
                DAY(g.full_date)                AS dy,
                CHOOSE(MONTH(g.full_date),
                       'January', 'February', 'March', 'April', 'May', 'June', 'July',
                       'August', 'September', 'October', 'November', 'December') AS month_name
            FROM
            (
                SELECT DATEADD(DAY, n.n, @start_date) AS full_date
                FROM
                (
                    SELECT d1.d + 10 * d2.d + 100 * d3.d + 1000 * d4.d AS n
                    FROM       (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d1(d)
                    CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d2(d)
                    CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d3(d)
                    CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d4(d)
                ) AS n
                WHERE n.n < @day_count
            ) AS g
        ) AS c;

        SELECT @row_count     = COUNT(*),
               @distinct_keys = COUNT(DISTINCT dd.date_key)
        FROM dim.dim_date AS dd;

        IF @row_count <> @day_count + 3 OR @distinct_keys <> @row_count
            THROW 50105, 'dim.usp_load_dim_date: row or key uniqueness check failed.', 1;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO