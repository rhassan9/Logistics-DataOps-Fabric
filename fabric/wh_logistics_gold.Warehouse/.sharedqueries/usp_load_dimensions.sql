CREATE OR ALTER PROCEDURE dim.usp_load_dimensions
    @run_id            VARCHAR(64),
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @step VARCHAR(128) = 'load_dimensions', @current VARCHAR(128), @msg VARCHAR(4000);
 
    IF @run_id IS NULL OR @allow_mass_delete IS NULL
        THROW 50391, 'dim.usp_load_dimensions: @run_id and @allow_mass_delete are required.', 1;
 
    EXEC log.usp_write_run_event @run_id, @step, NULL, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;
 
    BEGIN TRY
        SET @current = 'dim_customer';
        EXEC dim.usp_load_dim_customer @allow_mass_delete = @allow_mass_delete;
        SET @current = 'dim_trailer';
        EXEC dim.usp_load_dim_trailer  @allow_mass_delete = @allow_mass_delete;
        SET @current = 'dim_route';
        EXEC dim.usp_load_dim_route    @allow_mass_delete = @allow_mass_delete;
        SET @current = 'dim_facility';
        EXEC dim.usp_load_dim_facility @allow_mass_delete = @allow_mass_delete;
        SET @current = 'dim_driver';
        EXEC dim.usp_load_dim_driver   @allow_mass_delete = @allow_mass_delete;
        SET @current = 'dim_truck';
        EXEC dim.usp_load_dim_truck    @allow_mass_delete = @allow_mass_delete;
 
        SET @current = 'special_members_derived';
        EXEC dim.usp_seed_special_members_derived;
        SET @current = 'dim_load_type';
        EXEC dim.usp_load_dim_load_type;
        SET @current = 'dim_delivery_status';
        EXEC dim.usp_load_dim_delivery_status;
        SET @current = 'dim_maintenance_class';
        EXEC dim.usp_load_dim_maintenance_class;
        SET @current = 'dim_incident_class';
        EXEC dim.usp_load_dim_incident_class;
        SET @current = 'dim_location';
        EXEC dim.usp_load_dim_location;
 
        EXEC log.usp_write_run_event @run_id, @step, NULL, 'succeeded',
             NULL, NULL, NULL, NULL, NULL, 'six reference dimensions, four junk dimensions and dim_location loaded';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @msg = LEFT(@current + ': ' + ERROR_MESSAGE(), 4000);
        EXEC log.usp_write_run_event @run_id, @step, NULL, 'failed',
             NULL, NULL, NULL, NULL, NULL, @msg;
        THROW;
    END CATCH;
END;