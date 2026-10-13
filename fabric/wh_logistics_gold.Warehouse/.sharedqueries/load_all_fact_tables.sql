DECLARE @run_id VARCHAR(64) = 'manual-20261011-L4C';
EXEC fact.usp_load_fact_trip            @run_id = @run_id, @allow_mass_delete = 0;
EXEC fact.usp_load_fact_delivery_event  @run_id = @run_id, @allow_mass_delete = 0;
EXEC fact.usp_load_fact_fuel_purchase   @run_id = @run_id, @allow_mass_delete = 0;
EXEC fact.usp_load_fact_maintenance     @run_id = @run_id, @allow_mass_delete = 0;
EXEC fact.usp_load_fact_safety_incident @run_id = @run_id, @allow_mass_delete = 0;
EXEC fact.usp_load_fact_load_lifecycle  @run_id = @run_id, @allow_mass_delete = 0;