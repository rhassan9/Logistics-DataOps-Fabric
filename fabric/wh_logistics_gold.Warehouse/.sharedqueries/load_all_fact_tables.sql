EXEC fact.usp_load_fact_trip            @run_id = 'manual-20261010-05b', @allow_mass_delete = 0;
EXEC fact.usp_load_fact_delivery_event  @run_id = 'manual-20261010-05b', @allow_mass_delete = 0;
EXEC fact.usp_load_fact_fuel_purchase   @run_id = 'manual-20261010-05b', @allow_mass_delete = 0;
EXEC fact.usp_load_fact_maintenance     @run_id = 'manual-20261010-05b', @allow_mass_delete = 0;
EXEC fact.usp_load_fact_safety_incident @run_id = 'manual-20261010-05b', @allow_mass_delete = 0;