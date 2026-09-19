[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_customers_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "customer_id", DestinationColumnName = "customer_id"], [SourceColumnName = "customer_name", DestinationColumnName = "customer_name"], [SourceColumnName = "customer_type", DestinationColumnName = "customer_type"], [SourceColumnName = "credit_terms_days", DestinationColumnName = "credit_terms_days"], [SourceColumnName = "primary_freight_type", DestinationColumnName = "primary_freight_type"], [SourceColumnName = "account_status", DestinationColumnName = "account_status"], [SourceColumnName = "contract_start_date", DestinationColumnName = "contract_start_date"], [SourceColumnName = "annual_revenue_potential", DestinationColumnName = "annual_revenue_potential"], [SourceColumnName = "_source_ingest_date", DestinationColumnName = "_source_ingest_date"], [SourceColumnName = "_silver_created_at", DestinationColumnName = "_silver_created_at"], [SourceColumnName = "_silver_updated_at", DestinationColumnName = "_silver_updated_at"], [SourceColumnName = "_silver_run_id", DestinationColumnName = "_silver_run_id"], [SourceColumnName = "_dq_status", DestinationColumnName = "_dq_status"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared silver_customers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a77071e4-bd2a-4979-8910-91ddb8cd2a09"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "344f3ccc-7a2a-43a7-b62d-572ac6168344"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_customers", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"_ingested_at", "_source_system", "_load_mode"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"_ingest_date", "_source_ingest_date"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns", "_silver_created_at", each DateTimeZone.UtcNow()), {{"_silver_created_at", type datetime}}),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "_silver_updated_at", each DateTimeZone.UtcNow()),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 1", {{"_silver_updated_at", type datetime}}),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "_silver_run_id", each "df_gen2_" & DateTime.ToText(DateTime.LocalNow(), "yyyyMMdd_HHmmss")), {{"_silver_run_id", type text}}),
  #"Added custom 3" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 2", "_dq_status", each "valid"), {{"_dq_status", type text}})
in
  #"Added custom 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_drivers_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "driver_id", DestinationColumnName = "driver_id"], [SourceColumnName = "first_name", DestinationColumnName = "first_name"], [SourceColumnName = "last_name", DestinationColumnName = "last_name"], [SourceColumnName = "hire_date", DestinationColumnName = "hire_date"], [SourceColumnName = "termination_date", DestinationColumnName = "termination_date"], [SourceColumnName = "license_number", DestinationColumnName = "license_number"], [SourceColumnName = "license_state", DestinationColumnName = "license_state"], [SourceColumnName = "date_of_birth", DestinationColumnName = "date_of_birth"], [SourceColumnName = "home_terminal", DestinationColumnName = "home_terminal"], [SourceColumnName = "employment_status", DestinationColumnName = "employment_status"], [SourceColumnName = "cdl_class", DestinationColumnName = "cdl_class"], [SourceColumnName = "years_experience", DestinationColumnName = "years_experience"], [SourceColumnName = "tenure_days", DestinationColumnName = "tenure_days"], [SourceColumnName = "age_at_hire", DestinationColumnName = "age_at_hire"], [SourceColumnName = "_source_ingest_date", DestinationColumnName = "_source_ingest_date"], [SourceColumnName = "_silver_created_at", DestinationColumnName = "_silver_created_at"], [SourceColumnName = "_silver_updated_at", DestinationColumnName = "_silver_updated_at"], [SourceColumnName = "_silver_run_id", DestinationColumnName = "_silver_run_id"], [SourceColumnName = "_dq_status", DestinationColumnName = "_dq_status"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared silver_drivers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a77071e4-bd2a-4979-8910-91ddb8cd2a09"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "344f3ccc-7a2a-43a7-b62d-572ac6168344"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_drivers", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"_ingested_at", "_source_system", "_load_mode"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"_ingest_date", "_source_ingest_date"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns", "_silver_created_at", each DateTimeZone.UtcNow()), {{"_silver_created_at", type datetime}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom", "_silver_updated_at", each DateTimeZone.UtcNow()), {{"_silver_updated_at", type datetime}}),
  #"Added custom 4" = Table.AddColumn(#"Added custom 1", "tenure_days", each Duration.Days(
    Date.From([termination_date] ?? as_of_date) - Date.From([hire_date])
)),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 4", {{"tenure_days", Int64.Type}}),
  #"Added custom 5" = Table.AddColumn(#"Changed column type", "age_at_hire", each Date.Year([hire_date]) - Date.Year([date_of_birth])),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 5", "_silver_run_id", each "df_gen2_" & DateTime.ToText(DateTime.LocalNow(), "yyyyMMdd_HHmmss")), {{"_silver_run_id", type text}}),
  #"Added custom 3" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 2", "_dq_status", each if [termination_date] <> null and [termination_date] < [hire_date]
then "flagged" else "valid"), {{"_dq_status", type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 3", {{"age_at_hire", Int64.Type}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_facilities_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "facility_id", DestinationColumnName = "facility_id"], [SourceColumnName = "facility_name", DestinationColumnName = "facility_name"], [SourceColumnName = "facility_type", DestinationColumnName = "facility_type"], [SourceColumnName = "city", DestinationColumnName = "city"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "dock_doors", DestinationColumnName = "dock_doors"], [SourceColumnName = "operating_hours", DestinationColumnName = "operating_hours"], [SourceColumnName = "is_centroid_coordinate", DestinationColumnName = "is_centroid_coordinate"], [SourceColumnName = "_source_ingest_date", DestinationColumnName = "_source_ingest_date"], [SourceColumnName = "_silver_created_at", DestinationColumnName = "_silver_created_at"], [SourceColumnName = "_silver_updated_at", DestinationColumnName = "_silver_updated_at"], [SourceColumnName = "_silver_run_id", DestinationColumnName = "_silver_run_id"], [SourceColumnName = "_dq_status", DestinationColumnName = "_dq_status"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared silver_facilities = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a77071e4-bd2a-4979-8910-91ddb8cd2a09"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "344f3ccc-7a2a-43a7-b62d-572ac6168344"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_facilities", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"_ingested_at", "_source_system", "_load_mode"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"_ingest_date", "_source_ingest_date"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns", "is_centroid_coordinate", each true), {{"is_centroid_coordinate", type logical}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"facility_id", "facility_name", "facility_type", "city", "state", "latitude", "longitude", "dock_doors", "operating_hours", "is_centroid_coordinate", "_source_ingest_date"}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Reordered columns", "_silver_created_at", each DateTimeZone.UtcNow()), {{"_silver_created_at", type datetime}}),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 1", "_silver_updated_at", each DateTimeZone.UtcNow()), {{"_silver_updated_at", type datetime}}),
  #"Added custom 3" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 2", "_silver_run_id", each "df_gen2_" & DateTime.ToText(DateTime.LocalNow(), "yyyyMMdd_HHmmss")), {{"_silver_run_id", type text}}),
  #"Added custom 4" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 3", "_dq_status", each "valid"), {{"_dq_status", type text}})
in
  #"Added custom 4";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_routes_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "route_id", DestinationColumnName = "route_id"], [SourceColumnName = "origin_city", DestinationColumnName = "origin_city"], [SourceColumnName = "origin_state", DestinationColumnName = "origin_state"], [SourceColumnName = "destination_city", DestinationColumnName = "destination_city"], [SourceColumnName = "destination_state", DestinationColumnName = "destination_state"], [SourceColumnName = "typical_distance_miles", DestinationColumnName = "typical_distance_miles"], [SourceColumnName = "base_rate_per_mile", DestinationColumnName = "base_rate_per_mile"], [SourceColumnName = "fuel_surcharge_rate", DestinationColumnName = "fuel_surcharge_rate"], [SourceColumnName = "typical_transit_days", DestinationColumnName = "typical_transit_days"], [SourceColumnName = "route_name", DestinationColumnName = "route_name"], [SourceColumnName = "_source_ingest_date", DestinationColumnName = "_source_ingest_date"], [SourceColumnName = "_silver_created_at", DestinationColumnName = "_silver_created_at"], [SourceColumnName = "_silver_updated_at", DestinationColumnName = "_silver_updated_at"], [SourceColumnName = "_silver_run_id", DestinationColumnName = "_silver_run_id"], [SourceColumnName = "_dq_status", DestinationColumnName = "_dq_status"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared silver_routes = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a77071e4-bd2a-4979-8910-91ddb8cd2a09"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "344f3ccc-7a2a-43a7-b62d-572ac6168344"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_routes", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"_ingested_at", "_source_system", "_load_mode"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"_ingest_date", "_source_ingest_date"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns", "route_name", each [origin_city] & " → " & [destination_city]), {{"route_name", type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom", {"route_id", "origin_city", "origin_state", "destination_city", "destination_state", "typical_distance_miles", "base_rate_per_mile", "fuel_surcharge_rate", "typical_transit_days", "route_name", "_source_ingest_date"}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Reordered columns", "_silver_created_at", each DateTimeZone.UtcNow()), {{"_silver_created_at", type datetime}}),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 1", "_silver_updated_at", each DateTimeZone.UtcNow()), {{"_silver_updated_at", type datetime}}),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "_silver_run_id", each "df_gen2_" & DateTime.ToText(DateTime.LocalNow(), "yyyyMMdd_HHmmss")),
  #"Added custom 4" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 3", "_dq_status", each "valid"), {{"_dq_status", type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 4", {{"_silver_run_id", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_trailers_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "trailer_id", DestinationColumnName = "trailer_id"], [SourceColumnName = "trailer_number", DestinationColumnName = "trailer_number"], [SourceColumnName = "trailer_type", DestinationColumnName = "trailer_type"], [SourceColumnName = "length_feet", DestinationColumnName = "length_feet"], [SourceColumnName = "model_year", DestinationColumnName = "model_year"], [SourceColumnName = "vin", DestinationColumnName = "vin"], [SourceColumnName = "acquisition_date", DestinationColumnName = "acquisition_date"], [SourceColumnName = "status", DestinationColumnName = "status"], [SourceColumnName = "current_location", DestinationColumnName = "current_location"], [SourceColumnName = "trailer_age_years_as_of", DestinationColumnName = "trailer_age_years_as_of"], [SourceColumnName = "_source_ingest_date", DestinationColumnName = "_source_ingest_date"], [SourceColumnName = "_silver_created_at", DestinationColumnName = "_silver_created_at"], [SourceColumnName = "_silver_updated_at", DestinationColumnName = "_silver_updated_at"], [SourceColumnName = "_silver_run_id", DestinationColumnName = "_silver_run_id"], [SourceColumnName = "_dq_status", DestinationColumnName = "_dq_status"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared silver_trailers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a77071e4-bd2a-4979-8910-91ddb8cd2a09"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "344f3ccc-7a2a-43a7-b62d-572ac6168344"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_trailers", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"_ingested_at", "_source_system", "_load_mode"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"_ingest_date", "_source_ingest_date"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns", "trailer_age_years_as_of", each Date.Year(as_of_date) - [model_year]), {{"trailer_age_years_as_of", Int64.Type}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom", "_silver_created_at", each DateTimeZone.UtcNow()), {{"_silver_created_at", type datetime}}),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 1", "_silver_updated_at", each DateTimeZone.UtcNow()), {{"_silver_updated_at", type datetime}}),
  #"Added custom 3" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 2", "_silver_run_id", each "df_gen2_" & DateTime.ToText(DateTime.LocalNow(), "yyyyMMdd_HHmmss")), {{"_silver_run_id", type text}}),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "_dq_status", each "valid"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 4", {{"_silver_run_id", type text}, {"_dq_status", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_trucks_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "truck_id", DestinationColumnName = "truck_id"], [SourceColumnName = "unit_number", DestinationColumnName = "unit_number"], [SourceColumnName = "make", DestinationColumnName = "make"], [SourceColumnName = "model_year", DestinationColumnName = "model_year"], [SourceColumnName = "vin", DestinationColumnName = "vin"], [SourceColumnName = "acquisition_date", DestinationColumnName = "acquisition_date"], [SourceColumnName = "acquisition_mileage", DestinationColumnName = "acquisition_mileage"], [SourceColumnName = "fuel_type", DestinationColumnName = "fuel_type"], [SourceColumnName = "tank_capacity_gallons", DestinationColumnName = "tank_capacity_gallons"], [SourceColumnName = "status", DestinationColumnName = "status"], [SourceColumnName = "home_terminal", DestinationColumnName = "home_terminal"], [SourceColumnName = "truck_age_years_as_of", DestinationColumnName = "truck_age_years_as_of"], [SourceColumnName = "_source_ingest_date", DestinationColumnName = "_source_ingest_date"], [SourceColumnName = "_silver_created_at", DestinationColumnName = "_silver_created_at"], [SourceColumnName = "_silver_updated_at", DestinationColumnName = "_silver_updated_at"], [SourceColumnName = "_silver_run_id", DestinationColumnName = "_silver_run_id"], [SourceColumnName = "_dq_status", DestinationColumnName = "_dq_status"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared silver_trucks = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a77071e4-bd2a-4979-8910-91ddb8cd2a09"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "344f3ccc-7a2a-43a7-b62d-572ac6168344"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze_trucks", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"_ingested_at", "_source_system", "_load_mode"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"_ingest_date", "_source_ingest_date"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "truck_age_years_as_of", each Date.Year(as_of_date) - [model_year]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"truck_age_years_as_of", Int64.Type}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "_silver_created_at", each DateTimeZone.UtcNow()), {{"_silver_created_at", type datetime}}),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 1", "_silver_updated_at", each DateTimeZone.UtcNow()), {{"_silver_updated_at", type datetime}}),
  #"Added custom 3" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 2", "_silver_run_id", each "df_gen2_" & DateTime.ToText(DateTime.LocalNow(), "yyyyMMdd_HHmmss")), {{"_silver_run_id", type text}}),
  #"Added custom 4" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom 3", "_dq_status", each "valid"), {{"_dq_status", type text}})
in
  #"Added custom 4";
shared as_of_date = #datetime(2024, 12, 31, 0, 0, 0) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetime];
shared silver_drivers_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "lh_logistics_silver"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silver_drivers", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared silver_customers_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "a77071e4-bd2a-4979-8910-91ddb8cd2a09"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e781f9bd-98b2-4482-9e9d-a2731909476f"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silver_customers", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared silver_facilities_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "lh_logistics_silver"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silver_facilities", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared silver_routes_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "lh_logistics_silver"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silver_routes", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared silver_trailers_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "lh_logistics_silver"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silver_trailers", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared silver_trucks_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "lh_logistics_silver"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silver_trucks", ItemKind = "Table"]}[Data]
in
  TableNavigation;
