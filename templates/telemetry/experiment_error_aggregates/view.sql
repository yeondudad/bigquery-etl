CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_error_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.experiment_error_aggregates_v1`
