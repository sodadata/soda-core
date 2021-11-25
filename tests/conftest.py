from sodasql.telemetry.soda_telemetry import SodaTelemetry

# Re-initialize telemetry in test mode.
soda_telemetry = SodaTelemetry.get_instance(test_mode=True)
