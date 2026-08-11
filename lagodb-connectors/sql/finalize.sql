CREATE FOREIGN DATA WRAPPER lakebase_fdw
  HANDLER lakebase_fdw_handler
  VALIDATOR lakebase_fdw_validator;
