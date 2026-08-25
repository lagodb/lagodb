CREATE FOREIGN DATA WRAPPER lagodb_connectors
  HANDLER lagodb_connectors_fdw_handler
  VALIDATOR lagodb_connectors_fdw_validator;
