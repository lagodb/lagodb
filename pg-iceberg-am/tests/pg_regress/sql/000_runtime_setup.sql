-- Install the shared Lakebase runtime once for the regression database.
-- Individual AM tests may drop/recreate pg_iceberg_am, but the base-owned
-- maintenance queue and database-local worker registration must survive.
CREATE EXTENSION pg_lakebase_runtime;

SELECT extname
FROM pg_extension
WHERE extname = 'pg_lakebase_runtime';

-- Runtime setup complete.
