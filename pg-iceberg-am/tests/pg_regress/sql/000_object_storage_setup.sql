-- Provision the object-storage fixture used by object-backed pg_regress tests.
-- pg_regress unsets PGDATABASE and passes the database only via psql -d, so
-- export it for the fixture's own psql calls. `\!` does not interpolate psql
-- variables (OT_WHOLE_LINE), but `\setenv` arguments (OT_NORMAL) do.
\setenv PGDATABASE :DBNAME
\! bin/object_storage_fixture setup
