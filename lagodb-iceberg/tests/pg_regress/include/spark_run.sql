-- Caller supplies the PG schema and a matching Spark scenario step.
\setenv LAGODB_SPARK_RUN_SCHEMA :spark_run_schema
\setenv LAGODB_SPARK_RUN_STEP :spark_run_step
\! python3 ../../../scripts/pg_regress/regress_fixture.py spark-run "$LAGODB_SPARK_RUN_SCHEMA" "$LAGODB_SPARK_RUN_STEP"
\i ../../../scripts/pg_regress/fixture_command_result.sql
