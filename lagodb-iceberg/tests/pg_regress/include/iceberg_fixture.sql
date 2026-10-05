-- Caller supplies a consumer selection; failure must stop this SQL testcase.
\setenv LAGODB_ICEBERG_FIXTURE :iceberg_fixture
\! python3 ../../../scripts/pg_regress/regress_fixture.py provision "$LAGODB_ICEBERG_FIXTURE"
\i ../../../scripts/pg_regress/fixture_command_result.sql
