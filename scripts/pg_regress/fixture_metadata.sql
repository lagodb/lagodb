-- Storage is always present; REST endpoints exist only for the Iceberg suite.
BEGIN;
SET client_min_messages = warning;
CREATE SCHEMA IF NOT EXISTS lagodb_regress;
DROP TABLE IF EXISTS lagodb_regress.object_storage_fixture;
RESET client_min_messages;
CREATE TABLE lagodb_regress.object_storage_fixture (
    endpoint text NOT NULL,
    bucket text NOT NULL,
    fallback_bucket text NOT NULL,
    fallback_second_bucket text NOT NULL,
    region text NOT NULL,
    access_key_id text NOT NULL,
    secret_access_key text NOT NULL,
    rest_uri text,
    fallback_rest_uri text,
    failure_rest_uri text
);
INSERT INTO lagodb_regress.object_storage_fixture
VALUES (
    :'endpoint', :'bucket', :'fallback_bucket', :'fallback_second_bucket',
    :'region', :'access_key_id', :'secret_access_key',
    NULLIF(:'rest_uri', ''), NULLIF(:'fallback_rest_uri', ''), NULLIF(:'failure_rest_uri', '')
);
COMMIT;
