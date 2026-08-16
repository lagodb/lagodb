-- PostgreSQL rejects CREATE FOREIGN TABLE ... LIKE before an FDW callback can
-- run. Keep the repeated regression-table shapes in psql variables so tests
-- exercise valid core syntax without duplicating long column lists.
\set common_columns 'id integer, bool_col boolean, smallint_col smallint, integer_col integer, bigint_col bigint, real_col real, double_col double precision, numeric_col numeric(12, 3), text_col text, varchar_col varchar(20), char_col character(5), name_col name, bytea_col bytea, uuid_col uuid, date_col date, time_col time without time zone, timestamp_col timestamp without time zone, timestamptz_col timestamp with time zone'
\set json_columns :common_columns ', json_col json, jsonb_col jsonb'
\set parquet_columns :common_columns ', json_col json, bool_array boolean[], smallint_array smallint[], integer_array integer[], bigint_array bigint[], real_array real[], double_array double precision[], text_array text[], varchar_array varchar(20)[], bpchar_array character(5)[], name_array name[], json_array json[]'
\set stream_extra_columns 'id integer, json_col json, jsonb_col jsonb, bool_array boolean[], int_array integer[], text_array text[]'
\set id_payload_columns 'id integer, payload text'
