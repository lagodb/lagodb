-- Nested structs, lists and maps, including null and empty containers.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    record struct<label:string,missing:string,amount:decimal(38,8),large:bigint,
                  bytes:binary,event_date:date,event_time:timestamp_ntz,
                  event_tz:timestamp,special:array<double>,single:array<float>>,
    items array<struct<n:integer,tags:array<string>>>,
    labels map<string,string>,
    indexed map<integer,array<string>>,
    structured map<struct<code:integer>,string>,
    numbers array<integer>
) USING iceberg
TBLPROPERTIES ('format-version'='2');

INSERT INTO $table VALUES
    (1,
     named_struct(
         'label', 'hello', 'missing', CAST(NULL AS string),
         'amount', CAST('123456789012345678901234567890.12345678' AS decimal(38,8)),
         'large', CAST('9223372036854775807' AS bigint),
         'bytes', unhex('00ff10'), 'event_date', DATE '2024-01-02',
         'event_time', TIMESTAMP_NTZ '1969-12-31 23:59:59.123456',
         'event_tz', TIMESTAMP '2024-01-02 03:04:05.123456',
         'special', array(CAST('1.2345678901234567' AS double),
                          CAST('NaN' AS double), CAST('Infinity' AS double),
                          CAST('-Infinity' AS double), CAST('-0.0' AS double),
                          CAST('0.0' AS double)),
         'single', array(CAST('1.25' AS float), CAST('-0.0' AS float),
                         CAST('0.0' AS float))),
     array(named_struct('n', 1, 'tags', array('x', CAST(NULL AS string))),
           NULL, named_struct('n', 2, 'tags', CAST(array() AS array<string>))),
     map('a', CAST(NULL AS string), 'b', 'text'),
     map(7, array('x', CAST(NULL AS string)), 8, CAST(NULL AS array<string>)),
     map(named_struct('code', 9), 'nine'),
     array(1, NULL, 2)),
    (2, NULL, array(), map(), map(), map(), array()),
    (3, NULL, NULL, NULL, NULL, NULL, NULL);
