DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    payload string,
    event_date date
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO $table VALUES
    (1, 'one', DATE '2024-01-01'),
    (2, 'two', DATE '2024-01-02'),
    (3, NULL, DATE '2024-01-03'),
    (4, 'four', NULL);

