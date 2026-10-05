-- Basic rows for transaction, import, and credential-routing consumers.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (id integer, payload string)
USING iceberg
$location
TBLPROPERTIES (
    'format-version'='2',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
INSERT INTO $table VALUES $values;
