-- step: external-namespace
CREATE NAMESPACE IF NOT EXISTS rest.${namespace};

-- step: external-drop
DROP TABLE IF EXISTS rest.${namespace}.external;

-- step: external-create
CREATE TABLE rest.${namespace}.external (
    id INT,
    payload STRING
) USING iceberg
TBLPROPERTIES (
    'format-version'='2',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);

-- step: external-insert
-- One writer task fixes the physical layout used by the cross-engine mutations.
INSERT INTO rest.${namespace}.external
SELECT /*+ REPARTITION(1) */ *
FROM VALUES
    (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')
AS seed(id, payload);

-- step: external-delete
DELETE FROM rest.${namespace}.external WHERE id = 2;

-- step: external-update
UPDATE rest.${namespace}.external
SET payload = 'three-spark' WHERE id = 3;
