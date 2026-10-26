-- step: rows
SELECT id, region, payload
FROM ${catalog}.${namespace}.mutations
ORDER BY id
;
