-- step: delete-files
SELECT min(content), max(content), min(file_format), max(file_format),
       sum(record_count)
FROM ${catalog}.${namespace}.mutations.delete_files
;

-- step: partitioned-rows
SELECT id, region, payload
FROM ${catalog}.${namespace}.partitioned_mutations
ORDER BY id
;

-- step: partitioned-delete-files
SELECT to_json(partition), min(content), max(content),
       min(file_format), max(file_format), sum(record_count)
FROM ${catalog}.${namespace}.partitioned_mutations.delete_files
GROUP BY partition
ORDER BY to_json(partition)
;

-- step: external-rows
SELECT id, payload
FROM rest.${namespace}.external
ORDER BY id
;

-- step: external-delete-files
SELECT min(content), max(content), min(file_format), max(file_format),
       sum(record_count)
FROM rest.${namespace}.external.delete_files
;
