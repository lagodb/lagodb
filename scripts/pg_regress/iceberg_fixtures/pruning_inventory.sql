-- Expectations come from the source's physical files metadata, independently
-- of LagoDB planning. Retain each spec's contribution, including zero counts.
-- This recipe belongs only to the immutable pruning read fixture.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    case_name string,
    spec_id integer,
    file_count bigint,
    metrics_absent boolean
) USING iceberg TBLPROPERTIES ('format-version'='2');
INSERT INTO $table
WITH files AS (
    SELECT * FROM $namespace.partition_evolution_v2.files WHERE content = 0
), wanted_bucket AS (
    SELECT min(partition.id_bucket_4) AS value FROM files
    WHERE file_path IN (
        SELECT _file FROM $namespace.partition_evolution_v2 WHERE id = 4
    )
)
SELECT cases.case_name, files.spec_id,
       sum(CASE WHEN
           CASE cases.case_name
               WHEN 'all' THEN true
               WHEN 'old_day' THEN partition.event_date_day = DATE '2024-01-01'
               WHEN 'new_day' THEN partition.event_date_day = DATE '2024-01-03'
               WHEN 'new_range' THEN partition.event_date_day >= DATE '2024-01-03'
                                    AND partition.event_date_day < DATE '2024-01-05'
               WHEN 'cross_range' THEN partition.event_date_day >= DATE '2024-01-02'
                                      AND partition.event_date_day < DATE '2024-01-04'
               WHEN 'category' THEN partition.category_trunc = 'al'
               -- The historical spec has no id partition field. Current
               -- bucket collisions are retained, regardless of row filtering.
               WHEN 'bucket' THEN files.spec_id = 0 OR partition.id_bucket_4 = wanted_bucket.value
               WHEN 'null_day' THEN partition.event_date_day IS NULL
           END
       THEN 1 ELSE 0 END) AS file_count,
       min(CASE WHEN (lower_bounds IS NULL OR size(lower_bounds) = 0)
                     AND (upper_bounds IS NULL OR size(upper_bounds) = 0)
                     AND (null_value_counts IS NULL OR size(null_value_counts) = 0)
                THEN 1 ELSE 0 END) = 1 AS metrics_absent
FROM files CROSS JOIN wanted_bucket
CROSS JOIN (VALUES ('all'), ('old_day'), ('new_day'), ('new_range'),
                   ('cross_range'), ('category'), ('bucket'), ('null_day')) AS cases(case_name)
GROUP BY cases.case_name, files.spec_id;
