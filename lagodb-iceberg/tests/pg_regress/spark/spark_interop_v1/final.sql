-- step: type-schema
SELECT typeof(id), typeof(b), typeof(si), typeof(i), typeof(bi),
       typeof(r), typeof(dp), typeof(num), typeof(t), typeof(vc),
       typeof(c), typeof(n), typeof(j), typeof(jb), typeof(d),
       typeof(ts), typeof(tstz), typeof(u), typeof(by),
       typeof(details), typeof(numbers)
FROM ${catalog}.${namespace}.all_types
WHERE id = 1
;

-- step: type-values
SELECT id,
       b, si, i, bi, r, dp, CAST(num AS STRING),
       t, vc, c, n,
       get_json_object(j, '$$.json_key'), get_json_object(jb, '$$.jsonb_key'),
       CAST(d AS STRING), CAST(ts AS STRING), CAST(tstz AS STRING), u, hex(by),
       details.score, details.label, CAST(numbers AS STRING)
FROM ${catalog}.${namespace}.all_types
WHERE id = 1
;

-- step: type-nulls
SELECT b, si, i, bi, r, dp, num, t, vc, c, n, j, jb, d, ts, tstz, u, by,
       details, numbers
FROM ${catalog}.${namespace}.all_types
WHERE id = 2
;

-- step: list-data
SELECT region, count(*)
FROM ${catalog}.${namespace}.list_partitioned
GROUP BY region
ORDER BY region
;

-- step: list-partitions
SELECT to_json(partition), record_count
FROM ${catalog}.${namespace}.list_partitioned.partitions
ORDER BY to_json(partition)
;

-- step: range-data
SELECT CAST(event_date AS STRING), count(*)
FROM ${catalog}.${namespace}.range_partitioned
GROUP BY event_date
ORDER BY event_date
;

-- step: range-partitions
SELECT to_json(partition), record_count
FROM ${catalog}.${namespace}.range_partitioned.partitions
ORDER BY to_json(partition)
;

-- step: external-rows
SELECT id, payload
FROM rest.${namespace}.external
ORDER BY id
;
