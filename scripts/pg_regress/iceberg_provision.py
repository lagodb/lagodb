"""Ordered SQL and Java recipes for each Iceberg fixture selection.

setup creates immutable read sources and the cached-plan target. Write selections
restore only their consumer's tables; cached-plan replaces one table UUID.
Recipes describe storage states once, independently of their consumers.
"""

from dataclasses import dataclass
from pathlib import Path
from string import Template


@dataclass(frozen=True)
class SqlFixture:
    # Namespace and name are Spark SQL identifier fragments, quoted where needed.
    template: str
    catalog: str
    namespace: str
    name: str
    values: str = ""
    location: str = ""
    metrics_mode: str = "truncate(16)"

    def render(self, templates: Path) -> str:
        namespace = f"{self.catalog}.{self.namespace}"
        return Template((templates / self.template).read_text()).substitute(
            namespace=namespace,
            table=f"{namespace}.{self.name}",
            values=self.values,
            location=self.location,
            metrics_mode=self.metrics_mode,
        )


@dataclass(frozen=True)
class JavaFixture:
    """A single table created through the Java API in the REST vended catalog."""

    source: str
    namespace: str
    name: str


@dataclass(frozen=True)
class IcebergProvision:
    """One SQL batch followed by the listed Java jobs."""

    sql: tuple[SqlFixture, ...]
    java: tuple[JavaFixture, ...] = ()

    SELECTIONS = ("setup", "fdw-writes", "cached-plan", "query-offload-writes")

    @classmethod
    def for_selection(cls, selection: str) -> "IcebergProvision":
        if selection == "setup":
            return cls(
                cls.read_sources() + cls.cached_plan() + cls.query_sources(),
                (
                    JavaFixture("NanosecondTimestamps.java", "fdw_nanos", "rows_v3"),
                    JavaFixture("ScalarPredicates.java", "fdw_predicates", "rows_v2"),
                ),
            )
        if selection == "fdw-writes":
            return cls(
                cls.write_targets(),
                (JavaFixture("NanosecondTimestamps.java", "fdw_writes", "nanos_v3"),),
            )
        if selection == "cached-plan":
            return cls((SqlFixture("cached_plan_replaced.sql", "rest", "fdw_cached_plan",
                                   "filter_rows_v2"),))
        if selection == "query-offload-writes":
            return cls((
                cls.query_left("query_offload_writes"),
                SqlFixture("nested_rows_v2.sql", "rest", "query_offload_writes",
                           "nested_rows_v2"),
                SqlFixture("parallel_groups_v2.sql", "rest", "query_offload_writes",
                           "parallel_source"),
            ))
        raise ValueError(f"unknown fixture selection: {selection}")

    @staticmethod
    def read_sources() -> tuple[SqlFixture, ...]:
        return (
            # Keep these immutable sources outside fdw_reads so its IMPORT
            # tests retain their existing table inventory.
            SqlFixture("schema_evolution_v2.sql", "rest", "fdw_schema", "evolving"),
            SqlFixture("decimal_evolution_v2.sql", "rest", "fdw_schema", "decimals"),
            SqlFixture("nested_rows_v2.sql", "rest", "fdw_nested", "rows_v2"),
            SqlFixture("nested_batches_v2.sql", "rest", "fdw_nested", "batches_v2"),
            SqlFixture("nested_invalid_string_v2.sql", "rest", "fdw_nested",
                       "invalid_string_v2"),
            SqlFixture("rows_v2.sql", "rest", "fdw_reads", "import_rows_v2", "(10, 'ten')"),
            SqlFixture("filters_v2.sql", "rest", "fdw_reads", "filter_rows_v2"),
            SqlFixture("deletion_vectors_v3.sql", "rest", "fdw_reads", "deletion_vectors_v3"),
            # The inventory reads this source's files metadata; keep it after
            # partition evolution and only in the immutable read selection.
            SqlFixture("partition_evolution_v2.sql", "rest", "fdw_reads",
                       "partition_evolution_v2", metrics_mode="none"),
            SqlFixture("pruning_inventory.sql", "rest", "fdw_reads",
                       "partition_pruning_inventory"),
            SqlFixture("dropped_partition_source_v2.sql", "rest", "fdw_reads",
                       "dropped_partition_source_v2"),
            SqlFixture("rows_v2.sql", "fallback", "fdw_reads", "rows_v2", "(100, 'fallback')"),
            SqlFixture("rows_v2.sql", "fallback", "fdw_reads", "second_bucket_v2",
                       "(200, 'second-bucket')",
                       "LOCATION 's3://${hiveconf:fallback_second_bucket}"
                       "/iceberg-fallback/fdw_reads/second_bucket_v2'"),
            # Two namespace components, quoted case and a dotted table name.
            # JdbcCatalog splits namespace components on dots: only the table
            # name contains a literal dot.
            SqlFixture("schema_import_v3.sql", "rest", "fdw_import.`MixedCase`", "`rows.v3`"),
            SqlFixture("unsupported_only_v3.sql", "rest", "fdw_import.`MixedCase`",
                       "unsupported_only"),
        )

    @staticmethod
    def write_targets() -> tuple[SqlFixture, ...]:
        return (
            SqlFixture("nested_rows_v2.sql", "rest", "fdw_writes", "nested_rows_v2"),
            SqlFixture("rows_v2.sql", "rest", "fdw_writes", "rows_v2",
                       "(1, 'one'), (2, 'two'), (3, 'three')"),
            SqlFixture("rows_v2.sql", "rest", "fdw_writes", "import_rows_v2", "(10, 'ten')"),
            SqlFixture("deletion_vectors_v3.sql", "rest", "fdw_writes", "deletion_vectors_v3"),
            SqlFixture("partition_evolution_v2.sql", "rest", "fdw_writes",
                       "partition_evolution_v2"),
            SqlFixture("rows_v2.sql", "fallback", "fdw_writes", "rows_v2", "(100, 'fallback')"),
        )

    @staticmethod
    def cached_plan() -> tuple[SqlFixture, ...]:
        return (SqlFixture("filters_v2.sql", "rest", "fdw_cached_plan", "filter_rows_v2"),)

    @staticmethod
    def query_left(namespace: str) -> SqlFixture:
        return SqlFixture(
            "query_rows_v2.sql", "rest", namespace, "left_source",
            "(1, 1, 10, 'alpha'), (2, 2, 20, 'beta'), "
            "(3, 2, NULL, 'beta'), (4, 3, 40, NULL), (5, NULL, 50, 'orphan')",
        )

    @classmethod
    def query_sources(cls) -> tuple[SqlFixture, ...]:
        return (
            cls.query_left("query_offload_reads"),
            SqlFixture(
                "query_rows_v2.sql", "rest", "query_offload_reads", "right_source",
                "(101, 1, 100, 'one'), (102, 2, 200, 'two-a'), "
                "(103, 2, 300, 'two-b'), (104, 4, 400, 'four')",
            ),
        )

    def render_sql(self) -> str:
        templates = Path(__file__).with_name("iceberg_fixtures")
        namespaces = dict.fromkeys(
            f"{fixture.catalog}.{fixture.namespace}" for fixture in self.sql
        )
        statements = [f"CREATE NAMESPACE IF NOT EXISTS {name};" for name in namespaces]
        statements.extend(fixture.render(templates) for fixture in self.sql)
        return "\n\n".join(statements)
