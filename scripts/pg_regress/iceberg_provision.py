"""Internal table recipes rendered for the Iceberg fixture's Spark execution.

setup creates immutable read sources and the cached-plan target. Write selections
restore only their consumer's tables; cached-plan replaces one table UUID.
SQL templates describe storage states once, independently of their consumers.
"""

from dataclasses import dataclass
from pathlib import Path
from string import Template


@dataclass(frozen=True)
class IcebergFixture:
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


class IcebergProvision:
    SELECTIONS = ("setup", "fdw-writes", "cached-plan", "query-offload-writes")

    def __init__(self) -> None:
        self.templates = Path(__file__).with_name("iceberg_fixtures")

    def fixtures(self, selection: str) -> tuple[IcebergFixture, ...]:
        if selection == "setup":
            return self.read_sources() + self.cached_plan() + self.query_sources()
        if selection == "fdw-writes":
            return self.write_targets()
        if selection == "cached-plan":
            return self.cached_plan()
        if selection == "query-offload-writes":
            return (
                self.query_left("query_offload_writes"),
                IcebergFixture("parallel_groups_v2.sql", "rest", "query_offload_writes",
                               "parallel_source"),
            )
        raise ValueError(f"unknown fixture selection: {selection}")

    def read_sources(self) -> tuple[IcebergFixture, ...]:
        return (
            IcebergFixture("rows_v2.sql", "rest", "fdw_reads", "import_rows_v2",
                           "(10, 'ten')"),
            IcebergFixture("filters_v2.sql", "rest", "fdw_reads", "filter_rows_v2"),
            IcebergFixture("deletion_vectors_v3.sql", "rest", "fdw_reads",
                           "deletion_vectors_v3"),
            IcebergFixture("partition_evolution_v2.sql", "rest", "fdw_reads",
                           "partition_evolution_v2", metrics_mode="none"),
            IcebergFixture("pruning_inventory.sql", "rest", "fdw_reads",
                           "partition_pruning_inventory"),
            IcebergFixture("dropped_partition_source_v2.sql", "rest", "fdw_reads",
                           "dropped_partition_source_v2"),
            IcebergFixture("rows_v2.sql", "fallback", "fdw_reads", "rows_v2",
                           "(100, 'fallback')"),
            IcebergFixture("rows_v2.sql", "fallback", "fdw_reads", "second_bucket_v2",
                           "(200, 'second-bucket')",
                           "LOCATION 's3://${hiveconf:fallback_second_bucket}"
                           "/iceberg-fallback/fdw_reads/second_bucket_v2'"),
        )

    def write_targets(self) -> tuple[IcebergFixture, ...]:
        return (
            IcebergFixture("rows_v2.sql", "rest", "fdw_writes", "rows_v2",
                           "(1, 'one'), (2, 'two'), (3, 'three')"),
            IcebergFixture("rows_v2.sql", "rest", "fdw_writes", "import_rows_v2",
                           "(10, 'ten')"),
            IcebergFixture("deletion_vectors_v3.sql", "rest", "fdw_writes",
                           "deletion_vectors_v3"),
            IcebergFixture("partition_evolution_v2.sql", "rest", "fdw_writes",
                           "partition_evolution_v2"),
            IcebergFixture("rows_v2.sql", "fallback", "fdw_writes", "rows_v2",
                           "(100, 'fallback')"),
        )

    def cached_plan(self) -> tuple[IcebergFixture, ...]:
        return (IcebergFixture("filters_v2.sql", "rest", "fdw_cached_plan",
                              "filter_rows_v2"),)

    def query_left(self, namespace: str) -> IcebergFixture:
        return IcebergFixture(
            "query_rows_v2.sql", "rest", namespace, "left_source",
            "(1, 1, 10, 'alpha'), (2, 2, 20, 'beta'), "
            "(3, 2, NULL, 'beta'), (4, 3, 40, NULL), (5, NULL, 50, 'orphan')",
        )

    def query_sources(self) -> tuple[IcebergFixture, ...]:
        return (
            self.query_left("query_offload_reads"),
            IcebergFixture(
                "query_rows_v2.sql", "rest", "query_offload_reads", "right_source",
                "(101, 1, 100, 'one'), (102, 2, 200, 'two-a'), "
                "(103, 2, 300, 'two-b'), (104, 4, 400, 'four')",
            ),
        )

    def render(self, selection: str) -> str:
        fixtures = self.fixtures(selection)
        namespaces = dict.fromkeys(
            f"{fixture.catalog}.{fixture.namespace}" for fixture in fixtures
        )
        statements = [f"CREATE NAMESPACE IF NOT EXISTS {name};" for name in namespaces]
        statements.extend(fixture.render(self.templates) for fixture in fixtures)
        return "\n\n".join(statements)
