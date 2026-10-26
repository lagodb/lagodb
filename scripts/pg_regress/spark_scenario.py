"""Spark interoperability scenarios owned by their pg_regress tests."""

from pathlib import Path
from string import Template

from fixture_runtime import DockerClient, FixtureConfig, FixtureError, FixtureRuntime
from host_tunnel import HostPortTunnel
from spark_engine import SparkEngine


class SparkScenario:
    RUNNER_CLASS = "SparkScenarioRunner"
    RUNNER_COMPILE_JOB = "spark-scenario-runner-compile"

    def __init__(self, runtime: FixtureRuntime) -> None:
        self.runtime = runtime
        self._spark = SparkEngine(runtime)

    @staticmethod
    def prepare_images(
        docker: DockerClient, config: FixtureConfig, refresh: bool = False,
    ) -> None:
        HostPortTunnel.prepare_image(docker, config, refresh)

    def run(self, schema: str, step: str) -> None:
        scenario_file = self._render(schema, step)
        runner = self._prepare_runner()
        self._ensure_tunnel()
        name = f"spark-run-{schema}-{step}"
        stdout = self._spark.run_scenario(
            name, scenario_file, runner, self.RUNNER_CLASS, self._jdbc_settings(),
        )
        for line in stdout.splitlines():
            if line.startswith("spark: "):
                print(line)

    def _prepare_runner(self) -> Path:
        runner = (
            self.runtime.directory / "spark-scenario-runner" / f"{self.RUNNER_CLASS}.jar"
        )
        if not runner.is_file():
            self._spark.compile_java_jar(
                self.RUNNER_COMPILE_JOB,
                self.runtime.config.scripts / f"{self.RUNNER_CLASS}.java",
                runner,
            )
        return runner

    def _ensure_tunnel(self) -> None:
        # JDBC interoperability is optional: REST-only setup and provisioning
        # must not depend on sshd or host ssh. Start once, then retain to teardown.
        HostPortTunnel(self.runtime).ensure_started()

    def _jdbc_settings(self) -> dict[str, str]:
        config = self.runtime.config
        catalog = config.pgdatabase
        postgres_address = self.runtime.state["services"]["postgres_address"]
        settings = self._spark.catalog_settings(
            catalog,
            "jdbc",
            f"jdbc:postgresql://{postgres_address}/{catalog}",
            f"s3://{config.bucket}/iceberg-jdbc",
        )
        settings.update({
            f"spark.sql.catalog.{catalog}.jdbc.init-catalog-tables": "false",
            f"spark.sql.catalog.{catalog}.jdbc.user": config.pguser,
        })
        return settings

    def _render(self, schema: str, step: str) -> Path:
        source = Path.cwd() / "spark" / schema / f"{step}.sql"
        if not source.is_file():
            raise FixtureError(f"Spark scenario does not exist: {source}")
        destination = self.runtime.directory / f"spark-run-{schema}-{step}.sql"
        destination.write_text(Template(source.read_text()).substitute(
            catalog=self.runtime.config.pgdatabase,
            namespace=schema,
        ))
        destination.chmod(0o644)
        return destination
