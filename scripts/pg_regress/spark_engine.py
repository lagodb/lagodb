"""Spark jobs sharing the Iceberg regression service configuration."""

import shlex
from pathlib import Path
from typing import Optional

from fixture_runtime import FixtureRuntime


class SparkEngine:
    """Run short-lived Spark jobs on the fixture network."""

    def __init__(self, runtime: FixtureRuntime) -> None:
        self.runtime = runtime
        self.config = runtime.config

    def run_sql(
        self, name: str, sql_file: Path,
        hiveconf: Optional[dict[str, str]] = None,
    ) -> None:
        arguments = self._base_arguments()
        arguments.extend([
            "--volume", f"{sql_file}:/fixture/query.sql:ro",
            "--entrypoint", "/opt/spark/bin/spark-sql", self.config.images["spark"],
        ])
        for key, value in (hiveconf or {}).items():
            arguments.extend(["--hiveconf", f"{key}={value}"])
        arguments.extend(self._session_arguments())
        arguments.extend(["-f", "/fixture/query.sql"])
        self.runtime.run_job(name, arguments, self.runtime.logs / f"{name}.log")

    def compile_java_jar(self, name: str, source: Path, output: Path) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        # The Spark image runs as its own non-root user. Compile in-container and
        # publish only the completed jar into the fixture-owned host directory.
        output.parent.chmod(0o733)
        mounted_source = f"/fixture/{source.name}"
        mounted_output = f"/fixture/output/{output.name}"
        pending_output = f"{mounted_output}.pending"
        arguments = self._base_arguments()
        arguments.extend([
            "--volume", f"{source}:{mounted_source}:ro",
            "--volume", f"{output.parent}:/fixture/output",
            "--entrypoint", "/bin/sh", self.config.images["spark"],
            "-c",
            f"set -eu; rm -f {shlex.quote(pending_output)}; "
            "mkdir /tmp/spark-java-classes; "
            "javac -cp '/opt/spark/jars/*' -d /tmp/spark-java-classes "
            f"{shlex.quote(mounted_source)}; "
            f"jar --create --file {shlex.quote(pending_output)} "
            "-C /tmp/spark-java-classes .; "
            f"mv {shlex.quote(pending_output)} {shlex.quote(mounted_output)}",
        ])
        self.runtime.run_job(
            name, arguments, self.runtime.logs / f"{name}.log",
        )

    def run_scenario(
        self, name: str, scenario_file: Path, runner: Path,
        main_class: str, additional_settings: dict[str, str],
    ) -> str:
        arguments = self._base_arguments()
        arguments.extend([
            "--volume", f"{scenario_file}:/fixture/scenario.sql:ro",
            "--volume", f"{runner}:/fixture/runner.jar:ro",
            "--entrypoint", "/opt/spark/bin/spark-submit", self.config.images["spark"],
        ])
        arguments.extend(self._session_arguments(additional_settings))
        arguments.extend([
            "--class", main_class, "/fixture/runner.jar", "/fixture/scenario.sql",
        ])
        return self.runtime.run_captured_job(
            name, arguments, self.runtime.logs / f"{name}.log",
        )

    def run_java_source(self, name: str, source: Path, arguments: list[str]) -> None:
        container_arguments = self._base_arguments()
        container_arguments.extend([
            "--volume", f"{source}:/fixture/{source.name}:ro",
            "--entrypoint", "java", self.config.images["spark"],
            "--class-path", "/opt/spark/jars/*", f"/fixture/{source.name}",
            *arguments,
        ])
        self.runtime.run_job(
            name, container_arguments, self.runtime.logs / f"{name}.log",
        )

    def catalog_settings(
        self, name: str, catalog_type: str, uri: str, warehouse: str,
    ) -> dict[str, str]:
        prefix = f"spark.sql.catalog.{name}"
        return {
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.type": catalog_type,
            f"{prefix}.uri": uri,
            f"{prefix}.warehouse": warehouse,
            f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{prefix}.s3.endpoint": "http://minio:9000",
            f"{prefix}.s3.path-style-access": "true",
        }

    def _base_arguments(self) -> list[str]:
        arguments = ["--network", self.runtime.state["network"]]
        for key, value in self.config.aws_environment().items():
            arguments.extend(["--env", f"{key}={value}"])
        return arguments

    def _session_arguments(
        self, additional_settings: Optional[dict[str, str]] = None,
    ) -> list[str]:
        arguments = []
        settings = self._session_settings()
        settings.update(additional_settings or {})
        for key, value in settings.items():
            arguments.extend(["--conf", f"{key}={value}"])
        return arguments

    def _session_settings(self) -> dict[str, str]:
        settings = {
            "spark.master": "local[*]",
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
        }
        for catalog, alias, bucket, prefix in (
            ("rest", "rest-vended", self.config.bucket, "iceberg-vended"),
            ("fallback", "rest-fallback", self.config.fallback_bucket, "iceberg-fallback"),
        ):
            settings.update(self.catalog_settings(
                catalog,
                "rest",
                f"http://{alias}:8181",
                f"s3://{bucket}/{prefix}",
            ))
        settings["spark.ui.enabled"] = "false"
        return settings
