"""Iceberg services composed over the shared MinIO regression runtime."""

from pathlib import Path

from fixture_runtime import DockerClient, FixtureConfig, FixtureRuntime
from iceberg_provision import IcebergProvision


class IcebergServices:
    def __init__(self, runtime: FixtureRuntime) -> None:
        self.runtime = runtime
        self.config = runtime.config

    @staticmethod
    def prepare_images(
        docker: DockerClient, config: FixtureConfig, logs: Path, refresh: bool = False,
    ) -> None:
        docker.ensure_image(config.images["rest"], refresh)
        if not refresh and docker.image_exists(config.images["spark"]):
            return
        if config.images["spark"] != config.default_spark_image:
            docker.run("pull", config.images["spark"])
            return
        docker.ensure_image(config.images["spark-base"], refresh)
        docker.logged([
            "build", "--build-arg", f"SPARK_BASE_IMAGE={config.images['spark-base']}",
            "--tag", config.images["spark"], "--file", str(config.scripts / "spark.Dockerfile"),
            str(config.scripts),
        ], logs / "spark-image-build.log")

    def start(self, storage_endpoint: str) -> None:
        self.start_catalog("rest-vended", self.config.bucket, "iceberg-vended", True)
        self.start_catalog("rest-fallback", self.config.fallback_bucket, "iceberg-fallback", False)
        self.provision("setup")
        services = self.runtime.state["services"]
        services["rest_uri"] = self.runtime.start_proxy(
            "vending-proxy", services["rest_upstream_uri"], [
                "--s3-endpoint", storage_endpoint, "--s3-region", self.config.region,
                "--require-vended-credentials",
            ],
        )
        self.runtime.save()
        services["failure_rest_uri"] = self.runtime.start_proxy(
            "failure-proxy", services["rest_uri"], ["--reject-transaction-commit"],
        )
        self.runtime.save()

    def start_catalog(self, alias: str, bucket: str, prefix: str, vending: bool) -> None:
        environment = self.config.aws_environment()
        environment.update({
            "CATALOG_CATALOG__IMPL": "org.apache.iceberg.jdbc.JdbcCatalog",
            "CATALOG_URI": "jdbc:sqlite:file:/tmp/catalog.db",
            "CATALOG_WAREHOUSE": f"s3://{bucket}/{prefix}",
            "CATALOG_IO__IMPL": "org.apache.iceberg.aws.s3.S3FileIO",
            "CATALOG_S3_ENDPOINT": "http://minio:9000",
            "CATALOG_S3_PATH__STYLE__ACCESS": "true",
            "CATALOG_INCLUDE__CREDENTIALS": str(vending).lower(),
            "CATALOG_S3_ACCESS__KEY__ID": self.config.user,
            "CATALOG_S3_SECRET__ACCESS__KEY": self.config.password,
        })
        uri = self.runtime.start_container(alias, self.config.images["rest"], 8181, environment, [])
        service = "rest_upstream_uri" if vending else "fallback_rest_uri"
        self.runtime.state["services"][service] = uri
        self.runtime.save()
        self.runtime.wait_ready(alias, f"{uri}/v1/config")

    def provision(self, selection: str) -> None:
        sql_file = self.runtime.directory / f"iceberg-{selection}.sql"
        sql_file.write_text(IcebergProvision().render(selection))
        # The Spark image runs as its own non-root user and mounts this read-only.
        sql_file.chmod(0o644)
        args = ["--network", self.runtime.state["network"]]
        for key, value in self.config.aws_environment().items():
            args.extend(["--env", f"{key}={value}"])
        args.extend([
            "--volume", f"{sql_file}:/fixture/iceberg_provision.sql:ro",
            "--entrypoint", "/opt/spark/bin/spark-sql", self.config.images["spark"],
            "--hiveconf", f"fallback_second_bucket={self.config.second_bucket}",
            "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        ])
        for catalog, alias, bucket, prefix in (
            ("rest", "rest-vended", self.config.bucket, "iceberg-vended"),
            ("fallback", "rest-fallback", self.config.fallback_bucket, "iceberg-fallback"),
        ):
            settings = {
                f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
                f"spark.sql.catalog.{catalog}.type": "rest",
                f"spark.sql.catalog.{catalog}.uri": f"http://{alias}:8181",
                f"spark.sql.catalog.{catalog}.warehouse": f"s3://{bucket}/{prefix}",
                f"spark.sql.catalog.{catalog}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"spark.sql.catalog.{catalog}.s3.endpoint": "http://minio:9000",
                f"spark.sql.catalog.{catalog}.s3.path-style-access": "true",
            }
            for key, value in settings.items():
                args.extend(["--conf", f"{key}={value}"])
        args.extend(["--conf", "spark.ui.enabled=false", "-f", "/fixture/iceberg_provision.sql"])
        self.runtime.run_job(f"spark-{selection}", args, self.runtime.logs / f"spark-{selection}.log")
