#!/usr/bin/env python3
"""psql-callable storage fixture, with optional Iceberg catalog services.

Run from tests/pg_regress with PGHOST/PGPORT/PGDATABASE supplied by psql.
Connectors require only MinIO; Iceberg setup explicitly requests --iceberg.
Setup/teardown/provision emit fixed pg_regress markers; diagnostics go to stderr.
Images can be prepared from the workspace root without a PostgreSQL connection.
"""

import argparse
import json
import signal
import subprocess
import sys
import time
from pathlib import Path

from fixture_runtime import DockerClient, FixtureConfig, FixtureError, FixtureRuntime
from iceberg_fixture import IcebergServices
from iceberg_provision import IcebergProvision


class RegressionFixture:
    def __init__(self, config: FixtureConfig) -> None:
        self.config = config
        self.runtime = FixtureRuntime(config)

    def setup(self, iceberg: bool) -> None:
        self.runtime.cleanup()
        self.runtime = FixtureRuntime(self.config)
        self.runtime.save()
        try:
            self.runtime.docker.ensure_image(self.config.images["minio"])
            if iceberg:
                IcebergServices.prepare_images(self.runtime.docker, self.config, self.runtime.logs)
            self.runtime.create_network()
            endpoint = self.start_storage()
            if iceberg:
                IcebergServices(self.runtime).start(endpoint)
            self.publish_metadata()
        except BaseException as error:
            self.collect_logs()
            try:
                self.runtime.cleanup()
            except (OSError, FixtureError) as cleanup_error:
                raise FixtureError(f"{error}\n{cleanup_error}") from error
            raise
        print("object_storage_setup: true")

    def start_storage(self) -> str:
        # All buckets and both REST catalogs share this single MinIO service.
        environment = {
            "MINIO_ROOT_USER": self.config.user,
            "MINIO_ROOT_PASSWORD": self.config.password,
            "REGRESS_BUCKET": self.config.bucket,
            "REGRESS_FALLBACK_BUCKET": self.config.fallback_bucket,
            "REGRESS_FALLBACK_SECOND_BUCKET": self.config.second_bucket,
        }
        endpoint = self.runtime.start_container(
            "minio", self.config.images["minio"], 9000, environment, [
                "-c", 'mkdir -p "/data/${REGRESS_BUCKET}" "/data/${REGRESS_FALLBACK_BUCKET}" '
                '"/data/${REGRESS_FALLBACK_SECOND_BUCKET}" && exec /usr/bin/silo server /data',
            ], entrypoint="/bin/sh",
        )
        self.runtime.state["services"]["endpoint"] = endpoint
        self.runtime.save()
        self.runtime.wait_ready("MinIO", f"{endpoint}/minio/health/ready")
        return endpoint

    def publish_metadata(self) -> None:
        values = {
            "bucket": self.config.bucket,
            "fallback_bucket": self.config.fallback_bucket,
            "fallback_second_bucket": self.config.second_bucket,
            "region": self.config.region,
            "access_key_id": self.config.user,
            "secret_access_key": self.config.password,
            **self.runtime.state["services"],
        }
        command = ["psql", "-X", "-v", "ON_ERROR_STOP=1"]
        for name in (
            "endpoint", "bucket", "fallback_bucket", "fallback_second_bucket", "region",
            "access_key_id", "secret_access_key", "rest_uri", "fallback_rest_uri", "failure_rest_uri",
        ):
            command.extend(["--set", f"{name}={values.get(name, '')}"])
        command.extend(["--file", str(self.config.scripts / "fixture_metadata.sql")])
        result = subprocess.run(command, env=self.config.env, text=True, capture_output=True)
        if result.returncode:
            raise FixtureError(f"fixture metadata publication failed: {result.stderr.strip()}")

    def collect_logs(self) -> None:
        try:
            live = self.runtime.docker.run(
                "container", "ls", "-aq", "--filter", f"label={self.runtime.LABEL}",
                "--filter", f"label=lagodb.pgport={self.config.pgport}",
            ).split()
        except (OSError, FixtureError) as error:
            print(f"failed to list containers for logs: {error}", file=sys.stderr)
            return
        for alias, container in self.runtime.state["containers"].items():
            if container not in live:
                continue
            try:
                self.runtime.docker.logged(
                    ["logs", "--tail", "80", container], self.runtime.logs / f"{alias}.log",
                )
            except (OSError, FixtureError) as error:
                print(f"failed to collect {alias} logs: {error}", file=sys.stderr)

    def wait_storage(self) -> None:
        deadline = time.monotonic() + self.config.timeout
        query = (
            "SELECT json_build_object('enabled', enabled, 'state', state, "
            "'error', last_error) FROM lagodb.storage_service_status()"
        )
        while True:
            result = subprocess.run(
                ["psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", query],
                env=self.config.env, text=True, capture_output=True,
            )
            if result.returncode:
                raise FixtureError(f"storage readiness query failed: {result.stderr.strip()}")
            status = json.loads(result.stdout)
            if status["enabled"] and status["state"] == "running":
                print("storage_service_ready: true")
                return
            if not status["enabled"] or status["state"] == "failed":
                raise FixtureError(f"storage service cannot start: {status}")
            if time.monotonic() >= deadline:
                raise FixtureError(f"storage service readiness timed out: {status}")
            time.sleep(0.1)

    def teardown(self) -> None:
        self.collect_logs()
        self.runtime.cleanup()
        print("object_storage_teardown: true")

    def provision(self, selection: str) -> None:
        IcebergServices(self.runtime).provision(selection)
        print(f"iceberg_fixture_provisioned: {selection}")

    @staticmethod
    def interrupted(signum: int, frame: object) -> None:
        raise InterruptedError(f"fixture command interrupted by signal {signum}")

    @classmethod
    def main(cls) -> int:
        parser = argparse.ArgumentParser(description=__doc__)
        commands = parser.add_subparsers(dest="action", required=True)
        for action in ("setup", "prepare-images"):
            commands.add_parser(action).add_argument("--iceberg", action="store_true")
        commands.add_parser("teardown")
        commands.add_parser("wait-storage")
        commands.add_parser("provision").add_argument(
            "selection", choices=IcebergProvision.SELECTIONS[1:],
        )
        args = parser.parse_args()
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, cls.interrupted)
        try:
            config = FixtureConfig()
            if args.action == "prepare-images":
                docker = DockerClient(config.docker, config.env)
                print(docker.run("version"))
                print(docker.run("info"))
                docker.ensure_image(config.images["minio"], refresh=True)
                if args.iceberg:
                    IcebergServices.prepare_images(
                        docker, config, Path.cwd() / "target" / "fixture-images", refresh=True,
                    )
                print("fixture_images_ready: true")
                return 0
            fixture = cls(config)
            if args.action == "setup":
                fixture.setup(args.iceberg)
            elif args.action == "wait-storage":
                fixture.wait_storage()
            elif args.action == "teardown":
                fixture.teardown()
            elif args.action == "provision":
                fixture.provision(args.selection)
            return 0
        except (OSError, ValueError, KeyError, FixtureError) as error:
            print(f"regression fixture failed: {error}", file=sys.stderr)
            return 1
        except KeyboardInterrupt:
            print("regression fixture interrupted", file=sys.stderr)
            return 130


if __name__ == "__main__":
    sys.exit(RegressionFixture.main())
