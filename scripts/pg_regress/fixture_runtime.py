"""Owned Docker resources and proxy processes for SQL regression fixtures."""

import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener


class FixtureError(RuntimeError):
    pass


class FixtureConfig:
    def __init__(self) -> None:
        self.scripts = Path(__file__).resolve().parent
        config_file = Path(os.environ.get(
            "OBJECT_STORAGE_ENV", self.scripts / "object_storage.defaults.env",
        ))
        settings = dict(os.environ)
        for line in config_file.read_text().splitlines():
            words = shlex.split(line, comments=True)
            if not words:
                continue
            if len(words) != 1 or "=" not in words[0]:
                raise FixtureError(f"expected a literal KEY=VALUE in {config_file}: {line}")
            key, value = words[0].split("=", 1)
            settings[key] = value

        self.images = {
            name: settings.get(key) or settings[f"DEFAULT_{key}"]
            for name, key in (
                ("minio", "MINIO_IMAGE"), ("rest", "ICEBERG_REST_IMAGE"),
                ("spark-base", "SPARK_BASE_IMAGE"), ("spark", "SPARK_IMAGE"),
            )
        }
        self.default_spark_image = settings["DEFAULT_SPARK_IMAGE"]
        self.user = settings.get("MINIO_USER") or settings["DEFAULT_MINIO_USER"]
        self.password = settings.get("MINIO_PASSWORD") or settings["DEFAULT_MINIO_PASSWORD"]
        self.region = settings.get("LAGODB_REGRESS_REGION") or (
            settings.get("MINIO_REGION") or settings["DEFAULT_MINIO_REGION"]
        )
        self.bucket = settings.get("LAGODB_REGRESS_BUCKET") or "lagodb-regress"
        self.fallback_bucket = settings.get("LAGODB_REGRESS_FALLBACK_BUCKET") or (
            f"{self.bucket}-iceberg-fallback-a"
        )
        self.second_bucket = settings.get("LAGODB_REGRESS_FALLBACK_SECOND_BUCKET") or (
            f"{self.bucket}-iceberg-fallback-b"
        )
        self.timeout = float(settings.get("LAGODB_REGRESS_READY_TIMEOUT_SECONDS") or "30")
        self.pgport = settings.get("PGPORT", "")
        self.docker = settings.get("DOCKER") or next((
            str(path) for path in map(Path, (
                "/Applications/Docker.app/Contents/Resources/bin/docker",
                "/opt/homebrew/bin/docker", "/usr/local/bin/docker",
            )) if os.access(path, os.X_OK)
        ), "docker")
        self.env = settings

    def aws_environment(self) -> dict[str, str]:
        return {
            "AWS_ACCESS_KEY_ID": self.user,
            "AWS_SECRET_ACCESS_KEY": self.password,
            "AWS_REGION": self.region,
        }


class DockerClient:
    def __init__(self, program: str, env: dict[str, str]) -> None:
        self.program = program
        self.env = dict(env)
        if "/" in program:
            self.env["PATH"] = f"{Path(program).parent}:{self.env.get('PATH', '')}"

    def run(self, *args: str) -> str:
        result = subprocess.run(
            [self.program, *args], env=self.env, text=True, capture_output=True,
        )
        if result.returncode:
            # Docker arguments can contain credentials; report its diagnostics.
            raise FixtureError(f"Docker {args[0]} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def image_exists(self, image: str) -> bool:
        result = subprocess.run(
            [self.program, "image", "inspect", image], env=self.env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0

    def ensure_image(self, image: str, refresh: bool = False) -> None:
        if refresh or not self.image_exists(image):
            self.run("pull", image)

    def remove_container(self, container: str) -> None:
        # Fixture containers own their anonymous data volumes.
        self.run("container", "rm", "--force", "--volumes", container)

    def logged(self, args: list[str], log_file: Path) -> None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("w") as log:
            result = subprocess.run(
                [self.program, *args], env=self.env, stdout=log,
                stderr=subprocess.STDOUT, text=True,
            )
        if result.returncode:
            raise FixtureError(
                f"Docker {args[0]} failed; log: {log_file}\n{log_file.read_text()}"
            )


class FixtureRuntime:
    LABEL = "lagodb.fixture=pg-regress-object-storage"

    def __init__(self, config: FixtureConfig) -> None:
        if not config.pgport.isascii() or not config.pgport.isdecimal():
            raise FixtureError("PGPORT must be a decimal port number to isolate fixture state")
        self.config = config
        self.directory = Path(f"/tmp/lagodb_object_regress_{config.pgport}")
        self.state_file = self.directory / "state.json"
        self.state = json.loads(self.state_file.read_text()) if self.state_file.exists() else {
            "docker": config.docker, "network": "", "containers": {},
            "proxies": [], "services": {}, "logs": str(Path.cwd() / "log" / "fixture"),
        }
        self.docker = DockerClient(self.state["docker"], config.env)
        self.logs = Path(self.state["logs"])
        self.processes: dict[int, subprocess.Popen] = {}

    def save(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        pending = self.state_file.with_suffix(".pending")
        pending.write_text(json.dumps(self.state, indent=2) + "\n")
        pending.replace(self.state_file)

    def labels(self) -> list[str]:
        return [
            "--label", self.LABEL, "--label", f"lagodb.pgport={self.config.pgport}",
            "--label", f"lagodb.state-dir={self.directory}",
        ]

    @contextmanager
    def registering(self) -> Iterator[None]:
        # Creation and registration form one interruptible operation. Readiness
        # waits happen afterwards, with signals enabled and ownership persisted.
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, {
            signal.SIGINT, signal.SIGTERM, signal.SIGHUP,
        })
        try:
            yield
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    def create_network(self) -> None:
        with self.registering():
            self.state["network"] = self.docker.run(
                "network", "create", *self.labels(),
                f"lagodb-regress-{self.config.pgport}-{os.getpid()}",
            )
            self.save()

    def start_container(
        self, alias: str, image: str, port: int, env: dict[str, str],
        arguments: list[str], entrypoint: str = "",
    ) -> str:
        args = [
            "run", "--detach", "--publish", f"127.0.0.1::{port}",
            "--network", self.state["network"], "--network-alias", alias,
            "--name", f"lagodb-regress-{alias}-{self.config.pgport}-{os.getpid()}",
            *self.labels(),
        ]
        for key, value in env.items():
            args.extend(["--env", f"{key}={value}"])
        if entrypoint:
            args.extend(["--entrypoint", entrypoint])
        with self.registering():
            container = self.docker.run(*args, image, *arguments)
            self.state["containers"][alias] = container
            self.save()
        try:
            mapping = self.docker.run("port", container, f"{port}/tcp").splitlines()[0]
            return f"http://127.0.0.1:{int(mapping.rsplit(':', 1)[1])}"
        except (FixtureError, IndexError, ValueError) as error:
            raise FixtureError(f"invalid {alias} port mapping: {error}") from error

    def run_job(self, name: str, arguments: list[str], log_file: Path) -> None:
        container = None
        failure = None
        try:
            with self.registering():
                container = self.docker.run(
                    "create", *self.labels(), "--name",
                    f"lagodb-regress-{name}-{self.config.pgport}-{os.getpid()}", *arguments,
                )
                self.state["containers"][name] = container
                self.save()
            # docker start --attach forwards the container's exit status.
            self.docker.logged(["start", "--attach", container], log_file)
        except BaseException as error:
            failure = error
        if container is not None:
            try:
                with self.registering():
                    self.docker.remove_container(container)
                    del self.state["containers"][name]
                    self.save()
            except (OSError, FixtureError) as cleanup_error:
                if failure is not None:
                    raise FixtureError(f"{failure}\njob cleanup failed: {cleanup_error}") from failure
                raise
        if failure is not None:
            raise failure

    def wait_ready(self, description: str, url: str) -> None:
        deadline = time.monotonic() + self.config.timeout
        opener = build_opener(ProxyHandler({}))
        while True:
            try:
                with opener.open(url, timeout=1) as response:
                    response.read()
                return
            except (URLError, OSError) as error:
                if time.monotonic() >= deadline:
                    raise FixtureError(f"{description} did not become ready: {error}") from error
                time.sleep(0.1)

    def start_proxy(self, name: str, upstream: str, options: list[str]) -> str:
        port_file = self.directory / f"{name}.port"
        log_file = self.logs / f"{name}.log"
        self.logs.mkdir(parents=True, exist_ok=True)
        # Persist ownership before a pending interrupt can initiate cleanup.
        with self.registering():
            with log_file.open("w") as log:
                process = subprocess.Popen([
                    sys.executable, str(self.config.scripts / "rest_catalog_proxy.py"),
                    "--upstream", upstream, "--port-file", str(port_file), *options,
                ], env=self.config.env, stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True)
            self.processes[process.pid] = process
            self.state["proxies"].append({"pid": process.pid, "port_file": str(port_file)})
            self.save()
        deadline = time.monotonic() + self.config.timeout
        while not port_file.exists():
            if process.poll() is not None or time.monotonic() >= deadline:
                raise FixtureError(f"{name} failed to start; log: {log_file}\n{log_file.read_text()}")
            time.sleep(0.1)
        uri = f"http://127.0.0.1:{int(port_file.read_text().strip())}"
        self.wait_ready(name, f"{uri}/v1/config")
        return uri

    def stop_proxy(self, record: dict) -> None:
        pid = record["pid"]
        process = self.processes.get(pid)
        if process is not None:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            return
        # A later CLI invocation must establish ownership before using a stored PID.
        if not self.proxy_is_owned(record):
            return
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + 5
        killed = False
        while self.proxy_is_owned(record):
            if time.monotonic() >= deadline:
                if killed:
                    raise FixtureError(f"proxy {pid} did not terminate; state retained")
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    return
                killed = True
                deadline = time.monotonic() + 1
            time.sleep(0.1)

    def proxy_is_owned(self, record: dict) -> bool:
        result = subprocess.run(
            ["ps", "-ww", "-p", str(record["pid"]), "-o", "stat=", "-o", "args="],
            text=True, capture_output=True,
        )
        if result.returncode == 1:
            return False
        if result.returncode:
            raise FixtureError(f"failed to inspect proxy {record['pid']}: {result.stderr.strip()}")
        status, _, command = result.stdout.strip().partition(" ")
        return not status.startswith("Z") and (
            str(self.config.scripts / "rest_catalog_proxy.py") in command
            and f"--port-file {record['port_file']}" in command
        )

    def cleanup(self) -> None:
        errors = []
        for proxy in reversed(self.state["proxies"]):
            try:
                self.stop_proxy(proxy)
            except (OSError, FixtureError) as error:
                errors.append(str(error))
        # Labels also cover a Docker resource created just before an interrupt.
        for kind, listing in (("container", ["ls", "-aq"]), ("network", ["ls", "-q"])):
            try:
                ids = self.docker.run(kind, *listing, "--filter", f"label={self.LABEL}").split()
            except (OSError, FixtureError) as error:
                errors.append(str(error))
                continue
            if kind == "container":
                owned = list(reversed(self.state["containers"].values()))
                ids = [resource for resource in ids if resource not in owned] + [
                    resource for resource in owned if resource in ids
                ]
            for resource in ids:
                try:
                    info = json.loads(self.docker.run(kind, "inspect", resource))[0]
                    labels = info["Config"]["Labels"] if kind == "container" else info["Labels"]
                    owner = labels.get("lagodb.state-dir")
                    if labels.get("lagodb.pgport") != self.config.pgport and (
                        not owner or Path(owner).is_dir()
                    ):
                        continue
                    if kind == "container":
                        self.docker.remove_container(resource)
                    else:
                        self.docker.run("network", "rm", resource)
                except (OSError, FixtureError) as error:
                    errors.append(str(error))
        if errors:
            raise FixtureError("fixture cleanup failed; state retained:\n" + "\n".join(errors))
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.state["network"] = ""
        self.state["containers"] = {}
        self.state["proxies"] = []
        self.state["services"] = {}
