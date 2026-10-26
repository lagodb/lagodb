"""Expose the host PostgreSQL port inside the fixture Docker network."""

import secrets
import socket
import subprocess
import time

from fixture_runtime import DockerClient, FixtureConfig, FixtureError, FixtureRuntime


class HostPortTunnel:
    """Own an SSH reverse tunnel from the fixture network to host loopback."""

    CONTAINER_ALIAS = "lagodb-host"
    PROCESS_NAME = "host-tunnel"

    def __init__(self, runtime: FixtureRuntime) -> None:
        self.runtime = runtime
        self.config = runtime.config

    @staticmethod
    def prepare_image(
        docker: DockerClient, config: FixtureConfig, refresh: bool = False,
    ) -> None:
        docker.ensure_image(config.images["sshd"], refresh)

    def ensure_started(self) -> None:
        services = self.runtime.state["services"]
        if "postgres_address" in services:
            return
        if self.CONTAINER_ALIAS in self.runtime.state["containers"] or any(
            record["name"] == self.PROCESS_NAME
            for record in self.runtime.state["processes"]
        ):
            raise FixtureError("host tunnel has incomplete state")

        with self.runtime.rollback_on_error("host tunnel startup") as rollback:
            self.prepare_image(self.runtime.docker, self.config)
            password = secrets.token_urlsafe(32)
            ssh_port = self.runtime.start_container(
                self.CONTAINER_ALIAS,
                self.config.images["sshd"],
                22,
                {"PASSWORD": password},
                [],
            )
            rollback.callback(
                self.runtime.release_container, self.CONTAINER_ALIAS,
            )
            rollback.callback(
                self.runtime.capture_container_logs, self.CONTAINER_ALIAS,
            )
            self._wait_for_sshd("127.0.0.1", ssh_port)

            askpass = self.runtime.directory / "ssh-askpass"
            askpass.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' \"$LAGODB_HOST_TUNNEL_PASSWORD\"\n"
            )
            askpass.chmod(0o700)

            forward = (
                f"0.0.0.0:{self.config.pgport}:"
                f"127.0.0.1:{self.config.pgport}"
            )
            environment = dict(self.config.env)
            environment.update({
                "LAGODB_HOST_TUNNEL_PASSWORD": password,
                "SSH_ASKPASS": str(askpass),
                "SSH_ASKPASS_REQUIRE": "force",
            })
            process = self.runtime.start_process(
                self.PROCESS_NAME,
                [
                    "ssh", "-N", "-T", "-p", str(ssh_port),
                    "-o", "ExitOnForwardFailure=yes",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "PreferredAuthentications=password",
                    "-o", "PubkeyAuthentication=no",
                    "-o", "NumberOfPasswordPrompts=1",
                    "-o", "LogLevel=ERROR",
                    "-R", forward, "root@127.0.0.1",
                ],
                [forward, "root@127.0.0.1"],
                environment,
            )
            rollback.callback(self.runtime.release_process, process)
            self._wait_for_forward(process)
            services["postgres_address"] = (
                f"{self.CONTAINER_ALIAS}:{self.config.pgport}"
            )
            rollback.callback(services.pop, "postgres_address", None)
            self.runtime.save()

    def _wait_for_sshd(self, host: str, port: int) -> None:
        deadline = time.monotonic() + self.config.timeout
        diagnostic = "no SSH identification received"
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((host, port), timeout=1) as connection:
                    connection.settimeout(1)
                    with connection.makefile("rb") as stream:
                        banner = stream.readline(256)
                if banner.startswith(b"SSH-2.0-"):
                    return
                diagnostic = f"unexpected SSH identification: {banner!r}"
            except OSError as error:
                diagnostic = str(error)
            time.sleep(0.1)
        raise FixtureError(f"host tunnel SSH did not become ready: {diagnostic}")

    def _wait_for_forward(self, process: subprocess.Popen) -> None:
        deadline = time.monotonic() + self.config.timeout
        container = self.runtime.state["containers"][self.CONTAINER_ALIAS]
        while True:
            if process.poll() is not None:
                log_file = self.runtime.logs / f"{self.PROCESS_NAME}.log"
                raise FixtureError(
                    f"host tunnel exited before becoming ready; log: {log_file}\n"
                    f"{log_file.read_text()}"
                )
            try:
                self.runtime.docker.run(
                    "exec", container, "nc", "-z", "127.0.0.1", self.config.pgport,
                )
                return
            except FixtureError as error:
                if time.monotonic() >= deadline:
                    raise FixtureError(
                        "host PostgreSQL tunnel did not become ready"
                    ) from error
                time.sleep(0.1)
