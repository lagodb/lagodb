#!/usr/bin/env python3
"""Regression REST adapter for client-visible config and commit failures."""

import argparse
import http.client
import json
import pathlib
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional


class RestCatalogProxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    upstream: urllib.parse.ParseResult
    s3_endpoint: Optional[str]
    s3_region: Optional[str]
    require_vended_credentials: bool
    reject_transaction_commit: bool

    def do_GET(self) -> None:
        self._forward()

    def do_HEAD(self) -> None:
        self._forward()

    def do_POST(self) -> None:
        if (
            self.reject_transaction_commit
            and urllib.parse.urlparse(self.path)
            .path.rstrip("/")
            .endswith("/transactions/commit")
        ):
            self._send_json(
                503,
                {
                    "error": {
                        "message": "regression fixture rejected publication",
                        "type": "CommitStateUnknown",
                        "code": 503,
                    }
                },
            )
            return
        self._forward()

    def do_DELETE(self) -> None:
        self._forward()

    def _forward(self) -> None:
        if self._table_request_without_vending():
            self._send_json(
                400,
                {
                    "error": {
                        "message": "vended credentials were not requested",
                        "type": "BadRequestException",
                        "code": 400,
                    }
                },
            )
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else None
        connection = http.client.HTTPConnection(
            self.upstream.hostname,
            self.upstream.port,
            timeout=10,
        )
        headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower()
            not in {"host", "connection", "content-length", "accept-encoding"}
        }
        upstream_path = self.path
        if self.upstream.path and self.upstream.path != "/":
            upstream_path = self.upstream.path.rstrip("/") + self.path
        connection.request(self.command, upstream_path, body=body, headers=headers)
        response = connection.getresponse()
        response_body = response.read()
        response_headers = response.getheaders()
        response_body = self._catalog_config(response.status, response_body)

        self.send_response(response.status)
        for name, value in response_headers:
            if name.lower() not in {
                "connection",
                "content-length",
                "transfer-encoding",
            }:
                self.send_header(name, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(response_body)
        connection.close()

    def _table_request_without_vending(self) -> bool:
        if not self.require_vended_credentials:
            return False
        path = urllib.parse.urlparse(self.path).path
        if "/tables" not in path:
            return False
        delegation = self.headers.get("X-Iceberg-Access-Delegation", "")
        return "vended-credentials" not in {
            value.strip() for value in delegation.split(",")
        }

    def _catalog_config(self, status: int, body: bytes) -> bytes:
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        if (
            self.command != "GET"
            or path != "/v1/config"
            or not 200 <= status < 300
            or self.s3_endpoint is None
        ):
            return body
        payload = json.loads(body)
        defaults = payload.setdefault("defaults", {})
        defaults.setdefault("s3.endpoint", self.s3_endpoint)
        defaults.setdefault("s3.path-style-access", "true")
        if self.s3_region is not None:
            defaults.setdefault("s3.region", self.s3_region)
        return json.dumps(payload, separators=(",", ":")).encode()

    def _send_json(self, status: int, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--port-file", required=True)
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-region")
    parser.add_argument("--require-vended-credentials", action="store_true")
    parser.add_argument("--reject-transaction-commit", action="store_true")
    args = parser.parse_args()
    upstream = urllib.parse.urlparse(args.upstream)
    if upstream.scheme != "http" or not upstream.hostname or not upstream.port:
        raise SystemExit("upstream must be an absolute HTTP URL with a port")
    RestCatalogProxy.upstream = upstream
    RestCatalogProxy.s3_endpoint = args.s3_endpoint
    RestCatalogProxy.s3_region = args.s3_region
    RestCatalogProxy.require_vended_credentials = args.require_vended_credentials
    RestCatalogProxy.reject_transaction_commit = args.reject_transaction_commit
    server = ThreadingHTTPServer(("127.0.0.1", 0), RestCatalogProxy)
    pathlib.Path(args.port_file).write_text(
        f"{server.server_port}\n", encoding="ascii"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
