"""Owned fixture only: no external forwarding, native TLS and OAuth wire."""
import http.client
import http.server
import json
import pathlib
import select
import socket
import ssl
import sys
import threading
import urllib.parse
from datetime import datetime, timedelta, timezone

config = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(config["root"])
inference = urllib.parse.urlsplit(config["inference"])
assert inference.hostname == "127.0.0.1"
log_lock = threading.Lock()
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(root / "server.pem", root / "server.key")
context.set_alpn_protocols(["http/1.1"])
verified = "fixture-xai-verified-token"
candidate = "fixture-xai-candidate-token"
quota_release = threading.Event()


def is_catalog_mode():
    return config["mode"] in {"api", "quota", "quota-wait"}


def event(kind, **fields):
    with log_lock:
        with open(config["log"], "a", encoding="utf-8") as output:
            output.write(json.dumps({"kind": kind, **fields}) + "\n")


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def reply(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True

    def json_reply(self, status, value):
        self.reply(status, json.dumps(value).encode())

    def do_CONNECT(self):
        allowed = {"auth.x.ai:443"}
        if is_catalog_mode():
            allowed.add("cli-chat-proxy.grok.com:443")
        if self.path not in allowed:
            event("forbidden", target=self.path)
            self.json_reply(403, {})
            return
        event("connect", target=self.path)
        self.send_response(200)
        self.end_headers()
        self.wfile.flush()
        try:
            self.connection = context.wrap_socket(self.connection, server_side=True)
            self.rfile = self.connection.makefile("rb")
            self.wfile = self.connection.makefile("wb", buffering=0)
            self.close_connection = False
            while not self.close_connection:
                self.handle_one_request()
        except (ssl.SSLError, OSError):
            event("tls_closed")
            self.close_connection = True

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        assert 0 <= length <= 1024 * 1024
        return self.rfile.read(length)

    def forward(self):
        target = urllib.parse.urlsplit(self.path)
        if target.scheme != "http" or target.netloc != inference.netloc:
            event("forbidden", target=self.path)
            self.json_reply(403, {})
            return
        if config["mode"] == "api":
            expected = "fixture-provider-secret"
        else:
            expected = verified if config["mode"] == "refresh" else candidate
        event("inference", authorization=self.headers.get("Authorization") == "Bearer " + expected)
        if config["mode"] == "api" and self.headers.get("Upgrade", "").lower() == "websocket":
            with socket.create_connection(("127.0.0.1", inference.port), timeout=5) as upstream:
                head = f"{self.command} {target.path or '/'} HTTP/1.1\r\n"
                head += "".join(f"{name}: {value}\r\n" for name, value in self.headers.items() if name.lower() != "proxy-connection")
                upstream.sendall((head + "\r\n").encode("latin-1"))
                self.bridge(upstream)
            return
        headers = {name: value for name, value in self.headers.items() if name.lower() not in {"proxy-connection", "connection"}}
        connection = http.client.HTTPConnection("127.0.0.1", inference.port, timeout=5)
        try:
            connection.request(self.command, target.path or "/", self.body(), headers)
            response = connection.getresponse()
            self.reply(response.status, response.read(), response.getheader("Content-Type", "application/octet-stream"))
        finally:
            connection.close()

    def bridge(self, upstream):
        # Only the exact owned loopback endpoint above can reach this tunnel.
        peers = [self.connection, upstream]
        while True:
            ready, _, _ = select.select(peers, [], [], 30)
            if not ready:
                break
            for peer in ready:
                data = peer.recv(65536)
                if not data:
                    self.close_connection = True
                    return
                (upstream if peer is self.connection else self.connection).sendall(data)
        self.close_connection = True

    def catalog(self):
        authorization = self.headers.get("Authorization")
        valid = authorization in {"Bearer fixture-provider-secret", "Bearer fixture-forbidden-secret"}
        event("catalog", path=self.path, authorization=valid,
              subject=self.headers.get("x-userid") == config["subject"],
              token_auth=self.headers.get("x-xai-token-auth") == "xai-grok-cli")
        if not valid:
            self.json_reply(401, {})
        elif self.path == "/v1/models":
            self.json_reply(200, {"object": "list", "data": [{
                "id": "grok-4.5-catalog-entry", "model": "grok-4.5", "name": "Grok 4.5",
                "contextWindow": 1000000, "maxCompletionTokens": 131072,
                "apiBackend": "responses", "supportedInApi": True,
            }]})
        elif self.path == "/v1/billing?format=credits":
            if config["mode"] == "quota-wait" and not quota_release.wait(15):
                self.json_reply(504, {})
                return
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = lambda value: value.isoformat().replace("+00:00", "Z")
            self.json_reply(200, {"config": {"creditUsagePercent": 25,
                "currentPeriod": {"type": "USAGE_PERIOD_TYPE_WEEKLY",
                    "start": stamp(now - timedelta(days=1)), "end": stamp(now + timedelta(days=6))},
                "futureWindow": {"kind": "rolling"}}})
        elif self.path == "/v1/user?include=subscription":
            self.json_reply(200, {"userId": "fixture-native-user", "email": "fixture-private@example.test", "subscriptionTier": "SuperGrokPro"})
        else:
            event("forbidden", target=self.path)
            self.json_reply(403, {})

    def do_GET(self):
        if not isinstance(self.connection, ssl.SSLSocket):
            self.forward()
            return
        if is_catalog_mode() and self.headers.get("Host") == "cli-chat-proxy.grok.com":
            self.catalog()
            return
        if self.path == "/.well-known/openid-configuration":
            event("discovery")
            self.json_reply(200, {
                "issuer": "https://foreign.invalid" if config["mode"] == "issuer" else "https://auth.x.ai",
                "authorization_endpoint": "https://auth.x.ai/oauth2/authorize",
                "token_endpoint": "https://auth.x.ai/oauth2/token",
                "jwks_uri": "https://auth.x.ai/.well-known/jwks.json",
                "userinfo_endpoint": "https://auth.x.ai/oauth2/userinfo",
                "id_token_signing_alg_values_supported": ["ES256"],
            })
        elif self.path == "/oauth2/userinfo":
            expected = verified if config["mode"] == "refresh" else candidate
            valid = self.headers.get("Authorization") == "Bearer " + expected
            event("userinfo", authorization=valid)
            if not valid:
                self.json_reply(401, {})
                return
            self.json_reply(200, {"sub": "foreign-native-subject" if config["mode"] == "subject" else config["subject"]})
        else:
            event("forbidden", target=self.path)
            self.json_reply(403, {})

    def do_POST(self):
        if not isinstance(self.connection, ssl.SSLSocket) and config["mode"] == "quota-wait" and self.path == "/__fixture/release":
            quota_release.set()
            self.json_reply(200, {})
            return
        if not isinstance(self.connection, ssl.SSLSocket):
            self.forward()
            return
        if self.path != "/oauth2/token" or config["mode"] != "refresh":
            event("forbidden", target=self.path)
            self.json_reply(403, {})
            return
        form = urllib.parse.parse_qs(self.body().decode())
        valid = form.get("grant_type") == ["refresh_token"] and form.get("refresh_token") == ["fixture-refresh-token"] and form.get("client_id") == ["b1a00492-073a-47ea-816f-4c329264a828"]
        event("refresh", validated=valid)
        if not valid:
            self.json_reply(400, {})
            return
        self.json_reply(200, {"access_token": verified, "refresh_token": "fixture-xai-verified-refresh", "token_type": "Bearer", "expires_in": 3600, "scope": "openid profile email offline_access grok-cli:access api:access"})


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True


if config.get("direct"):
    # This address exists only on the offline test container's loopback device.
    # Keep official DNS policy and TLS verification active in the native client.
    server = Server(("93.184.216.34", 443), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    url = "https://auth.x.ai"
else:
    server = Server(("127.0.0.1", 0), Handler)
    url = "http://127.0.0.1:" + str(server.server_port)
print(json.dumps({"url": url}), flush=True)
server.serve_forever()
