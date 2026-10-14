"""Owned fixture only: no external forwarding, native TLS and OAuth wire."""
import http.client
import http.server
import json
import pathlib
import ssl
import sys
import threading
import urllib.parse

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
        if self.path != "auth.x.ai:443":
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
        expected = verified if config["mode"] == "refresh" else candidate
        event("inference", authorization=self.headers.get("Authorization") == "Bearer " + expected)
        headers = {name: value for name, value in self.headers.items() if name.lower() not in {"proxy-connection", "connection"}}
        connection = http.client.HTTPConnection("127.0.0.1", inference.port, timeout=5)
        try:
            connection.request(self.command, target.path or "/", self.body(), headers)
            response = connection.getresponse()
            self.reply(response.status, response.read(), response.getheader("Content-Type", "application/octet-stream"))
        finally:
            connection.close()

    def do_GET(self):
        if not isinstance(self.connection, ssl.SSLSocket):
            self.forward()
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


server = Server(("127.0.0.1", 0), Handler)
print(json.dumps({"url": "http://127.0.0.1:" + str(server.server_port)}), flush=True)
server.serve_forever()
