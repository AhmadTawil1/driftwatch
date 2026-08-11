"""A small local HTTP server standing in for an OpenAI-compatible
provider — used only by tests, no real network and no real cost. Runs
on a background thread so tests hit it over real TCP, which proves the
client's retry/timeout handling honestly instead of mocking the
transport object away.

Usage:
    with MockProviderServer(mode="normal") as server:
        client = ProviderClient("openai", server.base_url, "fake-key", concurrency_limit=5)
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _success_body(model_version: str = "gpt-4o-mini-mock", prompt_tokens: int = 10, completion_tokens: int = 5) -> bytes:
    return json.dumps(
        {
            "model": model_version,
            "choices": [{"message": {"content": "mock response"}}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens},
        }
    ).encode()


class MockProviderServer:
    def __init__(self, mode: str = "normal", fail_times: int = 2, sleep_s: float = 2.0):
        self.mode = mode
        self.fail_times = fail_times  # for "rate_limited_then_success"
        self.sleep_s = sleep_s  # for "timeout" / "slow_normal"
        self.call_count = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self._lock = threading.Lock()

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self.base_url = f"http://127.0.0.1:{self._httpd.server_address[1]}"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def __enter__(self) -> "MockProviderServer":
        self._thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()

    def _make_handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # keep test output quiet

            def do_POST(self):
                with server._lock:
                    server.call_count += 1
                    server.in_flight += 1
                    server.max_in_flight = max(server.max_in_flight, server.in_flight)
                try:
                    self._respond()
                finally:
                    with server._lock:
                        server.in_flight -= 1

            def _write(self, status: int, body: bytes, content_length: int | None = None) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(content_length if content_length is not None else len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _respond(self) -> None:
                mode = server.mode

                if mode == "normal":
                    self._write(200, _success_body())

                elif mode == "rate_limited_then_success":
                    if server.call_count <= server.fail_times:
                        self._write(429, b'{"error": "rate limited"}')
                    else:
                        self._write(200, _success_body())

                elif mode == "timeout":
                    time.sleep(server.sleep_s)
                    self._write(200, _success_body())

                elif mode == "malformed_json":
                    self._write(200, b"not valid json {")

                elif mode == "truncated":
                    full_body = _success_body()
                    half = full_body[: len(full_body) // 2]
                    # Claim a Content-Length longer than what we actually
                    # send, then drop the connection — the client should
                    # see this as an incomplete read, not a clean parse.
                    self._write(200, half, content_length=len(full_body) + 200)
                    self.close_connection = True

                elif mode == "slow_normal":
                    time.sleep(server.sleep_s)
                    self._write(200, _success_body())

                else:
                    raise ValueError(f"unknown mock mode: {mode}")

        return Handler
