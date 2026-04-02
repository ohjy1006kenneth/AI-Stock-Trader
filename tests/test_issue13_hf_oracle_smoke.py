from __future__ import annotations

import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from pi_edge.execution.decision_adapter import translate_oracle_response_to_execution_intents
from pi_edge.network.hf_api_client import call_oracle, fetch_endpoint_ready_manifest, validate_response_against_ready_manifest


class _StubHFHandler(BaseHTTPRequestHandler):
    response_payload: dict = {}
    ready_manifest_payload: dict = {}
    captured_headers: dict[str, str] = {}
    captured_body: dict = {}

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/ready.json":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(_StubHFHandler.ready_manifest_payload).encode("utf-8"))

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length).decode("utf-8")
        _StubHFHandler.captured_headers = {k: v for k, v in self.headers.items()}
        _StubHFHandler.captured_body = json.loads(raw_body)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(_StubHFHandler.response_payload).encode("utf-8"))

    def log_message(self, format: str, *args) -> None:
        return


class TestIssue13HFOracleSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _StubHFHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def test_edge_can_call_hf_validate_response_and_translate_to_execution_intent(self) -> None:
        request_payload = {
            "portfolio": {
                "cash": 400.0,
                "positions": [
                    {"ticker": "AAPL", "qty": 1, "entry_price": 90.0},
                ],
            },
            "universe": [
                {
                    "ticker": "AAPL",
                    "history": [
                        {"date": "2026-04-01", "open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0, "volume": 1000},
                    ],
                    "news": [],
                },
                {
                    "ticker": "MSFT",
                    "history": [
                        {"date": "2026-04-01", "open": 49.0, "high": 51.0, "low": 48.0, "close": 50.0, "volume": 2000},
                    ],
                    "news": [],
                },
            ],
        }
        price_snapshot = {
            "items": [
                {"ticker": "AAPL", "close": 100.0},
                {"ticker": "MSFT", "close": 50.0},
            ]
        }
        broker_snapshot = {
            "account": {"equity": "1000", "cash": "400"},
            "positions": [
                {"symbol": "AAPL", "qty": "1", "avg_entry_price": "90.0"},
            ],
        }

        _StubHFHandler.ready_manifest_payload = {
            "manifest_version": "oracle_endpoint_ready_v1",
            "repo_id": "FunkMonk87/ai-stock-trader-oracle",
            "endpoint": "cloud_oracle",
            "approved_bundle": {
                "artifact_name": "stub-oracle-v1",
                "repo_path": "bundles/stub-oracle-v1.bundle.json",
                "bundle_manifest_path": "manifests/bundles/stub-oracle-v1.manifest.json",
                "approved_manifest_path": "channels/approved/manifest.json",
            },
        }
        _StubHFHandler.response_payload = {
            "model_version": "stub-oracle-v1",
            "generated_at": "2026-04-02T04:00:00+00:00",
            "request_id": "req-stub-smoke",
            "predictions": [
                {"ticker": "AAPL", "target_weight": 0.2, "confidence": 0.9, "signal_type": "long"},
                {"ticker": "MSFT", "target_weight": 0.1, "confidence": 0.8, "signal_type": "long"},
            ],
        }

        with patch.dict(os.environ, {
            "HF_INFERENCE_URL": self.base_url,
            "HF_API_TOKEN": "stub-token",
            "HF_MODEL_REPO_READY_MANIFEST_URL": f"{self.base_url}/ready.json",
            "HF_ENFORCE_READY_MANIFEST": "true",
        }, clear=False):
            oracle_response = call_oracle(request_payload)
            ready_manifest = fetch_endpoint_ready_manifest(token="stub-token")

        self.assertEqual(oracle_response["model_version"], "stub-oracle-v1")
        self.assertEqual(_StubHFHandler.captured_headers.get("Authorization"), "Bearer stub-token")
        self.assertIn("request_id", _StubHFHandler.captured_body)
        self.assertEqual(_StubHFHandler.captured_body["inputs"], request_payload)
        self.assertEqual(ready_manifest["approved_bundle"]["artifact_name"], "stub-oracle-v1")

        intents = translate_oracle_response_to_execution_intents(
            oracle_response=oracle_response,
            broker_snapshot=broker_snapshot,
            price_snapshot=price_snapshot,
        )
        by_ticker = {row["ticker"]: row for row in intents}

        self.assertEqual(by_ticker["AAPL"]["action"], "BUY")
        self.assertEqual(by_ticker["AAPL"]["order_qty"], 1)
        self.assertEqual(by_ticker["MSFT"]["action"], "BUY")
        self.assertEqual(by_ticker["MSFT"]["order_qty"], 2)
        self.assertTrue(all("action" in row and "order_qty" in row for row in intents))

    def test_ready_manifest_validation_detects_model_version_mismatch(self) -> None:
        ready_manifest = {
            "manifest_version": "oracle_endpoint_ready_v1",
            "repo_id": "FunkMonk87/ai-stock-trader-oracle",
            "endpoint": "cloud_oracle",
            "approved_bundle": {
                "artifact_name": "expected-bundle",
                "repo_path": "bundles/expected-bundle.bundle.json",
                "bundle_manifest_path": "manifests/bundles/expected-bundle.manifest.json",
                "approved_manifest_path": "channels/approved/manifest.json",
            },
        }
        response_payload = {
            "model_version": "different-bundle",
            "generated_at": "2026-04-02T04:00:00+00:00",
            "request_id": "req-stub-smoke",
            "predictions": [],
        }
        with self.assertRaisesRegex(RuntimeError, "hf_model_version_mismatch"):
            validate_response_against_ready_manifest(response_payload, ready_manifest)

    def test_call_oracle_only_warns_on_mismatch_when_enforcement_disabled(self) -> None:
        request_payload = {
            "portfolio": {"cash": 1000.0, "positions": []},
            "universe": [
                {
                    "ticker": "MSFT",
                    "history": [
                        {"date": "2026-04-01", "open": 49.0, "high": 51.0, "low": 48.0, "close": 50.0, "volume": 2000},
                    ],
                    "news": [],
                },
            ],
        }
        _StubHFHandler.ready_manifest_payload = {
            "manifest_version": "oracle_endpoint_ready_v1",
            "repo_id": "FunkMonk87/ai-stock-trader-oracle",
            "endpoint": "cloud_oracle",
            "approved_bundle": {
                "artifact_name": "expected-bundle",
                "repo_path": "bundles/expected-bundle.bundle.json",
                "bundle_manifest_path": "manifests/bundles/expected-bundle.manifest.json",
                "approved_manifest_path": "channels/approved/manifest.json",
            },
        }
        _StubHFHandler.response_payload = {
            "model_version": "different-bundle",
            "generated_at": "2026-04-02T04:00:00+00:00",
            "request_id": "req-stub-smoke",
            "predictions": [
                {"ticker": "MSFT", "target_weight": 0.1, "confidence": 0.8, "signal_type": "long"},
            ],
        }

        with patch.dict(os.environ, {
            "HF_INFERENCE_URL": self.base_url,
            "HF_API_TOKEN": "stub-token",
            "HF_MODEL_REPO_READY_MANIFEST_URL": f"{self.base_url}/ready.json",
            "HF_ENFORCE_READY_MANIFEST": "false",
        }, clear=False):
            oracle_response = call_oracle(request_payload)

        self.assertEqual(oracle_response["model_version"], "different-bundle")


if __name__ == "__main__":
    unittest.main()
