from __future__ import annotations

import io
import json
import unittest
from unittest.mock import patch
import urllib.error

from cloud_training.data_pipelines import alpaca_news


class TestIssue21AlpacaNews(unittest.TestCase):
    def test_fetch_news_page_retries_once_on_429_with_retry_after(self) -> None:
        calls = []

        def fake_urlopen(req, timeout=60):  # type: ignore[override]
            calls.append(req.full_url)
            if len(calls) == 1:
                raise urllib.error.HTTPError(
                    req.full_url,
                    429,
                    "Too Many Requests",
                    {"Retry-After": "0"},
                    io.BytesIO(b""),
                )
            return io.BytesIO(json.dumps({"news": [], "next_page_token": None}).encode("utf-8"))

        with patch("cloud_training.data_pipelines.alpaca_news.urllib.request.urlopen", side_effect=fake_urlopen), patch(
            "cloud_training.data_pipelines.alpaca_news.time.sleep"
        ) as sleep_mock:
            payload = alpaca_news._fetch_news_page("https://example.test/news", {"X-Test": "1"})

        self.assertEqual(payload["news"], [])
        self.assertEqual(len(calls), 2)
        sleep_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
