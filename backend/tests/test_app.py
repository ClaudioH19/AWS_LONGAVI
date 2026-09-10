import json
import os
import tempfile
import unittest


TEST_DIRECTORY = tempfile.TemporaryDirectory()
os.environ["DB_PATH"] = os.path.join(TEST_DIRECTORY.name, "weather_test.db")
os.environ["LOG_FILE"] = ""
os.environ["DRAGONFLY_URL"] = ""
os.environ["ENABLE_DIAGNOSTIC_ROUTES"] = "false"
os.environ["MIN_FREE_DISK_BYTES"] = "0"
os.environ["TRUST_PROXY_COUNT"] = "0"
os.environ["INGEST_ALLOWED_IPS"] = ""

from backend.app.main import app  # noqa: E402
from backend.app.realtime import socketio  # noqa: E402


VALID_PAYLOAD = {
    "": 655,
    "DeviceID": "station-test",
    "Timestamp": "2026-08-30 12:00:00",
    "ch0": 34,
    "ch1": 180,
    "ch2": 215,
    "ch3": 12,
    "ch4": 560,
}


class WeatherApplicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app.config.update(TESTING=True)
        cls.client = app.test_client()

    def test_01_liveness_and_readiness_are_independent_of_station_data(self):
        self.assertEqual(self.client.get("/health/live").status_code, 200)
        self.assertEqual(self.client.get("/health/ready").status_code, 200)
        station = self.client.get("/status/station")
        self.assertEqual(station.status_code, 200)
        self.assertEqual(station.get_json()["status"], "no_data")

    def test_02_invalid_payload_is_rejected_without_poisoning_database(self):
        invalid_payloads = (
            "{bad",
            '{"ch0": 10, "ch0": 20}',
            '{"DeviceID": {"unexpected": true}, "ch0": 10}',
            json.dumps({**VALID_PAYLOAD, "upload": "<script>alert(1)</script>"}),
            json.dumps({key: value for key, value in VALID_PAYLOAD.items() if key != "ch4"}),
            json.dumps({**VALID_PAYLOAD, "DeviceID": "<script>alert(1)</script>"}),
            json.dumps({**VALID_PAYLOAD, "DeviceID": "=HYPERLINK(\"https://invalid\")"}),
        )
        for payload in invalid_payloads:
            response = self.client.post("/weather", data=payload, content_type="application/json")
            self.assertEqual(response.status_code, 400, payload)
        self.assertEqual(self.client.get("/health").get_json()["db_total_registros"], 0)

    def test_02b_ingestion_requires_json_content_type(self):
        response = self.client.post("/weather", data=json.dumps(VALID_PAYLOAD), content_type="text/plain")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.client.get("/health").get_json()["db_total_registros"], 0)

    def test_02c_ingestion_rejects_sources_outside_the_allowlist(self):
        import backend.app.routes.weather as weather_routes
        from ipaddress import ip_network

        original_networks = weather_routes.INGEST_ALLOWED_NETWORKS
        weather_routes.INGEST_ALLOWED_NETWORKS = (ip_network("203.0.113.10/32"),)
        try:
            response = self.client.post(
                "/weather",
                data=json.dumps(VALID_PAYLOAD),
                content_type="application/json",
                environ_base={"REMOTE_ADDR": "198.51.100.20"},
            )
            self.assertEqual(response.status_code, 403)
        finally:
            weather_routes.INGEST_ALLOWED_NETWORKS = original_networks

    def test_03_valid_payload_is_persisted(self):
        response = self.client.post(
            "/weather",
            data=json.dumps(VALID_PAYLOAD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.get("/health").get_json()["db_total_registros"], 1)

    def test_04_query_limits_and_dates_are_validated(self):
        self.assertEqual(self.client.get("/weather/range?limit=-1").status_code, 400)
        self.assertEqual(self.client.get("/weather/range?limit=5001").status_code, 400)
        self.assertEqual(self.client.get("/weather/range?desde=no-fecha").status_code, 400)
        response = self.client.get("/weather/range?limit=1&offset=0")
        self.assertEqual(response.status_code, 200)
        self.assertIn("has_more", response.get_json())

    def test_05_sensitive_diagnostic_routes_are_disabled(self):
        for path in ("/weather/raw", "/weather/raw/db", "/weather/count", "/weather/devices"):
            self.assertEqual(self.client.get(path).status_code, 404, path)

    def test_06_health_does_not_expose_storage_paths(self):
        payload = self.client.get("/health").get_json()
        self.assertNotIn("db_path", payload)
        self.assertNotIn("db_directory", payload)

    def test_07_unknown_routes_return_real_404(self):
        for path in ("/no-existe", "/.env", "/backend/app/config.py", "/weather/no-existe"):
            response = self.client.get(path)
            try:
                self.assertEqual(response.status_code, 404, path)
            finally:
                response.close()

    def test_08_responses_include_security_and_request_headers(self):
        response = self.client.get("/health/live", headers={"X-Request-ID": "test-request"})
        self.assertEqual(response.headers["X-Request-ID"], "test-request")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        csp = response.headers["Content-Security-Policy"]
        self.assertIn("frame-ancestors 'none'", csp)
        self.assertIn("style-src 'self' 'unsafe-inline'", csp)
        self.assertIn("script-src 'self'", csp)
        self.assertNotIn("script-src 'self' 'unsafe-inline'", csp)

    def test_09_realtime_event_works_without_external_message_queue(self):
        realtime_client = socketio.test_client(app, flask_test_client=self.client)
        try:
            self.assertTrue(realtime_client.is_connected())
            response = self.client.post(
                "/weather",
                data=json.dumps({**VALID_PAYLOAD, "Timestamp": "2026-08-30 12:01:00"}),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 200)
            event_names = [event["name"] for event in realtime_client.get_received()]
            self.assertIn("weather:reading", event_names)
        finally:
            realtime_client.disconnect()


if __name__ == "__main__":
    unittest.main()
