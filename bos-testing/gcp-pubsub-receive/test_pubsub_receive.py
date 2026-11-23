#!/usr/bin/env python3
"""
test_pubsub_receive.py

Unit test suite for GCP Pub/Sub Receive fixes, pubsub_engine, and web server.
"""

import json
import time
import unittest
from unittest.mock import MagicMock

from pubsub_engine import (
    AckMode,
    FilterConfig,
    MessageBuffer,
    MetricsTracker,
    TopicTree,
    parse_message_payload,
    parse_pubsub_message,
)
from web.server import start_server
from pubsub_engine import PubSubManager
import urllib.request


class TestPubSubEngine(unittest.TestCase):
    def test_parse_message_payload_valid_udmi(self):
        sample = {
            "timestamp": "2024-05-19T20:16:29.125Z",
            "version": "1.4.1",
            "points": {
                "discharge_air_temperature_sensor": {"present_value": 24.27},
                "occupancy_status": {"present_value": "active"},
            },
        }
        data_bytes = json.dumps(sample).encode("utf-8")
        text, parsed_json, points = parse_message_payload(data_bytes)

        self.assertIsNotNone(parsed_json)
        self.assertEqual(len(points), 2)
        self.assertEqual(points["discharge_air_temperature_sensor"]["present_value"], 24.27)
        self.assertEqual(points["occupancy_status"]["present_value"], "active")

    def test_parse_message_payload_invalid_json(self):
        raw_bytes = b"non-json-raw-telemetry-bytes"
        text, parsed_json, points = parse_message_payload(raw_bytes)
        self.assertEqual(text, "non-json-raw-telemetry-bytes")
        self.assertIsNone(parsed_json)
        self.assertEqual(points, {})

    def test_parse_pubsub_message_missing_attributes(self):
        mock_msg = MagicMock()
        mock_msg.data = b'{"timestamp": "2024-01-01T00:00:00Z"}'
        mock_msg.attributes = {}  # Empty attributes - must not throw KeyError!
        mock_msg.message_id = "msg-12345"
        mock_msg.ordering_key = ""
        mock_msg.delivery_attempt = 1

        parsed = parse_pubsub_message(mock_msg)
        self.assertEqual(parsed["id"], "msg-12345")
        self.assertEqual(parsed["deviceId"], "")
        self.assertEqual(parsed["gatewayId"], "")
        self.assertEqual(parsed["deviceRegistryId"], "")

    def test_filter_gateway_substring_fix(self):
        # Previously, "DDC-123" in "DDC" was False (bug!). Now target in val must be True.
        cfg = FilterConfig(gateway_id="DDC")
        msg = {
            "attributes": {
                "deviceId": "FCU-1",
                "gatewayId": "DDC-108",
            },
            "points": {},
        }
        self.assertTrue(cfg.matches(msg))

        cfg_strict = FilterConfig(gateway_id="DDC-999")
        self.assertFalse(cfg_strict.matches(msg))

    def test_filter_registry_check(self):
        # Previously registry was parsed in CLI but never checked in callback!
        cfg = FilterConfig(registry_id="building-a")
        msg1 = {"attributes": {"deviceRegistryId": "building-a"}, "points": {}}
        msg2 = {"attributes": {"deviceRegistryId": "building-b"}, "points": {}}
        self.assertTrue(cfg.matches(msg1))
        self.assertFalse(cfg.matches(msg2))

    def test_filter_regex(self):
        cfg = FilterConfig(device_id=r"^FCU-\d+$", use_regex=True)
        msg_match = {"attributes": {"deviceId": "FCU-12"}, "points": {}}
        msg_no_match = {"attributes": {"deviceId": "VAV-12"}, "points": {}}
        self.assertTrue(cfg.matches(msg_match))
        self.assertFalse(cfg.matches(msg_no_match))

    def test_filter_subfolder_and_type(self):
        cfg = FilterConfig(subfolder="pointset", subtype="state")
        msg1 = {"attributes": {"subFolder": "pointset", "subType": "state"}, "points": {}}
        msg2 = {"attributes": {"subFolder": "pointset", "subType": "events"}, "points": {}}
        self.assertTrue(cfg.matches(msg1))
        self.assertFalse(cfg.matches(msg2))

    def test_filter_point_name(self):
        cfg = FilterConfig(point_name="temperature")
        msg_has_pt = {"attributes": {}, "points": {"temperature": 21.5, "humidity": 50}}
        msg_no_pt = {"attributes": {}, "points": {"humidity": 50}}
        self.assertTrue(cfg.matches(msg_has_pt))
        self.assertFalse(cfg.matches(msg_no_pt))


class TestTopicTree(unittest.TestCase):
    def test_topic_tree_hierarchy(self):
        tree = TopicTree(mode="standard")
        msg = {
            "id": "1",
            "received_at": time.time(),
            "attributes": {
                "deviceRegistryId": "REG-1",
                "gatewayId": "GW-1",
                "deviceId": "FCU-1",
                "subFolder": "pointset",
            },
            "points": {
                "temp": {"present_value": 23.5},
            },
        }
        updated_paths = tree.update(msg)
        self.assertIn("REG-1/GW-1/FCU-1/pointset/temp", updated_paths)

        tree_dict = tree.to_dict()
        self.assertEqual(len(tree_dict["children"]), 1)
        reg_node = tree_dict["children"][0]
        self.assertEqual(reg_node["key"], "REG-1")
        gw_node = reg_node["children"][0]
        self.assertEqual(gw_node["key"], "GW-1")
        dev_node = gw_node["children"][0]
        self.assertEqual(dev_node["key"], "FCU-1")


class TestMessageBuffer(unittest.TestCase):
    def test_buffer_capacity_and_point_history(self):
        buf = MessageBuffer(max_capacity=5)
        for i in range(10):
            msg = {
                "id": f"msg-{i}",
                "deviceId": "FCU-1",
                "received_at": time.time(),
                "text": f'{{"val": {i}}}',
                "points": {"temp": i * 1.5},
                "attributes": {"deviceId": "FCU-1"},
            }
            buf.add(msg)

        # Ensure buffer capacity capped at 5
        self.assertEqual(len(buf), 5)
        # Old messages should be evicted
        self.assertIsNone(buf.get_by_id("msg-0"))
        self.assertIsNotNone(buf.get_by_id("msg-9"))

        # Check point history
        history = buf.get_point_history("FCU-1", "temp")
        self.assertEqual(len(history), 10)
        self.assertEqual(history[-1]["v"], 9 * 1.5)

        # Check export
        json_export = buf.export(format_type="json")
        self.assertTrue(len(json.loads(json_export)) == 5)


class TestWebServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manager = PubSubManager()
        # Prepopulate with a sample message
        sample_msg = {
            "id": "test-msg-1",
            "publish_time": "2024-05-19T20:16:29Z",
            "received_at": time.time(),
            "deviceId": "DDC-1",
            "gatewayId": "GW-1",
            "deviceRegistryId": "REG-1",
            "subFolder": "pointset",
            "subType": "",
            "size": 128,
            "text": '{"points": {"temp": {"present_value": 22.0}}}',
            "json": {"points": {"temp": {"present_value": 22.0}}},
            "points": {"temp": {"present_value": 22.0}},
            "attributes": {"deviceId": "DDC-1", "gatewayId": "GW-1"},
        }
        cls.manager.tree.update(sample_msg)
        cls.manager.buffer.add(sample_msg)

        cls.server = start_server(cls.manager, host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        import threading
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_http_status_endpoint(self):
        url = f"http://127.0.0.1:{self.port}/api/status"
        with urllib.request.urlopen(url) as response:
            self.assertEqual(response.status, 200)
            data = json.loads(response.read().decode("utf-8"))
            self.assertIn("isConnected", data)
            self.assertIn("stats", data)

    def test_http_tree_endpoint(self):
        url = f"http://127.0.0.1:{self.port}/api/tree"
        with urllib.request.urlopen(url) as response:
            self.assertEqual(response.status, 200)
            data = json.loads(response.read().decode("utf-8"))
            self.assertEqual(data["type"], "root")
            self.assertTrue(len(data["children"]) > 0)

    def test_http_static_assets(self):
        for path in ["/", "/app.css", "/app.js"]:
            url = f"http://127.0.0.1:{self.port}{path}"
            with urllib.request.urlopen(url) as response:
                self.assertEqual(response.status, 200)
                content = response.read().decode("utf-8")
                self.assertTrue(len(content) > 0)
                if path == "/":
                    self.assertIn("modalConnect", content)
                    self.assertIn("presetChipsContainer", content)
                    self.assertIn("btnEmptyConnect", content)
                    self.assertIn("infoConfigureBadge", content)

    def test_http_connect_validation_and_disconnect(self):
        # Empty project/subscription should return failure message
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/connect",
            data=json.dumps({"projectId": "", "subscriptionId": ""}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as response:
            self.assertEqual(response.status, 200)
            data = json.loads(response.read().decode("utf-8"))
            self.assertFalse(data["success"])
            self.assertIn("required", data["message"].lower())

        # Test disconnect
        req_disc = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/disconnect",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req_disc) as response:
            self.assertEqual(response.status, 200)
            data = json.loads(response.read().decode("utf-8"))
            self.assertTrue(data["success"])
            self.assertFalse(data["status"]["isConnected"])


if __name__ == "__main__":
    unittest.main()

