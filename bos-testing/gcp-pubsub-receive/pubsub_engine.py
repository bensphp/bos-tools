#!/usr/bin/env python3
"""
pubsub_engine.py

Core engine for GCP Pub/Sub message receiving, filtering, tree building,
and subscription management. Designed to be shared by both the CLI and Local Web UI.
"""

from __future__ import annotations

import collections
import datetime
import enum
import json
import logging
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import SeekRequest, Timestamp

logger = logging.getLogger("pubsub_engine")


class AckMode(str, enum.Enum):
    PEEK = "peek"              # Do not acknowledge messages (safe monitoring)
    ACK_MATCHED = "matched"    # Only ack messages that match the active filter
    ACK_ALL = "all"            # Acknowledge all received messages


class FilterConfig:
    """Holds filtering criteria for Pub/Sub messages."""

    def __init__(
        self,
        device_id: str = "",
        registry_id: str = "",
        gateway_id: str = "",
        subfolder: str = "",
        subtype: str = "",
        point_name: str = "",
        use_regex: bool = False,
    ):
        self.device_id = device_id.strip()
        self.registry_id = registry_id.strip()
        self.gateway_id = gateway_id.strip()
        self.subfolder = subfolder.strip()
        self.subtype = subtype.strip()
        self.point_name = point_name.strip()
        self.use_regex = use_regex

        self._re_device = self._compile_regex(self.device_id) if use_regex else None
        self._re_registry = self._compile_regex(self.registry_id) if use_regex else None
        self._re_gateway = self._compile_regex(self.gateway_id) if use_regex else None
        self._re_subfolder = self._compile_regex(self.subfolder) if use_regex else None
        self._re_subtype = self._compile_regex(self.subtype) if use_regex else None
        self._re_point = self._compile_regex(self.point_name) if use_regex else None

    @staticmethod
    def _compile_regex(pattern: str) -> Optional[re.Pattern]:
        if not pattern:
            return None
        try:
            return re.compile(pattern)
        except re.error as e:
            logger.warning("Invalid regex pattern '%s': %s", pattern, e)
            return None

    def matches(self, parsed_msg: Dict[str, Any]) -> bool:
        """Evaluates whether a parsed message satisfies the filter."""
        attrs = parsed_msg.get("attributes", {})
        dev_id = attrs.get("deviceId", "")
        reg_id = attrs.get("deviceRegistryId", "")
        gw_id = attrs.get("gatewayId", "")
        folder = attrs.get("subFolder", "")
        msg_type = attrs.get("subType", "")

        def _match_val(target: str, val: str, compiled_re: Optional[re.Pattern]) -> bool:
            if not target:
                return True
            if self.use_regex:
                return bool(compiled_re.search(val)) if compiled_re else False
            return target in val

        if not _match_val(self.device_id, dev_id, self._re_device):
            return False
        if not _match_val(self.registry_id, reg_id, self._re_registry):
            return False
        if not _match_val(self.gateway_id, gw_id, self._re_gateway):
            return False

        # SubFolder & SubType: exact or regex if specified
        if self.subfolder:
            if self.use_regex:
                if not (self._re_subfolder and self._re_subfolder.search(folder)):
                    return False
            elif self.subfolder != folder:
                return False

        if self.subtype:
            if self.use_regex:
                if not (self._re_subtype and self._re_subtype.search(msg_type)):
                    return False
            elif self.subtype != msg_type:
                return False

        # Point name check if payload contains points
        if self.point_name:
            points = parsed_msg.get("points", {})
            if self.use_regex:
                if not any(self._re_point.search(k) for k in points.keys()) if self._re_point else False:
                    return False
            elif self.point_name not in points:
                return False

        return True


def parse_message_payload(data_bytes: bytes) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Safely parses message payload into text, JSON object (if valid), and points dictionary.
    Returns (text_payload, json_obj_or_none, points_dict).
    """
    text = ""
    try:
        text = data_bytes.decode("utf-8", errors="replace")
    except Exception:
        text = str(data_bytes)

    json_obj = None
    points = {}
    try:
        json_obj = json.loads(text)
        if isinstance(json_obj, dict):
            # Check for UDMI / telemetry points structure
            raw_points = json_obj.get("points")
            if isinstance(raw_points, dict):
                points = raw_points
            elif "present_value" in json_obj or "value" in json_obj:
                # Single point payload
                points = {"value": json_obj}
    except Exception:
        json_obj = None

    return text, json_obj, points


def parse_pubsub_message(message: pubsub_v1.subscriber.message.Message) -> Dict[str, Any]:
    """Extracts all attributes and payload safely from a pubsub message."""
    raw_data = message.data or b""
    text, json_obj, points = parse_message_payload(raw_data)

    # Attributes safely extracted
    attrs = dict(message.attributes or {})
    device_id = attrs.get("deviceId", "")
    gateway_id = attrs.get("gatewayId", "")
    registry_id = attrs.get("deviceRegistryId", "")
    sub_folder = attrs.get("subFolder", "")
    sub_type = attrs.get("subType", "")
    project_id = attrs.get("projectId", "")

    # Timestamp extraction: payload timestamp or pubsub publish time
    ts_str = ""
    if json_obj and isinstance(json_obj, dict) and "timestamp" in json_obj:
        ts_str = str(json_obj["timestamp"])
    elif hasattr(message, "publish_time") and message.publish_time:
        ts_str = message.publish_time.isoformat()
    else:
        ts_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    msg_id = getattr(message, "message_id", "") or str(time.time_ns())

    return {
        "id": msg_id,
        "publish_time": ts_str,
        "attributes": attrs,
        "deviceId": device_id,
        "gatewayId": gateway_id,
        "deviceRegistryId": registry_id,
        "subFolder": sub_folder,
        "subType": sub_type,
        "projectId": project_id,
        "size": len(raw_data),
        "text": text,
        "json": json_obj,
        "points": points,
        "ordering_key": getattr(message, "ordering_key", ""),
        "delivery_attempt": getattr(message, "delivery_attempt", 0),
        "received_at": time.time(),
    }


class TreeNode:
    """Represents a node in the hierarchical topic/device tree (MQTT Explorer style)."""

    def __init__(self, key: str, label: str, full_path: str, node_type: str = "node"):
        self.key = key
        self.label = label
        self.full_path = full_path
        self.node_type = node_type  # 'root', 'registry', 'gateway', 'device', 'subfolder', 'point'
        self.message_count = 0
        self.last_updated: float = 0.0
        self.last_value: Any = None
        self.last_message_id: str = ""
        self.children: Dict[str, TreeNode] = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "path": self.full_path,
            "type": self.node_type,
            "count": self.message_count,
            "lastUpdated": self.last_updated,
            "lastValue": self.last_value,
            "lastMessageId": self.last_message_id,
            "children": [c.to_dict() for c in sorted(self.children.values(), key=lambda x: x.label)],
        }


class TopicTree:
    """
    Constructs and maintains the hierarchical topic tree.
    Supports groupings:
      - 'standard': Registry -> Gateway -> Device -> SubFolder -> Point
      - 'device_first': Device -> SubFolder -> Point
    """

    def __init__(self, mode: str = "standard"):
        self.mode = mode
        self.root = TreeNode("root", "Root", "", "root")
        self._lock = threading.Lock()

    def update(self, msg: Dict[str, Any]) -> List[str]:
        """
        Inserts/updates nodes for the given message.
        Returns list of node paths that were updated (for live UI flash animations).
        """
        updated_paths = []
        attrs = msg.get("attributes", {})
        device_id = attrs.get("deviceId", "") or "unknown-device"
        gateway_id = attrs.get("gatewayId", "") or "default-gateway"
        registry_id = attrs.get("deviceRegistryId", "") or "default-registry"
        sub_folder = attrs.get("subFolder", "") or "default"
        sub_type = attrs.get("subType", "")
        folder_label = f"{sub_folder}/{sub_type}" if sub_type else sub_folder
        now = msg.get("received_at", time.time())
        msg_id = msg.get("id", "")

        # Determine path segments based on hierarchy mode
        if self.mode == "device_first":
            segments = [
                ("device", device_id, device_id),
                ("subfolder", folder_label, folder_label),
            ]
        else:
            segments = [
                ("registry", registry_id, registry_id),
                ("gateway", gateway_id, gateway_id),
                ("device", device_id, device_id),
                ("subfolder", folder_label, folder_label),
            ]

        with self._lock:
            current = self.root
            current.message_count += 1
            current.last_updated = now
            current_path = ""

            for ntype, seg_key, seg_label in segments:
                current_path = f"{current_path}/{seg_key}" if current_path else seg_key
                if seg_key not in current.children:
                    current.children[seg_key] = TreeNode(seg_key, seg_label, current_path, ntype)
                current = current.children[seg_key]
                current.message_count += 1
                current.last_updated = now
                current.last_message_id = msg_id
                updated_paths.append(current_path)

            # Record point nodes if message has points
            points = msg.get("points", {})
            for pt_name, pt_data in points.items():
                pt_path = f"{current_path}/{pt_name}"
                if pt_name not in current.children:
                    current.children[pt_name] = TreeNode(pt_name, pt_name, pt_path, "point")
                pt_node = current.children[pt_name]
                pt_node.message_count += 1
                pt_node.last_updated = now
                pt_node.last_message_id = msg_id

                # Extract preview value
                val = None
                if isinstance(pt_data, dict):
                    val = pt_data.get("present_value", pt_data.get("value"))
                else:
                    val = pt_data
                pt_node.last_value = val
                updated_paths.append(pt_path)

        return updated_paths

    def clear(self):
        with self._lock:
            self.root = TreeNode("root", "Root", "", "root")

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return self.root.to_dict()


class MessageBuffer:
    """Thread-safe circular ring buffer for recent messages."""

    def __init__(self, max_capacity: int = 5000):
        self.max_capacity = max_capacity
        self._buffer: collections.deque = collections.deque(maxlen=max_capacity)
        self._by_id: Dict[str, Dict[str, Any]] = {}
        self._point_history: Dict[str, collections.deque] = collections.defaultdict(
            lambda: collections.deque(maxlen=200)
        )
        self._lock = threading.Lock()

    def add(self, msg: Dict[str, Any]):
        with self._lock:
            msg_id = msg["id"]
            if len(self._buffer) == self.max_capacity:
                oldest = self._buffer[0]
                self._by_id.pop(oldest["id"], None)

            self._buffer.append(msg)
            self._by_id[msg_id] = msg

            # Track numeric point histories for sparklines
            dev_id = msg.get("deviceId", "")
            points = msg.get("points", {})
            ts = msg.get("received_at", time.time())
            for pt_name, pt_data in points.items():
                val = None
                if isinstance(pt_data, dict):
                    val = pt_data.get("present_value", pt_data.get("value"))
                else:
                    val = pt_data

                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    key = f"{dev_id}/{pt_name}"
                    self._point_history[key].append({"t": ts, "v": float(val)})

    def get_by_id(self, msg_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._by_id.get(msg_id)

    def get_point_history(self, device_id: str, point_name: str) -> List[Dict[str, Any]]:
        key = f"{device_id}/{point_name}"
        with self._lock:
            return list(self._point_history.get(key, []))

    def query(
        self,
        filter_path: str = "",
        search_query: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        with self._lock:
            matches = []
            q = search_query.lower()

            for msg in reversed(self._buffer):
                if filter_path:
                    # Path matching check
                    attrs = msg.get("attributes", {})
                    path_str = "/".join(
                        filter(
                            None,
                            [
                                attrs.get("deviceRegistryId"),
                                attrs.get("gatewayId"),
                                attrs.get("deviceId"),
                                attrs.get("subFolder"),
                            ],
                        )
                    )
                    if filter_path not in path_str and filter_path not in msg.get("deviceId", ""):
                        # Check if matches point
                        if filter_path not in msg.get("points", {}):
                            continue

                if q:
                    txt = msg.get("text", "").lower()
                    dev = msg.get("deviceId", "").lower()
                    if q not in txt and q not in dev:
                        continue

                matches.append(msg)

            total = len(matches)
            sliced = matches[offset : offset + limit]
            return sliced, total

    def export(self, format_type: str = "json") -> str:
        with self._lock:
            data_list = list(self._buffer)
        if format_type == "jsonl":
            return "\n".join(json.dumps(m, default=str) for m in data_list)
        return json.dumps(data_list, indent=2, default=str)

    def clear(self):
        with self._lock:
            self._buffer.clear()
            self._by_id.clear()
            self._point_history.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)


class MetricsTracker:
    """Tracks real-time message rates, byte rates, and aggregate metrics."""

    def __init__(self, window_sec: float = 10.0):
        self.window_sec = window_sec
        self.total_messages = 0
        self.total_bytes = 0
        self.devices_seen = set()
        self._timestamps: collections.deque = collections.deque()
        self._bytes_window: collections.deque = collections.deque()
        self._lock = threading.Lock()

    def record(self, msg_size: int, device_id: str):
        now = time.time()
        with self._lock:
            self.total_messages += 1
            self.total_bytes += msg_size
            if device_id:
                self.devices_seen.add(device_id)
            self._timestamps.append(now)
            self._bytes_window.append((now, msg_size))
            self._evict_old(now)

    def _evict_old(self, now: float):
        cutoff = now - self.window_sec
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        while self._bytes_window and self._bytes_window[0][0] < cutoff:
            self._bytes_window.popleft()

    def get_stats(self) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            self._evict_old(now)
            count_in_window = len(self._timestamps)
            rate_msgs = round(count_in_window / self.window_sec, 2)
            bytes_in_window = sum(b for _, b in self._bytes_window)
            rate_kb = round((bytes_in_window / 1024.0) / self.window_sec, 2)
            return {
                "total_messages": self.total_messages,
                "total_bytes": self.total_bytes,
                "msg_rate": rate_msgs,
                "kb_rate": rate_kb,
                "devices_count": len(self.devices_seen),
            }

    def reset(self):
        with self._lock:
            self.total_messages = 0
            self.total_bytes = 0
            self.devices_seen.clear()
            self._timestamps.clear()
            self._bytes_window.clear()


class PubSubManager:
    """
    Manages Pub/Sub subscription streaming pull, seek operations,
    message dissemination to tree/buffer/listeners, and publishing.
    """

    def __init__(
        self,
        project_id: str = "",
        subscription_id: str = "",
        ack_mode: AckMode = AckMode.PEEK,
        filter_config: Optional[FilterConfig] = None,
        tree_mode: str = "standard",
        buffer_capacity: int = 5000,
    ):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.ack_mode = ack_mode
        self.filter_config = filter_config or FilterConfig()

        self.tree = TopicTree(mode=tree_mode)
        self.buffer = MessageBuffer(max_capacity=buffer_capacity)
        self.metrics = MetricsTracker()

        self.is_connected = False
        self.is_paused = False
        self._subscriber: Optional[pubsub_v1.SubscriberClient] = None
        self._future: Optional[Any] = None
        self._sub_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Listeners for real-time SSE push (queues)
        self._listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._listeners_lock = threading.Lock()

    def add_listener(self, callback: Callable[[Dict[str, Any]], None]):
        with self._listeners_lock:
            self._listeners.append(callback)

    def remove_listener(self, callback: Callable[[Dict[str, Any]], None]):
        with self._listeners_lock:
            if callback in self._listeners:
                self._listeners.remove(callback)

    def _broadcast(self, event_data: Dict[str, Any]):
        with self._listeners_lock:
            active_listeners = list(self._listeners)
        for cb in active_listeners:
            try:
                cb(event_data)
            except Exception as e:
                logger.debug("Error broadcasting to listener: %s", e)

    def _message_callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            parsed = parse_pubsub_message(message)
            matches = self.filter_config.matches(parsed)

            # Ack policy decision: default to acknowledging matched messages
            if self.ack_mode == AckMode.ACK_ALL:
                message.ack()
            elif self.ack_mode == AckMode.ACK_MATCHED:
                if matches:
                    message.ack()
                # Note: non-matching messages are left unacknowledged rather than hammering nack RPCs
            elif self.ack_mode == AckMode.PEEK:
                # In peek mode, do not acknowledge so messages remain in Pub/Sub
                pass

            if not matches or self.is_paused:
                return

            # Record metrics
            self.metrics.record(parsed["size"], parsed["deviceId"])

            # Update tree & buffer
            updated_paths = self.tree.update(parsed)
            self.buffer.add(parsed)

            # Broadcast to SSE UI listeners
            self._broadcast({
                "type": "message",
                "message": parsed,
                "updatedPaths": updated_paths,
                "stats": self.metrics.get_stats(),
            })

        except Exception as e:
            logger.error("Error in subscriber message callback: %s", e, exc_info=True)

    def connect(
        self,
        project_id: Optional[str] = None,
        subscription_id: Optional[str] = None,
        seek_to_now: bool = False,
    ) -> Tuple[bool, str]:
        """Starts the Pub/Sub streaming pull subscriber."""
        if project_id:
            self.project_id = project_id
        if subscription_id:
            self.subscription_id = subscription_id

        if not self.project_id or not self.subscription_id:
            return False, "Project ID and Subscription ID are required."

        if self.is_connected:
            self.disconnect()

        if seek_to_now:
            logger.info("Seeking subscription to now before subscribing...")
            seek_ok, seek_err = self.seek_subscription()
            if not seek_ok:
                logger.error("Initial seek failed: %s", seek_err)
                return False, f"Subscription error: {seek_err}"

        try:
            self._subscriber = pubsub_v1.SubscriberClient()
            sub_path = self._subscriber.subscription_path(self.project_id, self.subscription_id)
            self._stop_event.clear()

            self._future = self._subscriber.subscribe(
                sub_path,
                callback=self._message_callback,
            )

            def _on_future_done(fut):
                try:
                    exc = fut.exception()
                    if exc and self.is_connected:
                        logger.error("Streaming pull background error: %s", exc)
                        self.is_connected = False
                        self._broadcast({
                            "type": "disconnect",
                            "error": str(exc),
                            "status": {
                                "isConnected": False,
                                "isPaused": False,
                                "projectId": self.project_id,
                                "subscriptionId": self.subscription_id,
                                "ackMode": self.ack_mode.value,
                                "stats": self.metrics.get_stats(),
                            }
                        })
                except Exception:
                    pass

            self._future.add_done_callback(_on_future_done)
            self.is_connected = True
            logger.info("Connected and listening on %s", sub_path)
            self._broadcast({
                "type": "connect",
                "status": {
                    "isConnected": True,
                    "isPaused": self.is_paused,
                    "projectId": self.project_id,
                    "subscriptionId": self.subscription_id,
                    "ackMode": self.ack_mode.value,
                    "stats": self.metrics.get_stats(),
                }
            })
            return True, f"Listening on {sub_path}"
        except Exception as e:
            self.is_connected = False
            logger.error("Failed to connect to Pub/Sub: %s", e)
            return False, f"Connection failed: {str(e)}"

    def disconnect(self):
        """Stops the streaming subscriber."""
        self.is_connected = False
        if self._future:
            try:
                self._future.cancel()
                time.sleep(0.15)
            except Exception:
                pass
            self._future = None
        if self._subscriber:
            try:
                self._subscriber.close()
            except Exception:
                pass
            self._subscriber = None
        logger.info("Disconnected from Pub/Sub.")
        self._broadcast({
            "type": "disconnect",
            "status": {
                "isConnected": False,
                "isPaused": False,
                "projectId": self.project_id,
                "subscriptionId": self.subscription_id,
                "ackMode": self.ack_mode.value,
                "stats": self.metrics.get_stats(),
            }
        })

    def set_pause(self, paused: bool):
        self.is_paused = paused

    def clear(self):
        self.tree.clear()
        self.buffer.clear()
        self.metrics.reset()
        self._broadcast({"type": "clear"})

    def seek_subscription(
        self,
        timestamp: Optional[datetime.datetime] = None,
        snapshot_id: Optional[str] = None,
        relative_minutes: Optional[int] = None,
    ) -> Tuple[bool, str]:
        """Seeks the subscription to a given timestamp, snapshot, or relative past time."""
        if not self.project_id or not self.subscription_id:
            return False, "No active project/subscription configured."

        client = None
        try:
            client = pubsub_v1.SubscriberClient()
            sub_path = client.subscription_path(self.project_id, self.subscription_id)
            seek_request = SeekRequest(subscription=sub_path)

            if relative_minutes is not None:
                target_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=relative_minutes)
                pb_ts = Timestamp()
                pb_ts.FromDatetime(target_dt)
                seek_request.time = pb_ts
            elif timestamp:
                pb_ts = Timestamp()
                pb_ts.FromDatetime(timestamp)
                seek_request.time = pb_ts
            elif snapshot_id:
                seek_request.snapshot = client.snapshot_path(self.project_id, snapshot_id)
            else:
                # Seek to current time (purges backlog)
                target_dt = datetime.datetime.now(datetime.timezone.utc)
                pb_ts = Timestamp()
                pb_ts.FromDatetime(target_dt)
                seek_request.time = pb_ts

            response = client.seek(request=seek_request)
            return True, f"Seek operation completed successfully: {response}"
        except Exception as e:
            logger.error("Seek operation failed: %s", e)
            return False, f"Seek failed: {str(e)}"
        finally:
            if client:
                try:
                    client.close()
                except Exception:
                    pass

    def publish_message(
        self,
        topic_id: str,
        data: str,
        attributes: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, str]:
        """Publishes a test message to a Pub/Sub topic."""
        if not self.project_id:
            return False, "Project ID is not set."
        if not topic_id:
            return False, "Topic ID is required."

        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(self.project_id, topic_id)
            data_bytes = data.encode("utf-8")
            attrs = attributes or {}
            # Attributes in Pub/Sub must be string -> string
            str_attrs = {str(k): str(v) for k, v in attrs.items()}

            future = publisher.publish(topic_path, data_bytes, **str_attrs)
            msg_id = future.result(timeout=10)
            return True, f"Published message ID: {msg_id}"
        except Exception as e:
            logger.error("Publish failed: %s", e)
            return False, f"Publish failed: {str(e)}"

