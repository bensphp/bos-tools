#!/usr/bin/env python3
"""
web/server.py

Zero-dependency HTTP server with Server-Sent Events (SSE) and REST API for
the GCP Pub/Sub Explorer UI (MQTT Explorer style).
"""

from __future__ import annotations

import datetime
import json
import logging
import mimetypes
import os
import queue
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

from pubsub_engine import AckMode, FilterConfig, PubSubManager

logger = logging.getLogger("pubsub_server")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class ExplorerRequestHandler(BaseHTTPRequestHandler):
    """Handles HTTP requests and SSE streams for GCP Pub/Sub Explorer."""

    # Reference to PubSubManager set on server instance
    manager: PubSubManager

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress noisy SSE logging in terminal
        if "/api/stream" in args[0] if args else "":
            return
        logger.debug("%s - - [%s] %s", self.address_string(), self.log_date_time_string(), format % args)

    def _send_json(self, data: Any, status: int = 200, headers: Optional[Dict[str, str]] = None):
        response_body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(response_body)

    def _send_error_json(self, message: str, status: int = 400):
        self._send_json({"error": message, "success": False}, status=status)

    def _read_json_body(self) -> Dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length <= 0:
            return {}
        body = self.rfile.read(content_length)
        try:
            return json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.warning("Failed to decode JSON body: %s", e)
            return {}

    def do_OPTIONS(self):
        """Handle CORS pre-flight."""
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        # Route matching
        if path == "/api/stream":
            self._handle_sse_stream()
        elif path == "/api/status":
            self._handle_status()
        elif path == "/api/tree":
            self._handle_tree()
        elif path == "/api/messages":
            self._handle_messages(query)
        elif path.startswith("/api/messages/"):
            msg_id = path[len("/api/messages/") :]
            self._handle_message_detail(msg_id)
        elif path == "/api/point-history":
            self._handle_point_history(query)
        elif path == "/api/export":
            self._handle_export(query)
        else:
            self._serve_static(path)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = self._read_json_body()

        if path == "/api/connect":
            self._handle_connect(body)
        elif path == "/api/disconnect":
            self._handle_disconnect()
        elif path == "/api/pause":
            self._handle_pause(body)
        elif path == "/api/clear":
            self._handle_clear()
        elif path == "/api/seek":
            self._handle_seek(body)
        elif path == "/api/publish":
            self._handle_publish(body)
        elif path == "/api/filter":
            self._handle_set_filter(body)
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "API route not found")

    def _serve_static(self, path: str):
        if path in ("", "/"):
            path = "/index.html"

        # Sanitize path to prevent directory traversal
        rel_path = os.path.normpath(path.lstrip("/"))
        file_path = os.path.join(STATIC_DIR, rel_path)

        if not os.path.commonpath([STATIC_DIR, file_path]) == STATIC_DIR:
            self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
            return

        if not os.path.exists(file_path) or os.path.isdir(file_path):
            # Fallback to index.html for SPA routing
            file_path = os.path.join(STATIC_DIR, "index.html")

        if not os.path.exists(file_path):
            self.send_error(HTTPStatus.NOT_FOUND, "File Not Found")
            return

        ctype, _ = mimetypes.guess_type(file_path)
        if not ctype:
            ctype = "application/octet-stream"

        try:
            with open(file_path, "rb") as f:
                content = f.read()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))

    def _handle_sse_stream(self):
        """Streams live message arrivals and statistics to the browser via SSE."""
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        msg_queue: queue.Queue = queue.Queue(maxsize=1000)

        def on_event(event_dict: Dict[str, Any]):
            try:
                msg_queue.put_nowait(event_dict)
            except queue.Full:
                pass

        self.manager.add_listener(on_event)

        # Initial state payload
        try:
            initial_event = {
                "type": "init",
                "status": self._get_status_data(),
                "tree": self.manager.tree.to_dict(),
            }
            init_line = f"data: {json.dumps(initial_event, default=str)}\n\n"
            self.wfile.write(init_line.encode("utf-8"))
            self.wfile.flush()
        except Exception:
            self.manager.remove_listener(on_event)
            return

        last_ping = time.time()
        try:
            while True:
                now = time.time()
                try:
                    event = msg_queue.get(timeout=1.0)
                    line = f"data: {json.dumps(event, default=str)}\n\n"
                    self.wfile.write(line.encode("utf-8"))
                    self.wfile.flush()
                except queue.Empty:
                    # Send periodic SSE keep-alive ping
                    if now - last_ping > 15:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        last_ping = now
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            logger.debug("SSE connection ended: %s", e)
        finally:
            self.manager.remove_listener(on_event)

    def _get_status_data(self) -> Dict[str, Any]:
        stats = self.manager.metrics.get_stats()
        return {
            "isConnected": self.manager.is_connected,
            "isPaused": self.manager.is_paused,
            "projectId": self.manager.project_id,
            "subscriptionId": self.manager.subscription_id,
            "ackMode": self.manager.ack_mode.value,
            "stats": stats,
            "bufferCount": len(self.manager.buffer),
        }

    def _handle_status(self):
        self._send_json(self._get_status_data())

    def _handle_tree(self):
        self._send_json(self.manager.tree.to_dict())

    def _handle_messages(self, query: Dict[str, Any]):
        filter_path = query.get("filterPath", [""])[0]
        search = query.get("search", [""])[0]
        limit = int(query.get("limit", [100])[0])
        offset = int(query.get("offset", [0])[0])

        msgs, total = self.manager.buffer.query(
            filter_path=filter_path,
            search_query=search,
            limit=limit,
            offset=offset,
        )
        self._send_json({"messages": msgs, "total": total, "limit": limit, "offset": offset})

    def _handle_message_detail(self, msg_id: str):
        msg = self.manager.buffer.get_by_id(msg_id)
        if not msg:
            self._send_error_json("Message not found", status=404)
        else:
            self._send_json(msg)

    def _handle_point_history(self, query: Dict[str, Any]):
        dev = query.get("device", [""])[0]
        pt = query.get("point", [""])[0]
        if not dev or not pt:
            self._send_error_json("Both 'device' and 'point' query parameters are required")
            return
        history = self.manager.buffer.get_point_history(dev, pt)
        self._send_json({"device": dev, "point": pt, "history": history})

    def _handle_export(self, query: Dict[str, Any]):
        fmt = query.get("format", ["json"])[0].lower()
        if fmt not in ("json", "jsonl"):
            fmt = "json"
        content = self.manager.buffer.export(fmt)
        filename = f"pubsub-messages-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.{fmt}"
        ctype = "application/json" if fmt == "json" else "text/plain"

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(content.encode("utf-8"))))
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def _handle_connect(self, body: Dict[str, Any]):
        p_id = body.get("projectId", "").strip()
        s_id = body.get("subscriptionId", "").strip()
        ack_m_str = body.get("ackMode", "matched")
        seek_now = bool(body.get("seekNow", True))

        try:
            ack_mode = AckMode(ack_m_str)
        except ValueError:
            ack_mode = AckMode.ACK_MATCHED

        self.manager.ack_mode = ack_mode
        success, msg = self.manager.connect(p_id, s_id, seek_to_now=seek_now)
        self._send_json({"success": success, "message": msg, "status": self._get_status_data()})

    def _handle_disconnect(self):
        self.manager.disconnect()
        self._send_json({"success": True, "message": "Disconnected", "status": self._get_status_data()})

    def _handle_pause(self, body: Dict[str, Any]):
        paused = bool(body.get("paused", not self.manager.is_paused))
        self.manager.set_pause(paused)
        self._send_json({"success": True, "isPaused": paused})

    def _handle_clear(self):
        self.manager.clear()
        self._send_json({"success": True, "message": "Buffer and tree cleared"})

    def _handle_seek(self, body: Dict[str, Any]):
        rel_min = body.get("relativeMinutes")
        ts_str = body.get("timestamp")
        target_ts = None
        if ts_str:
            try:
                target_ts = datetime.datetime.fromisoformat(ts_str)
            except Exception:
                pass

        success, msg = self.manager.seek_subscription(
            timestamp=target_ts,
            relative_minutes=int(rel_min) if rel_min is not None else None,
        )
        self._send_json({"success": success, "message": msg})

    def _handle_publish(self, body: Dict[str, Any]):
        topic_id = body.get("topicId", "").strip()
        data_str = body.get("data", "")
        attrs = body.get("attributes", {})
        success, msg = self.manager.publish_message(topic_id, data_str, attrs)
        self._send_json({"success": success, "message": msg})

    def _handle_set_filter(self, body: Dict[str, Any]):
        cfg = FilterConfig(
            device_id=body.get("deviceId", ""),
            registry_id=body.get("registryId", ""),
            gateway_id=body.get("gatewayId", ""),
            subfolder=body.get("subfolder", ""),
            subtype=body.get("subtype", ""),
            point_name=body.get("pointName", ""),
            use_regex=bool(body.get("useRegex", False)),
        )
        self.manager.filter_config = cfg
        self._send_json({"success": True, "message": "Filter updated"})


def start_server(
    manager: PubSubManager,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> ThreadingHTTPServer:
    """Starts the ThreadingHTTPServer with ExplorerRequestHandler."""
    ExplorerRequestHandler.manager = manager
    server = ThreadingHTTPServer((host, port), ExplorerRequestHandler)
    return server
