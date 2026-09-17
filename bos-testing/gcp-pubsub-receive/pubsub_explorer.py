#!/usr/bin/env python3
"""
pubsub_explorer.py

Standalone launcher for the GCP Pub/Sub Explorer local Web UI (MQTT Explorer style).
Runs out-of-the-box in the virtual environment without any external npm or pip dependencies.

Usage:
  ./pubsub_explorer.py [-p PROJECT] [-s SUBSCRIPTION] [--port 8080] [--no-browser]
"""

import argparse
import logging
import os
import signal
import sys
import threading
import webbrowser

from pubsub_engine import AckMode, FilterConfig, PubSubManager
from web.server import start_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pubsub_explorer")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Launch the local GCP Pub/Sub Explorer web UI (similar to MQTT Explorer)."
    )
    parser.add_argument("-p", "--project", default="", help="GCP Project ID")
    parser.add_argument("-s", "--sub", default="", help="GCP Pub/Sub Subscription ID")
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Local port to serve web UI on (default: 8080)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface to bind server to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--ack-mode",
        choices=["peek", "matched", "all"],
        default="matched",
        help="Ack policy: 'matched' (default, consume matching messages), 'peek' (passive monitor, no ack), 'all' (consume all). Default: matched",
    )
    parser.add_argument(
        "--seek",
        action="store_true",
        default=False,
        help="Seek subscription to current time on connect (purges accumulated backlog and resets queue)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        default=False,
        help="Do not automatically open default web browser on launch",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    ack_mode = AckMode(args.ack_mode)

    manager = PubSubManager(
        project_id=args.project,
        subscription_id=args.sub,
        ack_mode=ack_mode,
    )

    server = start_server(manager, host=args.host, port=args.port)
    url = f"http://{args.host}:{args.port}"

    print("=" * 65)
    print("  GCP Pub/Sub Explorer (MQTT Explorer Style)")
    print(f"  Local Web UI running at: {url}")
    print("=" * 65)

    if args.project and args.sub:
        seek_str = " with backlog purge (--seek)" if args.seek else ""
        print(f"Connecting to: projects/{args.project}/subscriptions/{args.sub} (mode: {ack_mode.value}){seek_str}")
        success, msg = manager.connect(args.project, args.sub, seek_to_now=args.seek)
        if not success:
            print(f"[Warning] Initial connection attempt: {msg}")
    else:
        print("Tip: Specify project and subscription via Web UI Connect dialog or CLI flags (-p, -s).")

    if not args.no_browser:
        def open_browser():
            try:
                webbrowser.open(url)
            except Exception:
                pass
        threading.Timer(0.8, open_browser).start()

    def shutdown_signal_handler(signum, frame):
        print("\nShutting down GCP Pub/Sub Explorer...")
        manager.disconnect()
        threading.Thread(target=server.shutdown).start()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_signal_handler)
    signal.signal(signal.SIGTERM, shutdown_signal_handler)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        shutdown_signal_handler(None, None)


if __name__ == "__main__":
    main()

