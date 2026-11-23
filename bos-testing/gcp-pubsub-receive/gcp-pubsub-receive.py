#!/usr/bin/env python3
"""
  gcp-pubsub-receive.py

  Read data stream for specific devices.

  [Usage] 
  python3 gcp-pubsub-receive.py -p PROJECT_ID -s SUBSCRIPTION_ID [options]
"""

__author__ = "Francesco Anselmo"
__copyright__ = "Copyright 2023"
__credits__ = ["Francesco Anselmo"]
__license__ = "MIT"
__version__ = "0.3"
__maintainer__ = "Francesco Anselmo"
__email__ = "francesco.anselmo@gmail.com"
__status__ = "Dev"

import argparse
import datetime
import json
import logging
import signal
import sys
import time
from concurrent.futures import TimeoutError
from typing import Any, Dict, Optional

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import SeekRequest, Timestamp
from pyfiglet import Figlet
import pytz
from tabulate import tabulate

from pubsub_engine import AckMode, FilterConfig, parse_pubsub_message

# Shared filter configuration
filter_config = FilterConfig()
CURRENT_ACK_MODE = AckMode.ACK_MATCHED


def print_message(message: pubsub_v1.subscriber.message.Message, pointname: str = "") -> None:
    """Formats and prints message telemetry or details to terminal."""
    body = message.data or b""
    device_id = message.attributes.get("deviceId", "")
    gateway_id = message.attributes.get("gatewayId", "")
    sub_folder = message.attributes.get("subFolder", "")
    sub_type = message.attributes.get("subType", "")

    timestamp = ""
    json_data = None
    try:
        json_data = json.loads(body.decode("utf-8", errors="replace"))
        timestamp = json_data.get("timestamp", "")
    except Exception:
        timestamp = ""

    if pointname:
        points = json_data.get("points", {}) if isinstance(json_data, dict) else {}
        point_data = points.get(pointname, "")
        if isinstance(point_data, dict):
            point_value = point_data.get("present_value", point_data.get("value", point_data))
        else:
            point_value = point_data

        print(tabulate([["Timestamp", "Device ID", "Gateway ID", "Subfolder", "Type", "Point name", "Point value"],
                        [timestamp, device_id, gateway_id, sub_folder, sub_type, pointname, point_value]]))
    else:
        print(tabulate([["Timestamp", "Device ID", "Gateway ID", "Subfolder", "Type"],
                        [timestamp, device_id, gateway_id, sub_folder, sub_type]]))
        print(message)
        print(body)
        print(70 * "-")


def message_callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Subscriber callback executed for each incoming Pub/Sub message."""
    global filter_config, CURRENT_ACK_MODE

    try:
        parsed = parse_pubsub_message(message)
        matches = filter_config.matches(parsed)

        # Handle acking according to policy
        if CURRENT_ACK_MODE == AckMode.ACK_ALL:
            message.ack()
        elif CURRENT_ACK_MODE == AckMode.ACK_MATCHED:
            if matches:
                message.ack()
        elif CURRENT_ACK_MODE == AckMode.PEEK:
            pass

        if matches:
            print_message(message, filter_config.point_name)

    except Exception as e:
        sys.stderr.write(f"Error processing message: {e}\n")


def seek_subscription(project_id: str, subscription_id: str, timestamp: Optional[datetime.datetime] = None, snapshot_id: Optional[str] = None):
    """Seeks a Pub/Sub subscription to a given timestamp or snapshot.

    Args:
        project_id (str): Google Cloud Project ID.
        subscription_id (str): The ID of the Pub/Sub subscription.
        timestamp (datetime.datetime, optional): The timestamp to seek to. Defaults to UTC now.
        snapshot_id (str, optional): The ID of the snapshot to seek to.
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    seek_request = SeekRequest(subscription=subscription_path)

    if snapshot_id:
        seek_request.snapshot = subscriber.snapshot_path(project_id, snapshot_id)
    else:
        # Default to now if not provided
        target_time = timestamp or datetime.datetime.now(tz=pytz.utc)
        timestamp_pb = Timestamp()
        timestamp_pb.FromDatetime(target_time)
        seek_request.time = timestamp_pb

    try:
        response = subscriber.seek(request=seek_request)
        print(f"Seek operation completed successfully on {subscription_path}: {response}")
    except Exception as e:
        print(f"Error during seek operation on {subscription_path}: {e}")


def show_title():
    """Show the program title ASCII art."""
    f1 = Figlet(font="standard")
    print(f1.renderText("gcp-pubsub"))
    print(f1.renderText("receive"))


def main():
    global filter_config, CURRENT_ACK_MODE

    parser = argparse.ArgumentParser(description="Read data stream for specific devices from GCP Pub/Sub.")
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="increase verbosity level")
    parser.add_argument("-p", "--project", default="", help="GCP project id (required)")
    parser.add_argument("-s", "--sub", default="", help="GCP Pub/Sub subscription (required)")
    parser.add_argument("-d", "--device", default="", help="device name or abbreviation (optional, shows all if omitted)")
    parser.add_argument("-R", "--registry", default="", help="filter for deviceRegistryId attribute (optional)")
    parser.add_argument("-g", "--gateway", default="", help="filter for gatewayId attribute (optional)")
    parser.add_argument("-f", "--folder", default="", help="filter for subFolder attribute (optional)")
    parser.add_argument("-y", "--type", default="", help="filter for subType attribute (optional)")
    parser.add_argument("-n", "--pointname", default="", help="filter for point name in pointset (optional)")
    parser.add_argument("-r", "-x", "--regex", action="store_true", default=False, help="use regular expressions for filtering")
    parser.add_argument("-t", "--timeout", default="3600", help="time interval in seconds to receive messages (default: 3600s)")
    parser.add_argument("--seek", action="store_true", default=False, help="seek subscription to now (acknowledges backlog and resets retention queue)")
    parser.add_argument("--seek-hours", type=float, default=0.0, help="seek backwards in time by specified number of hours for replay")
    parser.add_argument(
        "--ack-mode",
        choices=["peek", "matched", "all"],
        default="matched",
        help="ack policy: 'peek' (do not ack), 'matched' (ack only matched, default), 'all' (ack every message)",
    )
    parser.add_argument("--ui", action="store_true", default=False, help="launch local MQTT Explorer-style web UI")
    parser.add_argument("--port", type=int, default=8080, help="port for the web UI (default: 8080)")

    args = parser.parse_args()

    # Launch local Web UI if requested
    if args.ui:
        from pubsub_explorer import main as ui_main
        sys.argv = [sys.argv[0]]
        if args.project:
            sys.argv.extend(["-p", args.project])
        if args.sub:
            sys.argv.extend(["-s", args.sub])
        if args.port:
            sys.argv.extend(["--port", str(args.port)])
        if args.ack_mode:
            sys.argv.extend(["--ack-mode", args.ack_mode])
        if args.seek:
            sys.argv.append("--seek")
        ui_main()
        return

    show_title()

    if not args.project or not args.sub:
        print("Error: Both -p/--project and -s/--sub are required in CLI mode.")
        print(f"Please run '{sys.argv[0]} -h' to see the program options, or use '--ui' to launch the web explorer.")
        sys.exit(1)

    if args.verbose:
        print("Program arguments:")
        print(args)
        print()

    PROJECT_ID = args.project
    SUBSCRIPTION_ID = args.sub
    CURRENT_ACK_MODE = AckMode(args.ack_mode)
    TIMEOUT = int(args.timeout)

    filter_config = FilterConfig(
        device_id=args.device,
        registry_id=args.registry,
        gateway_id=args.gateway,
        subfolder=args.folder,
        subtype=args.type,
        point_name=args.pointname,
        use_regex=args.regex,
    )

    if args.seek_hours > 0:
        target_ts = datetime.datetime.now(tz=pytz.utc) - datetime.timedelta(hours=args.seek_hours)
        print(f"Seeking subscription backwards by {args.seek_hours} hour(s) to {target_ts.isoformat()}...")
        seek_subscription(PROJECT_ID, SUBSCRIPTION_ID, timestamp=target_ts)
    elif args.seek:
        now_utc = datetime.datetime.now(tz=pytz.utc)
        print(f"Seeking subscription to current time {now_utc.isoformat()} (purging backlog)...")
        seek_subscription(PROJECT_ID, SUBSCRIPTION_ID, timestamp=now_utc)

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=message_callback)

    target_desc = args.device if args.device else "all devices"
    print(f"Listening for messages from {target_desc} on {subscription_path}")
    print(f"Ack Mode: {CURRENT_ACK_MODE.value.upper()} | Timeout: {TIMEOUT}s")
    print("Press Ctrl+C to stop listening.\n")

    # Graceful shutdown handler
    def shutdown_handler(signum, frame):
        print("\nStopping subscriber...")
        streaming_pull_future.cancel()
        subscriber.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    with subscriber:
        try:
            streaming_pull_future.result(timeout=TIMEOUT)
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        except KeyboardInterrupt:
            shutdown_handler(None, None)


if __name__ == "__main__":
    main()
