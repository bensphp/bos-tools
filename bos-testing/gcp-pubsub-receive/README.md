# gcp-pubsub-receive & Pub/Sub Explorer

A suite of tools for monitoring, filtering, and exploring Google Cloud Pub/Sub message streams from IoT devices (including UDMI, BACnet, Modbus, ClearBlade, and Cloud IoT Core devices).

Includes both an enhanced **CLI tool** (`gcp-pubsub-receive.py`) and a local **MQTT Explorer-style Web UI** (`pubsub_explorer.py`).

---

## Features

- **Local Web UI (MQTT Explorer Style)**:
  - **Hierarchical Topic / Device Tree**: Real-time tree view grouping messages by `Registry -> Gateway -> Device -> SubFolder -> Point`.
  - **Live Telemetry & Activity Pulse**: Tree nodes highlight green dynamically as new messages arrive.
  - **Instant Search & Filter**: Real-time filtering across devices, gateways, and point topics.
  - **Multi-Tab Inspector**:
    - **Payload**: Collapsible, syntax-colored JSON tree, raw text, and hex viewers with copy-to-clipboard.
    - **Points Telemetry**: Live table of all telemetry points with values, engineering units, active/alarm badges, and sparkline triggers.
    - **Attributes & Metadata**: Comprehensive view of all Pub/Sub attributes, message IDs, publish timestamps, and sizing.
    - **History & Live Charts**: Time-series sparkline/line chart (HTML5 Canvas) for numeric points with min/max/current indicators, plus historical message logs.
  - **Publish Tester**: Publish test Pub/Sub messages with custom attributes and JSON payloads directly from the UI.
  - **Seek & Replay**: Purge backlog (seek to now) or rewind up to 24 hours into the past to replay messages.
  - **Safe Monitoring (Ack Modes)**: Choose between `peek` (no ack / leaves messages for other consumers), `matched` (only ack matching messages), and `all`.
  - **Export Data**: Download captured message streams as JSON or JSONL.
  - **Zero Extra Dependencies**: Runs out-of-the-box in the virtual environment without needing Node, npm, or extra pip packages!

- **Refactored CLI Tool**:
  - Fixed inverted string matching bug for gateway IDs.
  - Fixed missing registry filter logic.
  - Fixed `KeyError` crashes on messages missing attributes.
  - Fixed unhandled JSON decode exceptions on non-JSON payloads.
  - Replaced repetitive 60-line branching logic with a clean `FilterConfig` engine.
  - Graceful shutdown on `Ctrl+C` without stack traces.
  - Seamless `--ui` flag to start the web interface directly from the CLI.

---

## Installation & Authentication

```bash
# Ensure gcloud credentials are set
gcloud auth login
gcloud auth application-default login

# Install requirements in your virtual environment
python3 -m pip install -r requirements.txt
```

---

## 1. Local Web UI (Pub/Sub Explorer)

### Launch directly and configure in browser:
```bash
./pubsub_explorer.py
```
When opened at `http://localhost:8080`, the **Connection Settings** dialog pops up automatically pre-filled with your saved settings or presets (`bos-platform-staging / uk-lon-glab-subscription`). Click **Connect & Listen** or press **Enter**.

### Or launch pre-connected via CLI:
```bash
./pubsub_explorer.py -p bos-platform-staging -s uk-lon-glab-subscription --seek
```

### Launch via `gcp-pubsub-receive.py`:
```bash
./gcp-pubsub-receive.py -p bos-platform-staging -s uk-lon-glab-subscription --seek --ui
```

### Options:
```
  -p, --project PROJECT       GCP Project ID
  -s, --sub SUB               GCP Pub/Sub Subscription ID
  --port PORT                 Local port to serve web UI on (default: 8080)
  --host HOST                 Host interface to bind to (default: 127.0.0.1)
  --ack-mode {peek,matched,all}
                              Ack policy: 'matched' (default), 'peek', 'all'
  --seek                      Purge backlog and seek to current time on connect
  --no-browser                Do not automatically open default web browser
```

Once launched, open `http://localhost:8080` in your web browser.

---

## 2. Command Line Usage (`gcp-pubsub-receive.py`)

```
usage: gcp-pubsub-receive.py [-h] [-v] [-p PROJECT] [-s SUB] [-d DEVICE]
                             [-R REGISTRY] [-g GATEWAY] [-f FOLDER] [-y TYPE]
                             [-n POINTNAME] [-r] [-t TIMEOUT] [--seek]
                             [--seek-hours SEEK_HOURS]
                             [--ack-mode {peek,matched,all}] [--ui]
                             [--port PORT]

options:
  -h, --help            show this help message and exit
  -v, --verbose         increase verbosity level
  -p, --project PROJECT GCP project id (required)
  -s, --sub SUB         GCP Pub/Sub subscription (required)
  -d, --device DEVICE   device name or abbreviation (optional)
  -R, --registry REGISTRY
                        filter for deviceRegistryId attribute (optional)
  -g, --gateway GATEWAY filter for gatewayId attribute (optional)
  -f, --folder FOLDER   filter for subFolder attribute (optional)
  -y, --type TYPE       filter for subType attribute (optional)
  -n, --pointname POINTNAME
                        filter for point name in pointset (optional)
  -r, -x, --regex       use regular expressions for filtering
  -t, --timeout TIMEOUT time interval in seconds to receive messages (default: 3600s)
  --seek                seek subscription to now (purges accumulated backlog)
  --seek-hours HOURS    seek backwards in time by specified hours for replay
  --ack-mode {peek,matched,all}
                        ack policy: 'peek' (do not ack), 'matched' (default), 'all'
  --ui                  launch local MQTT Explorer-style web UI
  --port PORT           port for the web UI (default: 8080)
```

---

## CLI Examples

### Get pointset values for any DDC device for 30 minutes
```bash
python3 gcp-pubsub-receive.py -p PROJECT_NAME -s SUBSCRIPTION_NAME -d DDC -f pointset -t 1800
```

### Get pointset values for a specific device DDC-12 using regex
```bash
python3 gcp-pubsub-receive.py -p PROJECT_NAME -s SUBSCRIPTION_NAME -d "DDC-12$" -r -f pointset -t 3600
```

### Get a single point value ("temperature_sensor") from all devices
```bash
python3 gcp-pubsub-receive.py -p PROJECT_NAME -s SUBSCRIPTION_NAME -f pointset -n temperature_sensor -t 3600
```

### Peek safely without acknowledging messages (non-destructive monitoring)
```bash
python3 gcp-pubsub-receive.py -p PROJECT_NAME -s SUBSCRIPTION_NAME -d FCU-2 --ack-mode peek
```

### Replay past 2 hours of messages
```bash
python3 gcp-pubsub-receive.py -p PROJECT_NAME -s SUBSCRIPTION_NAME --seek-hours 2 -d FCU-2
```

---

## Running Automated Tests

A comprehensive unit test suite is included to verify filtering, parsing, tree building, and web endpoints:

```bash
python3 -m unittest test_pubsub_receive.py -v
```
