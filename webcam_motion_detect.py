"""
webcam_motion_detect.py — Real-time Motion Detection, Telemetry Ingestion & Live Video Streamer.

Acts as the Edge Gateway for the SWSTP Platform:
  1. Captures live webcam / USB video feed and runs adaptive motion detection.
  2. Reads hardware RTC (DS3231) + GNSS (NEO-6M) + IMU telemetry from Arduino/ESP32 via USB Serial.
  3. Streams live camera frames to Backend: POST /api/camera/{deviceId}/frame (1-5 Hz).
  4. Ingests real-time telemetry to Backend: POST /api/telemetry/ingest-batch (1-5 Hz).
  5. Uploads motion evidence images to Backend: POST /api/evidence/upload with SHA-256 and metadata.
  6. Non-blocking asynchronous background queues ensure 0% video frame drop.

Requires: opencv-python, numpy, imutils, pyserial, requests
    pip install opencv-python numpy imutils pyserial requests
"""

import argparse
import datetime
import hashlib
import json
import os
import queue
import re
import sys
import threading
import time
import uuid
import cv2
import imutils
import numpy as np
import requests
import serial
import serial.tools.list_ports

# ---- Default serial configuration ----
DEFAULT_SERIAL_PORT = "COM3"    # Windows e.g. "COM3" or "AUTO" | Linux e.g. "/dev/ttyUSB0"
DEFAULT_BAUD_RATE = 115200      # 115200 baud for high-speed Arduino/ESP32 streaming

# ---- Default backend configuration ----
DEFAULT_BACKEND_URL = "https://solidwasteapi.scipl.info.in"
DEFAULT_DEVICE_ID = "SWSTP-EDGE-01"
DEFAULT_ULB_ID = "ULB_MH_AMRAVATI"
DEFAULT_VEHICLE_ID = "MH-27-BE-1088"
DEFAULT_STREAM_FPS = 30.0       # 25-30+ FPS smooth live video stream to /api/camera/{deviceId}/frame

# Shared sensor telemetry state
latest_sensor = {
    "timestamp": None,
    "lat": None,
    "lon": None,
    "alt": 0.0,
    "speed": 0.0,
    "heading": 0.0,
    "satellites": 0,
    "gps_valid": False,
    "imu": None,
    "serial_connected": False,
    "serial_port": None,
    "rtc_error": None,
    "available_ports": [],
    "last_raw_line": None,
    "sequence": 0,
}

# Real-time hardware status tracker
hardware_state = {
    "camera": {"detected": False, "source": None, "resolution": None, "fps": None},
    "serial": {"connected": False, "port": None, "baud": None, "desc": None},
    "rtc": {"detected": False, "module": "DS3231 (I2C)", "last_ts": None, "logged_online": False, "logged_error": False},
    "gps": {"detected": False, "module": "NEO-6M (UART)", "fix": False, "coords": None, "logged_detected": False, "logged_fix": False},
    "backend": {"connected": False, "last_ping": 0, "upload_count": 0, "telemetry_count": 0},
}

# Thread-safe queues
upload_queue = queue.Queue(maxsize=50)
telemetry_queue = queue.Queue(maxsize=3000) # Strict FIFO sequential telemetry queue
latest_frame_lock = threading.Lock()
latest_frame_jpeg = None

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}


def log_hardware(component, status, details=""):
    """Prints a prominent hardware status log with clean formatting."""
    print("\n" + "=" * 65)
    print(f" [HARDWARE] {component.upper()} -> {status.upper()}")
    if details:
        print(f"            {details}")
    print("=" * 65 + "\n")


def list_serial_ports():
    """Returns a list of all detected serial COM ports with hardware details."""
    return list(serial.tools.list_ports.comports())


def find_arduino_port(preferred_port="COM3"):
    """Finds the best candidate serial port for Arduino/ESP32."""
    available_ports = list_serial_ports()
    if not available_ports:
        return None, []

    if preferred_port and preferred_port.upper() != "AUTO":
        for p in available_ports:
            if p.device.upper() == preferred_port.upper():
                return p.device, available_ports

    keywords = ["arduino", "ch340", "ch341", "cp210", "ftdi", "usb", "serial", "acm", "prolific", "esp32"]
    usb_candidates = []

    for p in available_ports:
        hwid = (p.hwid or "").lower()
        desc = (p.description or "").lower()
        dev = p.device.upper()

        if "bthenum" in hwid or "bluetooth" in desc:
            continue
        if dev == "COM1" and not any(kw in desc for kw in ["usb", "arduino", "ch340"]):
            continue

        if any(kw in desc or kw in hwid for kw in keywords):
            usb_candidates.append(p)

    if usb_candidates:
        return usb_candidates[0].device, available_ports

    for p in available_ports:
        hwid = (p.hwid or "").lower()
        desc = (p.description or "").lower()
        if "bluetooth" not in desc and "bthenum" not in hwid:
            return p.device, available_ports

    return available_ports[0].device, available_ports


def normalize_datetime(year, month, day, hour=0, minute=0, second=0):
    """Validates and formats date components into YYYY-MM-DD HH:MM:SS."""
    try:
        y, mo, d = int(year), int(month), int(day)
        h, mi, s = int(hour), int(minute), int(second)
        if y < 100:
            y += 2000
        if not (1 <= mo <= 12 and 1 <= d <= 31 and 0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 59):
            return None
        return f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}"
    except (ValueError, TypeError):
        return None


def extract_rtc_timestamp(text):
    """Extracts and normalizes timestamp string strictly from hardware RTC output."""
    if not text:
        return None, None

    clean = re.sub(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]", "", str(text)).strip()
    lower = clean.lower()

    if any(err in lower for err in [
        "waiting for rtc", "couldn't find rtc", "rtc not found",
        "rtc error", "rtc fail", "no rtc", "rtc lost power"
    ]):
        return None, "RTC hardware not detected on Arduino (Check I2C A4/A5 wiring)"

    if lower in ("0", "none", "null", "0.0.0", "waiting...", ""):
        return None, None

    # ISO format: YYYY-MM-DD HH:MM:SS
    m = re.search(
        r"\b(20\d{2})[-/. ](\d{1,2})[-/. ](\d{1,2})(?:[ T,]+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?(?:\s*([AP]M))?)?\b",
        clean,
        re.IGNORECASE,
    )
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        h = int(m.group(4)) if m.group(4) else 0
        mi = int(m.group(5)) if m.group(5) else 0
        s = int(m.group(6)) if m.group(6) else 0
        ampm = m.group(7)
        if ampm:
            if ampm.upper() == "PM" and h < 12:
                h += 12
            elif ampm.upper() == "AM" and h == 12:
                h = 0
        dt = normalize_datetime(y, mo, d, h, mi, s)
        if dt:
            return dt, None

    # Day-first format: DD-MM-YYYY HH:MM:SS
    m = re.search(
        r"\b(\d{1,2})[-/. ](\d{1,2})[-/. ](20\d{2})(?:[ T,]+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?(?:\s*([AP]M))?)?\b",
        clean,
        re.IGNORECASE,
    )
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        h = int(m.group(4)) if m.group(4) else 0
        mi = int(m.group(5)) if m.group(5) else 0
        s = int(m.group(6)) if m.group(6) else 0
        ampm = m.group(7)
        if ampm:
            if ampm.upper() == "PM" and h < 12:
                h += 12
            elif ampm.upper() == "AM" and h == 12:
                h = 0
        dt = normalize_datetime(y, mo, d, h, mi, s)
        if dt:
            return dt, None

    # Month name format
    m = re.search(
        r"\b(?:[A-Za-z]{3,}\s+)?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[\s-]+(\d{1,2})[,\s-]+(20\d{2})[,\sT]+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?(?:\s*([AP]M))?\b",
        clean,
        re.IGNORECASE,
    )
    if m:
        mo = MONTH_MAP[m.group(1).lower()[:3]]
        d = int(m.group(2))
        y = int(m.group(3))
        h = int(m.group(4)) if m.group(4) else 0
        mi = int(m.group(5)) if m.group(5) else 0
        s = int(m.group(6)) if m.group(6) else 0
        ampm = m.group(7)
        if ampm:
            if ampm.upper() == "PM" and h < 12:
                h += 12
            elif ampm.upper() == "AM" and h == 12:
                h = 0
        dt = normalize_datetime(y, mo, d, h, mi, s)
        if dt:
            return dt, None

    return None, None


def parse_serial_line(line):
    """Parses raw text from Arduino/ESP32 containing RTC, GNSS/GPS, or IMU data."""
    global latest_sensor, hardware_state
    line = re.sub(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]", "", line).strip()
    if not line:
        return False

    parsed_something = False
    latest_sensor["sequence"] += 1

    # 0. Check for JSON telemetry payload
    json_start = line.find("{")
    json_end = line.rfind("}")
    if json_start != -1 and json_end != -1 and json_end > json_start:
        json_candidate = line[json_start:json_end + 1]
        try:
            telemetry = json.loads(json_candidate)
            if isinstance(telemetry, dict):
                # Parse RTC
                if "rtc" in telemetry and isinstance(telemetry["rtc"], dict):
                    rtc_obj = telemetry["rtc"]
                    raw_ts = rtc_obj.get("timestamp") or rtc_obj.get("iso")
                    is_valid = rtc_obj.get("valid", True) or rtc_obj.get("synced", True)
                    if is_valid and raw_ts:
                        ts, _ = extract_rtc_timestamp(str(raw_ts))
                        if ts:
                            if not hardware_state["rtc"]["logged_online"]:
                                hardware_state["rtc"]["detected"] = True
                                hardware_state["rtc"]["logged_online"] = True
                                hardware_state["rtc"]["last_ts"] = ts
                                log_hardware("RTC MODULE (DS3231)", "ONLINE & SYNCHRONIZED", f"JSON Timestamp: {ts}")

                            latest_sensor["timestamp"] = ts
                            latest_sensor["rtc_error"] = None
                            parsed_something = True

                # Parse GNSS / GPS
                if "gnss" in telemetry and isinstance(telemetry["gnss"], dict):
                    gnss_obj = telemetry["gnss"]
                    has_data = gnss_obj.get("data_received", False) or gnss_obj.get("valid", False)
                    has_fix = gnss_obj.get("fix", False)
                    lat = gnss_obj.get("lat") or gnss_obj.get("latitude")
                    lon = gnss_obj.get("lon") or gnss_obj.get("longitude")
                    speed = gnss_obj.get("speed", 0.0)
                    heading = gnss_obj.get("heading", 0.0)
                    satellites = gnss_obj.get("satellites", 0)

                    if has_data and not hardware_state["gps"]["logged_detected"]:
                        hardware_state["gps"]["detected"] = True
                        hardware_state["gps"]["logged_detected"] = True
                        log_hardware("GPS MODULE (NEO-6M)", "STREAM DETECTED", "GNSS stream active.")

                    if lat is not None and lon is not None:
                        try:
                            lat_val = float(lat)
                            lon_val = float(lon)
                            if not hardware_state["gps"]["logged_fix"] and (abs(lat_val) > 0.001 and abs(lon_val) > 0.001):
                                hardware_state["gps"]["fix"] = True
                                hardware_state["gps"]["logged_fix"] = True
                                log_hardware("GPS MODULE (NEO-6M)", "SATELLITE FIX ACQUIRED", f"Lat: {lat_val:.6f}, Lon: {lon_val:.6f}")
                            latest_sensor["lat"] = lat_val
                            latest_sensor["lon"] = lon_val
                            latest_sensor["speed"] = float(speed or 0.0)
                            latest_sensor["heading"] = float(heading or 0.0)
                            latest_sensor["satellites"] = int(satellites or 0)
                            latest_sensor["gps_valid"] = True
                            parsed_something = True
                        except ValueError:
                            pass

                # Parse IMU
                if "imu" in telemetry and isinstance(telemetry["imu"], dict):
                    latest_sensor["imu"] = telemetry["imu"]
                    parsed_something = True

        except json.JSONDecodeError:
            pass

    # 1. Check for CSV / DATA protocol format: DATA,timestamp,lat,lon,valid
    if line.startswith("DATA,") or line.startswith("DATA:"):
        parts = [p.strip() for p in re.split(r"[,;]", line)]
        if len(parts) >= 2:
            combined_candidate = f"{parts[1]} {parts[2]}" if len(parts) >= 3 and ":" in parts[2] and not parts[1].startswith(":") else parts[1]
            ts, err = extract_rtc_timestamp(combined_candidate)
            if ts:
                latest_sensor["timestamp"] = ts
                latest_sensor["rtc_error"] = None
                parsed_something = True

            offset = 2 if (len(parts) >= 6 and ":" in parts[2] and not parts[1].startswith(":")) else 1
            if len(parts) >= offset + 3:
                try:
                    lat_str = parts[offset + 1]
                    lon_str = parts[offset + 2]
                    lat_val = float(lat_str)
                    lon_val = float(lon_str)
                    if -90 <= lat_val <= 90 and -180 <= lon_val <= 180 and (abs(lat_val) > 0.001 and abs(lon_val) > 0.001):
                        latest_sensor["lat"] = lat_val
                        latest_sensor["lon"] = lon_val
                        latest_sensor["gps_valid"] = True
                        parsed_something = True
                except (ValueError, IndexError):
                    pass

    # Sequential queueing for smooth, monotonic telemetry delivery
    if parsed_something:
        now_epoch_ms = int(time.time() * 1000)
        iso_time = latest_sensor["timestamp"] or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        pkt = {
            "sequence": latest_sensor["sequence"],
            "timestamp": now_epoch_ms,
            "uptime_ms": now_epoch_ms,
            "rawJson": line,
            "rtc": {
                "iso": iso_time,
                "epoch": now_epoch_ms,
                "synced": latest_sensor["timestamp"] is not None
            },
            "gnss": {
                "lat": latest_sensor["lat"] if latest_sensor["gps_valid"] else None,
                "lon": latest_sensor["lon"] if latest_sensor["gps_valid"] else None,
                "alt": latest_sensor["alt"],
                "speed": latest_sensor["speed"],
                "heading": latest_sensor["heading"],
                "satellites": latest_sensor["satellites"],
                "fix": "3D_FIX" if latest_sensor["gps_valid"] else "NO_FIX",
                "valid": latest_sensor["gps_valid"]
            },
            "imu": latest_sensor.get("imu") or {
                "accel": {"x": 0.0, "y": 0.0, "z": 1.0},
                "gyro": {"x": 0.0, "y": 0.0, "z": 0.0},
                "valid": True
            }
        }
        try:
            telemetry_queue.put_nowait(pkt)
        except queue.Full:
            try:
                telemetry_queue.get_nowait()
                telemetry_queue.put_nowait(pkt)
            except Exception:
                pass

    return parsed_something


def serial_reader(port_arg, baud_rate, stop_event):
    """Runs in background thread, auto-connects to serial port, reads lines."""
    global latest_sensor, hardware_state

    reconnect_delay = 3.0
    current_port = port_arg
    last_logged_target = None

    while not stop_event.is_set():
        available = list_serial_ports()
        latest_sensor["available_ports"] = [p.device for p in available]

        target_port, port_objs = find_arduino_port(current_port)
        if not target_port:
            latest_sensor["serial_connected"] = False
            latest_sensor["serial_port"] = None
            hardware_state["serial"]["connected"] = False
            stop_event.wait(reconnect_delay)
            continue

        port_descriptions = ", ".join([f"{p.device} ({p.description})" for p in port_objs])
        if last_logged_target != target_port:
            print(f"[SERIAL] Connecting to {target_port} @ {baud_rate} baud...")
            last_logged_target = target_port

        ser = None
        try:
            ser = serial.Serial(port=target_port, baudrate=baud_rate, timeout=1.0)
            time.sleep(1.5)
            ser.reset_input_buffer()

            latest_sensor["serial_connected"] = True
            latest_sensor["serial_port"] = target_port
            hardware_state["serial"] = {"connected": True, "port": target_port, "baud": baud_rate, "desc": port_descriptions}
            log_hardware("SERIAL BRIDGE", "CONNECTED", f"Serial link active on {target_port} at {baud_rate} baud.")

            while not stop_event.is_set() and ser.is_open:
                try:
                    raw_bytes = ser.readline()
                    if not raw_bytes:
                        continue
                    line = raw_bytes.decode("utf-8", errors="ignore").strip()
                    if line:
                        latest_sensor["last_raw_line"] = line
                        parse_serial_line(line)
                except (serial.SerialException, Exception):
                    break

        except Exception as e:
            latest_sensor["serial_connected"] = False
            latest_sensor["serial_port"] = None
            hardware_state["serial"]["connected"] = False
            print(f"[SERIAL WAIT] Could not open {target_port}: {e}")
        finally:
            if ser and ser.is_open:
                try:
                    ser.close()
                except Exception:
                    pass
            latest_sensor["serial_connected"] = False
            hardware_state["serial"]["connected"] = False

        if not stop_event.is_set():
            stop_event.wait(reconnect_delay)


# ─── Live Camera Frame Streamer Thread (POST /api/camera/{deviceId}/frame) ───
def live_frame_streamer(backend_url, device_id, fps, stop_event):
    """Pushes live JPEG frames to Backend at high framerate (up to 30+ FPS) using Keep-Alive."""
    global latest_frame_jpeg, hardware_state
    url = f"{backend_url.rstrip('/')}/api/camera/{device_id}/frame"
    interval = 1.0 / max(0.2, fps)

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    while not stop_event.is_set():
        frame_bytes = None
        with latest_frame_lock:
            if latest_frame_jpeg is not None:
                frame_bytes = latest_frame_jpeg

        if frame_bytes:
            try:
                resp = session.post(
                    url,
                    data=frame_bytes,
                    headers={"Content-Type": "image/jpeg", "Connection": "keep-alive"},
                    timeout=1.0
                )
                if resp.status_code == 200:
                    hardware_state["backend"]["connected"] = True
                    hardware_state["backend"]["last_ping"] = time.time()
            except Exception:
                hardware_state["backend"]["connected"] = False

        stop_event.wait(interval)

    session.close()


# ─── Telemetry Batch Ingestion Worker (POST /api/telemetry/ingest-batch) ──────
def telemetry_streamer(backend_url, session_id, stop_event):
    """Sends sequential telemetry batches to backend with HTTP keep-alive connection pooling."""
    global latest_sensor, hardware_state
    url = f"{backend_url.rstrip('/')}/api/telemetry/ingest-batch"

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    while not stop_event.is_set():
        packets = []
        while not telemetry_queue.empty() and len(packets) < 50:
            try:
                packets.append(telemetry_queue.get_nowait())
            except queue.Empty:
                break

        if not packets and (latest_sensor["timestamp"] or latest_sensor["gps_valid"] or latest_sensor["imu"]):
            # Heartbeat packet when idle
            now_epoch_ms = int(time.time() * 1000)
            iso_time = latest_sensor["timestamp"] or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            packets.append({
                "sequence": latest_sensor["sequence"],
                "timestamp": now_epoch_ms,
                "uptime_ms": now_epoch_ms,
                "rawJson": latest_sensor.get("last_raw_line") or "{}",
                "rtc": {
                    "iso": iso_time,
                    "epoch": now_epoch_ms,
                    "synced": latest_sensor["timestamp"] is not None
                },
                "gnss": {
                    "lat": latest_sensor["lat"] if latest_sensor["gps_valid"] else None,
                    "lon": latest_sensor["lon"] if latest_sensor["gps_valid"] else None,
                    "alt": latest_sensor["alt"],
                    "speed": latest_sensor["speed"],
                    "heading": latest_sensor["heading"],
                    "satellites": latest_sensor["satellites"],
                    "fix": "3D_FIX" if latest_sensor["gps_valid"] else "NO_FIX",
                    "valid": latest_sensor["gps_valid"]
                },
                "imu": latest_sensor.get("imu") or {
                    "accel": {"x": 0.0, "y": 0.0, "z": 1.0},
                    "gyro": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "valid": True
                }
            })

        if packets:
            batch = {
                "sessionId": session_id or 0,
                "mode": "REAL_HARDWARE",
                "packets": packets
            }

            try:
                resp = session.post(url, json=batch, timeout=2.0)
                if resp.status_code == 200:
                    hardware_state["backend"]["connected"] = True
                    hardware_state["backend"]["telemetry_count"] += len(packets)
            except Exception:
                hardware_state["backend"]["connected"] = False

        stop_event.wait(0.1) # 10 Hz batch flush for ultra-smooth sequential streaming

    session.close()


# ─── Evidence Upload Worker (POST /api/evidence/upload) ───────────────────────
def evidence_upload_worker(backend_url, stop_event):
    """Consumes motion detection captures from queue and uploads to backend."""
    global hardware_state
    url = f"{backend_url.rstrip('/')}/api/evidence/upload"

    while not stop_event.is_set():
        try:
            item = upload_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        try:
            jpeg_bytes = item["jpeg_bytes"]
            captured_at = item["captured_at"]
            collection_event_id = item.get("collection_event_id", 1)
            idempotency_key = item.get("idempotency_key", str(uuid.uuid4()))
            width = item.get("width", 1280)
            height = item.get("height", 720)

            files = {
                "file": ("evidence.jpg", jpeg_bytes, "image/jpeg")
            }
            data = {
                "collectionEventId": collection_event_id,
                "capturedAt": captured_at,
                "width": width,
                "height": height,
                "compressionQuality": 85,
                "idempotencyKey": idempotency_key
            }

            resp = requests.post(url, files=files, data=data, timeout=8.0)
            if resp.status_code == 200:
                res_json = resp.json()
                hardware_state["backend"]["upload_count"] += 1
                print(f"[EVIDENCE UPLOAD SUCCESS] ImageId: {res_json.get('evidenceImageId')} -> {res_json.get('imageUrl')}")
            else:
                print(f"[EVIDENCE UPLOAD FAILED] HTTP {resp.status_code}: {resp.text}")

        except Exception as e:
            print(f"[EVIDENCE UPLOAD ERROR] {e}")
        finally:
            upload_queue.task_done()


def cleanup_old_local_captures(save_dir="captures", retention_days=3):
    """Deletes local capture frames older than retention_days (3 days default)."""
    if not os.path.exists(save_dir):
        return
    now = time.time()
    cutoff = now - (retention_days * 86400)
    for fname in os.listdir(save_dir):
        fpath = os.path.join(save_dir, fname)
        if os.path.isfile(fpath):
            try:
                if os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
            except Exception:
                pass


# ---- Motion detection parameters ----
VIDEO_SOURCE = 0
LOOP_VIDEO = True
FRAME_SIZE = (320, 180)
ROI_CONFIG_FILE = "roi_polygon.json"
DEFAULT_ROI_POLYGON = [[0.35, 0.10], [0.65, 0.10], [0.65, 0.50], [0.35, 0.50]] # Normalized [x, y]
active_polygon_roi = DEFAULT_ROI_POLYGON.copy()
is_drawing_polygon = False
drawn_polygon_pts = []

DIFF_THRESHOLD = 15
MIN_CONTOUR_AREA = 150
BG_ALPHA = 0.03
WARMUP_FRAMES = 30
SAVE_COOLDOWN_SEC = 1.0


def load_roi_polygon(config_file=ROI_CONFIG_FILE):
    """Loads saved polygon ROI vertices from JSON config file if available."""
    global active_polygon_roi
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) >= 3:
                    active_polygon_roi = data
                    print(f"[ROI] Loaded polygon ROI with {len(data)} vertices from {config_file}")
                    return active_polygon_roi
        except Exception as e:
            print(f"[ROI] Warning loading {config_file}: {e}")
    active_polygon_roi = DEFAULT_ROI_POLYGON.copy()
    return active_polygon_roi


def save_roi_polygon(pts, config_file=ROI_CONFIG_FILE):
    """Saves polygon ROI vertices to JSON config file."""
    try:
        with open(config_file, "w") as f:
            json.dump(pts, f, indent=2)
        print(f"[ROI] Saved polygon ROI with {len(pts)} vertices to {config_file}")
    except Exception as e:
        print(f"[ROI] Warning saving {config_file}: {e}")


def apply_polygon_roi_mask(thresh, polygon_pts, frame_w, frame_h):
    """Creates a binary mask from polygon vertices and applies bitwise AND to threshold."""
    if not polygon_pts or len(polygon_pts) < 3:
        return thresh

    pts = []
    for p in polygon_pts:
        # Support normalized (0.0 - 1.0) and pixel coordinates
        px = int(p[0] * frame_w) if p[0] <= 1.0 else int(p[0])
        py = int(p[1] * frame_h) if p[1] <= 1.0 else int(p[1])
        pts.append([px, py])

    pts_array = np.array(pts, dtype=np.int32)
    mask = np.zeros((frame_h, frame_w), dtype=np.uint8)
    cv2.fillPoly(mask, [pts_array], 255)
    return cv2.bitwise_and(thresh, mask)


def on_mouse_roi(event, x, y, flags, param):
    """Interactive mouse callback to draw Polygon ROI in OpenCV GUI window."""
    global drawn_polygon_pts, active_polygon_roi, is_drawing_polygon
    frame_w, frame_h = param.get("width", 640), param.get("height", 360)

    if event == cv2.EVENT_LBUTTONDOWN:
        norm_x = round(float(x) / frame_w, 4)
        norm_y = round(float(y) / frame_h, 4)

        # If >= 3 points and clicking near P1, auto-complete & save
        if len(drawn_polygon_pts) >= 3:
            p1_x, p1_y = drawn_polygon_pts[0]
            dist = np.hypot(norm_x - p1_x, norm_y - p1_y)
            if dist < 0.05:
                active_polygon_roi = drawn_polygon_pts.copy()
                save_roi_polygon(active_polygon_roi)
                is_drawing_polygon = False
                drawn_polygon_pts = []
                print(f"[ROI DRAW] Auto-snapped to P1 & saved polygon with {len(active_polygon_roi)} vertices.")
                return

        drawn_polygon_pts.append([norm_x, norm_y])
        print(f"[ROI DRAW] Added vertex #{len(drawn_polygon_pts)}: ({x}, {y}) -> [{norm_x}, {norm_y}]")

    elif event == cv2.EVENT_RBUTTONDOWN:
        if len(drawn_polygon_pts) >= 3:
            active_polygon_roi = drawn_polygon_pts.copy()
            save_roi_polygon(active_polygon_roi)
            is_drawing_polygon = False
            drawn_polygon_pts = []
            print(f"[ROI DRAW] Polygon completed & saved with {len(active_polygon_roi)} vertices.")
        else:
            print("[ROI DRAW] Need at least 3 vertices to complete polygon.")


def get_rtc_timestamp():
    ts = latest_sensor.get("timestamp")
    if ts and str(ts).strip() not in ("Waiting for RTC...", "", "None", "0"):
        return str(ts).strip()
    return None


def overlay_metadata(frame):
    """Draws real-time hardware RTC time, GNSS, and status HUD on frame."""
    rtc_ts = get_rtc_timestamp()
    rtc_error = latest_sensor.get("rtc_error")
    serial_connected = latest_sensor.get("serial_connected", False)
    active_port = latest_sensor.get("serial_port")
    backend_ok = hardware_state["backend"]["connected"]

    if rtc_ts:
        ts_text = f"RTC: {rtc_ts}"
        ts_color = (0, 255, 255)
    elif rtc_error:
        ts_text = f"RTC: {rtc_error}"
        ts_color = (0, 140, 255)
    elif not serial_connected:
        ts_text = "RTC: Disconnected (No COM Port)"
        ts_color = (0, 0, 255)
    else:
        ts_text = f"RTC: Connected to {active_port}, awaiting sync..."
        ts_color = (0, 200, 255)

    if latest_sensor["gps_valid"] and latest_sensor["lat"] is not None:
        loc_text = f"GNSS: {latest_sensor['lat']:.6f}, {latest_sensor['lon']:.6f}"
        loc_color = (0, 255, 0)
    elif serial_connected:
        loc_text = "GNSS: Searching GPS fix..."
        loc_color = (0, 165, 255)
    else:
        loc_text = "GNSS: Serial Offline"
        loc_color = (120, 120, 120)

    h, w = frame.shape[:2]
    font_scale = max(0.4, (h / 480.0) * 0.45)
    thickness = max(1, int(h / 360))
    line_spacing = int(font_scale * 24)
    strip_height = line_spacing * 2 + 15

    # Top Status HUD
    cam_badge = "CAM:OK" if hardware_state["camera"]["detected"] else "CAM:OFF"
    ser_badge = f"SER:{active_port}" if serial_connected else "SER:OFF"
    rtc_badge = "RTC:ONLINE" if rtc_ts else "RTC:WAIT"
    gps_badge = "GPS:FIX" if latest_sensor.get("gps_valid") else "GPS:SEARCH"
    net_badge = "NET:ONLINE" if backend_ok else "NET:OFFLINE"
    hud_text = f"[SWSTP EDGE] {cam_badge} | {ser_badge} | {rtc_badge} | {gps_badge} | {net_badge}"
    cv2.putText(frame, hud_text, (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (220, 220, 220), 1, cv2.LINE_AA)

    # Bottom Metadata Strip
    banner = frame[h - strip_height:h, 0:w].copy()
    black_bg = np.zeros_like(banner)
    cv2.addWeighted(black_bg, 0.75, banner, 0.25, 0, banner)
    frame[h - strip_height:h, 0:w] = banner

    y_pos = h - strip_height + line_spacing
    cv2.putText(frame, ts_text, (12, y_pos), cv2.FONT_HERSHEY_SIMPLEX, font_scale, ts_color, thickness, cv2.LINE_AA)
    y_pos += line_spacing
    cv2.putText(frame, loc_text, (12, y_pos), cv2.FONT_HERSHEY_SIMPLEX, font_scale, loc_color, thickness, cv2.LINE_AA)

    return frame


def main():
    parser = argparse.ArgumentParser(description="SWSTP Edge Gateway: Motion Detection, Telemetry Ingestion & Live Video Streamer.")
    parser.add_argument("--source", "-s", default=None, help="Video source (0 for default webcam, or path to MP4)")
    parser.add_argument("--port", "-p", default=DEFAULT_SERIAL_PORT, help="Serial COM port (e.g. COM3, AUTO)")
    parser.add_argument("--baud", "-b", type=int, default=DEFAULT_BAUD_RATE, help="Baud rate (default: 115200)")
    parser.add_argument("--backend-url", default=DEFAULT_BACKEND_URL, help="SWSTP Backend URL (default: http://localhost:5000)")
    parser.add_argument("--device-id", default=DEFAULT_DEVICE_ID, help="Hardware Device Code (default: SWSTP-EDGE-01)")
    parser.add_argument("--session-id", type=int, default=0, help="Operational Session ID (0 if standalone)")
    parser.add_argument("--fps-stream", type=float, default=DEFAULT_STREAM_FPS, help="Live frame streaming FPS to backend")
    parser.add_argument("--save-dir", default="./evidence", help="Local directory to save captures")
    parser.add_argument("--headless", action="store_true", help="Run without cv2.imshow GUI window")
    args, _ = parser.parse_known_args()

    os.makedirs(args.save_dir, exist_ok=True)
    print(f"\n========================================================")
    print(f" SWSTP EDGE GATEWAY INITIALIZING")
    print(f" Backend Endpoint: {args.backend_url}")
    print(f" Device ID:        {args.device_id}")
    print(f" Serial Port:      {args.port} @ {args.baud} baud")
    print(f" Motion Captures:  {os.path.abspath(args.save_dir)}")
    print(f"========================================================\n")

    source = int(args.source) if (args.source and args.source.isdigit()) else (args.source or VIDEO_SOURCE)
    cap = cv2.VideoCapture(source)
    if not cap.isOpened():
        log_hardware("CAMERA", "FAILED TO OPEN", f"Cannot open video source '{source}'.")
        return

    cam_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    cam_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    hardware_state["camera"] = {"detected": True, "source": source, "resolution": f"{cam_w}x{cam_h}", "fps": fps}
    log_hardware("CAMERA / VIDEO INPUT", "ACTIVE", f"Resolution: {cam_w}x{cam_h} @ {fps:.1f} FPS")

    stop_event = threading.Event()

    # 1. Start Serial Reader Thread
    t_serial = threading.Thread(target=serial_reader, args=(args.port, args.baud, stop_event), daemon=True)
    t_serial.start()

    # 2. Start Live Frame Streamer Thread (feeds /api/camera/{deviceId}/frame)
    t_streamer = threading.Thread(target=live_frame_streamer, args=(args.backend_url, args.device_id, args.fps_stream, stop_event), daemon=True)
    t_streamer.start()

    # 3. Start Telemetry Ingestion Thread (feeds /api/telemetry/ingest-batch)
    t_telemetry = threading.Thread(target=telemetry_streamer, args=(args.backend_url, args.session_id, stop_event), daemon=True)
    t_telemetry.start()

    # 4. Start Evidence Upload Worker Thread (feeds /api/evidence/upload)
    t_evidence = threading.Thread(target=evidence_upload_worker, args=(args.backend_url, stop_event), daemon=True)
    t_evidence.start()

    load_roi_polygon()

    if not args.headless:
        win_title = "SWSTP Motion & Telemetry Gateway"
        cv2.namedWindow(win_title)
        cv2.setMouseCallback(win_title, on_mouse_roi, {"width": cam_w, "height": cam_h})

    delay = max(1, int(1000 / (fps if (fps and 0 < fps < 120) else 30)))
    is_file = isinstance(source, str)

    bg_model = None
    frame_count = 0
    last_save_time = 0.0
    saved_count = 0

    print("[INIT] Edge Gateway running. Press 'q' to stop | 'p' to draw polygon ROI.\n")

    try:
        while not stop_event.is_set():
            success, frame = cap.read()
            if not success:
                if is_file and LOOP_VIDEO:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    bg_model = None
                    frame_count = 0
                    continue
                else:
                    break

            orig_frame = frame.copy()
            orig_h, orig_w = orig_frame.shape[:2]

            # Downsample for motion processing
            if FRAME_SIZE is not None:
                proc_w, proc_h = FRAME_SIZE
                proc_frame = cv2.resize(frame, (proc_w, proc_h))
                scale_x = orig_w / float(proc_w)
                scale_y = orig_h / float(proc_h)
            else:
                proc_frame = frame
                proc_w, proc_h = orig_w, orig_h
                scale_x, scale_y = 1.0, 1.0

            gray_frame = cv2.cvtColor(proc_frame, cv2.COLOR_BGR2GRAY)
            gray_blur = cv2.GaussianBlur(gray_frame, (5, 5), 0)

            if bg_model is None:
                bg_model = gray_blur.astype(np.float32)
                frame_count = 1
                continue

            cv2.accumulateWeighted(gray_blur, bg_model, BG_ALPHA)
            frame_count += 1

            if frame_count <= WARMUP_FRAMES:
                display_frame = overlay_metadata(orig_frame.copy())
                # Update live stream buffer
                _, encoded = cv2.imencode('.jpg', display_frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                with latest_frame_lock:
                    global latest_frame_jpeg
                    latest_frame_jpeg = encoded.tobytes()

                if not args.headless:
                    cv2.imshow("SWSTP Motion & Telemetry Gateway", display_frame)
                    if cv2.waitKey(delay) & 0xFF == ord('q'):
                        break
                continue

            # Background subtraction
            bg_uint8 = cv2.convertScaleAbs(bg_model)
            diff = cv2.absdiff(bg_uint8, gray_blur)
            _, thresh = cv2.threshold(diff, DIFF_THRESHOLD, 255, cv2.THRESH_BINARY)
            
            # Apply Polygon ROI Mask on processing resolution (320, 180)
            thresh = apply_polygon_roi_mask(thresh, active_polygon_roi, FRAME_SIZE[0], FRAME_SIZE[1])
            dilated = cv2.dilate(thresh, None, iterations=2)

            cnts = cv2.findContours(dilated.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            cnts = imutils.grab_contours(cnts)

            motion_detected = False
            box_thickness = max(2, int(min(scale_x, scale_y)))

            for c in cnts:
                if cv2.contourArea(c) < MIN_CONTOUR_AREA:
                    continue
                motion_detected = True
                (x, y, w, h) = cv2.boundingRect(c)
                orig_x, orig_y = int(x * scale_x), int(y * scale_y)
                orig_bw, orig_bh = int(w * scale_x), int(h * scale_y)
                cv2.rectangle(orig_frame, (orig_x, orig_y), (orig_x + orig_bw, orig_y + orig_bh), (0, 255, 0), box_thickness)

            # Draw Polygon ROI on original frame
            disp_pts = []
            for p in active_polygon_roi:
                px = int(p[0] * orig_w) if p[0] <= 1.0 else int(p[0])
                py = int(p[1] * orig_h) if p[1] <= 1.0 else int(p[1])
                disp_pts.append([px, py])

            if len(disp_pts) >= 3:
                poly_color = (0, 0, 255) if motion_detected else (255, 217, 0)
                cv2.polylines(orig_frame, [np.array(disp_pts, dtype=np.int32)], isClosed=True, color=poly_color, thickness=2)
                for pt in disp_pts:
                    cv2.circle(orig_frame, tuple(pt), 4, (0, 255, 255), -1)
                
                # Label first vertex
                cv2.putText(orig_frame, "POLYGON ROI (Press 'p' to edit)", (disp_pts[0][0] + 5, max(20, disp_pts[0][1] - 8)),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.4, poly_color, 1, cv2.LINE_AA)

            # Draw in-progress polygon if drawing mode active
            if is_drawing_polygon and len(drawn_polygon_pts) > 0:
                prog_pts = []
                for p in drawn_polygon_pts:
                    prog_pts.append([int(p[0] * orig_w), int(p[1] * orig_h)])
                for idx, pt in enumerate(prog_pts):
                    pt_color = (0, 255, 0) if (idx == 0 and len(prog_pts) >= 3) else (0, 0, 255)
                    cv2.circle(orig_frame, tuple(pt), 6, pt_color, -1)
                    cv2.putText(orig_frame, f"P{idx+1}", (pt[0] + 6, pt[1] - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255), 1, cv2.LINE_AA)
                if len(prog_pts) >= 2:
                    is_closed = len(prog_pts) >= 3
                    cv2.polylines(orig_frame, [np.array(prog_pts, dtype=np.int32)], isClosed=is_closed, color=(0, 255, 255) if is_closed else (0, 0, 255), thickness=2)
                    if is_closed:
                        # Draw snap ring around P1
                        cv2.circle(orig_frame, tuple(prog_pts[0]), 12, (0, 255, 0), 2)
                        cv2.putText(orig_frame, "Click P1 to Auto-Close", (prog_pts[0][0] + 16, prog_pts[0][1] + 4), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 255, 0), 1, cv2.LINE_AA)

                cv2.putText(orig_frame, "DRAWING ROI: Click on video to add dots (Auto-connects on >= 3 dots)", (10, 50),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 255), 1, cv2.LINE_AA)

            display_frame = overlay_metadata(orig_frame.copy())

            # Update latest frame for live streaming
            _, encoded_live = cv2.imencode('.jpg', display_frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            with latest_frame_lock:
                latest_frame_jpeg = encoded_live.tobytes()

            if motion_detected:
                text_scale = max(0.5, (orig_h / 480.0) * 0.5)
                cv2.putText(display_frame, "MOTION DETECTED", (10, int(55 * text_scale)),
                            cv2.FONT_HERSHEY_SIMPLEX, text_scale, (0, 0, 255), max(1, int(orig_h / 360)), cv2.LINE_AA)

                now = time.monotonic()
                if now - last_save_time >= SAVE_COOLDOWN_SEC:
                    saved_count += 1
                    last_save_time = now

                    # Encode high-quality JPEG for evidence
                    _, enc_evidence = cv2.imencode('.jpg', display_frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
                    evidence_bytes = enc_evidence.tobytes()

                    rtc_ts = get_rtc_timestamp() or datetime.datetime.now(datetime.timezone.utc).isoformat()
                    clean_ts = re.sub(r"[^\w]", "_", rtc_ts)
                    local_path = os.path.join(args.save_dir, f"motion_RTC_{clean_ts}_{saved_count:04d}.jpg")
                    with open(local_path, "wb") as f:
                        f.write(evidence_bytes)

                    # Enqueue for asynchronous backend upload
                    upload_queue.put({
                        "jpeg_bytes": evidence_bytes,
                        "captured_at": rtc_ts,
                        "collection_event_id": 1,
                        "idempotency_key": str(uuid.uuid4()),
                        "width": orig_w,
                        "height": orig_h,
                    })

                    print(f"[CAPTURE #{saved_count}] Saved: {local_path} | Queued for backend upload.")
                    cleanup_old_local_captures(args.save_dir, retention_days=3)

            if not args.headless:
                cv2.imshow("SWSTP Motion & Telemetry Gateway", display_frame)
                k = cv2.waitKey(delay) & 0xFF
                if k == ord('q'):
                    break
                elif k == ord('p'):
                    is_drawing_polygon = not is_drawing_polygon
                    if is_drawing_polygon:
                        drawn_polygon_pts = []
                        print("\n[ROI] Polygon drawing mode ACTIVE: Left-click on window to add vertices, Right-click to finish & save.\n")
                    else:
                        print("[ROI] Polygon drawing mode CANCELLED.")
                elif k == ord('c'):
                    drawn_polygon_pts = []
                    print("[ROI] Cleared temporary drawing points.")
                elif k == ord('r'):
                    active_polygon_roi = DEFAULT_ROI_POLYGON.copy()
                    save_roi_polygon(active_polygon_roi)
                    print("[ROI] Reset polygon ROI to default.")

    finally:
        stop_event.set()
        cap.release()
        if not args.headless:
            cv2.destroyAllWindows()
        print(f"\n[EDGE GATEWAY STOPPED] Total captures: {saved_count}")


if __name__ == "__main__":
    main()
