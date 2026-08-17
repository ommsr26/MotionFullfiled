"""
Live webcam motion detection with real-world metadata (hardware RTC time + GPS
location) streamed in from an Arduino Uno over USB serial at 115200 baud.

This script features:
  - Live cv2.VideoCapture feed or video file input
  - Robust serial connection to Arduino with COM port auto-discovery (115200 baud default)
  - Native JSON telemetry parser (supports {"type":"telemetry", "rtc":{...}, "gnss":{...}})
  - Strict hardware RTC timestamp parsing (DS3231 / DS1307, ISO, CSV, Month-Name, Custom)
  - GPS lat/lon parser (NEO-6M / NMEA / CSV / JSON)
  - Detailed hardware component detection logs (Camera, Serial, RTC, GPS)
  - On-screen metadata overlay & hardware HUD with live status diagnostics
  - Motion-triggered frame capture saved locally with burned-in RTC metadata

Requires: opencv-python, numpy, imutils, pyserial
    pip install opencv-python numpy imutils pyserial
"""

import argparse
import datetime
import json
import os
import re
import sys
import threading
import time
import cv2
import imutils
import numpy as np
import serial
import serial.tools.list_ports

# ---- Default serial configuration ----
DEFAULT_SERIAL_PORT = "COM3"    # Windows e.g. "COM3" or "AUTO" | Linux e.g. "/dev/ttyUSB0"
DEFAULT_BAUD_RATE = 115200      # 115200 baud for high-speed Arduino RTC/GPS streaming

latest_sensor = {
    "timestamp": None,
    "lat": None,
    "lon": None,
    "gps_valid": False,
    "imu": None,
    "serial_connected": False,
    "serial_port": None,
    "rtc_error": None,
    "available_ports": [],
    "last_raw_line": None,
}

# Real-time hardware status tracker
hardware_state = {
    "camera": {"detected": False, "source": None, "resolution": None, "fps": None},
    "serial": {"connected": False, "port": None, "baud": None, "desc": None},
    "rtc": {"detected": False, "module": "DS3231 (I2C)", "last_ts": None, "logged_online": False, "logged_error": False},
    "gps": {"detected": False, "module": "NEO-6M (UART)", "fix": False, "coords": None, "logged_detected": False, "logged_fix": False},
}

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
    ports = list(serial.tools.list_ports.comports())
    return ports


def find_arduino_port(preferred_port="COM3"):
    """Finds the best candidate serial port for Arduino."""
    available_ports = list_serial_ports()
    if not available_ports:
        return None, []

    # 1. If preferred_port is specified (not AUTO) and exists in available ports, use it
    if preferred_port and preferred_port.upper() != "AUTO":
        for p in available_ports:
            if p.device.upper() == preferred_port.upper():
                return p.device, available_ports

    # 2. Prioritize USB-Serial devices (exclude motherboard COM1 and Bluetooth ports)
    keywords = ["arduino", "ch340", "ch341", "cp210", "ftdi", "usb", "serial", "acm", "prolific"]
    usb_candidates = []

    for p in available_ports:
        hwid = (p.hwid or "").lower()
        desc = (p.description or "").lower()
        dev = p.device.upper()

        # Skip Bluetooth ports
        if "bthenum" in hwid or "bluetooth" in desc:
            continue
        # Skip generic motherboard COM1 if not identified as USB
        if dev == "COM1" and not any(kw in desc for kw in ["usb", "arduino", "ch340"]):
            continue

        if any(kw in desc or kw in hwid for kw in keywords):
            usb_candidates.append(p)

    if usb_candidates:
        return usb_candidates[0].device, available_ports

    # 3. Fallback: return first non-bluetooth port
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
    """Extracts and normalizes timestamp string strictly from hardware RTC output.
    Returns (timestamp_str, error_str)."""
    if not text:
        return None, None

    # Clean out stray control chars
    clean = re.sub(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]", "", str(text)).strip()
    lower = clean.lower()

    # Check for known RTC module error messages from Arduino
    if any(err in lower for err in [
        "waiting for rtc", "couldn't find rtc", "rtc not found",
        "rtc error", "rtc fail", "no rtc", "rtc lost power"
    ]):
        return None, "RTC hardware not detected on Arduino (Check I2C A4/A5 wiring)"

    if lower in ("0", "none", "null", "0.0.0", "waiting...", ""):
        return None, None

    # 1. Standard ISO / Year-first: YYYY[-/. ]MM[-/. ]DD [HH:MM:SS] (with optional AM/PM or 'T')
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

    # 2. Day-first: DD[-/. ]MM[-/. ]YYYY [HH:MM:SS] (with optional AM/PM)
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

    # 3. Month Name Formats (e.g. "Aug 17 2026 17:39:36", "17-Aug-2026 17:39:36", "Mon Aug 17 17:39:36 2026")
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

    # 4. Check for separate Date and Time anywhere in the same string (e.g. "Date: 2026/8/17, Time: 17:39:36")
    date_part = re.search(r"\b(20\d{2})[-/. ](\d{1,2})[-/. ](\d{1,2})\b", clean)
    if not date_part:
        date_part = re.search(r"\b(\d{1,2})[-/. ](\d{1,2})[-/. ](20\d{2})\b", clean)
        if date_part:
            d_val, mo_val, y_val = date_part.group(1), date_part.group(2), date_part.group(3)
        else:
            y_val, mo_val, d_val = None, None, None
    else:
        y_val, mo_val, d_val = date_part.group(1), date_part.group(2), date_part.group(3)

    time_part = re.search(r"\b(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?(?:\s*([AP]M))?\b", clean, re.IGNORECASE)

    if y_val and mo_val and d_val and time_part:
        h = int(time_part.group(1))
        mi = int(time_part.group(2))
        s = int(time_part.group(3)) if time_part.group(3) else 0
        ampm = time_part.group(4)
        if ampm:
            if ampm.upper() == "PM" and h < 12:
                h += 12
            elif ampm.upper() == "AM" and h == 12:
                h = 0
        dt = normalize_datetime(y_val, mo_val, d_val, h, mi, s)
        if dt:
            return dt, None

    # 5. Time-only format from RTC: HH:MM:SS (strictly uses time received from RTC)
    if time_part:
        h = int(time_part.group(1))
        mi = int(time_part.group(2))
        s = int(time_part.group(3)) if time_part.group(3) else 0
        ampm = time_part.group(4)
        if ampm:
            if ampm.upper() == "PM" and h < 12:
                h += 12
            elif ampm.upper() == "AM" and h == 12:
                h = 0
        if 0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 59:
            return f"{h:02d}:{mi:02d}:{s:02d}", None

    # 6. Unix epoch timestamp from RTC (10 or 13 digits)
    m = re.search(r"\b(1[6-9]\d{8,11})\b", clean)
    if m:
        try:
            val = int(m.group(1))
            if val > 1e11:  # Milliseconds
                val = val / 1000.0
            dt = datetime.datetime.fromtimestamp(val).strftime("%Y-%m-%d %H:%M:%S")
            return dt, None
        except Exception:
            pass

    return None, None


def parse_serial_line(line):
    """Parses raw text from Arduino containing RTC, GNSS/GPS, or IMU data (supports JSON telemetry and CSV)."""
    global latest_sensor, hardware_state
    line = re.sub(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]", "", line).strip()
    if not line:
        return False

    parsed_something = False

    # 0. Check for JSON telemetry payload (e.g. {"type":"telemetry", "rtc":{...}, "gnss":{...}})
    json_start = line.find("{")
    json_end = line.rfind("}")
    if json_start != -1 and json_end != -1 and json_end > json_start:
        json_candidate = line[json_start:json_end + 1]
        try:
            telemetry = json.loads(json_candidate)
            if isinstance(telemetry, dict):
                # 0.1 Parse RTC from JSON
                if "rtc" in telemetry and isinstance(telemetry["rtc"], dict):
                    rtc_obj = telemetry["rtc"]
                    raw_ts = rtc_obj.get("timestamp")
                    is_valid = rtc_obj.get("valid", True)
                    if is_valid and raw_ts:
                        ts, _ = extract_rtc_timestamp(str(raw_ts))
                        if ts:
                            if not hardware_state["rtc"]["logged_online"]:
                                hardware_state["rtc"]["detected"] = True
                                hardware_state["rtc"]["logged_online"] = True
                                hardware_state["rtc"]["last_ts"] = ts
                                log_hardware("RTC MODULE (DS3231)", "ONLINE & SYNCHRONIZED", f"JSON Timestamp: {ts}")
                            elif latest_sensor["timestamp"] != ts:
                                print(f"[RTC STREAM] {ts}")

                            latest_sensor["timestamp"] = ts
                            latest_sensor["rtc_error"] = None
                            parsed_something = True
                    elif not is_valid:
                        latest_sensor["rtc_error"] = "RTC reports invalid state in telemetry"

                # 0.2 Parse GNSS / GPS from JSON
                if "gnss" in telemetry and isinstance(telemetry["gnss"], dict):
                    gnss_obj = telemetry["gnss"]
                    has_data = gnss_obj.get("data_received", False)
                    has_fix = gnss_obj.get("fix", False)
                    lat = gnss_obj.get("latitude")
                    lon = gnss_obj.get("longitude")

                    if has_data and not hardware_state["gps"]["logged_detected"]:
                        hardware_state["gps"]["detected"] = True
                        hardware_state["gps"]["logged_detected"] = True
                        log_hardware("GPS MODULE (NEO-6M)", "STREAM DETECTED", "GNSS stream active in telemetry.")

                    if has_fix and lat is not None and lon is not None:
                        try:
                            lat_val = float(lat)
                            lon_val = float(lon)
                            if not hardware_state["gps"]["logged_fix"]:
                                hardware_state["gps"]["fix"] = True
                                hardware_state["gps"]["logged_fix"] = True
                                log_hardware("GPS MODULE (NEO-6M)", "SATELLITE FIX ACQUIRED", f"Lat: {lat_val:.6f}, Lon: {lon_val:.6f}")
                            latest_sensor["lat"] = lat_val
                            latest_sensor["lon"] = lon_val
                            latest_sensor["gps_valid"] = True
                            parsed_something = True
                        except ValueError:
                            pass
                    else:
                        latest_sensor["gps_valid"] = False

                # 0.3 Parse IMU from JSON
                if "imu" in telemetry and isinstance(telemetry["imu"], dict):
                    latest_sensor["imu"] = telemetry["imu"]

                if parsed_something:
                    return True
        except json.JSONDecodeError:
            pass

    # Check for direct hardware error announcements
    lower = line.lower()
    if any(err in lower for err in ["waiting for rtc", "couldn't find rtc", "rtc not found", "rtc error", "rtc fail"]):
        latest_sensor["rtc_error"] = "Hardware Not Detected on Arduino (Check I2C A4/A5)"
        if not hardware_state["rtc"]["logged_error"]:
            hardware_state["rtc"]["detected"] = False
            hardware_state["rtc"]["logged_error"] = True
            log_hardware("RTC MODULE (DS3231)", "HARDWARE NOT DETECTED",
                         "Arduino cannot communicate with DS3231. Check I2C wiring (SDA->A4, SCL->A5, VCC 5V, GND).")
        return True

    # 1. Check for CSV / DATA protocol format: DATA,timestamp,lat,lon,valid
    if line.startswith("DATA,") or line.startswith("DATA:"):
        parts = [p.strip() for p in re.split(r"[,;]", line)]
        if len(parts) >= 2:
            combined_candidate = f"{parts[1]} {parts[2]}" if len(parts) >= 3 and ":" in parts[2] and not parts[1].startswith(":") else parts[1]
            ts, err = extract_rtc_timestamp(combined_candidate)
            if err:
                latest_sensor["rtc_error"] = err
                if not hardware_state["rtc"]["logged_error"]:
                    hardware_state["rtc"]["detected"] = False
                    hardware_state["rtc"]["logged_error"] = True
                    log_hardware("RTC MODULE (DS3231)", "HARDWARE NOT DETECTED", err)
            elif ts:
                if not hardware_state["rtc"]["logged_online"]:
                    hardware_state["rtc"]["detected"] = True
                    hardware_state["rtc"]["logged_online"] = True
                    hardware_state["rtc"]["last_ts"] = ts
                    log_hardware("RTC MODULE (DS3231)", "ONLINE & SYNCHRONIZED", f"Hardware timestamp: {ts}")
                elif latest_sensor["timestamp"] != ts:
                    print(f"[RTC STREAM] {ts}")

                latest_sensor["timestamp"] = ts
                latest_sensor["rtc_error"] = None
                parsed_something = True

            # GPS extraction from DATA packet
            offset = 2 if (len(parts) >= 6 and ":" in parts[2] and not parts[1].startswith(":")) else 1
            if len(parts) >= offset + 3:
                try:
                    lat_str = parts[offset + 1]
                    lon_str = parts[offset + 2]
                    valid_str = parts[offset + 3] if len(parts) > offset + 3 else "1"
                    lat_val = float(lat_str)
                    lon_val = float(lon_str)
                    valid_flag = (valid_str.strip() == "1") or (abs(lat_val) > 0.0001 and abs(lon_val) > 0.0001)

                    if not hardware_state["gps"]["logged_detected"]:
                        hardware_state["gps"]["detected"] = True
                        hardware_state["gps"]["logged_detected"] = True
                        log_hardware("GPS MODULE (NEO-6M)", "STREAM DETECTED", "Receiving GNSS telemetry from Arduino.")

                    if valid_flag and -90 <= lat_val <= 90 and -180 <= lon_val <= 180:
                        if not hardware_state["gps"]["logged_fix"]:
                            hardware_state["gps"]["fix"] = True
                            hardware_state["gps"]["logged_fix"] = True
                            log_hardware("GPS MODULE (NEO-6M)", "SATELLITE FIX ACQUIRED", f"Lat: {lat_val:.6f}, Lon: {lon_val:.6f}")

                        latest_sensor["lat"] = lat_val
                        latest_sensor["lon"] = lon_val
                        latest_sensor["gps_valid"] = True
                        parsed_something = True
                except (ValueError, IndexError):
                    pass

    # 2. General timestamp extraction across arbitrary line formats
    if not parsed_something:
        ts, err = extract_rtc_timestamp(line)
        if err:
            latest_sensor["rtc_error"] = err
        elif ts:
            if not hardware_state["rtc"]["logged_online"]:
                hardware_state["rtc"]["detected"] = True
                hardware_state["rtc"]["logged_online"] = True
                hardware_state["rtc"]["last_ts"] = ts
                log_hardware("RTC MODULE (DS3231)", "ONLINE & SYNCHRONIZED", f"Parsed timestamp: {ts}")
            elif latest_sensor["timestamp"] != ts:
                print(f"[RTC STREAM] {ts}")

            latest_sensor["timestamp"] = ts
            latest_sensor["rtc_error"] = None
            parsed_something = True

    # 3. Check for standalone GPS format (e.g. "GPS: 12.971598, 77.594562" or "LAT: 12.971 LON: 77.594")
    gps_match = re.search(r"(-?\d{1,3}\.\d{3,})\s*,\s*(-?\d{1,3}\.\d{3,})", line)
    if gps_match:
        try:
            val1, val2 = float(gps_match.group(1)), float(gps_match.group(2))
            if -90 <= val1 <= 90 and -180 <= val2 <= 180 and (abs(val1) > 0.0001 or abs(val2) > 0.0001):
                if not hardware_state["gps"]["logged_fix"]:
                    hardware_state["gps"]["fix"] = True
                    hardware_state["gps"]["logged_fix"] = True
                    log_hardware("GPS MODULE (NEO-6M)", "SATELLITE FIX ACQUIRED", f"Lat: {val1:.6f}, Lon: {val2:.6f}")

                latest_sensor["lat"] = val1
                latest_sensor["lon"] = val2
                latest_sensor["gps_valid"] = True
                parsed_something = True
        except ValueError:
            pass

    return parsed_something


def serial_reader(port_arg, baud_rate, stop_event):
    """Runs in background thread, connects reliably to Arduino at 115200 baud,
    reads lines, and updates latest_sensor continuously."""
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
            if last_logged_target != "NONE":
                print(f"[SERIAL] No COM ports found. Waiting for Arduino connection...")
                last_logged_target = "NONE"
            stop_event.wait(reconnect_delay)
            continue

        port_descriptions = ", ".join([f"{p.device} ({p.description})" for p in port_objs])
        if last_logged_target != target_port:
            print(f"[SERIAL] Available COM ports: [{port_descriptions}]")
            print(f"[SERIAL] Connecting to {target_port} @ {baud_rate} baud...")
            last_logged_target = target_port

        ser = None
        try:
            ser = serial.Serial(
                port=target_port,
                baudrate=baud_rate,
                timeout=1.0,
            )
            # Give Arduino bootloader 1.5s to settle
            time.sleep(1.5)
            ser.reset_input_buffer()

            latest_sensor["serial_connected"] = True
            latest_sensor["serial_port"] = target_port
            hardware_state["serial"] = {
                "connected": True,
                "port": target_port,
                "baud": baud_rate,
                "desc": port_descriptions,
            }
            log_hardware("SERIAL BRIDGE", "CONNECTED", f"Arduino communication established on {target_port} at {baud_rate} baud.")

            idle_count = 0
            while not stop_event.is_set() and ser.is_open:
                try:
                    raw_bytes = ser.readline()
                    if not raw_bytes:
                        idle_count += 1
                        if idle_count >= 10:
                            idle_count = 0
                            print(f"[SERIAL] Port {target_port} open @ {baud_rate} baud, awaiting incoming sensor lines...")
                        continue

                    idle_count = 0
                    line = raw_bytes.decode("utf-8", errors="ignore").strip()
                    if line:
                        latest_sensor["last_raw_line"] = line
                        print(f"[SERIAL RAW] {line}")
                        parse_serial_line(line)

                except serial.SerialException as e:
                    print(f"[SERIAL ERROR] Connection lost on {target_port}: {e}")
                    break
                except Exception as e:
                    print(f"[SERIAL READ EXCEPTION] {e}")
                    break

        except serial.SerialException as e:
            latest_sensor["serial_connected"] = False
            latest_sensor["serial_port"] = None
            hardware_state["serial"]["connected"] = False
            print(f"[SERIAL LOCK/ERROR] Could not open {target_port}: {e}")
            print("=" * 65)
            print(f" [IMPORTANT] If Arduino IDE Serial Monitor is OPEN, CLOSE IT NOW!")
            print(f"             Windows allows only ONE program to use {target_port} at a time.")
            print("=" * 65)
        except Exception as e:
            latest_sensor["serial_connected"] = False
            latest_sensor["serial_port"] = None
            hardware_state["serial"]["connected"] = False
            print(f"[SERIAL EXCEPTION] {e}")
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


# ---- Motion detection settings ----
VIDEO_SOURCE = 0  # 0 for default webcam, or path to video file e.g. r"C:\Users\Admin\Desktop\Solid Waste\motion detect\motion test.mp4"
LOOP_VIDEO = True            # Loop video file continuously when reaching EOF
FRAME_SIZE = (320, 180)      # Resolution for motion detection processing; set to None to run at full res
ROI = (231, 17, 86, 43)      # (x, y, w, h) in the processing frame -- adjust to your scene
DIFF_THRESHOLD = 15          # Pixel-level threshold for background diff
MIN_CONTOUR_AREA = 150       # Ignore contours smaller than this (pixels^2 in proc frame)
BG_ALPHA = 0.03              # Running-average learning rate: lower = slower adaptation (0.01-0.1 is typical)
WARMUP_FRAMES = 30           # Number of frames to accumulate before starting detection

# ---- Frame capture settings ----
SAVE_DIR = r"C:\Users\Admin\Desktop\Solid Waste\motion detect\motion_captures"  # Directory to save captured frames
SAVE_COOLDOWN_SEC = 1.0      # Minimum seconds between saved frames (avoids flooding disk)
SAVE_ROI_CROP = False        # Also save a tight crop of just the ROI region alongside the full frame


def apply_roi_mask(thresh, roi):
    x, y, w, h = roi
    mask = np.zeros_like(thresh)
    mask[y:y + h, x:x + w] = 255
    return cv2.bitwise_and(thresh, mask)


def get_rtc_timestamp():
    """Returns hardware RTC module timestamp if received over serial, otherwise None."""
    ts = latest_sensor.get("timestamp")
    if ts and str(ts).strip() not in ("Waiting for RTC...", "", "None", "0"):
        return str(ts).strip()
    return None


def overlay_metadata(frame):
    """Draws real-time hardware RTC time, GNSS location, and hardware status HUD on the image frame."""
    rtc_ts = get_rtc_timestamp()
    rtc_error = latest_sensor.get("rtc_error")
    serial_connected = latest_sensor.get("serial_connected", False)
    active_port = latest_sensor.get("serial_port")

    # Determine RTC status text & indicator color (strictly hardware RTC)
    if rtc_ts:
        ts_text = f"RTC: {rtc_ts}"
        ts_color = (0, 255, 255)  # Bright Cyan / Yellow: Hardware RTC valid
    elif rtc_error:
        ts_text = f"RTC: {rtc_error}"
        ts_color = (0, 140, 255)  # Orange: Arduino reports RTC missing / I2C failure
    elif not serial_connected:
        ports = latest_sensor.get("available_ports", [])
        if ports:
            ts_text = f"RTC: Disconnected (Close Arduino IDE Serial Monitor)"
        else:
            ts_text = "RTC: Disconnected (No COM Port Found)"
        ts_color = (0, 0, 255)  # Red: Serial disconnected
    else:
        ts_text = f"RTC: Connected to {active_port}, waiting for RTC stream..."
        ts_color = (0, 200, 255)  # Amber: Connected, awaiting packets

    # GNSS / GPS location
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

    # Dynamic font scaling based on video height
    font_scale = max(0.4, (h / 480.0) * 0.45)
    thickness = max(1, int(h / 360))
    line_spacing = int(font_scale * 24)
    strip_height = line_spacing * 2 + 15

    # 1. Draw top Hardware Status HUD Banner
    cam_badge = "CAM:OK" if hardware_state["camera"]["detected"] else "CAM:OFF"
    ser_badge = f"SER:{active_port}" if serial_connected else "SER:OFF (PORT LOCKED?)"
    rtc_badge = "RTC:ONLINE" if rtc_ts else ("RTC:ERROR" if rtc_error else "RTC:WAIT")
    gps_badge = "GPS:FIX" if latest_sensor.get("gps_valid") else ("GPS:SEARCH" if serial_connected else "GPS:OFF")
    hud_text = f"[HW] {cam_badge} | {ser_badge} | {rtc_badge} | {gps_badge}"
    cv2.putText(frame, hud_text, (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (220, 220, 220), 1, cv2.LINE_AA)

    # 2. Draw semi-transparent dark banner at bottom of image
    banner = frame[h - strip_height:h, 0:w].copy()
    black_bg = np.zeros_like(banner)
    cv2.addWeighted(black_bg, 0.75, banner, 0.25, 0, banner)
    frame[h - strip_height:h, 0:w] = banner

    # Draw text metadata lines (RTC & GNSS)
    y_pos = h - strip_height + line_spacing
    cv2.putText(frame, ts_text, (12, y_pos), cv2.FONT_HERSHEY_SIMPLEX,
                font_scale, ts_color, thickness, cv2.LINE_AA)

    y_pos += line_spacing
    cv2.putText(frame, loc_text, (12, y_pos), cv2.FONT_HERSHEY_SIMPLEX,
                font_scale, loc_color, thickness, cv2.LINE_AA)

    return frame


def save_capture(display_frame, orig_frame, roi_scaled, save_dir, saved_count):
    """Save the annotated full frame with hardware RTC timestamp and synchronized filename."""
    rtc_ts = get_rtc_timestamp()

    if rtc_ts:
        clean_ts = re.sub(r"[^\w]", "_", rtc_ts)
        base_name = f"motion_RTC_{clean_ts}_{saved_count:04d}"
    else:
        base_name = f"motion_no_RTC_{saved_count:04d}"

    # Full annotated frame (contains burned-in RTC & GNSS metadata)
    full_path = os.path.join(save_dir, f"{base_name}_full.jpg")
    cv2.imwrite(full_path, display_frame, [cv2.IMWRITE_JPEG_QUALITY, 92])

    # ROI crop from the clean original frame (no text overlay)
    if SAVE_ROI_CROP:
        rx, ry, rw, rh = roi_scaled
        crop = orig_frame[ry:ry + rh, rx:rx + rw]
        if crop.size > 0:
            crop_path = os.path.join(save_dir, f"{base_name}_roi.jpg")
            cv2.imwrite(crop_path, crop, [cv2.IMWRITE_JPEG_QUALITY, 92])

    return full_path


def main():
    parser = argparse.ArgumentParser(description="Live motion detection with Arduino RTC + GPS metadata overlay.")
    parser.add_argument("--source", "-s", default=None, help="Path to video file or webcam index (e.g. 0)")
    parser.add_argument("--port", "-p", default=DEFAULT_SERIAL_PORT, help="Serial COM port (e.g. COM3, COM4, or AUTO)")
    parser.add_argument("--baud", "-b", type=int, default=DEFAULT_BAUD_RATE, help="Serial Baud rate (default: 115200)")
    parser.add_argument("--no-loop", action="store_true", help="Disable video looping on EOF for video files")
    parser.add_argument("--save-dir", default=None, help="Directory to save motion captures (overrides SAVE_DIR)")
    parser.add_argument("--list-ports", action="store_true", help="List all available COM ports and exit")
    args, _ = parser.parse_known_args()

    # If --list-ports requested, print detected ports and exit
    if args.list_ports:
        detected = list_serial_ports()
        if not detected:
            print("No serial COM ports detected.")
        else:
            print(f"Detected {len(detected)} serial port(s):")
            for p in detected:
                print(f"  - {p.device}: {p.description} (HWID: {p.hwid})")
        sys.exit(0)

    # Determine input source (CLI argument > VIDEO_SOURCE constant)
    if args.source is not None:
        source = int(args.source) if args.source.isdigit() else args.source
    else:
        source = VIDEO_SOURCE

    save_dir = args.save_dir if args.save_dir else SAVE_DIR
    os.makedirs(save_dir, exist_ok=True)
    print(f"[INIT] Motion captures directory: {save_dir}")

    # Open video capture device
    cap = cv2.VideoCapture(source)
    if not cap.isOpened():
        log_hardware("CAMERA", "FAILED TO OPEN", f"Could not access video source '{source}'.")
        return

    cam_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    cam_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    hardware_state["camera"] = {
        "detected": True,
        "source": source,
        "resolution": f"{cam_w}x{cam_h}",
        "fps": fps,
    }
    log_hardware("CAMERA / VIDEO INPUT", "DETECTED & ACTIVE",
                 f"Source: '{source}' | Native Resolution: {cam_w}x{cam_h} | Target FPS: {fps:.1f}")

    # Start background serial reader thread at 115200 baud
    stop_serial = threading.Event()
    serial_thread = threading.Thread(
        target=serial_reader,
        args=(args.port, args.baud, stop_serial),
        daemon=True,
    )
    serial_thread.start()

    # Calculate frame delay based on video FPS (default to ~30 FPS for webcam)
    delay = max(1, int(1000 / (fps if (fps and fps > 0 and fps < 120) else 30)))
    is_file = isinstance(source, str)
    loop_enabled = LOOP_VIDEO and not args.no_loop

    print("[INIT] Press 'q' in the video window to quit.\n")

    bg_model = None
    frame_count = 0
    last_save_time = 0.0
    saved_count = 0

    try:
        while True:
            success, frame = cap.read()
            if not success:
                if is_file and loop_enabled:
                    print("[VIDEO] End of file reached. Looping...")
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    bg_model = None
                    frame_count = 0
                    continue
                else:
                    print("[VIDEO] End of video stream.")
                    break

            # Keep original frame at full input resolution for display/output
            orig_frame = frame.copy()
            orig_h, orig_w = orig_frame.shape[:2]

            # Prepare processing frame for motion detection
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

            # --- Adaptive background model ---
            if bg_model is None:
                bg_model = gray_blur.astype(np.float32)
                frame_count = 1
                continue

            cv2.accumulateWeighted(gray_blur, bg_model, BG_ALPHA)
            frame_count += 1

            # Warm-up phase
            if frame_count <= WARMUP_FRAMES:
                display_frame = overlay_metadata(orig_frame.copy())
                warming = f"Warming up... ({frame_count}/{WARMUP_FRAMES})"
                cv2.putText(display_frame, warming, (10, 48),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 200, 255), 1, cv2.LINE_AA)
                cv2.imshow("Motion Detection", display_frame)
                if cv2.waitKey(delay) & 0xFF == ord('q'):
                    break
                continue

            # Diff current frame against running average background
            bg_uint8 = cv2.convertScaleAbs(bg_model)
            diff = cv2.absdiff(bg_uint8, gray_blur)
            _, thresh = cv2.threshold(diff, DIFF_THRESHOLD, 255, cv2.THRESH_BINARY)

            # Apply ROI mask and morphological dilation
            thresh = apply_roi_mask(thresh, ROI)
            dilated = cv2.dilate(thresh, None, iterations=2)

            cnts = cv2.findContours(dilated.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            cnts = imutils.grab_contours(cnts)

            motion_detected = False
            box_thickness = max(2, int(min(scale_x, scale_y)))

            for c in cnts:
                area = cv2.contourArea(c)
                if area < MIN_CONTOUR_AREA:
                    continue
                motion_detected = True
                (x, y, w, h) = cv2.boundingRect(c)

                # Map bounding box back to original resolution
                orig_x = int(x * scale_x)
                orig_y = int(y * scale_y)
                orig_bw = int(w * scale_x)
                orig_bh = int(h * scale_y)

                cv2.rectangle(orig_frame, (orig_x, orig_y), (orig_x + orig_bw, orig_y + orig_bh),
                              (0, 255, 0), box_thickness)

            # Draw ROI outline in thin red on the display frame
            roi_x = int(ROI[0] * scale_x)
            roi_y = int(ROI[1] * scale_y)
            roi_w = int(ROI[2] * scale_x)
            roi_h = int(ROI[3] * scale_y)
            cv2.rectangle(orig_frame, (roi_x, roi_y), (roi_x + roi_w, roi_y + roi_h), (0, 0, 200), 1)

            display_frame = overlay_metadata(orig_frame.copy())

            if motion_detected:
                text_scale = max(0.5, (orig_h / 480.0) * 0.5)
                text_thick = max(1, int(orig_h / 360))
                cv2.putText(display_frame, "MOTION DETECTED", (10, int(55 * text_scale)),
                            cv2.FONT_HERSHEY_SIMPLEX, text_scale, (0, 0, 255), text_thick, cv2.LINE_AA)

                # Save frame if cooldown has elapsed
                now = time.monotonic()
                if now - last_save_time >= SAVE_COOLDOWN_SEC:
                    roi_scaled_coords = (roi_x, roi_y, roi_w, roi_h)
                    saved_path = save_capture(display_frame, orig_frame, roi_scaled_coords, save_dir, saved_count)
                    saved_count += 1
                    last_save_time = now
                    print(f"[CAPTURE #{saved_count}] Saved: {saved_path}")

                # Show live capture counter on screen
                counter_text = f"Captures: {saved_count}"
                cv2.putText(display_frame, counter_text, (orig_w - 160, 25),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 200, 255), 1, cv2.LINE_AA)

            cv2.imshow("Motion Detection", display_frame)

            if cv2.waitKey(delay) & 0xFF == ord('q'):
                break

    finally:
        stop_serial.set()
        cap.release()
        cv2.destroyAllWindows()
        print(f"\n[SESSION ENDED] Total captures saved: {saved_count} -> {save_dir}")


if __name__ == "__main__":
    main()
