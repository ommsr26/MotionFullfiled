"""
Live webcam motion detection with real-world metadata (RTC time + GPS
location) streamed in from an Arduino Uno over USB serial.

This replaces the original Colab notebook's video-file + IP-geolocation
version with:
  - a live cv2.VideoCapture(0) webcam feed instead of a saved .mp4
  - timestamp + GPS lat/lon read from the Arduino (DS3231 + NEO-6M)
    instead of the laptop's system clock + IP-based location guess
  - continuous live display instead of scanning a whole file for the
    single "best" frame
  - motion-triggered frame capture saved locally

Requires: opencv-python, numpy, imutils, pyserial
    pip install opencv-python numpy imutils pyserial
"""

import argparse
import os
import threading
import time
import cv2
import imutils
import numpy as np
import serial

import re

# ---- Arduino serial setup ----
SERIAL_PORT = "COM3"   # Windows e.g. "COM3" | Linux e.g. "/dev/ttyUSB0" | Mac e.g. "/dev/tty.usbmodemXXXX"
BAUD_RATE = 9600

latest_sensor = {
    "timestamp": None,
    "lat": None,
    "lon": None,
    "gps_valid": False,
    "imu": None,
}


def parse_serial_line(line):
    """Parses raw text from Arduino containing RTC, GNSS/GPS, or IMU data."""
    global latest_sensor
    line = line.strip()
    if not line:
        return False

    parsed_something = False

    # 1. Check for standard format: DATA,timestamp,lat,lon,valid
    if line.startswith("DATA,"):
        parts = line.split(",")
        if len(parts) >= 5:
            _, ts, lat, lon, valid = parts[:5]
            ts_val = ts.strip()
            if ts_val and ts_val not in ("Waiting for RTC...", "0", "None", ""):
                if latest_sensor["timestamp"] != ts_val:
                    print(f"[RTC UPDATE] Timestamp: {ts_val}")
                latest_sensor["timestamp"] = ts_val
                parsed_something = True

            valid_flag = (valid.strip() == "1")
            latest_sensor["gps_valid"] = valid_flag
            if valid_flag:
                try:
                    lat_val = float(lat.strip())
                    lon_val = float(lon.strip())
                    if latest_sensor["lat"] != lat_val or latest_sensor["lon"] != lon_val:
                        print(f"[GNSS UPDATE] Lat: {lat_val}, Lon: {lon_val}")
                    latest_sensor["lat"] = lat_val
                    latest_sensor["lon"] = lon_val
                    parsed_something = True
                except ValueError:
                    pass
            return parsed_something

    # 2. Check for RTC timestamp (e.g. "2026-08-14 16:35:00" or "16:35:00")
    date_match = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})", line)
    if date_match:
        ts_val = date_match.group(1)
        if latest_sensor["timestamp"] != ts_val:
            print(f"[RTC UPDATE] Parsed date/time: {ts_val}")
        latest_sensor["timestamp"] = ts_val
        parsed_something = True
    else:
        time_match = re.search(r"\b(\d{2}:\d{2}:\d{2})\b", line)
        if time_match:
            ts_val = f"{time.strftime('%Y-%m-%d')} {time_match.group(1)}"
            if latest_sensor["timestamp"] != ts_val:
                print(f"[RTC UPDATE] Parsed time: {ts_val}")
            latest_sensor["timestamp"] = ts_val
            parsed_something = True

    # 3. Check for GPS coordinates (e.g. "GPS: 12.971598, 77.594562" or "LAT: 12.971 LON: 77.594")
    gps_match = re.search(r"(-?\d{1,3}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)", line)
    if gps_match:
        try:
            val1, val2 = float(gps_match.group(1)), float(gps_match.group(2))
            if -90 <= val1 <= 90 and -180 <= val2 <= 180:
                latest_sensor["lat"] = val1
                latest_sensor["lon"] = val2
                latest_sensor["gps_valid"] = True
                print(f"[GNSS UPDATE] Parsed coordinates: {val1}, {val2}")
                parsed_something = True
        except ValueError:
            pass

    return parsed_something


def serial_reader(port, primary_baud):
    """Runs in background, auto-detects Baud Rate (115200/9600), logs debug details,
    and updates latest_sensor continuously."""
    candidate_bauds = [primary_baud] + [b for b in [115200, 9600, 57600, 38400] if b != primary_baud]

    while True:
        ser = None
        confirmed_baud = None

        # 1. Probe candidate baud rates
        for b in candidate_bauds:
            try:
                test_ser = serial.Serial(port, b, timeout=1.5)
                print(f"[SERIAL DEBUG] Probing {port} at {b} baud...")
                time.sleep(0.5)
                # Read sample lines
                valid_line_read = False
                for _ in range(5):
                    raw = test_ser.readline().decode("utf-8", errors="ignore").strip()
                    if raw:
                        print(f"[SERIAL RAW @ {b} baud] {raw}")
                        valid_line_read = True
                        break
                if valid_line_read:
                    ser = test_ser
                    confirmed_baud = b
                    print(f"[SERIAL OK] Confirmed communication with Arduino on {port} at {b} baud.")
                    break
                else:
                    test_ser.close()
            except serial.SerialException as e:
                print(f"[SERIAL LOCK] Could not open {port} at {b} baud: {e}")
                print("[SERIAL LOCK] (Note: If Arduino Serial Monitor is open in Arduino IDE, close it to free the port).")
                time.sleep(2)
                break

        if ser is None:
            time.sleep(3)
            continue

        # 2. Main reading loop with active connection
        while ser and ser.is_open:
            try:
                line = ser.readline().decode("utf-8", errors="ignore").strip()
            except Exception as e:
                print(f"[SERIAL READ ERROR] Connection lost on {port}: {e}")
                break

            if line:
                print(f"[SERIAL RAW] {line}")
                parse_serial_line(line)

        if ser:
            try:
                ser.close()
            except Exception:
                pass
        time.sleep(2)


serial_thread = threading.Thread(
    target=serial_reader, args=(SERIAL_PORT, BAUD_RATE), daemon=True
)
serial_thread.start()

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
    """Draws real-time RTC time and GNSS location on the image frame."""
    # Burned RTC Timestamp: strictly from hardware RTC module
    rtc_ts = get_rtc_timestamp()
    if rtc_ts:
        ts_text = f"RTC: {rtc_ts}"
    else:
        ts_text = "RTC: Waiting for RTC..."

    # GNSS / GPS location
    if latest_sensor["gps_valid"] and latest_sensor["lat"] is not None:
        loc_text = f"GNSS: {latest_sensor['lat']:.6f}, {latest_sensor['lon']:.6f}"
    else:
        loc_text = "GNSS: Searching fix..."

    h, w = frame.shape[:2]

    # Dynamic font scaling based on video height
    font_scale = max(0.4, (h / 480.0) * 0.45)
    thickness = max(1, int(h / 360))
    line_spacing = int(font_scale * 24)
    strip_height = line_spacing * 2 + 15

    # Draw semi-transparent dark banner at bottom of image
    banner = frame[h - strip_height:h, 0:w].copy()
    black_bg = np.zeros_like(banner)
    cv2.addWeighted(black_bg, 0.75, banner, 0.25, 0, banner)
    frame[h - strip_height:h, 0:w] = banner

    # Draw text metadata lines (RTC & GNSS)
    y_pos = h - strip_height + line_spacing
    cv2.putText(frame, ts_text, (12, y_pos), cv2.FONT_HERSHEY_SIMPLEX,
                font_scale, (0, 255, 255) if rtc_ts else (0, 165, 255), thickness, cv2.LINE_AA)

    y_pos += line_spacing
    cv2.putText(frame, loc_text, (12, y_pos), cv2.FONT_HERSHEY_SIMPLEX,
                font_scale, (0, 255, 0) if latest_sensor["gps_valid"] else (0, 165, 255),
                thickness, cv2.LINE_AA)

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
    parser = argparse.ArgumentParser(description="Motion detection on webcam or video file.")
    parser.add_argument("--source", "-s", default=None, help="Path to video file or webcam index (e.g. 0)")
    parser.add_argument("--no-loop", action="store_true", help="Disable video looping on EOF")
    parser.add_argument("--save-dir", default=None, help="Directory to save motion captures (overrides SAVE_DIR)")
    args, _ = parser.parse_known_args()

    # Determine input source (CLI argument > VIDEO_SOURCE constant)
    if args.source is not None:
        source = int(args.source) if args.source.isdigit() else args.source
    else:
        source = VIDEO_SOURCE

    save_dir = args.save_dir if args.save_dir else SAVE_DIR
    os.makedirs(save_dir, exist_ok=True)
    print(f"Motion captures will be saved to: {save_dir}")

    cap = cv2.VideoCapture(source)
    if not cap.isOpened():
        print(f"Error: Could not open video source '{source}'.")
        return

    # Calculate frame delay based on video FPS (default to ~30 FPS for webcam)
    fps = cap.get(cv2.CAP_PROP_FPS)
    delay = max(1, int(1000 / (fps if (fps and fps > 0 and fps < 120) else 30)))
    is_file = isinstance(source, str)
    loop_enabled = LOOP_VIDEO and not args.no_loop

    print(f"Reading from source: {source}")
    print("Press 'q' to quit.")

    # bg_model holds a float32 running average of the grayscale proc frame.
    # It is initialised from the very first frame and continuously updated
    # with cv2.accumulateWeighted so that a static scene always produces a
    # near-zero diff and only genuine change triggers detection.
    bg_model = None
    frame_count = 0
    last_save_time = 0.0
    saved_count = 0

    while True:
        success, frame = cap.read()
        if not success:
            if is_file and loop_enabled:
                print("End of video reached. Looping...")
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                # Reset background model on loop so stale bg doesn't invert detections
                bg_model = None
                frame_count = 0
                continue
            else:
                print("End of video stream.")
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
            # Seed the background with the first frame
            bg_model = gray_blur.astype(np.float32)
            frame_count = 1
            continue

        # Accumulate the running average background
        cv2.accumulateWeighted(gray_blur, bg_model, BG_ALPHA)
        frame_count += 1

        # Skip detection during warm-up so the model has time to settle
        if frame_count <= WARMUP_FRAMES:
            display_frame = overlay_metadata(orig_frame.copy())
            warming = f"Warming up... ({frame_count}/{WARMUP_FRAMES})"
            cv2.putText(display_frame, warming, (10, 30),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 200, 255), 1, cv2.LINE_AA)
            cv2.imshow("Motion Detection", display_frame)
            if cv2.waitKey(delay) & 0xFF == ord('q'):
                break
            continue

        # Diff current frame against the running-average background
        bg_uint8 = cv2.convertScaleAbs(bg_model)
        diff = cv2.absdiff(bg_uint8, gray_blur)
        _, thresh = cv2.threshold(diff, DIFF_THRESHOLD, 255, cv2.THRESH_BINARY)

        # Apply ROI mask and morphological cleanup
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
            cv2.putText(display_frame, "MOTION DETECTED", (10, int(35 * text_scale)),
                        cv2.FONT_HERSHEY_SIMPLEX, text_scale, (0, 0, 255), text_thick, cv2.LINE_AA)

            # Save frame if cooldown has elapsed
            now = time.monotonic()
            if now - last_save_time >= SAVE_COOLDOWN_SEC:
                roi_scaled_coords = (roi_x, roi_y, roi_w, roi_h)
                saved_path = save_capture(display_frame, orig_frame, roi_scaled_coords, save_dir, saved_count)
                saved_count += 1
                last_save_time = now
                print(f"[CAPTURE #{saved_count}] Saved: {saved_path}")

            # Show live save counter on screen
            counter_text = f"Captures: {saved_count}"
            cv2.putText(display_frame, counter_text, (orig_w - 160, 25),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 200, 255), 1, cv2.LINE_AA)

        cv2.imshow("Motion Detection", display_frame)

        if cv2.waitKey(delay) & 0xFF == ord('q'):
            break

    print(f"\nSession ended. Total captures saved: {saved_count} -> {save_dir}")
    cap.release()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
