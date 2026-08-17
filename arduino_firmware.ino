/*
  ===========================================================================
  Solid Waste Motion Detection - Arduino Node Firmware (DS3231 RTC + NEO-6M)
  ===========================================================================
  
  Streams hardware timestamp and GPS coordinates over USB Serial to the Python
  motion detection application.

  Serial Protocol Output:
    DATA,YYYY-MM-DD HH:MM:SS,Latitude,Longitude,GPS_Valid_Flag
    Example: DATA,2026-08-17 17:25:30,12.971598,77.594562,1

  Baud Rate: 115200
  
  Hardware Wiring:
  ----------------
  1. DS3231 RTC Module (I2C):
     - VCC  -> Arduino 5V
     - GND  -> Arduino GND
     - SDA  -> Arduino A4 (or dedicated SDA pin near AREF)
     - SCL  -> Arduino A5 (or dedicated SCL pin near AREF)

  2. NEO-6M GPS Module (UART via SoftwareSerial):
     - VCC  -> Arduino 5V (or 3.3V depending on module)
     - GND  -> Arduino GND
     - TX   -> Arduino Digital Pin 4 (SoftwareSerial RX)
     - RX   -> Arduino Digital Pin 3 (SoftwareSerial TX, via 1k/2k voltage divider if 3.3V logic)

  Required Arduino Libraries:
  ---------------------------
  - RTClib by Adafruit (Install via Arduino Library Manager)
  - TinyGPS++ by Mikal Hart (Install via Arduino Library Manager, optional)
*/

/*
 * SWSTP IoT TELEMETRY CONTROLLER — OPTIMISED BUILD
 * =================================================
 * Hardware:
 *   Arduino MCU (ATmega328P / ATmega2560)
 *   DS3231  RTC  (I2C)
 *   MPU6500 IMU  (I2C, 0x69)
 *   NEO-6M  GNSS (SoftwareSerial D3/D4 @ 9600)
 *
 * PC TELEMETRY: USB Serial @ 115200 baud, JSON Lines, 20 Hz target
 *
 * EEPROM layout (offset 0):
 *   [0..3]  magic 'S','W','S','T'
 *   [4..19] null-terminated Device ID (max 15 chars)
 *
 * Memory optimisations applied (vs previous build):
 *   OPT-1  GnssSatellite 16→8 bytes (×32 entries)  saves ~256 bytes SRAM
 *   OPT-2  GSA constellation array 96→16 bytes      saves  ~80 bytes SRAM
 *   OPT-3  String→char[] serial provisioning        removes heap alloc
 *   OPT-4  Inline hex parser, no strtoul/hex[3]     saves ~33 bytes Flash
 *   OPT-5  EEPROM byte-by-byte read, no stack struct saves ~20 bytes stack
 *   OPT-6  Single field[8] reused in GSV parser     saves ~56 bytes stack
 *   OPT-7  gnssNmeaLine 128→96 bytes                saves  ~32 bytes SRAM
 *   OPT-8  bootSplash 10 s→1.5 s                   saves ~200 bytes Flash
 */

#include <Wire.h>
#include <RTClib.h>
#include <MPU6500_WE.h>
#include <TinyGPS++.h>
#include <SoftwareSerial.h>
#include <EEPROM.h>
#include <math.h>

// ── Serial / telemetry rate ───────────────────────────────────────────────────
#define PC_BAUD_RATE          115200
#define GNSS_BAUD_RATE        9600
#define TELEMETRY_RATE_HZ     20
#define TELEMETRY_INTERVAL_MS (1000UL / TELEMETRY_RATE_HZ)

// ── EEPROM identity layout ────────────────────────────────────────────────────
#define EEPROM_MAGIC_0 'S'
#define EEPROM_MAGIC_1 'W'
#define EEPROM_MAGIC_2 'S'
#define EEPROM_MAGIC_3 'T'
#define EEPROM_ID_OFFSET 4     // bytes 4..19
#define DEVICE_ID_MAXLEN 15    // excludes null terminator

static char deviceId[16] = "UNPROVISIONED"; // 16 bytes

// ── EEPROM helpers — read/write byte by byte (no 20-byte stack struct) ────────

static uint8_t hexNibble(char c) {
  if (c >= '0' && c <= '9') return (uint8_t)(c - '0');
  if (c >= 'A' && c <= 'F') return (uint8_t)(c - 'A' + 10);
  if (c >= 'a' && c <= 'f') return (uint8_t)(c - 'a' + 10);
  return 0;
}

static void loadDeviceIdFromEEPROM() {
  if (EEPROM.read(0) == EEPROM_MAGIC_0 &&
      EEPROM.read(1) == EEPROM_MAGIC_1 &&
      EEPROM.read(2) == EEPROM_MAGIC_2 &&
      EEPROM.read(3) == EEPROM_MAGIC_3) {
    for (uint8_t i = 0; i < DEVICE_ID_MAXLEN; i++) {
      deviceId[i] = (char)EEPROM.read(EEPROM_ID_OFFSET + i);
      if (deviceId[i] == '\0') break;
    }
    deviceId[DEVICE_ID_MAXLEN] = '\0';
  }
  // If magic missing: deviceId keeps its default "UNPROVISIONED" value.
}

static void provisionDeviceIdToEEPROM(const char* newId) {
  // Write magic (EEPROM.update avoids write if byte unchanged — reduces wear)
  EEPROM.update(0, EEPROM_MAGIC_0);
  EEPROM.update(1, EEPROM_MAGIC_1);
  EEPROM.update(2, EEPROM_MAGIC_2);
  EEPROM.update(3, EEPROM_MAGIC_3);
  for (uint8_t i = 0; i < DEVICE_ID_MAXLEN; i++) {
    EEPROM.update(EEPROM_ID_OFFSET + i, (uint8_t)newId[i]);
    if (newId[i] == '\0') {
      // zero-fill remainder
      for (uint8_t j = i + 1; j < DEVICE_ID_MAXLEN; j++) {
        EEPROM.update(EEPROM_ID_OFFSET + j, 0);
      }
      break;
    }
  }
  strncpy(deviceId, newId, DEVICE_ID_MAXLEN);
  deviceId[DEVICE_ID_MAXLEN] = '\0';
  Serial.print(F("PROVISIONED:"));
  Serial.println(deviceId);
}

// ── Status LED pins ───────────────────────────────────────────────────────────
#define RTC_GREEN   9
#define IMU_GREEN   8
#define GNSS_GREEN  10
#define YELLOW      11
#define RED         12

static const uint8_t ALL_LEDS[] = { RTC_GREEN, IMU_GREEN, GNSS_GREEN, YELLOW, RED };
#define NUM_LEDS 5

// ── Module objects ────────────────────────────────────────────────────────────
static RTC_DS3231    rtc;
static MPU6500_WE    mpu(0x69);
static TinyGPSPlus   gps;
static SoftwareSerial gnssSerial(3, 4);   // RX=D3, TX=D4

// ── Module status ─────────────────────────────────────────────────────────────
static bool rtcOK        = false;
static bool imuOK        = false;
static bool gnssDataSeen = false;

// ── Telemetry state ───────────────────────────────────────────────────────────
static unsigned long sequenceNumber  = 0;
static unsigned long lastTelemetryMs = 0;
static unsigned long bootMillis      = 0;

// ── IMU orientation state ─────────────────────────────────────────────────────
static float rollDeg   = 0.0f;
static float pitchDeg  = 0.0f;
static float yawDeg    = 0.0f;
static unsigned long lastImuMicros = 0;
#define COMPLEMENTARY_ALPHA 0.98f

// ── LED timing ────────────────────────────────────────────────────────────────
static unsigned long lastFaultToggle  = 0;
static bool          faultBlinkState  = false;
#define FAULT_BLINK_INTERVAL  400UL

static unsigned long lastYellowToggle = 0;
static bool          yellowState      = false;
#define YELLOW_BLINK_INTERVAL 1000UL

// ── GNSS data tracking ────────────────────────────────────────────────────────
static unsigned long lastGnssDataMs = 0;
#define GNSS_DATA_TIMEOUT_MS  3000UL
#define GNSS_DETAIL_INTERVAL_MS 1000UL

// ── OPT-1: Compact satellite struct (8 bytes vs 16 bytes before) ──────────────
//
// Constellation encoding — 3 bytes (char[3]) is replaced by a 1-byte enum.
// JSON strings are produced at emit-time only.
//
// NEO-6M NMEA value ranges verified:
//   PRN         : 1–120 (uint8_t covers 1–255 safely; keep uint8_t)
//                 But GLONASS offset PRNs can reach 65+32=96, SBAS up to 120 —
//                 all fit in uint8_t. However some extended NMEA uses higher IDs.
//                 Keep uint16_t for future safety — no additional cost vs int16_t.
//   elevation   : 0–90°  → int8_t  (saves 1 byte vs int16_t)
//   azimuth     : 0–359° → uint16_t (same size as int16_t, but semantically correct)
//   SNR         : 0–99 dBHz, never negative → uint8_t (saves 1 byte vs int16_t)
//   snrValid    : bit 0 of flags byte
//   usedInFix   : bit 1 of flags byte
//   constellation: 7 values → uint8_t enum (saves 5 bytes vs char[6])
//
// sizeof(GnssSatellite) BEFORE: 16 bytes
// sizeof(GnssSatellite) AFTER : 8 bytes
// Arrays: 2 × 16 × (16-8) = 256 bytes saved

enum ConstellationId : uint8_t {
  CONST_GPS = 0,
  CONST_GLO,
  CONST_GAL,
  CONST_BDS,
  CONST_QZS,
  CONST_GNSS,
  CONST_UNK
};

// Bit flags inside GnssSatellite.flags
#define SAT_FLAG_SNR_VALID  0x01
#define SAT_FLAG_USED_IN_FIX 0x02

struct GnssSatellite {
  uint16_t       prn;          // 2 bytes
  int8_t         elevationDeg; // 1 byte  (was int16_t)
  uint16_t       azimuthDeg;   // 2 bytes
  uint8_t        snrDbHz;      // 1 byte  (was int16_t)
  uint8_t        flags;        // 1 byte  (snrValid|usedInFix)
  ConstellationId constId;     // 1 byte  (was char[6])
};                             // Total: 8 bytes

#define MAX_GNSS_SATELLITES 16

static GnssSatellite gnssWorkingSatellites[MAX_GNSS_SATELLITES]; // 128 bytes (was 256)
static GnssSatellite gnssSatellites[MAX_GNSS_SATELLITES];        // 128 bytes (was 256)
// SRAM saving vs before: 256 bytes

static uint8_t  gnssWorkingSatelliteCount       = 0;
static uint8_t  gnssSatelliteCount              = 0;
static uint16_t gnssSatellitesInView            = 0;
static uint8_t  gnssGsvExpectedMessages         = 0;
static uint8_t  gnssGsvCurrentMessage           = 0;
static bool     gnssGsvCycleActive              = false;
static unsigned long lastGnssSatelliteSnapshotMs = 0;
static unsigned long lastGnssDetailEmitMs        = 0;

// ── OPT-2: Compact GSA used-satellite storage ─────────────────────────────────
// BEFORE: uint16_t gnssUsedPrns[16] (32 B) + char gnssUsedConstellation[16][6] (96 B)
// AFTER : uint16_t gnssUsedPrns[16] (32 B) + ConstellationId (1 B each = 16 B)
// Saving: 80 bytes

static uint16_t       gnssUsedPrns[16];
static ConstellationId gnssUsedConstellationId[16]; // 16 bytes (was 96 bytes)
static uint8_t        gnssUsedPrnCount = 0;

// ── OPT-7: NMEA line buffer reduced 128→96 bytes ─────────────────────────────
// NEO-6M longest standard sentence (GSV with 4 sat blocks) fits in ≤82 chars
// per NMEA-0183 spec. 96 gives a comfortable margin.
static char    gnssNmeaLine[96];    // was 128 bytes; saves 32 bytes
static uint8_t gnssNmeaLineLength = 0;

// ── Forward declarations ──────────────────────────────────────────────────────
static void printJsonFloat(float value, uint8_t decimals);
static void printJsonNullableFloat(bool valid, float value, uint8_t decimals);
static void printUint64(uint64_t value);
static void printTwoDigits(int val);
static void bootSplash();
static void allOff();
static void allOn();
static void updateGNSS();
static void processGnssNmeaLine(const char* line);
static void parseGsvSentence(const char* line);
static void parseGsaSentence(const char* line);
static void commitGsvSnapshot();
static void applyUsedInFixFlags();
static void printGnssSatelliteDetails();
static int  getNmeaField(const char* line, uint8_t fieldIndex, char* output, uint8_t outputSize);
static ConstellationId getConstellationId(const char* line);
static const __FlashStringHelper* constellationName(ConstellationId id);
static void updateOrientation(float ax, float ay, float az, float gx, float gy, float gz);
static void emitTelemetry();
static void printQuaternion();
static void updateStatusLEDs();
static float normalizeAngle(float angle);
static void checkSerialCommands();

// =============================================================================
// SETUP
// =============================================================================

void setup() {
  Serial.begin(PC_BAUD_RATE);
  gnssSerial.begin(GNSS_BAUD_RATE);
  Wire.begin();

  // OPT-5: read EEPROM byte-by-byte — no 20-byte stack struct
  loadDeviceIdFromEEPROM();

  for (uint8_t i = 0; i < NUM_LEDS; i++) {
    pinMode(ALL_LEDS[i], OUTPUT);
    digitalWrite(ALL_LEDS[i], LOW);
  }

  bootMillis = millis();

  Serial.println(F("=== SWSTP TELEMETRY ==="));
  Serial.print(F("DEVICE:"));
  Serial.println(deviceId);
  Serial.println(F("BAUD:115200 RATE:20Hz"));

  bootSplash();

  Serial.println(F("=== INIT ==="));

  if (rtc.begin()) {
    rtcOK = true;
    Serial.println(F("RTC:OK"));
    if (rtc.lostPower()) {
      Serial.println(F("RTC:WARN power lost"));
    }
  } else {
    Serial.println(F("RTC:FAULT"));
  }
  if (mpu.init()) {
    imuOK = true;
    Serial.println(F("IMU:OK 0x69"));
    Serial.println(F("IMU:CAL hold still"));
    mpu.autoOffsets();
    mpu.setAccRange(MPU6500_ACC_RANGE_2G);
    mpu.setGyrRange(MPU6500_GYRO_RANGE_250);
    Serial.println(F("IMU:CAL done"));
  } else {
    Serial.println(F("IMU:FAULT 0x69"));
  }

  Serial.println(F("GNSS:wait..."));

  lastImuMicros  = micros();
  lastTelemetryMs = millis();

  Serial.println(F("=== READY ==="));
}

// =============================================================================
// MAIN LOOP
// =============================================================================

void loop() {
  checkSerialCommands();
  updateGNSS();

  unsigned long nowMs = millis();
  if (nowMs - lastTelemetryMs >= TELEMETRY_INTERVAL_MS) {
    lastTelemetryMs += TELEMETRY_INTERVAL_MS;
    emitTelemetry();
  }

  updateStatusLEDs();
}

// =============================================================================
// OPT-3: Serial provisioning — fixed char buffer, no String allocation
// =============================================================================
//
// Reads characters into a fixed 32-byte buffer until '\n'.
// Parses "PROVISION:SWSTP-DEV-000001\n".
// Does NOT call readStringUntil() (which allocates heap String objects).

static void checkSerialCommands() {
  static char   cmdBuf[32];   // static: lives in BSS, not stack-per-call
  static uint8_t cmdLen = 0;

  while (Serial.available() > 0) {
    char c = (char)Serial.read();
    if (c == '\r') continue;
    if (c == '\n') {
      cmdBuf[cmdLen] = '\0';
      // Parse "PROVISION:<id>"
      if (cmdLen > 10 &&
          cmdBuf[0]=='P' && cmdBuf[1]=='R' && cmdBuf[2]=='O' &&
          cmdBuf[3]=='V' && cmdBuf[4]=='I' && cmdBuf[5]=='S' &&
          cmdBuf[6]=='I' && cmdBuf[7]=='O' && cmdBuf[8]=='N' &&
          cmdBuf[9]==':') {
        char* idStart = cmdBuf + 10;
        uint8_t idLen = cmdLen - 10;
        if (idLen > 0 && idLen <= DEVICE_ID_MAXLEN) {
          provisionDeviceIdToEEPROM(idStart);
        }
      }
      cmdLen = 0;
    } else {
      if (cmdLen < (uint8_t)(sizeof(cmdBuf) - 1)) {
        cmdBuf[cmdLen++] = c;
      } else {
        cmdLen = 0; // overflow: discard and reset
      }
    }
  }
}

// =============================================================================
// GNSS UPDATE — unchanged logic, unchanged byte by byte NMEA parsing
// =============================================================================

static void updateGNSS() {
  while (gnssSerial.available() > 0) {
    char c = (char)gnssSerial.read();
    gps.encode(c);
    gnssDataSeen  = true;
    lastGnssDataMs = millis();

    if (c == '$') {
      gnssNmeaLineLength = 0;
      gnssNmeaLine[gnssNmeaLineLength++] = c;
    } else if (gnssNmeaLineLength > 0) {
      if (c == '\n') {
        gnssNmeaLine[gnssNmeaLineLength] = '\0';
        processGnssNmeaLine(gnssNmeaLine);
        gnssNmeaLineLength = 0;
      } else if (c != '\r') {
        if (gnssNmeaLineLength < (uint8_t)(sizeof(gnssNmeaLine) - 1)) {
          gnssNmeaLine[gnssNmeaLineLength++] = c;
        } else {
          gnssNmeaLineLength = 0; // oversized: drop and wait for next '$'
        }
      }
    }
  }
}

// =============================================================================
// NMEA CHECKSUM VERIFICATION
// OPT-4: inline hex nibble conversion — no hex[3] temp + no strtoul()
// =============================================================================

static void processGnssNmeaLine(const char* line) {
  if (!line || strlen(line) < 7 || line[0] != '$') return;

  const char* star = strchr(line, '*');
  if (star && star > line + 1) {
    uint8_t computed = 0;
    for (const char* p = line + 1; p < star; p++) {
      computed ^= (uint8_t)(*p);
    }
    // OPT-4: inline two-nibble hex parse, no temporary char[3]
    uint8_t received = (hexNibble(star[1]) << 4) | hexNibble(star[2]);
    if (computed != received) return;
  }

  if (line[3]=='G' && line[4]=='S' && line[5]=='V') {
    parseGsvSentence(line);
  } else if (line[3]=='G' && line[4]=='S' && line[5]=='A') {
    parseGsaSentence(line);
  }
}

// =============================================================================
// OPT-1+6: Compact constellation + single reused field buffer in GSV parser
// =============================================================================

// Returns a ConstellationId from the NMEA talker prefix (e.g. "$GP" → CONST_GPS)
static ConstellationId getConstellationId(const char* line) {
  if (!line || strlen(line) < 3) return CONST_UNK;
  char a = line[1], b = line[2];
  if (a=='G' && b=='P') return CONST_GPS;
  if (a=='G' && b=='L') return CONST_GLO;
  if (a=='G' && b=='A') return CONST_GAL;
  if (a=='B' && b=='D') return CONST_BDS;
  if (a=='G' && b=='Q') return CONST_QZS;
  if (a=='G' && b=='N') return CONST_GNSS;
  return CONST_UNK;
}

// Returns the JSON string literal stored in Flash for a ConstellationId
static const __FlashStringHelper* constellationName(ConstellationId id) {
  switch (id) {
    case CONST_GPS:  return F("GPS");
    case CONST_GLO:  return F("GLO");
    case CONST_GAL:  return F("GAL");
    case CONST_BDS:  return F("BDS");
    case CONST_QZS:  return F("QZS");
    case CONST_GNSS: return F("GNSS");
    default:         return F("UNK");
  }
}

// OPT-6: single field[8] buffer reused sequentially.
// NEO-6M GSV field widths verified:
//   field 1 (totalMsgs):  1 char
//   field 2 (msgNum):     1 char
//   field 3 (totalSats):  1-2 chars
//   PRN:                  1-3 chars
//   elevation:            1-2 chars
//   azimuth:              1-3 chars
//   SNR:                  1-2 chars
// All fit comfortably in 8 bytes (including null terminator).

static void parseGsvSentence(const char* line) {
  char field[8]; // OPT-6: 8 bytes, reused — was 5 separate buffers totalling 64 bytes

  if (getNmeaField(line, 1, field, sizeof(field)) <= 0) return;
  uint8_t totalMessages = (uint8_t)atoi(field);

  if (getNmeaField(line, 2, field, sizeof(field)) <= 0) return;
  uint8_t messageNumber = (uint8_t)atoi(field);

  if (getNmeaField(line, 3, field, sizeof(field)) <= 0) return;
  uint16_t totalSatellites = (uint16_t)atoi(field);

  if (messageNumber == 1) {
    gnssWorkingSatelliteCount = 0;
    gnssGsvExpectedMessages   = totalMessages;
    gnssGsvCurrentMessage     = messageNumber;
    gnssSatellitesInView      = totalSatellites;
    gnssGsvCycleActive        = true;
  } else if (!gnssGsvCycleActive || messageNumber != gnssGsvCurrentMessage + 1) {
    gnssWorkingSatelliteCount = 0;
    gnssGsvExpectedMessages   = totalMessages;
    gnssGsvCurrentMessage     = messageNumber;
    gnssSatellitesInView      = totalSatellites;
    gnssGsvCycleActive        = true;
  } else {
    gnssGsvCurrentMessage = messageNumber;
  }

  ConstellationId cid = getConstellationId(line); // OPT-1: enum, no char[6]

  for (uint8_t block = 0; block < 4; block++) {
    uint8_t base = 4 + (block * 4);

    if (getNmeaField(line, base, field, sizeof(field)) <= 0) continue;
    uint16_t prn = (uint16_t)atoi(field);
    if (prn == 0) continue;
    if (gnssWorkingSatelliteCount >= MAX_GNSS_SATELLITES) continue;

    GnssSatellite &sat = gnssWorkingSatellites[gnssWorkingSatelliteCount++];
    sat.prn         = prn;
    sat.elevationDeg = 0;
    sat.azimuthDeg  = 0;
    sat.snrDbHz     = 0;
    sat.flags       = 0;
    sat.constId     = cid;

    if (getNmeaField(line, base + 1, field, sizeof(field)) > 0) {
      sat.elevationDeg = (int8_t)atoi(field);   // 0–90° fits int8_t
    }
    if (getNmeaField(line, base + 2, field, sizeof(field)) > 0) {
      sat.azimuthDeg = (uint16_t)atoi(field);   // 0–359° fits uint16_t
    }
    if (getNmeaField(line, base + 3, field, sizeof(field)) > 0) {
      int v = atoi(field);
      sat.snrDbHz = (uint8_t)(v < 0 ? 0 : (v > 99 ? 99 : v));
      sat.flags  |= SAT_FLAG_SNR_VALID;
    }
  }

  if (gnssGsvExpectedMessages > 0 && gnssGsvCurrentMessage >= gnssGsvExpectedMessages) {
    commitGsvSnapshot();
  }
}

// OPT-2: GSA parser stores ConstellationId (1 byte) instead of char[6]
static void parseGsaSentence(const char* line) {
  char field[8]; // reuse same small buffer
  gnssUsedPrnCount = 0;
  ConstellationId cid = getConstellationId(line);

  for (uint8_t fi = 3; fi <= 14; fi++) {
    if (gnssUsedPrnCount >= 16) break;
    if (getNmeaField(line, fi, field, sizeof(field)) <= 0) continue;
    uint16_t prn = (uint16_t)atoi(field);
    if (prn == 0) continue;
    gnssUsedPrns[gnssUsedPrnCount]            = prn;
    gnssUsedConstellationId[gnssUsedPrnCount] = cid;
    gnssUsedPrnCount++;
  }
  applyUsedInFixFlags();
}

// Double-buffer commit — incomplete GSV cycles never corrupt last good snapshot
static void commitGsvSnapshot() {
  gnssSatelliteCount = gnssWorkingSatelliteCount;
  for (uint8_t i = 0; i < gnssSatelliteCount; i++) {
    gnssSatellites[i] = gnssWorkingSatellites[i];
  }
  applyUsedInFixFlags();
  lastGnssSatelliteSnapshotMs = millis();
  gnssGsvCycleActive = false;
}

// OPT-2: match by ConstellationId (integer), not strcmp on char[6]
static void applyUsedInFixFlags() {
  for (uint8_t i = 0; i < gnssSatelliteCount; i++) {
    gnssSatellites[i].flags &= ~SAT_FLAG_USED_IN_FIX;
    for (uint8_t j = 0; j < gnssUsedPrnCount; j++) {
      if (gnssSatellites[i].prn    == gnssUsedPrns[j] &&
          gnssSatellites[i].constId == gnssUsedConstellationId[j]) {
        gnssSatellites[i].flags |= SAT_FLAG_USED_IN_FIX;
        break;
      }
    }
  }
}

// =============================================================================
// NMEA FIELD EXTRACTION — unchanged logic
// =============================================================================

static int getNmeaField(const char* line, uint8_t fieldIndex, char* output, uint8_t outputSize) {
  if (!line || !output || outputSize == 0) return 0;
  uint8_t current = 0;
  const char* start = line;
  const char* p     = line;

  while (true) {
    if (*p == ',' || *p == '*' || *p == '\0') {
      if (current == fieldIndex) {
        size_t len = (size_t)(p - start);
        if (len >= outputSize) len = outputSize - 1;
        memcpy(output, start, len);
        output[len] = '\0';
        return (int)len;
      }
      if (*p == '\0' || *p == '*') break;
      current++;
      start = p + 1;
    }
    if (*p == '\0') break;
    p++;
  }
  output[0] = '\0';
  return 0;
}

// =============================================================================
// GNSS SATELLITE DETAIL EMIT — JSON output identical to original
// OPT-1: constellation string fetched from Flash via constellationName()
// =============================================================================

static void printGnssSatelliteDetails() {
  Serial.print(F(",\"satellites_detail\":["));
  for (uint8_t i = 0; i < gnssSatelliteCount; i++) {
    if (i > 0) Serial.print(',');
    const GnssSatellite &sat = gnssSatellites[i];

    Serial.print(F("{\"prn\":"));
    Serial.print(sat.prn);

    Serial.print(F(",\"constellation\":\""));
    Serial.print(constellationName(sat.constId));   // Flash string, no SRAM copy
    Serial.print('"');

    Serial.print(F(",\"elevation_deg\":"));
    Serial.print((int)sat.elevationDeg);             // int8_t cast to int for printing

    Serial.print(F(",\"azimuth_deg\":"));
    Serial.print(sat.azimuthDeg);

    Serial.print(F(",\"snr_db\":"));
    if (sat.flags & SAT_FLAG_SNR_VALID) {
      Serial.print(sat.snrDbHz);
    } else {
      Serial.print(F("null"));
    }

    Serial.print(F(",\"used_in_fix\":"));
    Serial.print((sat.flags & SAT_FLAG_USED_IN_FIX) ? F("true") : F("false"));
    Serial.print('}');
  }
  Serial.print(']');
}

// =============================================================================
// IMU ORIENTATION — unchanged complementary filter + gyro integration
// =============================================================================

static void updateOrientation(float ax, float ay, float az, float gx, float gy, float gz) {
  unsigned long nowMicros = micros();
  float dt = (float)(nowMicros - lastImuMicros) / 1000000.0f;
  lastImuMicros = nowMicros;
  if (dt <= 0.0f || dt > 0.5f) dt = 0.05f;

  float accelRoll  = atan2(ay, az) * (180.0f / PI);
  float hMag       = sqrt(ay * ay + az * az);
  float accelPitch = atan2(-ax, hMag) * (180.0f / PI);

  rollDeg  = COMPLEMENTARY_ALPHA * (rollDeg  + gx * dt) + (1.0f - COMPLEMENTARY_ALPHA) * accelRoll;
  pitchDeg = COMPLEMENTARY_ALPHA * (pitchDeg + gy * dt) + (1.0f - COMPLEMENTARY_ALPHA) * accelPitch;
  yawDeg   = normalizeAngle(yawDeg + gz * dt);
}

// =============================================================================
// TELEMETRY JSON EMITTER — identical JSON contract
// =============================================================================

static void emitTelemetry() {
  xyzFloat accel, gyro;
  float temperature = 0.0f;

  if (imuOK) {
    accel       = mpu.getGValues();
    gyro        = mpu.getGyrValues();
    temperature = mpu.getTemperature();
    updateOrientation(accel.x, accel.y, accel.z, gyro.x, gyro.y, gyro.z);
  }

  DateTime now;
  if (rtcOK) now = rtc.now();

  // ── Packet header ────────────────────────────────────────────────────────
  Serial.print(F("{\"type\":\"telemetry\",\"device_id\":\""));
  Serial.print(deviceId);
  Serial.print(F("\",\"seq\":"));
  Serial.print(sequenceNumber++);
  Serial.print(F(",\"uptime_ms\":"));
  Serial.print(millis() - bootMillis);

  // ── RTC ──────────────────────────────────────────────────────────────────
  Serial.print(F(",\"rtc\":{\"valid\":"));
  Serial.print(rtcOK ? F("true") : F("false"));
  if (rtcOK) {
    Serial.print(F(",\"timestamp\":\""));
    Serial.print(now.year()); Serial.print('-');
    printTwoDigits(now.month());  Serial.print('-');
    printTwoDigits(now.day());    Serial.print('T');
    printTwoDigits(now.hour());   Serial.print(':');
    printTwoDigits(now.minute()); Serial.print(':');
    printTwoDigits(now.second()); Serial.print('"');
    Serial.print(F(",\"epoch\":"));
    printUint64((uint64_t)now.unixtime() * 1000ULL);
  }
  Serial.print('}');

  // ── IMU ──────────────────────────────────────────────────────────────────
  Serial.print(F(",\"imu\":{\"valid\":"));
  Serial.print(imuOK ? F("true") : F("false"));
  if (imuOK) {
    const float G = 9.80665f;

    Serial.print(F(",\"accel_g\":{\"x\":"));
    printJsonFloat(accel.x, 4);
    Serial.print(F(",\"y\":"));
    printJsonFloat(accel.y, 4);
    Serial.print(F(",\"z\":"));
    printJsonFloat(accel.z, 4);
    Serial.print('}');

    Serial.print(F(",\"accel_ms2\":{\"x\":"));
    printJsonFloat(accel.x * G, 4);
    Serial.print(F(",\"y\":"));
    printJsonFloat(accel.y * G, 4);
    Serial.print(F(",\"z\":"));
    printJsonFloat(accel.z * G, 4);
    Serial.print('}');

    Serial.print(F(",\"accel_magnitude_ms2\":"));
    printJsonFloat(sqrt(accel.x*accel.x + accel.y*accel.y + accel.z*accel.z) * G, 4);

    Serial.print(F(",\"gyro_dps\":{\"x\":"));
    printJsonFloat(gyro.x, 4);
    Serial.print(F(",\"y\":"));
    printJsonFloat(gyro.y, 4);
    Serial.print(F(",\"z\":"));
    printJsonFloat(gyro.z, 4);
    Serial.print('}');

    Serial.print(F(",\"temperature_c\":"));
    printJsonFloat(temperature, 2);

    Serial.print(F(",\"orientation\":{\"roll\":"));
    printJsonFloat(rollDeg, 3);
    Serial.print(F(",\"pitch\":"));
    printJsonFloat(pitchDeg, 3);
    Serial.print(F(",\"yaw\":"));
    printJsonFloat(yawDeg, 3);
    Serial.print(F(",\"yaw_source\":\"GYRO_INTEGRATED\"}"));

    Serial.print(F(",\"quaternion\":"));
    printQuaternion();
  }
  Serial.print('}');

  // ── GNSS ─────────────────────────────────────────────────────────────────
  bool gnssFix = gps.location.isValid() && gps.location.age() < GNSS_DATA_TIMEOUT_MS;
  bool gnssComm = gnssDataSeen && (millis() - lastGnssDataMs < GNSS_DATA_TIMEOUT_MS);

  Serial.print(F(",\"gnss\":{\"data_received\":"));
  Serial.print(gnssComm ? F("true") : F("false"));
  Serial.print(F(",\"fix\":"));
  Serial.print(gnssFix ? F("true") : F("false"));
  Serial.print(F(",\"status\":\""));
  if      (!gnssComm) Serial.print(F("NO_DATA"));
  else if (!gnssFix)  Serial.print(F("NO_FIX"));
  else                Serial.print(F("FIX"));
  Serial.print('"');

  Serial.print(F(",\"latitude\":"));   printJsonNullableFloat(gnssFix, gps.location.lat(), 6);
  Serial.print(F(",\"longitude\":"));  printJsonNullableFloat(gnssFix, gps.location.lng(), 6);
  Serial.print(F(",\"altitude_m\":")); printJsonNullableFloat(gnssFix && gps.altitude.isValid(), gps.altitude.meters(), 2);
  Serial.print(F(",\"speed_kmh\":"));  printJsonNullableFloat(gnssFix && gps.speed.isValid(), gps.speed.kmph(), 2);
  Serial.print(F(",\"course_deg\":")); printJsonNullableFloat(gnssFix && gps.course.isValid(), gps.course.deg(), 2);

  Serial.print(F(",\"satellites\":"));
  Serial.print(gps.satellites.isValid() ? gps.satellites.value() : 0);

  Serial.print(F(",\"satellites_in_view\":"));
  Serial.print(gnssSatellitesInView);

  Serial.print(F(",\"satellite_records\":"));
  Serial.print(gnssSatelliteCount);

  // Satellite detail: once per second when a fresh GSV snapshot exists
  unsigned long nowMs = millis();
  if (nowMs - lastGnssSatelliteSnapshotMs < 5000UL &&
      nowMs - lastGnssDetailEmitMs >= GNSS_DETAIL_INTERVAL_MS) {
    lastGnssDetailEmitMs = nowMs;
    printGnssSatelliteDetails();
  } else {
    Serial.print(F(",\"satellites_detail\":null"));
  }

  Serial.print(F(",\"hdop\":"));
  printJsonNullableFloat(gnssFix && gps.hdop.isValid(), gps.hdop.hdop(), 2);
  Serial.print('}');

  // ── Status block ─────────────────────────────────────────────────────────
  Serial.print(F(",\"status\":{\"rtc\":\""));
  Serial.print(rtcOK ? F("ACTIVE") : F("FAULT"));
  Serial.print(F("\",\"imu\":\""));
  Serial.print(imuOK ? F("ACTIVE") : F("FAULT"));
  Serial.print(F("\",\"gnss\":\""));
  if      (!gnssComm) Serial.print(F("NO_DATA"));
  else if (!gnssFix)  Serial.print(F("NO_FIX"));
  else                Serial.print(F("ACTIVE"));
  Serial.print(F("\",\"serial\":\"CONNECTED\",\"baud\":"));
  Serial.print(PC_BAUD_RATE);
  Serial.print('}');

  Serial.println('}');
}

// =============================================================================
// QUATERNION — unchanged Euler → quaternion conversion
// =============================================================================

static void printQuaternion() {
  float r = rollDeg  * (PI / 180.0f);
  float p = pitchDeg * (PI / 180.0f);
  float y = yawDeg   * (PI / 180.0f);

  float cy = cos(y * 0.5f), sy = sin(y * 0.5f);
  float cp = cos(p * 0.5f), sp = sin(p * 0.5f);
  float cr = cos(r * 0.5f), sr = sin(r * 0.5f);

  Serial.print(F("{\"w\":"));
  printJsonFloat(cr*cp*cy + sr*sp*sy, 6);
  Serial.print(F(",\"x\":"));
  printJsonFloat(sr*cp*cy - cr*sp*sy, 6);
  Serial.print(F(",\"y\":"));
  printJsonFloat(cr*sp*cy + sr*cp*sy, 6);
  Serial.print(F(",\"z\":"));
  printJsonFloat(cr*cp*sy - sr*sp*cy, 6);
  Serial.print('}');
}

// =============================================================================
// STATUS LEDs — unchanged logic
// =============================================================================

static void updateStatusLEDs() {
  bool gnssFix = gnssDataSeen && gps.location.isValid() &&
                 gps.location.age() < GNSS_DATA_TIMEOUT_MS;
  bool allOK   = rtcOK && imuOK && gnssFix;

  unsigned long nowMs = millis();
  if (nowMs - lastFaultToggle > FAULT_BLINK_INTERVAL) {
    faultBlinkState  = !faultBlinkState;
    lastFaultToggle  = nowMs;
  }

  digitalWrite(RTC_GREEN,  rtcOK   ? HIGH : (uint8_t)faultBlinkState);
  digitalWrite(IMU_GREEN,  imuOK   ? HIGH : (uint8_t)faultBlinkState);
  digitalWrite(GNSS_GREEN, gnssFix ? HIGH : (uint8_t)faultBlinkState);
  digitalWrite(RED,        allOK   ? LOW  : (uint8_t)faultBlinkState);

  if (allOK) {
    if (nowMs - lastYellowToggle > YELLOW_BLINK_INTERVAL) {
      yellowState     = !yellowState;
      lastYellowToggle = nowMs;
      digitalWrite(YELLOW, yellowState);
    }
  } else {
    digitalWrite(YELLOW, LOW);
  }
}

// =============================================================================
// JSON FLOAT HELPERS
// =============================================================================

static void printJsonFloat(float value, uint8_t decimals) {
  if (isnan(value) || isinf(value)) { Serial.print(F("null")); return; }
  Serial.print(value, decimals);
}

static void printJsonNullableFloat(bool valid, float value, uint8_t decimals) {
  if (!valid) { Serial.print(F("null")); return; }
  printJsonFloat(value, decimals);
}

// =============================================================================
// UINT64 PRINTER — unchanged, no heap allocation
// =============================================================================

static void printUint64(uint64_t value) {
  if (value == 0) { Serial.print('0'); return; }
  char buf[21];
  uint8_t idx = 0;
  while (value > 0 && idx < sizeof(buf) - 1) {
    buf[idx++] = '0' + (uint8_t)(value % 10);
    value /= 10;
  }
  while (idx > 0) Serial.print(buf[--idx]);
}

// =============================================================================
// ANGLE NORMALIZATION
// =============================================================================

static float normalizeAngle(float a) {
  while (a >  180.0f) a -= 360.0f;
  while (a < -180.0f) a += 360.0f;
  return a;
}

// =============================================================================
// TWO-DIGIT ZERO-PADDED PRINT
// =============================================================================

static void printTwoDigits(int v) {
  if (v < 10) Serial.print('0');
  Serial.print(v);
}

// =============================================================================
// OPT-8: BOOT SPLASH — replaced 10-second animation with a 3×flash (≈1.5 s)
//
// Original rationale for the 10-second splash: allow time to view LEDs.
// Field deployment issue: during 10 s, SoftwareSerial RX buffer (64 bytes on
// ATmega328P) can overflow with GNSS NMEA data, causing missed sentences.
// The 3-cycle flash still exercises all LEDs for functional verification
// while completing in ~1.5 seconds.
// =============================================================================

static void bootSplash() {
  for (uint8_t i = 0; i < 3; i++) {
    allOn();
    delay(200);
    allOff();
    delay(250);
  }
  Serial.println(F("READY"));
}

// =============================================================================
// LED HELPERS
// =============================================================================

static void allOff() {
  for (uint8_t i = 0; i < NUM_LEDS; i++) digitalWrite(ALL_LEDS[i], LOW);
}

static void allOn() {
  for (uint8_t i = 0; i < NUM_LEDS; i++) digitalWrite(ALL_LEDS[i], HIGH);
}